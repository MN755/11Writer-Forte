from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from pydantic import TypeAdapter, ValidationError
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.config import reset_settings_cache
from src.db import get_session_factory, init_db, reset_db_state
from src.models import (
    AlertORM,
    Base,
    ScheduledTaskORM,
    ScheduledTaskRunORM,
    SourceDefinitionORM,
    SourceRunORM,
    StorageObjectORM,
    WatchORM,
    WatchRunORM,
)
from src.schemas import (
    ImageChangeRule,
    RuntimeSnapshotRead,
    ScheduledTaskCreate,
    WatchCreate,
    WatchUpdate,
)
from src.services.database_diagnostics_service import build_database_diagnostics
from src.services.runtime_snapshot_service import build_runtime_snapshot, restore_runtime_snapshot


def test_watch_schemas_validate_discriminated_rules_and_forbid_unknown_fields() -> None:
    payload = WatchCreate(
        name="Piston Peak Images",
        slug="piston-peak-images",
        objective="Retain and alert on newly changed construction images.",
        watch_type="image_change",
        rule_json={"mode": "image_change"},
    )

    assert isinstance(payload.rule_json, ImageChangeRule)
    assert payload.rule_json.comparison == "sha256"
    assert payload.rule_json.retention_class == "permanent"
    assert payload.notification_policy_json.api_enabled is True
    assert payload.notification_policy_json.rss_enabled is True
    assert payload.notification_policy_json.analysis_on_change is False

    with pytest.raises(ValidationError, match="must match watch_type"):
        WatchCreate(
            name="Broken Watch",
            slug="broken-watch",
            objective="Reject mismatched rule modes.",
            watch_type="source_delta",
            rule_json={"mode": "image_change"},
        )

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        WatchCreate(
            name="Unknown Rule Field",
            slug="unknown-rule-field",
            objective="Reject rule typos.",
            watch_type="image_change",
            rule_json={"mode": "image_change", "perceptual_similarity": 0.95},
        )

    with pytest.raises(ValidationError, match="must match watch_type"):
        WatchUpdate(
            watch_type="source_health",
            rule_json={"mode": "observation_rule"},
        )

    task = ScheduledTaskCreate(
        name="piston-peak-watch-evaluate",
        task_type="watch_evaluate",
        interval_seconds=300,
        payload_json={"watch_id": 1},
    )
    assert task.task_type == "watch_evaluate"


def test_watch_diagnostics_and_runtime_snapshot_restore_round_trip(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "watch-persistence.db"
    monkeypatch.setenv("ELEVENWRITER_DATABASE_URL", f"sqlite:///{database_path}")
    monkeypatch.setenv("ELEVENWRITER_DATA_DIR", str(tmp_path / "var"))
    reset_settings_cache()
    reset_db_state()
    init_db()
    observed_at = datetime.now(timezone.utc)
    evidence_path = tmp_path / "watch-evidence.jpg"
    evidence_path.write_bytes(b"watch-image-evidence")

    session = get_session_factory()()
    try:
        source = SourceDefinitionORM(
            name="persistence-watch-source",
            source_kind="http_json",
            layer_key="construction-images",
            target_uri="https://example.test/construction.json",
        )
        alert = AlertORM(
            severity="warning",
            status="open",
            dedupe_key="watch:pending:sha256:changed",
            message="A materially new image was retained.",
            trigger_basis_json={"watch_id": "pending"},
        )
        evidence = StorageObjectORM(
            object_key="watch:evidence:persistence:1",
            object_kind="watch_image_evidence",
            owner_type="watch_run",
            owner_id="pending",
            content_hash="0" * 64,
            media_type="image/jpeg",
            storage_tier="hot",
            retention_class="permanent",
            lifecycle_status="active",
            source_uri="https://example.test/construction.jpg",
            object_uri=str(evidence_path),
            byte_size=evidence_path.stat().st_size,
            observed_at=observed_at,
        )
        schedule = ScheduledTaskORM(
            name="persistence-watch-schedule",
            task_type="watch_evaluate",
            interval_seconds=300,
            payload_json={},
        )
        session.add_all([source, alert, evidence, schedule])
        session.flush()

        source_run = SourceRunORM(
            source_id=source.source_id,
            status="completed",
            started_at=observed_at,
            finished_at=observed_at,
            adapter_kind="http_json",
            fetch_mode="pull",
            records_seen=1,
            records_imported=1,
            output_json={"payload_sha256": "1" * 64},
        )
        task_run = ScheduledTaskRunORM(
            task_id=schedule.task_id,
            status="completed",
            started_at=observed_at,
            finished_at=observed_at,
            records_affected=1,
            output_json={"watch_id": "pending"},
        )
        session.add_all([source_run, task_run])
        session.flush()

        watch = WatchORM(
            name="Persistence Watch",
            slug="persistence-watch",
            objective="Prove durable deterministic watch state.",
            description="Snapshot round-trip fixture.",
            watch_type="source_delta",
            state="enabled",
            rule_json={
                "mode": "source_delta",
                "run_source": True,
                "change_basis": "payload_sha256",
                "alert_on_initial": False,
            },
            source_id=source.source_id,
            layer_key="construction-images",
            scheduled_task_id=schedule.task_id,
            interval_seconds=300,
            severity="warning",
            notification_policy_json={
                "api_enabled": True,
                "rss_enabled": True,
                "analysis_on_change": False,
            },
            baseline_json={"payload_sha256": "0" * 64},
            dedupe_json={"last_trigger": "1" * 64},
            last_evaluated_at=observed_at,
            last_changed_at=observed_at,
            next_run_at=observed_at,
            metadata_json={"fixture": True},
            provenance_json={"actor": "test"},
        )
        session.add(watch)
        session.flush()

        schedule.payload_json = {"watch_id": watch.watch_id}
        alert.trigger_basis_json = {"watch_id": watch.watch_id}
        evidence.owner_id = "1"
        task_run.output_json = {"watch_id": watch.watch_id}
        watch_run = WatchRunORM(
            watch_id=watch.watch_id,
            scheduled_task_run_id=task_run.task_run_id,
            source_run_id=source_run.source_run_id,
            alert_id=alert.alert_id,
            storage_object_id=evidence.storage_object_id,
            status="completed",
            outcome="change",
            started_at=observed_at,
            finished_at=observed_at,
            change_detected=True,
            baseline_initialized=False,
            dedupe_key="watch:1:sha256:changed",
            evidence_json={"storage_object_ids": [evidence.storage_object_id]},
            checkpoint_before_json={"payload_sha256": "0" * 64},
            checkpoint_after_json={"payload_sha256": "1" * 64},
            output_summary="A new source payload was detected.",
            metadata_json={"rule_mode": "source_delta"},
        )
        session.add(watch_run)
        session.commit()

        diagnostics = build_database_diagnostics(session)
        counts = {row["table_name"]: row["row_count"] for row in diagnostics["table_counts"]}
        assert counts["watches"] == 1
        assert counts["watch_runs"] == 1

        snapshot = build_runtime_snapshot(session)
        assert snapshot["watches"][0]["slug"] == "persistence-watch"
        assert snapshot["watches"][0]["rule_json"]["mode"] == "source_delta"
        assert snapshot["watch_runs"][0]["outcome"] == "change"
        assert snapshot["watch_runs"][0]["storage_object_id"] == evidence.storage_object_id

        legacy_payload = dict(snapshot)
        legacy_payload.pop("watches")
        legacy_payload.pop("watch_runs")
        legacy_snapshot = TypeAdapter(RuntimeSnapshotRead).validate_python(legacy_payload)
        assert legacy_snapshot.watches == []
        assert legacy_snapshot.watch_runs == []
    finally:
        session.close()

    restore_engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(restore_engine)
    with Session(restore_engine) as restore_session:
        result = restore_runtime_snapshot(restore_session, snapshot)
        restored_counts = {row["table_name"]: row["row_count"] for row in result["row_counts"]}
        assert restored_counts["watches"] == 1
        assert restored_counts["watch_runs"] == 1

        restored_watch = restore_session.get(WatchORM, watch.watch_id)
        restored_run = restore_session.get(WatchRunORM, watch_run.watch_run_id)
        assert restored_watch is not None
        assert restored_watch.scheduled_task_id == schedule.task_id
        assert restored_watch.baseline_json == {"payload_sha256": "0" * 64}
        assert restored_run is not None
        assert restored_run.watch_id == restored_watch.watch_id
        assert restored_run.scheduled_task_run_id == task_run.task_run_id
        assert restored_run.source_run_id == source_run.source_run_id
        assert restored_run.alert_id == alert.alert_id
        assert restored_run.storage_object_id == evidence.storage_object_id
    restore_engine.dispose()

    legacy_engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(legacy_engine)
    with Session(legacy_engine) as legacy_restore_session:
        legacy_result = restore_runtime_snapshot(
            legacy_restore_session,
            legacy_snapshot.model_dump(mode="python"),
        )
        legacy_counts = {row["table_name"]: row["row_count"] for row in legacy_result["row_counts"]}
        assert legacy_counts["watches"] == 0
        assert legacy_counts["watch_runs"] == 0
        assert legacy_counts["source_definitions"] == 1
        assert legacy_counts["scheduled_tasks"] == 1
    legacy_engine.dispose()
    reset_db_state()
    reset_settings_cache()
