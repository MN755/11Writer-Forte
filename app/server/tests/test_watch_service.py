from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterator
from xml.etree import ElementTree

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from src.db import get_session_factory
from src.models import (
    AlertORM,
    CustodyLogORM,
    DataLayerORM,
    ObservationORM,
    ScheduledTaskRunORM,
    SourceRunORM,
    StorageObjectORM,
    WatchRunORM,
)
from src.schemas import SourceDefinitionCreate, WatchCreate, WatchScheduleCreate
from src.services.scheduler_service import ScheduledTaskExecutionError, run_task
from src.services.source_service import create_source_definition
from src.services.watch_service import (
    WatchEvaluationError,
    attach_watch_schedule,
    create_watch,
    evaluate_watch,
    list_watch_alerts,
    list_watch_evidence,
    render_watch_alert_rss,
)


@contextmanager
def mutable_payload_server(
    initial_payload: bytes,
    *,
    content_type: str = "image/jpeg",
) -> Iterator[tuple[str, dict[str, object]]]:
    state: dict[str, object] = {
        "payload": initial_payload,
        "content_type": content_type,
        "request_count": 0,
    }

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            state["request_count"] = int(state["request_count"]) + 1
            payload = bytes(state["payload"])
            self.send_response(200)
            self.send_header("Content-Type", str(state["content_type"]))
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}/image", state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_image_watch_baseline_unchanged_change_retains_immutable_evidence(
    client: TestClient,
) -> None:
    first_image = b"\xff\xd8\xffbaseline-image\xff\xd9"
    changed_image = b"\xff\xd8\xffchanged-image\xff\xd9"
    with mutable_payload_server(first_image) as (target_uri, server_state):
        session = get_session_factory()()
        try:
            source = create_source_definition(
                session,
                SourceDefinitionCreate(
                    name="watch-image-http-source",
                    source_kind="http_text",
                    layer_key="watch-image-layer",
                    target_uri=target_uri,
                    metadata_json={
                        "retry_attempts": 1,
                        "request_timeout_seconds": 2,
                        "headers": {"X-Secret-Test": "must-not-persist"},
                    },
                ),
            )
            watch = create_watch(
                session,
                WatchCreate(
                    name="Piston Peak <Image> & Watch",
                    slug="piston-peak-image-watch",
                    objective="Retain each new construction image.",
                    watch_type="image_change",
                    source_id=source.source_id,
                    severity="warning",
                    rule_json={
                        "mode": "image_change",
                        "comparison": "sha256",
                        "alert_on_initial": False,
                        "accepted_media_types": ["image/jpeg"],
                        "retention_class": "permanent",
                    },
                ),
            )

            baseline = evaluate_watch(session, watch.watch_id, actor="test_watch")
            assert baseline.outcome == "baseline"
            assert baseline.change_detected is False
            assert baseline.storage_object_id is not None
            first_hash = baseline.evidence_json["payload_sha256"]
            first_object = session.get(StorageObjectORM, baseline.storage_object_id)
            assert first_object is not None
            assert first_object.object_key == f"watch:{watch.watch_id}:image:{first_hash}"
            assert first_object.object_kind == "watch_image_evidence"
            assert first_object.owner_type == "watch_run"
            assert first_object.owner_id == str(baseline.watch_run_id)
            assert first_object.retention_class == "permanent"
            assert Path(first_object.object_uri).read_bytes() == first_image
            serialized_metadata = json.dumps(first_object.metadata_json)
            assert "X-Secret-Test" not in serialized_metadata
            assert "Authorization" not in serialized_metadata

            unchanged = evaluate_watch(session, watch.watch_id, actor="test_watch")
            assert unchanged.outcome == "no_change"
            assert unchanged.storage_object_id == baseline.storage_object_id
            assert session.scalar(select(func.count(StorageObjectORM.storage_object_id))) == 1
            assert list_watch_alerts(session, watch.watch_id) == []

            server_state["payload"] = changed_image
            changed = evaluate_watch(session, watch.watch_id, actor="test_watch")
            assert changed.outcome == "change"
            assert changed.change_detected is True
            assert changed.alert_id is not None
            assert changed.storage_object_id != baseline.storage_object_id
            second_object = session.get(StorageObjectORM, changed.storage_object_id)
            assert second_object is not None
            assert Path(second_object.object_uri).read_bytes() == changed_image
            assert session.scalar(select(func.count(StorageObjectORM.storage_object_id))) == 2
            assert len(list_watch_evidence(session, watch.watch_id)) == 2

            repeated = evaluate_watch(session, watch.watch_id, actor="test_watch")
            assert repeated.outcome == "no_change"
            assert session.scalar(select(func.count(StorageObjectORM.storage_object_id))) == 2
            assert len(list_watch_alerts(session, watch.watch_id)) == 1

            custody = list(session.scalars(select(CustodyLogORM)))
            retained = [row for row in custody if row.action == "watch_evidence_retained"]
            assert len(retained) == 2
            assert all(row.details_json["payload_sha256"] for row in retained)

            rss = render_watch_alert_rss(
                session,
                "http://127.0.0.1:8000",
                watch.watch_id,
            )
            root = ElementTree.fromstring(rss)
            assert root.tag == "rss"
            assert root.findtext("./channel/link") == ("http://127.0.0.1:8000/api/watches/feed.rss")
            assert "Piston Peak <Image> & Watch" in (root.findtext("./channel/item/title") or "")
            assert int(server_state["request_count"]) == 4
        finally:
            session.close()


def test_source_delta_baseline_change_and_dedupe(client: TestClient, tmp_path: Path) -> None:
    source_path = tmp_path / "source-delta.json"
    source_path.write_text(json.dumps([{"title": "baseline"}]), encoding="utf-8")
    session = get_session_factory()()
    try:
        source = create_source_definition(
            session,
            SourceDefinitionCreate(
                name="source-delta-service-source",
                source_kind="local_file",
                layer_key="source-delta-layer",
                target_uri=str(source_path),
            ),
        )
        watch = create_watch(
            session,
            WatchCreate(
                name="Source delta service watch",
                slug="source-delta-service-watch",
                objective="Detect source payload changes.",
                watch_type="source_delta",
                source_id=source.source_id,
                rule_json={"mode": "source_delta", "run_source": True},
            ),
        )

        baseline = evaluate_watch(session, watch.watch_id)
        assert baseline.outcome == "baseline"
        source_path.write_text(json.dumps([{"title": "changed"}]), encoding="utf-8")
        changed = evaluate_watch(session, watch.watch_id)
        assert changed.outcome == "change"
        unchanged = evaluate_watch(session, watch.watch_id)
        assert unchanged.outcome == "no_change"
        assert len(list_watch_alerts(session, watch.watch_id)) == 1
        evidence_ids = {
            row.storage_object_id for row in list_watch_evidence(session, watch.watch_id)
        }
        assert changed.storage_object_id in evidence_ids
        assert changed.evidence_json["source_run_id"] == changed.source_run_id
        assert changed.evidence_json["storage_object_id"] == changed.storage_object_id
    finally:
        session.close()


def test_source_delta_recurring_state_change_alerts_again_after_intervening_state(
    client: TestClient,
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source-delta-cycle.json"
    source_path.write_text(json.dumps([{"title": "A"}]), encoding="utf-8")
    session = get_session_factory()()
    try:
        source = create_source_definition(
            session,
            SourceDefinitionCreate(
                name="source-delta-cycle-source",
                source_kind="local_file",
                layer_key="source-delta-cycle-layer",
                target_uri=str(source_path),
            ),
        )
        watch = create_watch(
            session,
            WatchCreate(
                name="Source delta cycle watch",
                slug="source-delta-cycle-watch",
                objective="Alert for recurring materially changed source states.",
                watch_type="source_delta",
                source_id=source.source_id,
                rule_json={"mode": "source_delta"},
            ),
        )
        assert evaluate_watch(session, watch.watch_id).outcome == "baseline"
        source_path.write_text(json.dumps([{"title": "B"}]), encoding="utf-8")
        assert evaluate_watch(session, watch.watch_id).outcome == "change"
        source_path.write_text(json.dumps([{"title": "A"}]), encoding="utf-8")
        assert evaluate_watch(session, watch.watch_id).outcome == "change"
        source_path.write_text(json.dumps([{"title": "B"}]), encoding="utf-8")
        recurring = evaluate_watch(session, watch.watch_id)
        assert recurring.outcome == "change"
        assert len(list_watch_alerts(session, watch.watch_id)) == 3
    finally:
        session.close()


def test_api_notification_policy_can_suppress_alert_creation(client: TestClient, tmp_path: Path) -> None:
    source_path = tmp_path / "source-delta-no-alert.json"
    source_path.write_text(json.dumps([{"title": "baseline"}]), encoding="utf-8")
    session = get_session_factory()()
    try:
        source = create_source_definition(
            session,
            SourceDefinitionCreate(
                name="source-delta-no-alert-source",
                source_kind="local_file",
                layer_key="source-delta-no-alert-layer",
                target_uri=str(source_path),
            ),
        )
        watch = create_watch(
            session,
            WatchCreate(
                name="No local alert watch",
                slug="no-local-alert-watch",
                objective="Retain deterministic changes without a local notification.",
                watch_type="source_delta",
                source_id=source.source_id,
                rule_json={"mode": "source_delta"},
                notification_policy_json={
                    "api_enabled": False,
                    "rss_enabled": False,
                    "analysis_on_change": False,
                },
            ),
        )
        assert evaluate_watch(session, watch.watch_id).outcome == "baseline"
        source_path.write_text(json.dumps([{"title": "changed"}]), encoding="utf-8")
        changed = evaluate_watch(session, watch.watch_id)
        assert changed.outcome == "change"
        assert changed.alert_id is None
        assert list_watch_alerts(session, watch.watch_id) == []
    finally:
        session.close()


def test_scheduled_image_failure_keeps_parent_task_run_for_retry_history(
    client: TestClient,
) -> None:
    with mutable_payload_server(b"not-an-image", content_type="text/plain") as (target_uri, _):
        session = get_session_factory()()
        try:
            source = create_source_definition(
                session,
                SourceDefinitionCreate(
                    name="scheduled-image-failure-source",
                    source_kind="http_text",
                    layer_key="scheduled-image-failure-layer",
                    target_uri=target_uri,
                    metadata_json={"retry_attempts": 1},
                ),
            )
            watch = create_watch(
                session,
                WatchCreate(
                    name="Scheduled image failure watch",
                    slug="scheduled-image-failure-watch",
                    objective="Retain failure history without losing scheduler state.",
                    watch_type="image_change",
                    source_id=source.source_id,
                    rule_json={"mode": "image_change"},
                ),
            )
            task = attach_watch_schedule(
                session,
                watch.watch_id,
                WatchScheduleCreate(interval_seconds=60, retry_attempts=2),
            )
            with pytest.raises(ScheduledTaskExecutionError) as exc_info:
                run_task(session, task.task_id, actor="test_watch")
            task_run = session.get(ScheduledTaskRunORM, exc_info.value.task_run_id)
            assert task_run is not None
            assert task_run.status == "failed"
            failures = list(
                session.scalars(
                    select(WatchRunORM)
                    .where(WatchRunORM.scheduled_task_run_id == task_run.task_run_id)
                    .order_by(WatchRunORM.watch_run_id.asc())
                )
            )
            assert len(failures) == 2
            assert all(run.outcome == "failure" for run in failures)
        finally:
            session.close()


def test_observation_rule_high_water_and_structured_predicate(client: TestClient) -> None:
    session = get_session_factory()()
    try:
        session.add(DataLayerORM(key="watch-observation-layer", name="Watch observations"))
        session.flush()
        existing = make_observation(
            layer_key="watch-observation-layer",
            raw_hash="a" * 64,
            content_json={"status": "ready", "score": 8},
        )
        session.add(existing)
        session.commit()
        watch = create_watch(
            session,
            WatchCreate(
                name="Observation rule service watch",
                slug="observation-rule-service-watch",
                objective="Detect new ready observations with sufficient score.",
                watch_type="observation_rule",
                layer_key="watch-observation-layer",
                rule_json={
                    "mode": "observation_rule",
                    "include_existing_on_first_run": False,
                    "min_confidence": 0.7,
                    "trust_levels": ["trusted"],
                    "predicates": [
                        {"field": "status", "operator": "eq", "value": "ready"},
                        {"field": "score", "operator": "gte", "value": 10},
                    ],
                },
            ),
        )
        baseline = evaluate_watch(session, watch.watch_id)
        assert baseline.outcome == "baseline"
        assert baseline.evidence_json["new_high_water"] == existing.observation_id

        matching = make_observation(
            layer_key="watch-observation-layer",
            raw_hash="b" * 64,
            confidence_score=0.9,
            trust_level="trusted",
            content_json={"status": "ready", "score": 12},
        )
        nonmatching = make_observation(
            layer_key="watch-observation-layer",
            raw_hash="c" * 64,
            confidence_score=0.9,
            trust_level="trusted",
            content_json={"status": "ready", "score": 9},
        )
        session.add_all([matching, nonmatching])
        session.commit()

        changed = evaluate_watch(session, watch.watch_id)
        assert changed.outcome == "change"
        assert changed.evidence_json["observation_ids"] == [matching.observation_id]
        no_change = evaluate_watch(session, watch.watch_id)
        assert no_change.outcome == "no_change"
        assert len(list_watch_alerts(session, watch.watch_id)) == 1
    finally:
        session.close()


def test_source_health_transition_and_recovery(client: TestClient, tmp_path: Path) -> None:
    session = get_session_factory()()
    try:
        source = create_source_definition(
            session,
            SourceDefinitionCreate(
                name="watch-health-source",
                source_kind="local_file",
                layer_key="watch-health-layer",
                target_uri=str(tmp_path / "not-run.json"),
            ),
        )
        watch = create_watch(
            session,
            WatchCreate(
                name="Source health service watch",
                slug="source-health-service-watch",
                objective="Detect unhealthy source state and recovery.",
                watch_type="source_health",
                source_id=source.source_id,
                rule_json={
                    "mode": "source_health",
                    "stale_after_seconds": 3600,
                    "alert_states": ["failed", "stale", "disabled", "never_run"],
                    "alert_on_initial_unhealthy": True,
                    "alert_on_recovery": True,
                },
            ),
        )
        unhealthy = evaluate_watch(session, watch.watch_id)
        assert unhealthy.outcome == "change"
        assert unhealthy.evidence_json["health_state"] == "never_run"

        source_run = SourceRunORM(
            source_id=source.source_id,
            status="completed",
            finished_at=datetime.now(timezone.utc),
            adapter_kind="local_file",
            fetch_mode="pull",
            output_json={"payload_sha256": "d" * 64},
        )
        session.add(source_run)
        session.commit()
        recovered = evaluate_watch(session, watch.watch_id)
        assert recovered.outcome == "change"
        assert recovered.evidence_json["health_state"] == "healthy"
        assert recovered.evidence_json["recovery"] is True
        assert len(list_watch_alerts(session, watch.watch_id)) == 2
    finally:
        session.close()


def test_failed_image_evaluation_commits_watch_run_and_never_invokes_codex(
    client: TestClient,
) -> None:
    with mutable_payload_server(b"not-an-image", content_type="text/plain") as (target_uri, _):
        session = get_session_factory()()
        try:
            source = create_source_definition(
                session,
                SourceDefinitionCreate(
                    name="watch-failure-image-source",
                    source_kind="http_text",
                    layer_key="watch-failure-layer",
                    target_uri=target_uri,
                    metadata_json={"retry_attempts": 1, "request_timeout_seconds": 2},
                ),
            )
            watch = create_watch(
                session,
                WatchCreate(
                    name="Failed image service watch",
                    slug="failed-image-service-watch",
                    objective="Record a deterministic MIME failure.",
                    watch_type="image_change",
                    source_id=source.source_id,
                    rule_json={"mode": "image_change"},
                ),
            )
            assert watch.notification_policy_json["analysis_on_change"] is False
            with pytest.raises(WatchEvaluationError) as exc_info:
                evaluate_watch(session, watch.watch_id)
            assert exc_info.value.watch_id == watch.watch_id
            failed = session.get(WatchRunORM, exc_info.value.watch_run_id)
            assert failed is not None
            assert failed.status == "failed"
            assert failed.outcome == "failure"
            assert "expected one of" in (failed.error_text or "")
            assert session.scalar(select(func.count(AlertORM.alert_id))) == 0
            assert not any(
                "codex" in row.actor.lower() for row in session.scalars(select(CustodyLogORM))
            )
        finally:
            session.close()


def make_observation(
    *,
    layer_key: str,
    raw_hash: str,
    content_json: dict[str, object],
    confidence_score: float = 0.5,
    trust_level: str = "neutral",
) -> ObservationORM:
    return ObservationORM(
        layer_key=layer_key,
        source_domain="watch-observation.example",
        source_type="test",
        record_format="json",
        trust_level=trust_level,
        approval_policy="manual_review",
        confidence_score=confidence_score,
        content_text=json.dumps(content_json, sort_keys=True),
        content_json=content_json,
        raw_hash=raw_hash,
    )
