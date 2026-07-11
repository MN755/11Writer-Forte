from fastapi.testclient import TestClient
import pytest
from sqlalchemy import select

from src.db import get_session_factory
from src.models import ObservationORM, SourceDefinitionORM
from src.schemas import (
    InvestigationWatchCompileRequest,
    WatchCreate,
    WatchScheduleCreate,
    WatchUpdate,
)
from src.services.watch_service import (
    archive_watch,
    attach_watch_schedule,
    compile_investigation_watch_instruction,
    create_watch,
    create_investigation_watch_candidate,
    evaluate_watch,
    generate_watch_report,
    list_watch_rule_versions,
    pause_watch,
    resume_watch,
    update_watch,
)


def test_observation_watch_baselines_then_creates_a_deduplicated_alert(client: TestClient) -> None:
    created = client.post(
        "/api/watches",
        json={
            "name": "Local observation watch",
            "slug": "local-observation-watch",
            "objective": "Detect new local observations.",
            "watch_type": "observation_rule",
            "rule_json": {"mode": "observation_rule"},
            "severity": "warning",
        },
    )
    assert created.status_code == 200, created.text
    watch_id = created.json()["watch_id"]

    first = client.post(f"/api/watches/{watch_id}/run")
    assert first.status_code == 200, first.text
    assert first.json()["outcome"] == "baseline"

    session = get_session_factory()()
    try:
        session.add(
            ObservationORM(
                layer_key="watch-test",
                source_type="test",
                content_text="new observation",
                content_json={},
                raw_hash="watch-test-1",
            )
        )
        session.commit()
    finally:
        session.close()

    changed = client.post(f"/api/watches/{watch_id}/run")
    assert changed.status_code == 200, changed.text
    assert changed.json()["outcome"] == "change"
    assert changed.json()["alert_id"] is not None

    repeated = client.post(f"/api/watches/{watch_id}/run")
    assert repeated.status_code == 200
    assert repeated.json()["outcome"] == "no_change"
    alerts = client.get(f"/api/watches/{watch_id}/alerts")
    assert alerts.status_code == 200
    assert len(alerts.json()) == 1

    feed = client.get("/api/watches/feed.rss")
    assert feed.status_code == 200
    assert "11Writer Forte Watch Alerts" in feed.text


def test_watch_schedule_executes_through_scheduler(client: TestClient) -> None:
    created = client.post(
        "/api/watches",
        json={
            "name": "Scheduled Watch",
            "slug": "scheduled-watch",
            "objective": "Run through the scheduler.",
            "watch_type": "observation_rule",
            "rule_json": {"mode": "observation_rule"},
        },
    )
    watch_id = created.json()["watch_id"]
    scheduled = client.post(f"/api/watches/{watch_id}/schedule", json={"interval_seconds": 60})
    assert scheduled.status_code == 200, scheduled.text
    task_run = client.post(f"/api/scheduler/tasks/{scheduled.json()['task_id']}/run")
    assert task_run.status_code == 200, task_run.text
    runs = client.get(f"/api/watches/{watch_id}/runs")
    assert len(runs.json()) == 1
    assert runs.json()[0]["outcome"] == "baseline"


def test_investigation_watch_compilation_is_deterministic_and_inspectable(
    client: TestClient,
) -> None:
    api_preview = client.post(
        "/api/watches/compile",
        json={
            "instruction": 'Monitor "Acme plant closure" in Duluth for credible updates.',
            "parent_investigation_id": "investigation-42",
            "query_version": "q1",
            "target_entities": ["Acme"],
        },
    )
    assert api_preview.status_code == 200, api_preview.text
    assert api_preview.json()["scope_preview_json"]["parent_investigation_id"] == "investigation-42"

    api_candidate = client.post(
        "/api/watches/investigation-candidates",
        json={
            "name": "API Acme closure",
            "slug": "api-acme-closure",
            "instruction": "Monitor Acme closure.",
            "parent_investigation_id": "investigation-api",
        },
    )
    assert api_candidate.status_code == 200, api_candidate.text
    assert api_candidate.json()["state"] == "paused"
    api_watch_id = api_candidate.json()["watch_id"]
    assert client.post(f"/api/watches/{api_watch_id}/resume").status_code == 200
    api_run = client.post(f"/api/watches/{api_watch_id}/run")
    assert api_run.status_code == 200, api_run.text
    assert api_run.json()["outcome"] == "baseline"

    request = InvestigationWatchCompileRequest(
        instruction='Monitor "Acme plant closure" in Duluth for credible updates.',
        parent_investigation_id="investigation-42",
        query_version="q1",
        target_entities=["Acme"],
    )
    first = compile_investigation_watch_instruction(request)
    second = compile_investigation_watch_instruction(request)
    assert first.rule_hash == second.rule_hash
    assert first.canonical_rule_json == second.canonical_rule_json
    assert first.scope_preview_json["parent_investigation_id"] == "investigation-42"
    assert first.scope_preview_json["targets"]["locations"] == ["duluth"]

    session = get_session_factory()()
    try:
        watch, compilation = create_investigation_watch_candidate(
            session,
            request,
            name="Acme closure",
            slug="acme-closure",
            actor="test",
        )
        assert watch.state == "paused"
        versions = list_watch_rule_versions(session, watch.watch_id)
        assert [(item.version_number, item.status) for item in versions] == [(1, "candidate")]
        assert versions[0].rule_hash == compilation.rule_hash

        scheduled = attach_watch_schedule(
            session,
            watch.watch_id,
            WatchScheduleCreate(interval_seconds=60),
            actor="test",
        )
        assert not scheduled.enabled
        resume_watch(session, watch.watch_id, actor="test")
        assert scheduled.enabled
        assert list_watch_rule_versions(session, watch.watch_id)[0].status == "active"

        revised = compile_investigation_watch_instruction(
            request.model_copy(
                update={
                    "instruction": 'Monitor "Acme plant closure" in Superior.',
                    "query_version": "q2",
                }
            )
        )
        update_watch(
            session,
            watch.watch_id,
            WatchUpdate(rule_json=revised.rule),
            actor="test",
        )
        versions = list_watch_rule_versions(session, watch.watch_id)
        assert [(item.version_number, item.query_version, item.status) for item in versions] == [
            (1, "q1", "superseded"),
            (2, "q2", "active"),
        ]

        pause_watch(session, watch.watch_id, actor="test")
        assert not scheduled.enabled
        with pytest.raises(ValueError, match="paused"):
            evaluate_watch(session, watch.watch_id, actor="test", force=True)
        report = generate_watch_report(session, watch.watch_id, actor="test")
        assert report.report_json["watch_id"] == watch.watch_id

        archive_watch(session, watch.watch_id, actor="test")
        assert not scheduled.enabled
        with pytest.raises(ValueError, match="archived"):
            resume_watch(session, watch.watch_id, actor="test")
    finally:
        session.close()


def test_failed_watch_source_records_bounded_retry_and_alternate_gap(
    client: TestClient, tmp_path
) -> None:
    session = get_session_factory()()
    try:
        missing = str(tmp_path / "missing-source.json")
        session.add_all(
            [
                SourceDefinitionORM(
                    name="failing-watch-source",
                    source_kind="local_file",
                    layer_key="watch-coverage",
                    target_uri=missing,
                ),
                SourceDefinitionORM(
                    name="alternate-watch-source",
                    source_kind="local_file",
                    layer_key="watch-coverage",
                    target_uri=missing,
                ),
            ]
        )
        session.commit()
        primary = session.scalar(
            select(SourceDefinitionORM).where(SourceDefinitionORM.name == "failing-watch-source")
        )
        assert primary is not None
        watch = create_watch(
            session,
            WatchCreate(
                name="coverage watch",
                slug="coverage-watch",
                objective="Track a source fleet.",
                watch_type="source_delta",
                source_id=primary.source_id,
                rule_json={"mode": "source_delta"},
            ),
            actor="test",
        )
        for _ in range(3):
            with pytest.raises(FileNotFoundError):
                evaluate_watch(session, watch.watch_id, actor="test")
        session.refresh(watch)
        assert len(watch.coverage_json["attempts"]) == 3
        assert watch.coverage_json["gaps"][f"source:{primary.source_id}"]["status"] == "open"
        actions = [
            item["action"]["action"] for item in watch.coverage_json["changes"] if "action" in item
        ]
        assert "try_alternate_source" in actions
    finally:
        session.close()
