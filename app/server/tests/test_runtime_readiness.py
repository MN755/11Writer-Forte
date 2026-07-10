from __future__ import annotations

import json
from pathlib import Path
from datetime import timedelta

from fastapi.testclient import TestClient

from src.db import get_session_factory
from src.models import ScheduledTaskORM
from src.services.platform_runtime_worker_service import run_platform_runtime_worker
from src.services.scheduler_service import ScheduledTaskExecutionError, run_task, scheduler_now


def test_runtime_readiness_flags_gaps_and_clears_after_baseline_setup(
    client: TestClient,
    tmp_path: Path,
) -> None:
    initial_response = client.get("/api/operations/readiness")
    assert initial_response.status_code == 200
    initial_payload = initial_response.json()
    assert initial_payload["overall_status"] == "not_ready"
    initial_checks = {item["key"]: item for item in initial_payload["checks"]}
    assert initial_checks["integrity_registry"]["status"] == "action_required"
    assert initial_checks["enabled_source_catalog"]["status"] == "action_required"
    assert initial_checks["enabled_schedule_catalog"]["status"] == "action_required"

    seed_response = client.post("/api/source-trust/seed-defaults")
    assert seed_response.status_code == 200
    assert seed_response.json()["created"] >= 1

    source_fixture = tmp_path / "readiness-source.json"
    source_fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Readiness source record",
                    "url": "https://readiness.example.com/1",
                    "lat": 44.98,
                    "lon": -93.26,
                }
            ]
        ),
        encoding="utf-8",
    )
    source_response = client.post(
        "/api/sources",
        json={
            "name": "readiness-source",
            "source_kind": "local_file",
            "layer_key": "readiness-layer",
            "target_uri": str(source_fixture),
        },
    )
    assert source_response.status_code == 200
    source_id = source_response.json()["source_id"]

    tasks = [
        {
            "name": "readiness-source-sync",
            "task_type": "source_sync",
            "interval_seconds": 300,
            "source_id": source_id,
        },
        {
            "name": "readiness-source-maintenance",
            "task_type": "source_maintenance",
            "interval_seconds": 600,
            "payload_json": {"stale_after_hours": 24.0, "source_limit": 25, "dead_letter_limit": 50},
        },
        {
            "name": "readiness-source-health",
            "task_type": "source_health_scan",
            "interval_seconds": 600,
            "payload_json": {"stale_after_hours": 24.0, "source_limit": 50},
        },
        {
            "name": "readiness-storage-lifecycle",
            "task_type": "storage_lifecycle",
            "interval_seconds": 3600,
            "payload_json": {"limit": 100},
        },
        {
            "name": "readiness-runtime-snapshot",
            "task_type": "runtime_snapshot_export",
            "interval_seconds": 21600,
            "payload_json": {"file_prefix": "readiness-runtime-snapshot"},
        },
        {
            "name": "readiness-entity-resolution",
            "task_type": "entity_resolution_refresh",
            "interval_seconds": 900,
            "payload_json": {"limit": 100, "min_observations": 2},
        },
        {
            "name": "readiness-event-fusion",
            "task_type": "event_fusion_refresh",
            "interval_seconds": 900,
            "payload_json": {
                "limit": 100,
                "time_window_minutes": 120,
                "distance_km": 10.0,
                "min_independent_signals": 2,
            },
        },
    ]
    for payload in tasks:
        response = client.post("/api/scheduler/tasks", json=payload)
        assert response.status_code == 200, response.text

    ready_response = client.get("/api/operations/readiness")
    assert ready_response.status_code == 200
    ready_payload = ready_response.json()
    assert ready_payload["overall_status"] == "not_ready"
    assert ready_payload["ready"] is False
    ready_checks = {item["key"]: item for item in ready_payload["checks"]}
    assert ready_checks["scheduler_executor_coverage"]["status"] == "action_required"
    assert ready_checks["source_runtime_executor_coverage"]["status"] == "pass"
    assert ready_checks["runtime_snapshot_coverage"]["status"] == "action_required"

    run_platform_runtime_worker(
        get_session_factory(),
        poll_seconds=0.0,
        once=True,
        max_iterations=1,
        include_stream_runtime=False,
        include_enabled_schedules=True,
        actor="test_runtime_readiness",
    )

    ready_response = client.get("/api/operations/readiness")
    assert ready_response.status_code == 200
    ready_payload = ready_response.json()
    assert ready_payload["overall_status"] == "ready"
    assert ready_payload["ready"] is True
    assert ready_payload["action_required_count"] == 0
    assert ready_payload["warning_count"] == 0
    ready_checks = {item["key"]: item for item in ready_payload["checks"]}
    assert ready_checks["integrity_registry"]["status"] == "pass"
    assert ready_checks["enabled_source_catalog"]["status"] == "pass"
    assert ready_checks["source_ops_coverage"]["status"] == "pass"
    assert ready_checks["source_sync_coverage"]["status"] == "pass"
    assert ready_checks["analytic_pipeline_coverage"]["status"] == "pass"
    assert ready_checks["camera_pipeline_coverage"]["status"] == "pass"
    assert ready_checks["scheduler_executor_coverage"]["status"] == "pass"
    assert ready_checks["runtime_snapshot_coverage"]["status"] == "pass"


def test_runtime_readiness_flags_missing_camera_pipeline_coverage(
    client: TestClient,
    tmp_path: Path,
) -> None:
    seed_response = client.post("/api/source-trust/seed-defaults")
    assert seed_response.status_code == 200

    camera_source = tmp_path / "camera-readiness-source.json"
    camera_source.write_text(
        json.dumps(
            [
                {
                    "camera_id": "readiness-cam-1",
                    "camera_name": "Readiness Camera 1",
                    "image_url": "https://cams.example.com/readiness-cam-1.jpg",
                    "page_url": "https://cams.example.com/readiness-cam-1",
                    "lat": 44.95,
                    "lon": -93.27,
                }
            ]
        ),
        encoding="utf-8",
    )

    source_response = client.post(
        "/api/sources",
        json={
            "name": "readiness-camera-stream",
            "source_kind": "local_file",
            "layer_key": "readiness-camera-layer",
            "target_uri": str(camera_source),
        },
    )
    assert source_response.status_code == 200
    source_id = source_response.json()["source_id"]

    sync_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "readiness-camera-source-sync",
            "task_type": "source_sync",
            "interval_seconds": 300,
            "source_id": source_id,
        },
    )
    assert sync_response.status_code == 200

    readiness_response = client.get("/api/operations/readiness")
    assert readiness_response.status_code == 200
    payload = readiness_response.json()
    checks = {item["key"]: item for item in payload["checks"]}
    assert checks["camera_pipeline_coverage"]["status"] == "action_required"
    assert checks["camera_pipeline_coverage"]["details_json"]["camera_runtime_expected"] is True
    assert set(checks["camera_pipeline_coverage"]["details_json"]["missing_task_types"]) == {
        "camera_inventory_refresh",
        "camera_source_verification",
    }

    refresh_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "readiness-camera-refresh",
            "task_type": "camera_inventory_refresh",
            "interval_seconds": 600,
            "layer_key": "readiness-camera-layer",
            "payload_json": {"limit": 25},
        },
    )
    assert refresh_response.status_code == 200
    verify_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "readiness-camera-verify",
            "task_type": "camera_source_verification",
            "interval_seconds": 600,
            "layer_key": "readiness-camera-layer",
            "payload_json": {"limit": 25, "timeout_seconds": 5.0},
        },
    )
    assert verify_response.status_code == 200

    readiness_response = client.get("/api/operations/readiness")
    assert readiness_response.status_code == 200
    payload = readiness_response.json()
    checks = {item["key"]: item for item in payload["checks"]}
    assert checks["camera_pipeline_coverage"]["status"] == "pass"


def test_runtime_readiness_flags_missing_runtime_snapshot_coverage(client: TestClient) -> None:
    seed_response = client.post("/api/source-trust/seed-defaults")
    assert seed_response.status_code == 200

    source_response = client.post(
        "/api/sources",
        json={
            "name": "readiness-backup-gap-source",
            "source_kind": "webhook_ingest",
            "layer_key": "readiness-backup-gap-layer",
            "target_uri": "webhook://backup-gap",
        },
    )
    assert source_response.status_code == 200

    for payload in (
        {
            "name": "readiness-backup-gap-maintenance",
            "task_type": "source_maintenance",
            "interval_seconds": 600,
            "payload_json": {"stale_after_hours": 24.0, "source_limit": 25, "dead_letter_limit": 50},
        },
        {
            "name": "readiness-backup-gap-health",
            "task_type": "source_health_scan",
            "interval_seconds": 600,
            "payload_json": {"stale_after_hours": 24.0, "source_limit": 50},
        },
        {
            "name": "readiness-backup-gap-storage",
            "task_type": "storage_lifecycle",
            "interval_seconds": 3600,
            "payload_json": {"limit": 100},
        },
        {
            "name": "readiness-backup-gap-entities",
            "task_type": "entity_resolution_refresh",
            "interval_seconds": 900,
            "payload_json": {"limit": 100, "min_observations": 2},
        },
        {
            "name": "readiness-backup-gap-events",
            "task_type": "event_fusion_refresh",
            "interval_seconds": 900,
            "payload_json": {
                "limit": 100,
                "time_window_minutes": 120,
                "distance_km": 10.0,
                "min_independent_signals": 2,
            },
        },
    ):
        response = client.post("/api/scheduler/tasks", json=payload)
        assert response.status_code == 200

    readiness_response = client.get("/api/operations/readiness")
    assert readiness_response.status_code == 200
    payload = readiness_response.json()
    checks = {item["key"]: item for item in payload["checks"]}
    assert checks["runtime_snapshot_coverage"]["status"] == "action_required"
    assert checks["runtime_snapshot_coverage"]["details_json"]["runtime_snapshot_task_count"] == 0


def test_runtime_readiness_fails_closed_for_broken_local_import_targets(
    client: TestClient,
    tmp_path: Path,
) -> None:
    healthy_fixture = tmp_path / "readiness-local-import.json"
    healthy_fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Healthy local import target",
                    "url": "https://local-import.example.com/1",
                }
            ]
        ),
        encoding="utf-8",
    )
    empty_bundle = tmp_path / "empty-bundle"
    empty_bundle.mkdir()
    (empty_bundle / "ignored.bin").write_bytes(b"\x00\x01")
    missing_fixture = tmp_path / "missing-local-import.json"

    healthy_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "readiness-local-import-healthy",
            "task_type": "local_import",
            "interval_seconds": 300,
            "target_path": str(healthy_fixture),
            "layer_key": "readiness-local-layer",
        },
    )
    assert healthy_response.status_code == 200
    missing_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "readiness-local-import-missing",
            "task_type": "local_import",
            "interval_seconds": 300,
            "target_path": str(missing_fixture),
            "layer_key": "readiness-local-layer",
        },
    )
    assert missing_response.status_code == 200
    empty_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "readiness-local-import-empty-dir",
            "task_type": "local_import",
            "interval_seconds": 300,
            "target_path": str(empty_bundle),
            "layer_key": "readiness-local-layer",
        },
    )
    assert empty_response.status_code == 200

    readiness_response = client.get("/api/operations/readiness")
    assert readiness_response.status_code == 200
    payload = readiness_response.json()
    checks = {item["key"]: item for item in payload["checks"]}
    assert checks["local_import_target_health"]["status"] == "action_required"
    assert checks["local_import_target_health"]["details_json"]["enabled_local_import_task_count"] == 3
    assert checks["local_import_target_health"]["details_json"]["affected_task_count"] == 2
    affected = {
        item["task_id"]: item["issue"]
        for item in checks["local_import_target_health"]["details_json"]["affected_tasks"]
    }
    assert missing_response.json()["task_id"] in affected
    assert empty_response.json()["task_id"] in affected
    assert affected[missing_response.json()["task_id"]] == "missing_path"
    assert affected[empty_response.json()["task_id"]] == "empty_supported_directory"


def test_runtime_readiness_fails_closed_for_unhealthy_source_fleet(client: TestClient) -> None:
    seed_response = client.post("/api/source-trust/seed-defaults")
    assert seed_response.status_code == 200

    source_response = client.post(
        "/api/sources",
        json={
            "name": "readiness-failing-source",
            "source_kind": "http_json",
            "layer_key": "readiness-failure-layer",
            "target_uri": "http://127.0.0.1:1/readiness-failing.json",
            "metadata_json": {
                "retry_attempts": 1,
                "request_timeout_seconds": 1,
            },
        },
    )
    assert source_response.status_code == 200
    source_id = source_response.json()["source_id"]

    tasks = [
        {
            "name": "readiness-failing-source-sync",
            "task_type": "source_sync",
            "interval_seconds": 300,
            "source_id": source_id,
        },
        {
            "name": "readiness-failing-maintenance",
            "task_type": "source_maintenance",
            "interval_seconds": 600,
            "payload_json": {"stale_after_hours": 24.0, "source_limit": 25, "dead_letter_limit": 50},
        },
        {
            "name": "readiness-failing-health",
            "task_type": "source_health_scan",
            "interval_seconds": 600,
            "payload_json": {"stale_after_hours": 24.0, "source_limit": 50},
        },
        {
            "name": "readiness-failing-storage",
            "task_type": "storage_lifecycle",
            "interval_seconds": 3600,
            "payload_json": {"limit": 100},
        },
        {
            "name": "readiness-failing-entities",
            "task_type": "entity_resolution_refresh",
            "interval_seconds": 900,
            "payload_json": {"limit": 100, "min_observations": 2},
        },
        {
            "name": "readiness-failing-events",
            "task_type": "event_fusion_refresh",
            "interval_seconds": 900,
            "payload_json": {
                "limit": 100,
                "time_window_minutes": 120,
                "distance_km": 10.0,
                "min_independent_signals": 2,
            },
        },
    ]
    for payload in tasks:
        response = client.post("/api/scheduler/tasks", json=payload)
        assert response.status_code == 200, response.text

    run_platform_runtime_worker(
        get_session_factory(),
        poll_seconds=0.0,
        once=True,
        max_iterations=1,
        include_stream_runtime=False,
        include_enabled_schedules=True,
        actor="test_runtime_readiness_failing_source",
    )

    readiness_response = client.get("/api/operations/readiness")
    assert readiness_response.status_code == 200
    payload = readiness_response.json()
    assert payload["overall_status"] == "not_ready"
    assert payload["ready"] is False
    checks = {item["key"]: item for item in payload["checks"]}
    assert checks["scheduler_executor_coverage"]["status"] == "pass"
    assert checks["source_runtime_health"]["status"] == "action_required"
    assert source_id in checks["source_runtime_health"]["details_json"]["failing_source_ids"]
    assert source_id in checks["source_runtime_health"]["details_json"]["stale_pull_source_ids"]
    assert checks["source_runtime_health"]["details_json"]["affected_source_count"] >= 1


def test_runtime_readiness_fails_closed_for_unhealthy_scheduler_fleet(
    client: TestClient,
    tmp_path: Path,
) -> None:
    seed_response = client.post("/api/source-trust/seed-defaults")
    assert seed_response.status_code == 200

    source_fixture = tmp_path / "readiness-scheduler-source.json"
    source_fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Scheduler readiness source record",
                    "url": "https://scheduler-readiness.example.com/1",
                    "lat": 44.98,
                    "lon": -93.26,
                }
            ]
        ),
        encoding="utf-8",
    )
    source_response = client.post(
        "/api/sources",
        json={
            "name": "readiness-scheduler-source",
            "source_kind": "local_file",
            "layer_key": "readiness-scheduler-layer",
            "target_uri": str(source_fixture),
        },
    )
    assert source_response.status_code == 200
    source_id = source_response.json()["source_id"]

    healthy_fixture = tmp_path / "readiness-scheduler-healthy.json"
    healthy_fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Healthy scheduled import",
                    "url": "https://scheduler-ready.example.com/ok",
                    "lat": 44.99,
                    "lon": -93.25,
                }
            ]
        ),
        encoding="utf-8",
    )

    broken_fixture = tmp_path / "missing-scheduler-import.json"

    task_payloads = [
        {
            "name": "readiness-scheduler-source-sync",
            "task_type": "source_sync",
            "interval_seconds": 300,
            "source_id": source_id,
        },
        {
            "name": "readiness-scheduler-maintenance",
            "task_type": "source_maintenance",
            "interval_seconds": 600,
            "payload_json": {"stale_after_hours": 24.0, "source_limit": 25, "dead_letter_limit": 50},
        },
        {
            "name": "readiness-scheduler-health",
            "task_type": "source_health_scan",
            "interval_seconds": 600,
            "payload_json": {"stale_after_hours": 24.0, "source_limit": 50},
        },
        {
            "name": "readiness-scheduler-storage",
            "task_type": "storage_lifecycle",
            "interval_seconds": 3600,
            "payload_json": {"limit": 100},
        },
        {
            "name": "readiness-scheduler-entity-resolution",
            "task_type": "entity_resolution_refresh",
            "interval_seconds": 900,
            "payload_json": {"limit": 100, "min_observations": 2},
        },
        {
            "name": "readiness-scheduler-event-fusion",
            "task_type": "event_fusion_refresh",
            "interval_seconds": 900,
            "payload_json": {
                "limit": 100,
                "time_window_minutes": 120,
                "distance_km": 10.0,
                "min_independent_signals": 2,
            },
        },
        {
            "name": "readiness-scheduler-healthy-import",
            "task_type": "local_import",
            "interval_seconds": 300,
            "target_path": str(healthy_fixture),
            "layer_key": "readiness-scheduler-local-layer",
        },
        {
            "name": "readiness-scheduler-broken-import",
            "task_type": "local_import",
            "interval_seconds": 300,
            "target_path": str(broken_fixture),
            "layer_key": "readiness-scheduler-local-layer",
        },
    ]

    task_ids_by_name: dict[str, int] = {}
    for payload in task_payloads:
        response = client.post("/api/scheduler/tasks", json=payload)
        assert response.status_code == 200, response.text
        task_ids_by_name[payload["name"]] = response.json()["task_id"]

    run_platform_runtime_worker(
        get_session_factory(),
        poll_seconds=0.0,
        once=True,
        max_iterations=1,
        include_stream_runtime=False,
        include_enabled_schedules=True,
        actor="test_runtime_readiness_scheduler",
    )

    broken_task_id = task_ids_by_name["readiness-scheduler-broken-import"]
    session = get_session_factory()()
    try:
        try:
            run_task(session, broken_task_id, actor="test_runtime_readiness_scheduler_failure")
        except ScheduledTaskExecutionError as exc:
            assert isinstance(exc.cause, FileNotFoundError)
        else:
            raise AssertionError("Expected broken scheduled import task to fail.")
    finally:
        session.close()

    overdue_task_id = task_ids_by_name["readiness-scheduler-storage"]
    session = get_session_factory()()
    try:
        overdue_task = session.get(ScheduledTaskORM, overdue_task_id)
        assert overdue_task is not None
        overdue_task.next_run_at = scheduler_now() - timedelta(hours=2)
        session.commit()
    finally:
        session.close()

    readiness_response = client.get("/api/operations/readiness")
    assert readiness_response.status_code == 200
    payload = readiness_response.json()
    assert payload["overall_status"] == "not_ready"
    assert payload["ready"] is False
    checks = {item["key"]: item for item in payload["checks"]}
    assert checks["source_runtime_health"]["status"] == "pass"
    assert checks["scheduler_runtime_health"]["status"] == "action_required"
    assert broken_task_id in checks["scheduler_runtime_health"]["details_json"]["failing_task_ids"]
    assert overdue_task_id in checks["scheduler_runtime_health"]["details_json"]["overdue_task_ids"]
