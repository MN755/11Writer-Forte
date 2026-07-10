from fastapi.testclient import TestClient

from src.db import get_session_factory
from src.services.platform_runtime_worker_service import run_platform_runtime_worker


def test_health(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["app_name"] == "11Writer Forte"
    assert payload["database_backend"] == "sqlite"
    assert payload["database_connected"] is True
    assert payload["spatial_backend"] == "python"
    assert payload["postgis_ready"] is True
    assert payload["warning_count"] == 0


def test_ready_reports_service_unavailable_until_runtime_is_ready(client: TestClient) -> None:
    initial_response = client.get("/ready")
    assert initial_response.status_code == 503
    initial_payload = initial_response.json()
    assert initial_payload["status"] == "not_ready"
    assert initial_payload["ready"] is False
    assert initial_payload["overall_status"] == "not_ready"
    assert initial_payload["action_required_count"] >= 1

    seed_response = client.post("/api/source-trust/seed-defaults")
    assert seed_response.status_code == 200

    source_response = client.post(
        "/api/sources",
        json={
            "name": "ready-probe-source",
            "source_kind": "webhook_ingest",
            "layer_key": "ready-probe-layer",
            "target_uri": "webhook://local",
        },
    )
    assert source_response.status_code == 200

    task_payloads = [
        {
            "name": "ready-probe-maintenance",
            "task_type": "source_maintenance",
            "interval_seconds": 600,
            "payload_json": {"stale_after_hours": 24.0, "source_limit": 25, "dead_letter_limit": 50},
        },
        {
            "name": "ready-probe-health",
            "task_type": "source_health_scan",
            "interval_seconds": 600,
            "payload_json": {"stale_after_hours": 24.0, "source_limit": 50},
        },
        {
            "name": "ready-probe-storage",
            "task_type": "storage_lifecycle",
            "interval_seconds": 3600,
            "payload_json": {"limit": 100},
        },
        {
            "name": "ready-probe-runtime-snapshot",
            "task_type": "runtime_snapshot_export",
            "interval_seconds": 21600,
            "payload_json": {"file_prefix": "ready-probe-runtime-snapshot"},
        },
        {
            "name": "ready-probe-entities",
            "task_type": "entity_resolution_refresh",
            "interval_seconds": 900,
            "payload_json": {"limit": 100, "min_observations": 2},
        },
        {
            "name": "ready-probe-events",
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
    for payload in task_payloads:
        task_response = client.post("/api/scheduler/tasks", json=payload)
        assert task_response.status_code == 200

    ready_response = client.get("/ready")
    assert ready_response.status_code == 503
    degraded_payload = ready_response.json()
    assert degraded_payload["status"] == "not_ready"
    assert degraded_payload["ready"] is False

    run_platform_runtime_worker(
        get_session_factory(),
        poll_seconds=0.0,
        once=True,
        max_iterations=1,
        include_stream_runtime=False,
        include_enabled_schedules=True,
        actor="test_ready_probe",
    )

    ready_response = client.get("/ready")
    assert ready_response.status_code == 200
    ready_payload = ready_response.json()
    assert ready_payload["status"] == "ok"
    assert ready_payload["ready"] is True
    assert ready_payload["overall_status"] == "ready"
    assert ready_payload["action_required_count"] == 0
