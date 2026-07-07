from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from fastapi.testclient import TestClient


@contextmanager
def flaky_scheduler_json_server(payload: list[dict[str, object]]):
    state = {"requests": 0}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            state["requests"] += 1
            if state["requests"] == 1:
                self.send_response(503)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(b"temporary failure")
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(payload).encode("utf-8"))

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/feed.json", state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_geofence_schedule_creates_alert_and_custody_log(
    client: TestClient,
    tmp_path: Path,
) -> None:
    geofence_response = client.post(
        "/api/geofences",
        json={
            "name": "Port Watch",
            "description": "Simple polygon around a port.",
            "geometry_geojson": {
                "type": "Polygon",
                "coordinates": [[[-96.0, 29.0], [-94.0, 29.0], [-94.0, 31.0], [-96.0, 31.0], [-96.0, 29.0]]],
            },
            "rule_expression": "observation enters polygon",
        },
    )
    assert geofence_response.status_code == 200
    geofence_id = geofence_response.json()["geofence_id"]

    fixture = tmp_path / "port.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Tracked vessel departure",
                    "url": "https://example.com/ship/1",
                    "lat": 29.76,
                    "lon": -95.36,
                }
            ]
        ),
        encoding="utf-8",
    )
    import_response = client.post(
        "/api/imports/local",
        json={"source_path": str(fixture), "layer_key": "marine-track"},
    )
    assert import_response.status_code == 200

    schedule_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "port-scan",
            "task_type": "geofence_scan",
            "interval_seconds": 300,
            "geofence_id": geofence_id,
        },
    )
    assert schedule_response.status_code == 200
    task_id = schedule_response.json()["task_id"]

    run_response = client.post(f"/api/scheduler/tasks/{task_id}/run")
    assert run_response.status_code == 200
    assert run_response.json()["status"] == "completed"
    assert run_response.json()["records_affected"] == 1

    alerts_response = client.get("/api/alerts")
    assert alerts_response.status_code == 200
    alerts = alerts_response.json()
    assert len(alerts) == 1
    assert alerts[0]["geofence_id"] == geofence_id
    assert "Observation" in alerts[0]["message"]
    alert_id = alerts[0]["alert_id"]

    update_response = client.patch(
        f"/api/alerts/{alert_id}",
        json={
            "status": "acknowledged",
            "severity": "warning",
            "disposition_note": "Reviewed by operator",
        },
    )
    assert update_response.status_code == 200
    assert update_response.json()["status"] == "acknowledged"
    assert update_response.json()["severity"] == "warning"
    assert update_response.json()["disposition_note"] == "Reviewed by operator"

    filtered_alerts = client.get("/api/alerts", params={"status": "acknowledged", "geofence_id": geofence_id})
    assert filtered_alerts.status_code == 200
    filtered_payload = filtered_alerts.json()
    assert len(filtered_payload) == 1
    assert filtered_payload[0]["alert_id"] == alert_id

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    custody_rows = custody_response.json()
    actions = [row["action"] for row in custody_rows]
    assert "alert_created" in actions
    assert "alert_updated" in actions
    assert "alerts_evaluated" in actions
    assert "task_run_completed" in actions
    assert any(
        row["object_type"] == "scheduled_task"
        and row["object_id"] == str(task_id)
        and row["action"] == "task_created"
        for row in custody_rows
    )
    assert any(
        row["object_type"] == "scheduled_task_run"
        and row["action"] == "task_run_started"
        for row in custody_rows
    )
    assert any(
        row["object_type"] == "scheduled_task_run"
        and row["action"] == "task_run_completed"
        for row in custody_rows
    )


def test_local_import_schedule_runs_manually(client: TestClient, tmp_path: Path) -> None:
    fixture = tmp_path / "scheduled.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Scheduled import",
                    "url": "https://example.com/source/1",
                    "lat": 30.0,
                    "lon": -95.0,
                }
            ]
        ),
        encoding="utf-8",
    )
    schedule_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "scheduled-import",
            "task_type": "local_import",
            "interval_seconds": 300,
            "target_path": str(fixture),
            "layer_key": "ops-feed",
        },
    )
    assert schedule_response.status_code == 200
    task_id = schedule_response.json()["task_id"]

    due_response = client.post("/api/scheduler/run-due")
    assert due_response.status_code == 200
    assert due_response.json()["runs_created"] == 0

    run_response = client.post(f"/api/scheduler/tasks/{task_id}/run")
    assert run_response.status_code == 200
    assert run_response.json()["records_affected"] == 1

    imports_response = client.get("/api/imports/runs")
    assert imports_response.status_code == 200
    assert imports_response.json()[0]["records_imported"] == 1

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    assert any(
        row["object_type"] == "scheduled_task"
        and row["object_id"] == str(task_id)
        and row["action"] == "task_created"
        for row in custody_response.json()
    )


def test_source_sync_schedule_skips_unchanged_payloads(client: TestClient, tmp_path: Path) -> None:
    fixture = tmp_path / "scheduled-stable.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Scheduled stable source",
                    "url": "https://stable.example.com/scheduled",
                    "lat": 29.9,
                    "lon": -95.2,
                }
            ]
        ),
        encoding="utf-8",
    )
    source_response = client.post(
        "/api/sources",
        json={
            "name": "scheduled-stable-source",
            "source_kind": "local_file",
            "layer_key": "stable-feed",
            "target_uri": str(fixture),
        },
    )
    assert source_response.status_code == 200
    source_id = source_response.json()["source_id"]

    schedule_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "scheduled-stable-task",
            "task_type": "source_sync",
            "interval_seconds": 300,
            "source_id": source_id,
        },
    )
    assert schedule_response.status_code == 200
    task_id = schedule_response.json()["task_id"]

    first_run = client.post(f"/api/scheduler/tasks/{task_id}/run")
    assert first_run.status_code == 200
    assert first_run.json()["records_affected"] == 1

    second_run = client.post(f"/api/scheduler/tasks/{task_id}/run")
    assert second_run.status_code == 200
    assert second_run.json()["records_affected"] == 0

    imports_response = client.get("/api/imports/runs")
    assert imports_response.status_code == 200
    assert len(imports_response.json()) == 1

    source_runs_response = client.get("/api/sources/runs")
    assert source_runs_response.status_code == 200
    source_runs = source_runs_response.json()
    assert source_runs[0]["status"] == "skipped"
    assert source_runs[1]["status"] == "completed"

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    assert any(
        row["object_type"] == "source_run"
        and row["action"] == "source_run_skipped"
        for row in custody_response.json()
    )


def test_source_sync_schedule_retries_transient_failure(client: TestClient) -> None:
    payload = [
        {
            "title": "Retry source record",
            "url": "https://retry.example.com/1",
            "lat": 29.88,
            "lon": -95.44,
        }
    ]
    with flaky_scheduler_json_server(payload) as (target_uri, state):
        source_response = client.post(
            "/api/sources",
            json={
                "name": "retry-http-source",
                "source_kind": "http_json",
                "layer_key": "retry-feed",
                "target_uri": target_uri,
                "metadata_json": {
                    "retry_attempts": 1,
                    "retry_backoff_seconds": 0,
                    "request_timeout_seconds": 5,
                },
            },
        )
        assert source_response.status_code == 200
        source_id = source_response.json()["source_id"]

        schedule_response = client.post(
            "/api/scheduler/tasks",
            json={
                "name": "retry-source-sync-task",
                "task_type": "source_sync",
                "interval_seconds": 300,
                "retry_attempts": 2,
                "retry_backoff_seconds": 0,
                "source_id": source_id,
            },
        )
        assert schedule_response.status_code == 200
        task_id = schedule_response.json()["task_id"]
        assert schedule_response.json()["retry_attempts"] == 2

        run_response = client.post(f"/api/scheduler/tasks/{task_id}/run")
        assert run_response.status_code == 200
        payload = run_response.json()
        assert payload["status"] == "completed"
        assert payload["records_affected"] == 1
        assert payload["output_json"]["attempt_count"] == 2
        assert payload["output_json"]["max_attempts"] == 2
        assert len(payload["output_json"]["attempt_errors"]) == 1

        assert state["requests"] == 2

        source_runs_response = client.get("/api/sources/runs")
        assert source_runs_response.status_code == 200
        source_runs = source_runs_response.json()
        assert source_runs[0]["status"] == "completed"
        assert source_runs[1]["status"] == "failed"

        custody_response = client.get("/api/custody/logs")
        assert custody_response.status_code == 200
        custody_rows = custody_response.json()
        assert any(
            row["object_type"] == "scheduled_task_run"
            and row["action"] == "task_attempt_failed"
            for row in custody_rows
        )
        assert any(
            row["object_type"] == "scheduled_task_run"
            and row["action"] == "task_retry_scheduled"
            for row in custody_rows
        )
        assert any(
            row["object_type"] == "scheduled_task_run"
            and row["action"] == "task_run_completed"
            and row["details_json"]["attempt_count"] == 2
            for row in custody_rows
        )


def test_schedule_update_can_disable_then_reenable_task(client: TestClient, tmp_path: Path) -> None:
    fixture = tmp_path / "updated-schedule.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Updated schedule import",
                    "url": "https://schedule.example.com/1",
                    "lat": 30.1,
                    "lon": -95.1,
                }
            ]
        ),
        encoding="utf-8",
    )
    schedule_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "updated-schedule",
            "task_type": "local_import",
            "interval_seconds": 300,
            "target_path": str(fixture),
            "layer_key": "ops-feed",
        },
    )
    assert schedule_response.status_code == 200
    task_id = schedule_response.json()["task_id"]
    assert schedule_response.json()["next_run_at"] is not None

    disable_response = client.patch(
        f"/api/scheduler/tasks/{task_id}",
        json={
            "enabled": False,
            "interval_seconds": 600,
            "notes": "Paused for maintenance",
        },
    )
    assert disable_response.status_code == 200
    disabled_payload = disable_response.json()
    assert disabled_payload["enabled"] is False
    assert disabled_payload["interval_seconds"] == 600
    assert disabled_payload["notes"] == "Paused for maintenance"
    assert disabled_payload["next_run_at"] is None

    due_response = client.post("/api/scheduler/run-due")
    assert due_response.status_code == 200
    assert due_response.json()["runs_created"] == 0

    enable_response = client.patch(
        f"/api/scheduler/tasks/{task_id}",
        json={
            "enabled": True,
            "retry_attempts": 2,
            "retry_backoff_seconds": 5,
            "layer_key": "retargeted-ops-feed",
        },
    )
    assert enable_response.status_code == 200
    enabled_payload = enable_response.json()
    assert enabled_payload["enabled"] is True
    assert enabled_payload["retry_attempts"] == 2
    assert enabled_payload["retry_backoff_seconds"] == 5
    assert enabled_payload["layer_key"] == "retargeted-ops-feed"
    assert enabled_payload["next_run_at"] is not None

    layers_response = client.get("/api/layers")
    assert layers_response.status_code == 200
    assert any(layer["key"] == "retargeted-ops-feed" for layer in layers_response.json())

    run_response = client.post(f"/api/scheduler/tasks/{task_id}/run")
    assert run_response.status_code == 200
    assert run_response.json()["records_affected"] == 1

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    assert any(
        row["object_type"] == "scheduled_task"
        and row["object_id"] == str(task_id)
        and row["action"] == "task_updated"
        for row in custody_response.json()
    )
