from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient


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

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    custody_rows = custody_response.json()
    actions = [row["action"] for row in custody_rows]
    assert "alert_created" in actions
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
