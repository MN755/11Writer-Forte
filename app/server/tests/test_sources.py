from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient


def test_source_definition_run_creates_import_and_history(client: TestClient, tmp_path: Path) -> None:
    fixture = tmp_path / "source-file.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Source catalog record",
                    "url": "https://source.example.com/1",
                    "lat": 29.76,
                    "lon": -95.36,
                }
            ]
        ),
        encoding="utf-8",
    )

    source_response = client.post(
        "/api/sources",
        json={
            "name": "harbor-source",
            "source_kind": "local_file",
            "layer_key": "marine-track",
            "target_uri": str(fixture),
            "integrity_source": True,
        },
    )
    assert source_response.status_code == 200
    source_id = source_response.json()["source_id"]

    run_response = client.post(f"/api/sources/{source_id}/run")
    assert run_response.status_code == 200
    payload = run_response.json()
    assert payload["status"] == "completed"
    assert payload["records_imported"] == 1
    assert payload["import_run_id"] is not None

    runs_response = client.get("/api/sources/runs")
    assert runs_response.status_code == 200
    assert runs_response.json()[0]["source_id"] == source_id

    imports_response = client.get("/api/imports/runs")
    assert imports_response.status_code == 200
    assert imports_response.json()[0]["records_imported"] == 1


def test_source_sync_schedule_runs_source_definition(client: TestClient, tmp_path: Path) -> None:
    fixture = tmp_path / "scheduled-source.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Scheduled source sync",
                    "url": "https://sync.example.com/1",
                    "lat": 30.0,
                    "lon": -95.0,
                }
            ]
        ),
        encoding="utf-8",
    )

    source_response = client.post(
        "/api/sources",
        json={
            "name": "scheduled-source",
            "source_kind": "local_file",
            "layer_key": "ops-feed",
            "target_uri": str(fixture),
        },
    )
    assert source_response.status_code == 200
    source_id = source_response.json()["source_id"]

    schedule_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "source-sync-task",
            "task_type": "source_sync",
            "interval_seconds": 300,
            "source_id": source_id,
        },
    )
    assert schedule_response.status_code == 200
    task_id = schedule_response.json()["task_id"]

    run_response = client.post(f"/api/scheduler/tasks/{task_id}/run")
    assert run_response.status_code == 200
    assert run_response.json()["records_affected"] == 1

    source_runs = client.get("/api/sources/runs")
    assert source_runs.status_code == 200
    assert source_runs.json()[0]["records_imported"] == 1
