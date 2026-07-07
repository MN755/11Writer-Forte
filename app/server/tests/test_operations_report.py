from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient


def test_operations_report_summarizes_runtime_activity(client: TestClient, tmp_path: Path) -> None:
    source_fixture = tmp_path / "ops-source.json"
    source_fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Ops source record",
                    "url": "https://ops-source.example.com/1",
                    "lat": 29.76,
                    "lon": -95.36,
                }
            ]
        ),
        encoding="utf-8",
    )
    direct_fixture = tmp_path / "ops-direct.json"
    direct_fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Ops direct record",
                    "url": "https://ops-direct.example.com/1",
                    "lat": 29.77,
                    "lon": -95.35,
                }
            ]
        ),
        encoding="utf-8",
    )

    source_response = client.post(
        "/api/sources",
        json={
            "name": "ops-source",
            "source_kind": "local_file",
            "layer_key": "marine-track",
            "target_uri": str(source_fixture),
        },
    )
    assert source_response.status_code == 200
    source_id = source_response.json()["source_id"]

    source_run_response = client.post(f"/api/sources/{source_id}/run")
    assert source_run_response.status_code == 200

    direct_import_response = client.post(
        "/api/imports/local",
        json={"source_path": str(direct_fixture), "layer_key": "marine-track"},
    )
    assert direct_import_response.status_code == 200

    geofence_response = client.post(
        "/api/geofences",
        json={
            "name": "Ops Watch",
            "geometry_geojson": {
                "type": "Polygon",
                "coordinates": [[[-96.0, 29.0], [-94.0, 29.0], [-94.0, 31.0], [-96.0, 31.0], [-96.0, 29.0]]],
            },
            "rule_expression": "ops watch",
        },
    )
    assert geofence_response.status_code == 200
    geofence_id = geofence_response.json()["geofence_id"]

    schedule_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "ops-watch-task",
            "task_type": "geofence_scan",
            "interval_seconds": 300,
            "geofence_id": geofence_id,
        },
    )
    assert schedule_response.status_code == 200
    task_id = schedule_response.json()["task_id"]

    run_response = client.post(f"/api/scheduler/tasks/{task_id}/run")
    assert run_response.status_code == 200

    alert_update = client.patch(
        "/api/alerts/1",
        json={
            "status": "acknowledged",
            "disposition_note": "Ops reviewed",
        },
    )
    assert alert_update.status_code == 200

    report_response = client.get("/api/operations/report", params={"limit": 5})
    assert report_response.status_code == 200
    payload = report_response.json()
    summary = payload["summary"]
    assert summary["import_run_count"] == 2
    assert summary["imported_record_count"] == 2
    assert summary["source_run_count"] == 1
    assert summary["scheduled_task_run_count"] == 1
    assert summary["alert_count"] == 2
    assert summary["acknowledged_alert_count"] == 1
    assert len(payload["import_runs"]) == 2
    assert len(payload["source_runs"]) == 1
    assert len(payload["scheduled_task_runs"]) == 1
    assert len(payload["alerts"]) == 2
    assert payload["alerts"][0]["status"] in {"open", "acknowledged"}
    assert payload["custody_logs"]
    assert payload["storage_report"]["total_count"] >= 2
    assert payload["storage_report"]["active_count"] >= 2
    assert payload["clickhouse_diagnostics"]["status"] == "disabled"
    assert payload["clickhouse_diagnostics"]["enabled"] is False
    assert payload["clickhouse_diagnostics"]["storage_mode"] == "archive_only"
    assert payload["source_inventory_summary"]["total_count"] == 1
    assert payload["source_inventory_summary"]["scheduled_count"] == 0
    assert payload["source_report_index"]["sync_task_count"] == 0
    assert payload["source_report_index"]["recent_runs"][0]["source_id"] == source_id
    assert payload["camera_inventory_summary"]["total_count"] == 0
    assert payload["camera_report_index"]["refresh_task_count"] == 0

    camera_fixture = tmp_path / "ops-cameras.json"
    camera_fixture.write_text(
        json.dumps(
            [
                {
                    "camera_id": "ops-cam-1",
                    "camera_name": "Ops Cam 1",
                    "status": "offline",
                    "image_url": "https://cams.example.com/ops-cam-1.jpg",
                    "provider": "OpsDOT",
                    "lat": 29.78,
                    "lon": -95.34,
                    "observed_at": "2026-07-07T00:00:00Z",
                }
            ]
        ),
        encoding="utf-8",
    )
    camera_import = client.post(
        "/api/imports/local",
        json={"source_path": str(camera_fixture), "layer_key": "traffic-camera-feed"},
    )
    assert camera_import.status_code == 200
    camera_materialize = client.post(
        "/api/cameras/materialize",
        json={"layer_key": "traffic-camera-feed", "limit": 25},
    )
    assert camera_materialize.status_code == 200
    camera_schedule = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "ops-camera-refresh",
            "task_type": "camera_inventory_refresh",
            "interval_seconds": 300,
            "layer_key": "traffic-camera-feed",
            "payload_json": {"limit": 25, "source_domain": "cams.example.com"},
        },
    )
    assert camera_schedule.status_code == 200

    camera_report_response = client.get("/api/operations/report", params={"limit": 5})
    assert camera_report_response.status_code == 200
    camera_payload = camera_report_response.json()
    assert camera_payload["storage_report"]["total_count"] >= 5
    assert any(
        bucket["key"] == "operational" for bucket in camera_payload["storage_report"]["retention_class_counts"]
    )
    assert camera_payload["source_inventory_summary"]["total_count"] == 1
    assert camera_payload["camera_inventory_summary"]["total_count"] == 1
    assert camera_payload["camera_inventory_summary"]["inactive_count"] == 1
    assert camera_payload["camera_report_index"]["refresh_task_count"] == 1
    assert camera_payload["camera_report_index"]["recent_materializations"][0]["action"] == "camera_materialization_completed"
