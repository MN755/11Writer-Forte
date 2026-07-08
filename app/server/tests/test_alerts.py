from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from src.db import get_session_factory
from src.models import AlertORM
from src.services.scheduler_service import scheduler_now


def test_alert_summary_report_and_export_surfaces(client: TestClient, tmp_path: Path) -> None:
    geofence_response = client.post(
        "/api/geofences",
        json={
            "name": "Alert Ops Watch",
            "geometry_geojson": {
                "type": "Polygon",
                "coordinates": [[[-96.0, 29.0], [-94.0, 29.0], [-94.0, 31.0], [-96.0, 31.0], [-96.0, 29.0]]],
            },
            "rule_expression": "alert ops watch",
        },
    )
    assert geofence_response.status_code == 200
    geofence_id = geofence_response.json()["geofence_id"]

    fixture = tmp_path / "alert-ops-source.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Alert ops source",
                    "url": "https://alert-ops.example.com/1",
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
            "name": "alert-ops-scan",
            "task_type": "geofence_scan",
            "interval_seconds": 300,
            "geofence_id": geofence_id,
        },
    )
    assert schedule_response.status_code == 200
    task_id = schedule_response.json()["task_id"]

    run_response = client.post(f"/api/scheduler/tasks/{task_id}/run")
    assert run_response.status_code == 200

    create_manual = client.post(
        "/api/alerts",
        json={
            "event_id": 99,
            "severity": "warning",
            "status": "open",
            "message": "Manual alert",
        },
    )
    assert create_manual.status_code == 200
    manual_alert_id = create_manual.json()["alert_id"]

    session = get_session_factory()()
    try:
        manual_alert = session.get(AlertORM, manual_alert_id)
        assert manual_alert is not None
        manual_alert.created_at = scheduler_now() - timedelta(days=2)
        session.commit()
    finally:
        session.close()

    update_response = client.patch(
        "/api/alerts/1",
        json={
            "status": "acknowledged",
            "severity": "info",
            "disposition_note": "Reviewed",
        },
    )
    assert update_response.status_code == 200

    summary_response = client.get("/api/alerts/summary", params={"stale_after_hours": 24})
    assert summary_response.status_code == 200
    summary = summary_response.json()
    assert summary["total_count"] == 2
    assert summary["open_count"] == 1
    assert summary["acknowledged_count"] == 1
    assert summary["closed_count"] == 0
    assert summary["stale_open_count"] == 1
    assert summary["geofence_scoped_count"] == 1
    assert summary["event_scoped_count"] == 1
    assert any(bucket["key"] == "warning" for bucket in summary["severity_counts"])

    report_response = client.get(
        "/api/alerts/report-index",
        params={"stale_after_hours": 24, "limit": 10, "stale_alert_limit": 10},
    )
    assert report_response.status_code == 200
    report = report_response.json()
    assert report["geofence_scan_task_count"] == 1
    assert report["geofence_scan_run_count"] == 1
    assert report["geofence_scan_failure_count"] == 0
    assert report["inventory_summary"]["total_count"] == 2
    assert any(row["alert_id"] == manual_alert_id for row in report["stale_open_alerts"])
    assert report["unscoped_alerts"] == []

    export_response = client.get(
        "/api/alerts/export/summary",
        params={"alert_limit": 10, "report_limit": 10, "stale_alert_limit": 10},
    )
    assert export_response.status_code == 200
    export_payload = export_response.json()
    assert export_payload["filters_json"]["alert_limit"] == 10
    assert export_payload["report_index"]["inventory_summary"]["total_count"] == 2
    assert len(export_payload["alerts"]) == 2
