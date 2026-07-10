from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient


def test_api_summary_export_artifact_routes_register_storage_objects(
    client: TestClient,
    tmp_path: Path,
) -> None:
    source_fixture = tmp_path / "api-export-source.json"
    source_fixture.write_text(
        json.dumps(
            [
                {
                    "title": "API export source",
                    "url": "https://api-export.example.com/1",
                    "lat": 29.76,
                    "lon": -95.36,
                    "vessel_name": "MV API Export",
                    "mmsi": "321654987",
                }
            ]
        ),
        encoding="utf-8",
    )
    corroboration_fixture = tmp_path / "api-export-corroboration.json"
    corroboration_fixture.write_text(
        json.dumps(
            [
                {
                    "title": "API export corroboration",
                    "url": "https://api-export.example.com/2",
                    "lat": 29.77,
                    "lon": -95.35,
                    "vessel_name": "MV API Export",
                    "mmsi": "321654987",
                }
            ]
        ),
        encoding="utf-8",
    )
    camera_fixture = tmp_path / "api-export-cameras.json"
    camera_fixture.write_text(
        json.dumps(
            [
                {
                    "camera_id": "api-export-cam-1",
                    "camera_name": "API Export Camera",
                    "provider": "MnDOT",
                    "road_name": "I-35W",
                    "status": "online",
                    "image_url": "https://cams.api-export.example.com/cam-1.jpg",
                    "page_url": "https://511mn.org/camera/api-export-1",
                    "lat": 44.9482,
                    "lon": -93.2701,
                    "observed_at": "2026-07-07T01:00:00Z",
                }
            ]
        ),
        encoding="utf-8",
    )

    assert client.post(
        "/api/imports/local",
        json={"source_path": str(source_fixture), "layer_key": "marine-track"},
    ).status_code == 200
    assert client.post(
        "/api/imports/local",
        json={"source_path": str(corroboration_fixture), "layer_key": "news-track"},
    ).status_code == 200
    assert client.post(
        "/api/imports/local",
        json={"source_path": str(camera_fixture), "layer_key": "traffic-camera-feed"},
    ).status_code == 200
    source_definition = client.post(
        "/api/sources",
        json={
            "name": "api-export-source-def",
            "source_kind": "local_file",
            "layer_key": "marine-track",
            "target_uri": str(source_fixture),
        },
    )
    assert source_definition.status_code == 200
    source_id = source_definition.json()["source_id"]

    assert client.post(
        "/api/cameras/materialize",
        json={"layer_key": "traffic-camera-feed", "limit": 25},
    ).status_code == 200
    entity_resolution = client.post(
        "/api/entities/resolve",
        json={
            "min_lon": -96.0,
            "min_lat": 29.0,
            "max_lon": -94.0,
            "max_lat": 31.0,
            "min_observations": 2,
            "entity_type": "vessel",
        },
    )
    assert entity_resolution.status_code == 200
    fused = client.post(
        "/api/events/fuse",
        json={
            "min_lon": -96.0,
            "min_lat": 29.0,
            "max_lon": -94.0,
            "max_lat": 31.0,
            "distance_km": 10,
            "time_window_minutes": 120,
            "redaction_level": "public",
        },
    )
    assert fused.status_code == 200
    event_id = fused.json()["event_results"][0]["event_id"]
    alert_response = client.post(
        "/api/alerts",
        json={
            "event_id": event_id,
            "severity": "warning",
            "status": "open",
            "message": "API export alert",
        },
    )
    assert alert_response.status_code == 200

    assert client.post(
        "/api/scheduler/tasks",
        json={
            "name": "api-export-source-sync",
            "task_type": "source_sync",
            "interval_seconds": 300,
            "source_id": source_id,
        },
    ).status_code == 200
    assert client.post(
        "/api/scheduler/tasks",
        json={
            "name": "api-export-camera-refresh",
            "task_type": "camera_inventory_refresh",
            "interval_seconds": 300,
            "layer_key": "traffic-camera-feed",
            "payload_json": {"limit": 25},
        },
    ).status_code == 200

    exports_dir = tmp_path / "exports"
    requests = [
        (
            "/api/sources/export/summary/artifact",
            {
                "output_path": str(exports_dir / "source-summary.json"),
                "source_limit": 50,
                "report_limit": 25,
                "stale_source_limit": 25,
            },
            "source_summary_export",
            "source_export",
            "scoped",
        ),
        (
            "/api/cameras/export/summary/artifact",
            {
                "output_path": str(exports_dir / "camera-summary.json"),
                "layer_key": "traffic-camera-feed",
                "camera_limit": 50,
                "report_limit": 25,
                "stale_camera_limit": 25,
            },
            "camera_summary_export",
            "camera_export",
            "traffic-camera-feed",
        ),
        (
            "/api/camera-sources/export/summary/artifact",
            {
                "output_path": str(exports_dir / "camera-source-summary.json"),
                "layer_key": "traffic-camera-feed",
                "source_limit": 50,
                "report_limit": 25,
                "stale_source_limit": 25,
            },
            "camera_source_summary_export",
            "camera_source_export",
            "traffic-camera-feed",
        ),
        (
            "/api/events/export/summary/artifact",
            {
                "output_path": str(exports_dir / "event-summary.json"),
                "status": "open",
                "event_limit": 50,
                "report_limit": 25,
                "stale_event_limit": 25,
            },
            "event_summary_export",
            "event_export",
            "open",
        ),
        (
            "/api/entities/export/summary/artifact",
            {
                "output_path": str(exports_dir / "entity-summary.json"),
                "entity_type": "vessel",
                "entity_limit": 50,
                "report_limit": 25,
                "conflict_limit": 25,
            },
            "entity_summary_export",
            "entity_export",
            "vessel",
        ),
        (
            "/api/alerts/export/summary/artifact",
            {
                "output_path": str(exports_dir / "alert-summary.json"),
                "event_id": event_id,
                "alert_limit": 50,
                "report_limit": 25,
                "stale_alert_limit": 25,
            },
            "alert_summary_export",
            "alert_export",
            str(event_id),
        ),
        (
            "/api/scheduler/export/summary/artifact",
            {
                "output_path": str(exports_dir / "scheduler-summary.json"),
                "task_limit": 50,
                "report_limit": 25,
                "overdue_task_limit": 25,
            },
            "scheduler_summary_export",
            "scheduler_export",
            "scoped",
        ),
    ]

    for route, payload, object_kind, owner_type, owner_id in requests:
        response = client.post(route, json=payload)
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["storage_object"]["object_kind"] == object_kind
        assert body["storage_object"]["owner_type"] == owner_type
        assert body["storage_object"]["owner_id"] == owner_id
        assert body["output_path"] == str(Path(payload["output_path"]).resolve())
        assert Path(payload["output_path"]).exists()

        storage_rows = client.get(
            "/api/storage/objects",
            params={"owner_type": owner_type, "owner_id": owner_id, "object_kind": object_kind},
        )
        assert storage_rows.status_code == 200
        assert len(storage_rows.json()) == 1
