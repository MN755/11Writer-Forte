from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient


def test_camera_inventory_materialization_and_update(client: TestClient, tmp_path: Path) -> None:
    initial_fixture = tmp_path / "mndot-cameras-initial.json"
    initial_fixture.write_text(
        json.dumps(
            [
                {
                    "camera_id": "mndot-i35w-001",
                    "camera_name": "I-35W @ Lake St",
                    "road_name": "I-35W",
                    "status": "online",
                    "image_url": "https://cams.example.com/i35w-001.jpg",
                    "stream_url": "https://cams.example.com/i35w-001.m3u8",
                    "page_url": "https://511mn.org/camera/1",
                    "provider": "MnDOT",
                    "lat": 44.9482,
                    "lon": -93.2701,
                    "observed_at": "2026-07-07T01:00:00Z",
                },
                {
                    "camera_id": "mndot-i94-002",
                    "camera_name": "I-94 @ Snelling",
                    "road_name": "I-94",
                    "status": "offline",
                    "image_url": "https://cams.example.com/i94-002.jpg",
                    "page_url": "https://511mn.org/camera/2",
                    "provider": "MnDOT",
                    "lat": 44.9604,
                    "lon": -93.1676,
                    "observed_at": "2026-07-07T01:00:00Z",
                },
            ]
        ),
        encoding="utf-8",
    )

    import_response = client.post(
        "/api/imports/local",
        json={"source_path": str(initial_fixture), "layer_key": "traffic-camera-feed"},
    )
    assert import_response.status_code == 200

    materialize_response = client.post(
        "/api/cameras/materialize",
        json={"layer_key": "traffic-camera-feed", "limit": 50},
    )
    assert materialize_response.status_code == 200
    materialized = materialize_response.json()
    assert materialized["created_count"] == 2
    assert materialized["updated_count"] == 0
    assert materialized["scanned_count"] == 2
    assert {camera["external_id"] for camera in materialized["cameras"]} == {
        "mndot-i35w-001",
        "mndot-i94-002",
    }

    cameras_response = client.get(
        "/api/cameras",
        params={"layer_key": "traffic-camera-feed", "active": "true"},
    )
    assert cameras_response.status_code == 200
    active_cameras = cameras_response.json()
    assert len(active_cameras) == 1
    assert active_cameras[0]["external_id"] == "mndot-i35w-001"
    assert active_cameras[0]["provider"] == "MnDOT"

    bbox_response = client.get(
        "/api/cameras",
        params={
            "min_lon": -93.28,
            "min_lat": 44.94,
            "max_lon": -93.26,
            "max_lat": 44.95,
        },
    )
    assert bbox_response.status_code == 200
    bbox_cameras = bbox_response.json()
    assert len(bbox_cameras) == 1
    assert bbox_cameras[0]["external_id"] == "mndot-i35w-001"

    updated_fixture = tmp_path / "mndot-cameras-updated.json"
    updated_fixture.write_text(
        json.dumps(
            [
                {
                    "camera_id": "mndot-i35w-001",
                    "camera_name": "I-35W @ Lake St",
                    "road_name": "I-35W",
                    "status": "offline",
                    "image_url": "https://cams.example.com/i35w-001.jpg",
                    "stream_url": "https://cams.example.com/i35w-001.m3u8",
                    "page_url": "https://511mn.org/camera/1",
                    "provider": "MnDOT",
                    "lat": 44.9482,
                    "lon": -93.2701,
                    "observed_at": "2026-07-07T02:00:00Z",
                }
            ]
        ),
        encoding="utf-8",
    )

    second_import = client.post(
        "/api/imports/local",
        json={"source_path": str(updated_fixture), "layer_key": "traffic-camera-feed"},
    )
    assert second_import.status_code == 200

    second_materialization = client.post(
        "/api/cameras/materialize",
        json={"layer_key": "traffic-camera-feed", "limit": 50},
    )
    assert second_materialization.status_code == 200
    second_payload = second_materialization.json()
    assert second_payload["created_count"] == 0
    assert second_payload["updated_count"] == 1

    schedule_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "camera-refresh-ops",
            "task_type": "camera_inventory_refresh",
            "interval_seconds": 300,
            "layer_key": "traffic-camera-feed",
            "payload_json": {"limit": 50, "source_domain": "cams.example.com"},
        },
    )
    assert schedule_response.status_code == 200

    status_response = client.get(
        "/api/cameras",
        params={"source_domain": "cams.example.com", "status": "offline"},
    )
    assert status_response.status_code == 200
    offline_cameras = status_response.json()
    assert any(camera["external_id"] == "mndot-i35w-001" and camera["active"] is False for camera in offline_cameras)

    summary_response = client.get(
        "/api/cameras/summary",
        params={"layer_key": "traffic-camera-feed", "stale_after_hours": 0},
    )
    assert summary_response.status_code == 200
    summary = summary_response.json()
    assert summary["total_count"] == 2
    assert summary["active_count"] == 0
    assert summary["inactive_count"] == 2
    assert summary["stale_count"] == 2
    assert summary["layer_counts"][0]["key"] == "traffic-camera-feed"
    assert summary["provider_counts"][0]["key"] == "MnDOT"

    camera_id = next(
        camera["camera_inventory_id"]
        for camera in offline_cameras
        if camera["external_id"] == "mndot-i35w-001"
    )
    ops_response = client.get(f"/api/cameras/{camera_id}/ops")
    assert ops_response.status_code == 200
    ops_payload = ops_response.json()
    assert ops_payload["camera"]["camera_inventory_id"] == camera_id
    assert ops_payload["latest_observation"]["observation_id"] == second_payload["cameras"][0]["observation_id"]
    assert ops_payload["latest_import_run"]["status"] == "completed"
    assert ops_payload["refresh_tasks"][0]["task_type"] == "camera_inventory_refresh"
    assert any(log["action"] == "camera_updated" for log in ops_payload["custody_logs"])

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    custody_rows = custody_response.json()
    assert any(row["object_type"] == "camera_inventory" and row["action"] == "camera_registered" for row in custody_rows)
    assert any(row["object_type"] == "camera_inventory" and row["action"] == "camera_updated" for row in custody_rows)
    assert any(
        row["object_type"] == "camera_inventory_materialization"
        and row["action"] == "camera_materialization_completed"
        for row in custody_rows
    )
