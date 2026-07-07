from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient


def seed_runtime_state(client: TestClient, tmp_path: Path) -> None:
    fixture = tmp_path / "snapshot-source.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Snapshot source record",
                    "url": "https://snapshot.example.com/1",
                    "lat": 29.76,
                    "lon": -95.36,
                    "vessel_name": "MV Snapshot",
                    "mmsi": "102938475",
                }
            ]
        ),
        encoding="utf-8",
    )
    corroboration = tmp_path / "snapshot-source-2.json"
    corroboration.write_text(
        json.dumps(
            [
                {
                    "title": "Snapshot corroboration",
                    "url": "https://snapshot.example.com/2",
                    "lat": 29.77,
                    "lon": -95.35,
                    "vessel_name": "MV Snapshot",
                    "mmsi": "102938475",
                }
            ]
        ),
        encoding="utf-8",
    )

    trust_response = client.post(
        "/api/source-trust/profiles",
        json={
            "domain": "snapshot.example.com",
            "trust_level": "trusted",
            "approval_policy": "auto_approve_stable",
            "integrity_source": True,
            "notes": "snapshot seed",
        },
    )
    assert trust_response.status_code == 200

    source_response = client.post(
        "/api/sources",
        json={
            "name": "snapshot-source",
            "source_kind": "local_file",
            "layer_key": "marine-track",
            "target_uri": str(fixture),
            "metadata_json": {"skip_unchanged": True},
        },
    )
    assert source_response.status_code == 200
    source_id = source_response.json()["source_id"]

    source_run = client.post(f"/api/sources/{source_id}/run")
    assert source_run.status_code == 200

    direct_import = client.post(
        "/api/imports/local",
        json={"source_path": str(corroboration), "layer_key": "news-track"},
    )
    assert direct_import.status_code == 200

    camera_fixture = tmp_path / "snapshot-cameras.json"
    camera_fixture.write_text(
        json.dumps(
            [
                {
                    "camera_id": "snapshot-cam-1",
                    "camera_name": "Snapshot Camera",
                    "road_name": "I-35W",
                    "status": "online",
                    "image_url": "https://cams.snapshot.example.com/cam-1.jpg",
                    "page_url": "https://511mn.org/camera/snapshot-1",
                    "provider": "MnDOT",
                    "lat": 44.9482,
                    "lon": -93.2701,
                    "observed_at": "2026-07-07T01:00:00Z",
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
    camera_materialization = client.post(
        "/api/cameras/materialize",
        json={"layer_key": "traffic-camera-feed", "limit": 25},
    )
    assert camera_materialization.status_code == 200

    geofence_response = client.post(
        "/api/geofences",
        json={
            "name": "Snapshot Watch",
            "geometry_geojson": {
                "type": "Polygon",
                "coordinates": [[[-96.0, 29.0], [-94.0, 29.0], [-94.0, 31.0], [-96.0, 31.0], [-96.0, 29.0]]],
            },
            "rule_expression": "snapshot watch",
        },
    )
    assert geofence_response.status_code == 200
    geofence_id = geofence_response.json()["geofence_id"]

    schedule_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "snapshot-watch-task",
            "task_type": "geofence_scan",
            "interval_seconds": 300,
            "geofence_id": geofence_id,
        },
    )
    assert schedule_response.status_code == 200
    task_id = schedule_response.json()["task_id"]
    task_run = client.post(f"/api/scheduler/tasks/{task_id}/run")
    assert task_run.status_code == 200

    entity_resolution = client.post(
        "/api/entities/resolve",
        json={
            "min_lon": -96.0,
            "min_lat": 29.0,
            "max_lon": -94.0,
            "max_lat": 31.0,
            "min_observations": 2,
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
        },
    )
    assert fused.status_code == 200


def test_runtime_snapshot_export_and_restore_round_trip(client: TestClient, tmp_path: Path) -> None:
    seed_runtime_state(client, tmp_path)

    export_response = client.get("/api/operations/runtime/export")
    assert export_response.status_code == 200
    snapshot = export_response.json()
    assert snapshot["source_trust_profiles"]
    assert snapshot["source_definitions"]
    assert snapshot["local_import_runs"]
    assert snapshot["observations"]
    assert snapshot["events"]
    assert snapshot["entities"]
    assert snapshot["camera_inventory"]
    assert snapshot["camera_source_inventory"]
    assert snapshot["storage_objects"]
    assert snapshot["scheduled_tasks"]
    assert snapshot["scheduled_task_runs"]
    assert snapshot["source_runs"]
    assert snapshot["situation_products"]
    assert any(log["action"] == "runtime_exported" for log in snapshot["custody_logs"])

    conflict_response = client.post("/api/operations/runtime/restore", json=snapshot)
    assert conflict_response.status_code == 409
    assert "empty database" in conflict_response.json()["detail"]

    replace_response = client.post(
        "/api/operations/runtime/restore",
        params={"replace_existing": "true"},
        json=snapshot,
    )
    assert replace_response.status_code == 200
    restored = replace_response.json()
    assert restored["replaced_existing"] is True
    row_counts = {item["table_name"]: item["row_count"] for item in restored["row_counts"]}
    assert row_counts["observations"] >= 2
    assert row_counts["events"] >= 1
    assert row_counts["entities"] >= 1
    assert row_counts["camera_inventory"] >= 1
    assert row_counts["camera_source_inventory"] >= 1
    assert row_counts["storage_objects"] >= 1
    assert row_counts["scheduled_tasks"] >= 1
    assert row_counts["source_definitions"] >= 1
    assert row_counts["source_runs"] >= 1
    assert row_counts["custody_logs"] >= len(snapshot["custody_logs"])

    verify_response = client.get("/api/operations/runtime/export")
    assert verify_response.status_code == 200
    restored_snapshot = verify_response.json()
    assert len(restored_snapshot["observations"]) >= len(snapshot["observations"])
    assert len(restored_snapshot["events"]) >= len(snapshot["events"])
    assert len(restored_snapshot["entities"]) >= len(snapshot["entities"])
    assert len(restored_snapshot["camera_inventory"]) >= len(snapshot["camera_inventory"])
    assert len(restored_snapshot["camera_source_inventory"]) >= len(snapshot["camera_source_inventory"])
    assert len(restored_snapshot["storage_objects"]) >= len(snapshot["storage_objects"])
    assert any(log["action"] == "runtime_restored" for log in restored_snapshot["custody_logs"])
