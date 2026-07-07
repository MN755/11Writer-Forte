from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from src.config import reset_settings_cache
from src.services import clickhouse_service


class FakeClickHouseResponse:
    def __init__(self, payload: str) -> None:
        self.payload = payload

    def read(self) -> bytes:
        return self.payload.encode("utf-8")

    def __enter__(self) -> "FakeClickHouseResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def configure_fake_clickhouse(monkeypatch, *, row_count: int = 1) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_ENABLED", "true")
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_URL", "http://clickhouse.test:8123")
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_DATABASE", "elevenwriter")
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_USER", "forte")
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_PASSWORD", "secret")
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_R2_ENDPOINT", "https://acct.r2.cloudflarestorage.com")
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_R2_BUCKET", "11writer-archive")
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_R2_ACCESS_KEY_ID", "r2-key")
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_R2_SECRET_ACCESS_KEY", "r2-secret")
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_R2_ARCHIVE_PREFIX", "forte-archive")
    reset_settings_cache()

    def fake_urlopen(request, timeout=0):  # type: ignore[no-untyped-def]
        body = request.data.decode("utf-8") if request.data else ""
        if request.full_url.endswith("/ping"):
            return FakeClickHouseResponse("Ok.\n")
        if "SELECT version()" in body:
            return FakeClickHouseResponse('{"version":"26.6.1","current_database":"elevenwriter"}\n')
        if "SELECT count(*) AS row_count" in body:
            return FakeClickHouseResponse(f'{{"row_count":{row_count}}}\n')
        return FakeClickHouseResponse("")

    monkeypatch.setattr(clickhouse_service, "urlopen", fake_urlopen)


def restore_default_clickhouse_settings() -> None:
    reset_settings_cache()


def seed_runtime_state(client: TestClient, tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
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

    source_sync_schedule = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "snapshot-source-sync-task",
            "task_type": "source_sync",
            "interval_seconds": 300,
            "source_id": source_id,
        },
    )
    assert source_sync_schedule.status_code == 200
    source_sync_task_id = source_sync_schedule.json()["task_id"]
    source_sync_run = client.post(f"/api/scheduler/tasks/{source_sync_task_id}/run")
    assert source_sync_run.status_code == 200

    expired_at = datetime.now(timezone.utc) - timedelta(hours=2)
    storage_object = client.post(
        "/api/storage/objects",
        json={
            "object_key": "snapshot:expired:1",
            "object_kind": "raw_payload",
            "owner_type": "source_run",
            "owner_id": "snapshot-expired",
            "object_uri": "file:///tmp/snapshot-expired.json",
            "storage_tier": "warm",
            "retention_class": "operational",
            "lifecycle_status": "active",
            "expires_at": expired_at.isoformat(),
        },
    )
    assert storage_object.status_code == 200

    storage_schedule = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "snapshot-storage-lifecycle-task",
            "task_type": "storage_lifecycle",
            "interval_seconds": 300,
            "payload_json": {"retention_class": "operational", "limit": 25},
        },
    )
    assert storage_schedule.status_code == 200
    storage_task_id = storage_schedule.json()["task_id"]
    storage_run = client.post(f"/api/scheduler/tasks/{storage_task_id}/run")
    assert storage_run.status_code == 200

    camera_schedule = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "snapshot-camera-refresh-task",
            "task_type": "camera_inventory_refresh",
            "interval_seconds": 300,
            "layer_key": "traffic-camera-feed",
            "payload_json": {"limit": 25, "source_domain": "cams.snapshot.example.com"},
        },
    )
    assert camera_schedule.status_code == 200

    configure_fake_clickhouse(monkeypatch)
    clickhouse_sync_schedule = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "snapshot-clickhouse-sync-task",
            "task_type": "clickhouse_sync",
            "interval_seconds": 300,
            "layer_key": "marine-track",
            "payload_json": {"limit": 10},
        },
    )
    assert clickhouse_sync_schedule.status_code == 200
    clickhouse_sync_task_id = clickhouse_sync_schedule.json()["task_id"]
    clickhouse_sync_run = client.post(f"/api/scheduler/tasks/{clickhouse_sync_task_id}/run")
    assert clickhouse_sync_run.status_code == 200

    clickhouse_archive_schedule = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "snapshot-clickhouse-archive-task",
            "task_type": "clickhouse_archive",
            "interval_seconds": 300,
            "layer_key": "marine-track",
            "payload_json": {"limit": 25},
        },
    )
    assert clickhouse_archive_schedule.status_code == 200
    clickhouse_archive_task_id = clickhouse_archive_schedule.json()["task_id"]
    clickhouse_archive_run = client.post(f"/api/scheduler/tasks/{clickhouse_archive_task_id}/run")
    assert clickhouse_archive_run.status_code == 200

    entity_schedule = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "snapshot-entity-refresh-task",
            "task_type": "entity_resolution_refresh",
            "interval_seconds": 300,
            "payload_json": {
                "min_lon": -96.0,
                "min_lat": 29.0,
                "max_lon": -94.0,
                "max_lat": 31.0,
                "min_observations": 2,
            },
        },
    )
    assert entity_schedule.status_code == 200

    event_schedule = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "snapshot-event-fusion-task",
            "task_type": "event_fusion_refresh",
            "interval_seconds": 300,
            "payload_json": {
                "min_lon": -96.0,
                "min_lat": 29.0,
                "max_lon": -94.0,
                "max_lat": 31.0,
                "distance_km": 10,
                "time_window_minutes": 120,
            },
        },
    )
    assert event_schedule.status_code == 200

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


def test_runtime_snapshot_export_and_restore_round_trip(client: TestClient, tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    seed_runtime_state(client, tmp_path, monkeypatch)

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
    task_types = {row["task_type"] for row in snapshot["scheduled_tasks"]}
    assert {"geofence_scan", "source_sync", "storage_lifecycle", "camera_inventory_refresh"}.issubset(task_types)
    assert {"clickhouse_sync", "clickhouse_archive", "entity_resolution_refresh", "event_fusion_refresh"}.issubset(
        task_types
    )
    assert any(run["output_json"].get("clickhouse_database") == "elevenwriter" for run in snapshot["scheduled_task_runs"])
    assert any(log["action"] == "runtime_exported" for log in snapshot["custody_logs"])
    assert any(log["action"] == "clickhouse_synced" for log in snapshot["custody_logs"])
    assert any(log["action"] == "clickhouse_archived_to_r2" for log in snapshot["custody_logs"])

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
    assert row_counts["scheduled_tasks"] >= 7
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
    restored_task_types = {row["task_type"] for row in restored_snapshot["scheduled_tasks"]}
    assert task_types.issubset(restored_task_types)
    restored_scheduler_report = client.get("/api/scheduler/report-index", params={"limit": 25, "overdue_task_limit": 25})
    assert restored_scheduler_report.status_code == 200
    report_payload = restored_scheduler_report.json()
    assert report_payload["inventory_summary"]["maintenance_task_count"] >= 3
    assert any(row["task"]["task_type"] == "clickhouse_sync" for row in report_payload["maintenance_tasks"])
    assert any(row["task"]["task_type"] == "clickhouse_archive" for row in report_payload["maintenance_tasks"])
    assert any(log["action"] == "runtime_restored" for log in restored_snapshot["custody_logs"])
    restore_default_clickhouse_settings()
