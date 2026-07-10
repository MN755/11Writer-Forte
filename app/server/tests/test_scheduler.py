from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from fastapi.testclient import TestClient
from typer.testing import CliRunner

from src.cli import app as cli_app
from src.config import reset_settings_cache
from src.db import get_session_factory
from src.models import ScheduledTaskORM
from src.services import clickhouse_service
from src.services.scheduler_runtime_service import run_scheduler_worker
from src.services.scheduler_service import run_task, scheduler_now


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


class FakeClickHouseResponse:
    def __init__(self, payload: str) -> None:
        self.payload = payload

    def read(self) -> bytes:
        return self.payload.encode("utf-8")

    def __enter__(self) -> "FakeClickHouseResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def configure_fake_clickhouse(monkeypatch, *, row_count: int = 1) -> list[dict[str, object]]:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_ENABLED", "true")
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_URL", "http://clickhouse.test:8123")
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_DATABASE", "elevenwriter")
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_USER", "forte")
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_PASSWORD", "secret")
    monkeypatch.setenv(
        "ELEVENWRITER_CLICKHOUSE_R2_ENDPOINT",
        "https://acct.r2.cloudflarestorage.com",
    )
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_R2_BUCKET", "11writer-archive")
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_R2_ACCESS_KEY_ID", "r2-key")
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_R2_SECRET_ACCESS_KEY", "r2-secret")
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_R2_ARCHIVE_PREFIX", "forte-archive")
    reset_settings_cache()

    requests: list[dict[str, object]] = []

    def fake_urlopen(request, timeout=0):  # type: ignore[no-untyped-def]
        body = request.data.decode("utf-8") if request.data else ""
        requests.append(
            {
                "url": request.full_url,
                "method": request.get_method(),
                "body": body,
            }
        )
        if request.full_url.endswith("/ping"):
            return FakeClickHouseResponse("Ok.\n")
        if "SELECT version()" in body:
            return FakeClickHouseResponse('{"version":"26.6.1","current_database":"elevenwriter"}\n')
        if "SELECT count(*) AS row_count" in body:
            return FakeClickHouseResponse(f'{{"row_count":{row_count}}}\n')
        return FakeClickHouseResponse("")

    monkeypatch.setattr(clickhouse_service, "urlopen", fake_urlopen)
    return requests


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


def test_storage_lifecycle_schedule_expires_due_objects(client: TestClient) -> None:
    expired_at = datetime.now(timezone.utc) - timedelta(hours=3)
    create_response = client.post(
        "/api/storage/objects",
        json={
            "object_key": "scheduled:expired:1",
            "object_kind": "raw_payload",
            "owner_type": "source_run",
            "owner_id": "811",
            "object_uri": "file:///tmp/source-run-811.json",
            "storage_tier": "warm",
            "retention_class": "operational",
            "lifecycle_status": "active",
            "expires_at": expired_at.isoformat(),
        },
    )
    assert create_response.status_code == 200
    storage_object_id = create_response.json()["storage_object_id"]

    schedule_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "storage-lifecycle-schedule",
            "task_type": "storage_lifecycle",
            "interval_seconds": 300,
            "payload_json": {"retention_class": "operational", "limit": 25},
        },
    )
    assert schedule_response.status_code == 200
    task_id = schedule_response.json()["task_id"]

    run_response = client.post(f"/api/scheduler/tasks/{task_id}/run")
    assert run_response.status_code == 200
    run_payload = run_response.json()
    assert run_payload["status"] == "completed"
    assert run_payload["records_affected"] >= 1
    assert run_payload["output_json"]["transitioned_count"] >= 1
    assert storage_object_id in run_payload["output_json"]["storage_object_ids"]

    storage_response = client.get(
        "/api/storage/objects",
        params={"owner_type": "source_run", "owner_id": "811", "lifecycle_status": "expired"},
    )
    assert storage_response.status_code == 200
    storage_rows = storage_response.json()
    assert len(storage_rows) == 1
    assert storage_rows[0]["storage_object_id"] == storage_object_id

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    custody_rows = custody_response.json()
    assert any(
        row["object_type"] == "storage_object"
        and row["object_id"] == str(storage_object_id)
        and row["action"] == "storage_expired"
        for row in custody_rows
    )
    assert any(
        row["object_type"] == "scheduled_task_run"
        and row["action"] == "task_run_completed"
        and row["details_json"]["task_id"] == task_id
        for row in custody_rows
    )


def test_integrity_seed_schedule_seeds_profiles_and_custody(client: TestClient) -> None:
    schedule_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "integrity-seed-schedule",
            "task_type": "integrity_seed",
            "interval_seconds": 3600,
        },
    )
    assert schedule_response.status_code == 200
    task_id = schedule_response.json()["task_id"]

    run_response = client.post(f"/api/scheduler/tasks/{task_id}/run")
    assert run_response.status_code == 200
    run_payload = run_response.json()
    assert run_payload["status"] == "completed"
    assert run_payload["records_affected"] >= 1
    assert "nytimes.com" in run_payload["output_json"]["domains"]

    trust_response = client.get("/api/source-trust/profiles")
    assert trust_response.status_code == 200
    assert any(row["domain"] == "nytimes.com" for row in trust_response.json())

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    custody_rows = custody_response.json()
    assert any(
        row["object_type"] == "scheduled_task_run"
        and row["action"] == "task_run_completed"
        and row["details_json"]["task_id"] == task_id
        for row in custody_rows
    )
    assert any(row["action"] == "integrity_sources_seeded" for row in custody_rows)
    assert any(
        row["object_type"] == "source_trust_profile"
        and row["action"] == "source_trust_profile_created"
        for row in custody_rows
    )


def test_real_typer_add_integrity_seed_schedule(client: TestClient) -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli_app,
        [
            "add-integrity-seed-schedule",
            "cli-integrity-seed",
            "86400",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "created for integrity seeding" in result.output

    session = get_session_factory()()
    try:
        task = session.query(ScheduledTaskORM).filter_by(name="cli-integrity-seed").one()
        assert task.task_type == "integrity_seed"
        assert task.interval_seconds == 86400
        assert task.enabled is True
    finally:
        session.close()


def test_clickhouse_sync_schedule_mirrors_runtime_facts(
    client: TestClient,
    tmp_path: Path,
    monkeypatch,
) -> None:
    fixture = tmp_path / "scheduled-clickhouse-sync.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Scheduled ClickHouse sync",
                    "url": "https://clickhouse-sync.example.com/1",
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

    requests = configure_fake_clickhouse(monkeypatch)

    schedule_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "scheduled-clickhouse-sync",
            "task_type": "clickhouse_sync",
            "interval_seconds": 300,
            "layer_key": "marine-track",
            "payload_json": {"limit": 10},
        },
    )
    assert schedule_response.status_code == 200
    task_id = schedule_response.json()["task_id"]

    run_response = client.post(f"/api/scheduler/tasks/{task_id}/run")
    assert run_response.status_code == 200
    payload = run_response.json()
    assert payload["status"] == "completed"
    assert payload["records_affected"] == 2
    assert payload["output_json"]["observation_count"] == 1
    assert payload["output_json"]["storage_object_count"] == 1
    assert payload["output_json"]["clickhouse_database"] == "elevenwriter"

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    custody_rows = custody_response.json()
    assert any(row["action"] == "clickhouse_synced" for row in custody_rows)
    assert any(
        row["object_type"] == "scheduled_task_run"
        and row["action"] == "task_run_completed"
        and row["details_json"]["task_id"] == task_id
        for row in custody_rows
    )

    assert any("INSERT INTO elevenwriter.observation_facts FORMAT JSONEachRow" in str(item["body"]) for item in requests)
    assert any("INSERT INTO elevenwriter.storage_object_facts FORMAT JSONEachRow" in str(item["body"]) for item in requests)
    reset_settings_cache()


def test_clickhouse_archive_schedule_exports_r2(
    client: TestClient,
    tmp_path: Path,
    monkeypatch,
) -> None:
    fixture = tmp_path / "scheduled-clickhouse-archive.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Scheduled ClickHouse archive",
                    "url": "https://archive.example.com/1",
                    "lat": 29.8,
                    "lon": -95.3,
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

    requests = configure_fake_clickhouse(monkeypatch, row_count=1)

    schedule_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "scheduled-clickhouse-archive",
            "task_type": "clickhouse_archive",
            "interval_seconds": 300,
            "layer_key": "marine-track",
            "payload_json": {"limit": 25, "source_domain": "archive.example.com"},
        },
    )
    assert schedule_response.status_code == 200
    task_id = schedule_response.json()["task_id"]

    run_response = client.post(f"/api/scheduler/tasks/{task_id}/run")
    assert run_response.status_code == 200
    payload = run_response.json()
    assert payload["status"] == "completed"
    assert payload["records_affected"] == 1
    assert payload["output_json"]["exported_row_count"] == 1
    assert payload["output_json"]["archive_root_url"].endswith("/11writer-archive/forte-archive")
    assert payload["output_json"]["partition_strategy"] == "wildcard"

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    custody_rows = custody_response.json()
    assert any(row["action"] == "clickhouse_archived_to_r2" for row in custody_rows)
    assert any(
        row["object_type"] == "scheduled_task_run"
        and row["action"] == "task_run_completed"
        and row["details_json"]["task_id"] == task_id
        for row in custody_rows
    )

    assert any("INSERT INTO FUNCTION s3(" in str(item["body"]) for item in requests)
    reset_settings_cache()


def test_camera_inventory_refresh_schedule_materializes_camera_inventory(
    client: TestClient,
    tmp_path: Path,
) -> None:
    fixture = tmp_path / "scheduled-cameras.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "camera_id": "mndot-i35w-001",
                    "camera_name": "I-35W @ Lake St",
                    "provider": "MnDOT",
                    "road_name": "I-35W",
                    "status": "active",
                    "image_url": "https://images.511mn.org/camera-1.jpg",
                    "page_url": "https://511mn.org/camera/1",
                    "lat": 44.9485,
                    "lon": -93.2682,
                }
            ]
        ),
        encoding="utf-8",
    )
    import_response = client.post(
        "/api/imports/local",
        json={"source_path": str(fixture), "layer_key": "traffic-camera-feed"},
    )
    assert import_response.status_code == 200

    schedule_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "scheduled-camera-refresh",
            "task_type": "camera_inventory_refresh",
            "interval_seconds": 300,
            "layer_key": "traffic-camera-feed",
            "payload_json": {"limit": 25, "source_domain": "511mn.org"},
        },
    )
    assert schedule_response.status_code == 200
    task_id = schedule_response.json()["task_id"]

    run_response = client.post(f"/api/scheduler/tasks/{task_id}/run")
    assert run_response.status_code == 200
    payload = run_response.json()
    assert payload["status"] == "completed"
    assert payload["records_affected"] == 1
    assert payload["output_json"]["created_count"] == 1
    assert payload["output_json"]["updated_count"] == 0
    assert payload["output_json"]["scanned_count"] == 1
    assert payload["output_json"]["source_created_count"] == 2
    assert payload["output_json"]["source_updated_count"] == 0
    assert payload["output_json"]["source_scanned_endpoint_count"] == 2
    assert len(payload["output_json"]["camera_inventory_ids"]) == 1

    cameras_response = client.get("/api/cameras", params={"layer_key": "traffic-camera-feed"})
    assert cameras_response.status_code == 200
    cameras = cameras_response.json()
    assert len(cameras) == 1
    assert cameras[0]["external_id"] == "mndot-i35w-001"
    assert cameras[0]["source_domain"] == "images.511mn.org"

    camera_sources_response = client.get(
        "/api/camera-sources",
        params={"layer_key": "traffic-camera-feed", "limit": 10},
    )
    assert camera_sources_response.status_code == 200
    camera_sources = camera_sources_response.json()
    assert len(camera_sources) == 2
    assert {row["endpoint_kind"] for row in camera_sources} == {"image", "page"}

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    assert any(
        row["object_type"] == "camera_inventory_materialization"
        and row["action"] == "camera_materialization_completed"
        for row in custody_response.json()
    )


def test_camera_inventory_refresh_schedule_rejects_invalid_payload(client: TestClient) -> None:
    response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "broken-camera-refresh",
            "task_type": "camera_inventory_refresh",
            "interval_seconds": 300,
            "source_id": 1,
            "payload_json": {"limit": "nope"},
        },
    )
    assert response.status_code == 409
    assert "Camera inventory refresh" in response.json()["detail"]


def test_entity_resolution_refresh_schedule_materializes_entities(
    client: TestClient,
    tmp_path: Path,
) -> None:
    fixture_a = tmp_path / "scheduled-entity-a.json"
    fixture_a.write_text(
        json.dumps(
            [
                {
                    "title": "Vessel sighting",
                    "url": "https://alpha.example.com/vessel",
                    "lat": 29.76,
                    "lon": -95.36,
                    "vessel_name": "MV Example",
                    "mmsi": "123456789",
                }
            ]
        ),
        encoding="utf-8",
    )
    fixture_b = tmp_path / "scheduled-entity-b.json"
    fixture_b.write_text(
        json.dumps(
            [
                {
                    "title": "Port departure mention",
                    "url": "https://beta.example.com/vessel",
                    "lat": 29.77,
                    "lon": -95.35,
                    "vessel_name": "MV Example",
                    "mmsi": "123456789",
                }
            ]
        ),
        encoding="utf-8",
    )

    client.post("/api/imports/local", json={"source_path": str(fixture_a), "layer_key": "marine-track"})
    client.post("/api/imports/local", json={"source_path": str(fixture_b), "layer_key": "news-track"})

    schedule_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "scheduled-entity-refresh",
            "task_type": "entity_resolution_refresh",
            "interval_seconds": 300,
            "payload_json": {
                "min_lon": -96.0,
                "min_lat": 29.0,
                "max_lon": -94.0,
                "max_lat": 31.0,
                "min_observations": 2,
                "redaction_level": "public",
            },
        },
    )
    assert schedule_response.status_code == 200
    task_id = schedule_response.json()["task_id"]

    run_response = client.post(f"/api/scheduler/tasks/{task_id}/run")
    assert run_response.status_code == 200
    payload = run_response.json()
    assert payload["status"] == "completed"
    assert payload["records_affected"] == 1
    assert payload["output_json"]["created_entity_count"] == 1
    assert len(payload["output_json"]["entity_ids"]) == 1

    entities_response = client.get("/api/entities")
    assert entities_response.status_code == 200
    entities = entities_response.json()
    assert len(entities) == 1
    assert entities[0]["entity_type"] == "vessel"
    assert entities[0]["canonical_name"] == "MV Example"


def test_event_fusion_refresh_schedule_materializes_events(
    client: TestClient,
    tmp_path: Path,
) -> None:
    client.post(
        "/api/source-trust/profiles",
        json={
            "domain": "alpha.example.com",
            "trust_level": "trusted",
            "approval_policy": "auto_approve_stable",
            "integrity_source": True,
            "notes": "fixture",
        },
    )

    fixture_a = tmp_path / "scheduled-fusion-a.json"
    fixture_a.write_text(
        json.dumps(
            [
                {
                    "title": "Departure sighting",
                    "url": "https://alpha.example.com/departure",
                    "observed_at": "2026-07-06T20:00:00Z",
                    "ground_truth": True,
                    "lat": 29.76,
                    "lon": -95.36,
                }
            ]
        ),
        encoding="utf-8",
    )
    fixture_b = tmp_path / "scheduled-fusion-b.json"
    fixture_b.write_text(
        json.dumps(
            [
                {
                    "title": "Departure confirmation",
                    "url": "https://beta.example.com/departure",
                    "observed_at": "2026-07-06T20:05:00Z",
                    "lat": 29.77,
                    "lon": -95.35,
                }
            ]
        ),
        encoding="utf-8",
    )

    client.post("/api/imports/local", json={"source_path": str(fixture_a), "layer_key": "marine-track"})
    client.post("/api/imports/local", json={"source_path": str(fixture_b), "layer_key": "news-track"})

    schedule_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "scheduled-fusion-refresh",
            "task_type": "event_fusion_refresh",
            "interval_seconds": 300,
            "payload_json": {
                "min_lon": -96.0,
                "min_lat": 29.0,
                "max_lon": -94.0,
                "max_lat": 31.0,
                "distance_km": 10,
                "time_window_minutes": 120,
                "redaction_level": "public",
            },
        },
    )
    assert schedule_response.status_code == 200
    task_id = schedule_response.json()["task_id"]

    run_response = client.post(f"/api/scheduler/tasks/{task_id}/run")
    assert run_response.status_code == 200
    payload = run_response.json()
    assert payload["status"] == "completed"
    assert payload["records_affected"] == 1
    assert payload["output_json"]["created_event_count"] == 1
    assert len(payload["output_json"]["event_ids"]) == 1

    events_response = client.get("/api/events")
    assert events_response.status_code == 200
    events = events_response.json()
    assert len(events) == 1
    assert events[0]["redaction_level"] == "public"


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


def test_run_due_tasks_continues_after_failed_task_and_runs_other_due_tasks(
    client: TestClient,
    tmp_path: Path,
) -> None:
    failing_source_fixture = tmp_path / "failing-source.json"
    failing_source_fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Disabled source",
                    "url": "https://disabled.example.com/1",
                    "lat": 30.2,
                    "lon": -95.2,
                }
            ]
        ),
        encoding="utf-8",
    )
    source_response = client.post(
        "/api/sources",
        json={
            "name": "disabled-source",
            "source_kind": "local_file",
            "layer_key": "disabled-feed",
            "target_uri": str(failing_source_fixture),
            "enabled": False,
        },
    )
    assert source_response.status_code == 200
    disabled_source_id = source_response.json()["source_id"]

    good_fixture = tmp_path / "good-import.json"
    good_fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Healthy import",
                    "url": "https://healthy.example.com/1",
                    "lat": 30.3,
                    "lon": -95.3,
                }
            ]
        ),
        encoding="utf-8",
    )

    failing_task = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "disabled-source-task",
            "task_type": "source_sync",
            "interval_seconds": 300,
            "source_id": disabled_source_id,
        },
    )
    assert failing_task.status_code == 200
    failing_task_id = failing_task.json()["task_id"]

    good_task = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "healthy-import-task",
            "task_type": "local_import",
            "interval_seconds": 300,
            "target_path": str(good_fixture),
            "layer_key": "healthy-feed",
        },
    )
    assert good_task.status_code == 200
    good_task_id = good_task.json()["task_id"]

    session = get_session_factory()()
    try:
        for task_id in (failing_task_id, good_task_id):
            task = session.get(ScheduledTaskORM, task_id)
            assert task is not None
            task.next_run_at = scheduler_now()
        session.commit()
    finally:
        session.close()

    due_response = client.post("/api/scheduler/run-due")
    assert due_response.status_code == 200
    assert due_response.json()["runs_created"] == 2

    run_rows = client.get("/api/scheduler/runs")
    assert run_rows.status_code == 200
    payload = run_rows.json()
    status_by_task = {row["task_id"]: row["status"] for row in payload[:2]}
    assert status_by_task[failing_task_id] == "failed"
    assert status_by_task[good_task_id] == "completed"

    imports_response = client.get("/api/imports/runs")
    assert imports_response.status_code == 200
    assert imports_response.json()[0]["records_imported"] == 1


def test_scheduler_worker_once_runs_due_tasks(client: TestClient, tmp_path: Path) -> None:
    fixture = tmp_path / "worker-once.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Worker import",
                    "url": "https://worker.example.com/1",
                    "lat": 29.95,
                    "lon": -95.05,
                }
            ]
        ),
        encoding="utf-8",
    )
    task_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "worker-once-task",
            "task_type": "local_import",
            "interval_seconds": 300,
            "target_path": str(fixture),
            "layer_key": "worker-feed",
        },
    )
    assert task_response.status_code == 200
    task_id = task_response.json()["task_id"]

    session = get_session_factory()()
    try:
        task = session.get(ScheduledTaskORM, task_id)
        assert task is not None
        task.next_run_at = scheduler_now()
        session.commit()
    finally:
        session.close()

    result = run_scheduler_worker(
        get_session_factory(),
        poll_seconds=0,
        actor="test_scheduler_worker",
        once=True,
        sleep_fn=lambda _: None,
    )
    assert result.iterations == 1
    assert result.runs_created == 1
    assert len(result.task_run_ids) == 1

    imports_response = client.get("/api/imports/runs")
    assert imports_response.status_code == 200
    assert imports_response.json()[0]["records_imported"] == 1


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


def test_scheduler_summary_and_report_index_capture_overdue_failing_and_maintenance_tasks(
    client: TestClient,
    tmp_path: Path,
) -> None:
    healthy_fixture = tmp_path / "scheduler-summary-healthy.json"
    healthy_fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Healthy scheduled import",
                    "url": "https://scheduler-healthy.example.com/1",
                    "lat": 30.1,
                    "lon": -95.1,
                }
            ]
        ),
        encoding="utf-8",
    )
    failing_source = client.post(
        "/api/sources",
        json={
            "name": "scheduler-summary-failing-source",
            "source_kind": "http_json",
            "layer_key": "disabled-feed",
            "target_uri": "http://127.0.0.1:1/failing.json",
            "metadata_json": {
                "retry_attempts": 1,
                "request_timeout_seconds": 1,
            },
        },
    )
    assert failing_source.status_code == 200
    failing_source_id = failing_source.json()["source_id"]

    disabled_task = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "scheduler-summary-disabled-task",
            "task_type": "integrity_seed",
            "interval_seconds": 300,
            "enabled": False,
        },
    )
    assert disabled_task.status_code == 200

    disabled_fixture = tmp_path / "scheduler-summary-disabled-source.json"
    disabled_fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Disabled sync source",
                    "url": "https://scheduler-disabled.example.com/1",
                    "lat": 30.2,
                    "lon": -95.2,
                }
            ]
        ),
        encoding="utf-8",
    )

    healthy_task = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "scheduler-summary-healthy-task",
            "task_type": "local_import",
            "interval_seconds": 300,
            "target_path": str(healthy_fixture),
            "layer_key": "ops-feed",
        },
    )
    assert healthy_task.status_code == 200
    healthy_task_id = healthy_task.json()["task_id"]
    healthy_run = client.post(f"/api/scheduler/tasks/{healthy_task_id}/run")
    assert healthy_run.status_code == 200

    failing_task = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "scheduler-summary-failing-task",
            "task_type": "source_sync",
            "interval_seconds": 300,
            "source_id": failing_source_id,
        },
    )
    assert failing_task.status_code == 200
    failing_task_id = failing_task.json()["task_id"]
    session = get_session_factory()()
    try:
        try:
            run_task(session, failing_task_id, actor="test_scheduler")
        except RuntimeError:
            pass
        else:
            raise AssertionError("Expected scheduled source sync task to fail.")
    finally:
        session.close()

    maintenance_task = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "scheduler-summary-maintenance-task",
            "task_type": "storage_lifecycle",
            "interval_seconds": 300,
            "payload_json": {"limit": 25},
        },
    )
    assert maintenance_task.status_code == 200
    maintenance_task_id = maintenance_task.json()["task_id"]

    session = get_session_factory()()
    try:
        maintenance_record = session.get(ScheduledTaskORM, maintenance_task_id)
        assert maintenance_record is not None
        maintenance_record.next_run_at = scheduler_now() - timedelta(minutes=10)
        session.commit()
    finally:
        session.close()

    summary_response = client.get("/api/scheduler/summary")
    assert summary_response.status_code == 200
    summary = summary_response.json()
    assert summary["total_count"] == 4
    assert summary["enabled_count"] == 3
    assert summary["disabled_count"] == 1
    assert summary["due_count"] >= 1
    assert summary["overdue_count"] >= 1
    assert summary["failing_count"] == 1
    assert summary["maintenance_task_count"] == 1
    assert any(bucket["key"] == "storage_lifecycle" for bucket in summary["task_type_counts"])
    assert any(bucket["key"] == "failed" for bucket in summary["latest_status_counts"])

    report_response = client.get("/api/scheduler/report-index", params={"limit": 10, "overdue_task_limit": 10})
    assert report_response.status_code == 200
    report = report_response.json()
    assert report["task_run_count"] == 2
    assert report["task_run_failure_count"] == 1
    assert report["maintenance_run_count"] == 0
    assert report["maintenance_failure_count"] == 0
    assert report["inventory_summary"]["total_count"] == 4
    assert any(row["task"]["task_id"] == maintenance_task_id for row in report["overdue_tasks"])
    assert any(row["task"]["task_id"] == failing_task_id for row in report["failing_tasks"])
    assert any(row["task"]["task_id"] == maintenance_task_id for row in report["maintenance_tasks"])
    assert any(bucket["key"] == "source_sync" and bucket["failure_count"] == 1 for bucket in report["task_type_run_counts"])

    export_response = client.get(
        "/api/scheduler/export/summary",
        params={"task_limit": 10, "report_limit": 10, "overdue_task_limit": 10},
    )
    assert export_response.status_code == 200
    export_payload = export_response.json()
    assert export_payload["filters_json"]["task_limit"] == 10
    assert export_payload["report_index"]["inventory_summary"]["total_count"] == 4
    assert len(export_payload["tasks"]) == 4
    assert any(row["task_type"] == "storage_lifecycle" for row in export_payload["tasks"])
