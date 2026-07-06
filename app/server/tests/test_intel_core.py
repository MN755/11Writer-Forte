from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from src.app import create_application
from src.config.settings import get_settings
from src.forte.api.deps import db as forte_db
from src.intel.db import db as intel_db
from src.intel.db import init_db as init_intel_db
from src.intel.event_sync import EventFeedSyncService
from src.intel.models import (
    EntityCreate,
    EventCreate,
    EventFeedSyncRequest,
    GeofenceCreate,
    IngestFileRequest,
    ObservationCreate,
    SourceCreate,
)
from src.intel.service import IntelService


def _configure_sqlite(monkeypatch, tmp_path: Path) -> str:
    database_url = f"sqlite:///{(tmp_path / 'intel.db').as_posix()}"
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("REFERENCE_DATABASE_URL", database_url)
    monkeypatch.setenv("SOURCE_DISCOVERY_DATABASE_URL", database_url)
    monkeypatch.setenv("WAVE_MONITOR_DATABASE_URL", database_url)
    get_settings.cache_clear()
    intel_db.reconfigure(database_url)
    forte_db.reconfigure(database_url)
    init_intel_db(intel_db)
    return database_url


def _with_service(callback):
    generator = intel_db.get_session()
    session = next(generator)
    try:
        return callback(IntelService(session))
    finally:
        try:
            next(generator)
        except StopIteration:
            pass


def test_intel_service_recomputes_event_confidence_and_geofence_alerts(tmp_path: Path, monkeypatch) -> None:
    _configure_sqlite(monkeypatch, tmp_path)

    def _exercise(service: IntelService):
        source = service.create_source(
            SourceCreate(
                source_id="source:test-news",
                name="Test Newswire",
                kind="integrity_source",
                integrity_score=0.9,
                default_confidence=0.8,
                actor="pytest",
            )
        )
        entity = service.create_entity(
            EntityCreate(
                entity_id="entity:vessel-1",
                entity_type="vessel",
                name="MV Test",
                primary_source_id=source.source_id,
                latitude=29.95,
                longitude=-90.05,
                actor="pytest",
            )
        )
        event = service.create_event(
            EventCreate(
                event_id="event:departure-1",
                event_type="port_departure",
                title="Vessel departed port",
                latitude=29.95,
                longitude=-90.05,
                actor="pytest",
            )
        )
        observation = service.create_observation(
            ObservationCreate(
                source_id=source.source_id,
                event_id=event.event_id,
                entity_id=entity.entity_id,
                observation_type="ground_truth",
                title="Departure sighting",
                summary="Direct sighting confirms the vessel left berth.",
                latitude=29.95,
                longitude=-90.05,
                confidence_score=0.92,
                is_ground_truth=True,
                actor="pytest",
            )
        )
        assessment = service.recompute_event_assessment(event.event_id, actor="pytest")
        geofence = service.create_geofence(
            GeofenceCreate(
                geofence_id="geofence:port",
                name="Port Perimeter",
                min_latitude=29.90,
                min_longitude=-90.10,
                max_latitude=30.00,
                max_longitude=-90.00,
                actor="pytest",
            )
        )
        alerts = service.evaluate_geofences(actor="pytest")
        custody = service.list_custody(subject_kind="observation", subject_id=observation.observation_id)
        return {
            "source_kind": source.kind.value,
            "entity_type": entity.entity_type,
            "event_type": event.event_type,
            "assessment_score": assessment.score,
            "geofence_name": geofence.name,
            "alert_types": {alert.alert_type for alert in alerts},
            "alert_count": len(alerts),
            "custody_count": len(custody),
        }

    result = _with_service(_exercise)

    assert result["source_kind"] == "integrity_source"
    assert result["entity_type"] == "vessel"
    assert result["event_type"] == "port_departure"
    assert result["assessment_score"] > 0.8
    assert result["geofence_name"] == "Port Perimeter"
    assert result["alert_count"] == 2
    assert result["alert_types"] == {"geofence_event_entry", "geofence_entity_entry"}
    assert result["custody_count"] >= 1


def test_intel_ingest_supports_json_text_and_sqlite(tmp_path: Path, monkeypatch) -> None:
    _configure_sqlite(monkeypatch, tmp_path)
    json_path = tmp_path / "records.json"
    text_path = tmp_path / "notes.txt"
    sqlite_path = tmp_path / "historical.sqlite"

    json_path.write_text(
        json.dumps(
            [
                {"title": "Record A", "lat": 33.1, "lon": -84.2, "timestamp": "2026-07-06T12:00:00Z"},
                {"title": "Record B", "description": "Second row"},
            ]
        ),
        encoding="utf-8",
    )
    text_path.write_text("first line\nsecond line\n", encoding="utf-8")
    connection = sqlite3.connect(sqlite_path)
    connection.execute("CREATE TABLE sightings (id INTEGER PRIMARY KEY, title TEXT, latitude REAL, longitude REAL)")
    connection.execute("INSERT INTO sightings(title, latitude, longitude) VALUES ('SQLite Row', 41.0, -71.0)")
    connection.commit()
    connection.close()

    def _exercise(service: IntelService):
        event = service.create_event(
            EventCreate(
                event_id="event:ingest-1",
                event_type="bulk_import",
                title="Bulk Import Event",
                actor="pytest",
            )
        )
        json_result = service.ingest_file(
            IngestFileRequest(
                path=str(json_path),
                source_id="source:json-import",
                source_name="JSON Import",
                event_id=event.event_id,
                actor="pytest",
            )
        )
        text_result = service.ingest_file(
            IngestFileRequest(
                path=str(text_path),
                source_id="source:text-import",
                source_name="Text Import",
                event_id=event.event_id,
                actor="pytest",
            )
        )
        sqlite_result = service.ingest_file(
            IngestFileRequest(
                path=str(sqlite_path),
                source_id="source:sqlite-import",
                source_name="SQLite Import",
                event_id=event.event_id,
                actor="pytest",
            )
        )
        observations = service.list_observations(event_id=event.event_id, limit=50)
        custody = service.list_custody(subject_kind="ingest_job", limit=20)
        return {
            "json_created": json_result["observations_created"],
            "text_created": text_result["observations_created"],
            "sqlite_created": sqlite_result["observations_created"],
            "observation_count": len(observations),
            "custody_count": len(custody),
        }

    result = _with_service(_exercise)

    assert result["json_created"] == 2
    assert result["text_created"] == 2
    assert result["sqlite_created"] == 1
    assert result["observation_count"] >= 5
    assert result["custody_count"] >= 1


def test_intel_routes_expose_backend_core_and_file_intake(tmp_path: Path, monkeypatch) -> None:
    _configure_sqlite(monkeypatch, tmp_path)
    input_path = tmp_path / "packet.json"
    input_path.write_text(
        json.dumps([{"title": "CLI Packet", "summary": "route test"}]),
        encoding="utf-8",
    )

    with TestClient(create_application()) as client:
        source_response = client.post(
            "/api/intel/sources",
            json={
                "sourceId": "source:route-test",
                "name": "Route Test Feed",
                "kind": "data_feed_source",
                "integrityScore": 0.7,
                "actor": "pytest",
            },
        )
        event_response = client.post(
            "/api/intel/events",
            json={
                "eventId": "event:route-test",
                "eventType": "route_test",
                "title": "Route Test Event",
                "actor": "pytest",
            },
        )
        ingest_response = client.post(
            "/api/intel/intake/files",
            json={
                "path": str(input_path),
                "sourceId": "source:route-test",
                "eventId": "event:route-test",
                "actor": "pytest",
            },
        )
        overview_response = client.get("/api/intel/overview")
        observations_response = client.get("/api/intel/observations", params={"event_id": "event:route-test"})

    assert source_response.status_code == 201
    assert event_response.status_code == 201
    assert ingest_response.status_code == 200
    assert overview_response.status_code == 200
    assert observations_response.status_code == 200
    assert ingest_response.json()["observations_created"] == 1
    assert overview_response.json()["counts"]["observations"] >= 1
    assert observations_response.json()[0]["source_id"] == "source:route-test"


def test_event_feed_sync_service_handles_all_registered_feeds(tmp_path: Path, monkeypatch) -> None:
    _configure_sqlite(monkeypatch, tmp_path)
    settings = get_settings()

    def _exercise(service: IntelService):
        result = asyncio.run(
            EventFeedSyncService(settings, service).sync(
                EventFeedSyncRequest(actor="pytest", max_records_per_feed=1, evaluate_geofences=False)
            )
        )
        overview = service.overview()
        custody = service.list_custody(limit=500)
        return {
            "feed_count": result.feed_count,
            "synced_feed_count": result.synced_feed_count,
            "statuses": {item.feed_key: item.status for item in result.results},
            "observation_count": overview.counts["observations"],
            "event_count": overview.counts["events"],
            "custody_count": len(custody),
        }

    result = _with_service(_exercise)

    assert result["feed_count"] >= 20
    assert result["synced_feed_count"] == result["feed_count"]
    assert set(result["statuses"].values()) == {"ok"}
    assert result["observation_count"] >= result["feed_count"]
    assert result["event_count"] >= result["feed_count"]
    assert result["custody_count"] >= result["feed_count"]


def test_event_feed_sync_route_creates_intel_records(tmp_path: Path, monkeypatch) -> None:
    _configure_sqlite(monkeypatch, tmp_path)

    with TestClient(create_application()) as client:
        catalog_response = client.get("/api/intel/sync/event-feeds/catalog")
        sync_response = client.post(
            "/api/intel/sync/event-feeds",
            json={
                "feeds": ["earthquakes", "nws-alerts", "geonet"],
                "maxRecordsPerFeed": 2,
                "actor": "pytest",
                "evaluateGeofences": False,
            },
        )
        overview_response = client.get("/api/intel/overview")

    assert catalog_response.status_code == 200
    assert "earthquakes" in catalog_response.json()["feeds"]
    assert sync_response.status_code == 200
    payload = sync_response.json()
    assert payload["feedCount"] == 3
    assert payload["syncedFeedCount"] == 3
    assert all(item["status"] == "ok" for item in payload["results"])
    assert overview_response.status_code == 200
    assert overview_response.json()["counts"]["events"] >= 3
    assert overview_response.json()["counts"]["observations"] >= 3
