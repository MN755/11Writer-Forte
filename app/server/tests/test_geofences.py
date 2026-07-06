from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from src.app import create_application
from src.config.settings import Settings, get_settings
from src.reference.db import session_scope as reference_session_scope
from src.reference.repository import ReferenceRepository
from src.reference.schemas import ReferenceRecord
from src.services.ops_audit_service import list_alert_records, list_provenance_events


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,
        APP_ENV="test",
        APP_RUNTIME_MODE="backend-only",
        PRIMARY_DATABASE_URL=f"sqlite:///{(tmp_path / '11writer.db').as_posix()}",
        APP_CORS_ORIGINS="",
        WEBCAM_WORKER_ENABLED=False,
        WEBCAM_WORKER_RUN_ON_STARTUP=False,
    )


def _client(settings: Settings) -> TestClient:
    app = create_application()
    app.dependency_overrides[get_settings] = lambda: settings
    return TestClient(app)


def _seed_reference_fix(settings: Settings) -> None:
    with reference_session_scope(settings.reference_database_url) as session:
        ReferenceRepository(session).upsert_records(
            records=[
                ReferenceRecord(
                    ref_id="fix:test-austin",
                    object_type="fix",
                    canonical_name="AUSTIN TEST FIX",
                    primary_code="ATFIX",
                    source_dataset="fixture-reference",
                    source_key="austin-fix-1",
                    status="active",
                    country_code="US",
                    admin1_code="US-TX",
                    centroid_lat=30.2672,
                    centroid_lon=-97.7431,
                    bbox_min_lat=30.2672,
                    bbox_min_lon=-97.7431,
                    bbox_max_lat=30.2672,
                    bbox_max_lon=-97.7431,
                    geometry_json=None,
                    coverage_tier="baseline",
                    detail={"ident": "ATFIX", "fix_type": "named"},
                )
            ],
            dataset_name="fixture-reference",
            dataset_version="test-v1",
            coverage="local",
            checksum=None,
            source_path=None,
        )


def test_geofence_routes_persist_alerts_and_provenance(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _seed_reference_fix(settings)
    client = _client(settings)

    create_response = client.post(
        "/api/geofences",
        json={
            "geofenceId": "geofence:test-austin",
            "name": "Austin Test Box",
            "shapeKind": "bbox",
            "description": "Fixture geofence for API validation.",
            "minLat": 30.0,
            "minLon": -98.0,
            "maxLat": 30.5,
            "maxLon": -97.5,
            "tags": ["fixture", "austin"],
            "metadata": {"owner": "test-suite"},
        },
    )
    assert create_response.status_code == 200
    create_payload = create_response.json()
    assert create_payload["geofenceId"] == "geofence:test-austin"
    assert create_payload["shapeKind"] == "bbox"

    list_response = client.get("/api/geofences", params={"enabled_only": True})
    assert list_response.status_code == 200
    assert list_response.json()["count"] == 1

    evaluate_response = client.post(
        "/api/geofences/geofence:test-austin/evaluate",
        json={
            "lat": 30.2672,
            "lon": -97.7431,
            "subjectType": "event",
            "subjectId": "event:test-austin-crossing",
            "observationLabel": "Fixture observation crossed Austin test box.",
            "requestedBy": "test-suite",
            "referenceObjectTypes": ["fix"],
            "referenceLimit": 5,
            "metadata": {"feed": "fixture"},
        },
    )
    assert evaluate_response.status_code == 200
    evaluation_payload = evaluate_response.json()
    assert evaluation_payload["evaluation"]["matched"] is True
    assert evaluation_payload["evaluation"]["referenceMatchCount"] == 1
    assert evaluation_payload["evaluation"]["matchedReferenceObjects"][0]["refId"] == "fix:test-austin"
    assert evaluation_payload["evaluation"]["alertId"] is not None
    assert evaluation_payload["evaluation"]["provenanceEventId"] is not None

    history_response = client.get("/api/geofences/geofence:test-austin/evaluations")
    assert history_response.status_code == 200
    history_payload = history_response.json()
    assert history_payload["count"] == 1
    assert history_payload["evaluations"][0]["matched"] is True

    alerts = list_alert_records(settings, subsystem="geofences")
    provenance = list_provenance_events(settings, subsystem="geofences")

    assert alerts.count == 1
    assert alerts.alerts[0].alert_type == "geofence_match"
    assert alerts.alerts[0].subject_id == "event:test-austin-crossing"
    assert {event.event_kind for event in provenance.events} >= {"geofence_create", "geofence_evaluation"}


def test_geofence_create_rejects_invalid_polygon(tmp_path: Path) -> None:
    client = _client(_settings(tmp_path))

    response = client.post(
        "/api/geofences",
        json={
            "geofenceId": "geofence:bad-polygon",
            "name": "Bad Polygon",
            "shapeKind": "polygon",
            "polygonPoints": [[-97.8, 30.2], [-97.7, 30.3]],
        },
    )

    assert response.status_code == 400
    assert "at least three" in response.json()["detail"]
