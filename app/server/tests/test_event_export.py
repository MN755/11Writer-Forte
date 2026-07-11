from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import select

from src.db import get_session_factory
from src.models import EntityORM, SituationProductORM


def test_event_export_bundle_includes_evidence_products_and_runs(
    client: TestClient,
    tmp_path: Path,
) -> None:
    fixture = tmp_path / "source-bundle.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Bundle source record",
                    "url": "https://bundle.example.com/1",
                    "lat": 29.76,
                    "lon": -95.36,
                    "vessel_name": "MV Bundle",
                    "mmsi": "123456789",
                }
            ]
        ),
        encoding="utf-8",
    )

    source_response = client.post(
        "/api/sources",
        json={
            "name": "bundle-source",
            "source_kind": "local_file",
            "layer_key": "marine-track",
            "target_uri": str(fixture),
        },
    )
    assert source_response.status_code == 200
    source_id = source_response.json()["source_id"]

    source_run = client.post(f"/api/sources/{source_id}/run")
    assert source_run.status_code == 200

    second_fixture = tmp_path / "bundle-source-2.json"
    second_fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Bundle corroboration",
                    "url": "https://bundle-two.example.com/1",
                    "lat": 29.77,
                    "lon": -95.35,
                    "vessel_name": "MV Bundle",
                    "mmsi": "123456789",
                }
            ]
        ),
        encoding="utf-8",
    )
    client.post(
        "/api/imports/local", json={"source_path": str(second_fixture), "layer_key": "news-track"}
    )

    geofence_response = client.post(
        "/api/geofences",
        json={
            "name": "Bundle Watch",
            "geometry_geojson": {
                "type": "Polygon",
                "coordinates": [
                    [[-96.0, 29.0], [-94.0, 29.0], [-94.0, 31.0], [-96.0, 31.0], [-96.0, 29.0]]
                ],
            },
            "rule_expression": "bundle watch",
        },
    )
    assert geofence_response.status_code == 200
    geofence_id = geofence_response.json()["geofence_id"]
    schedule_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "bundle-watch-task",
            "task_type": "geofence_scan",
            "interval_seconds": 300,
            "geofence_id": geofence_id,
        },
    )
    assert schedule_response.status_code == 200
    geofence_task_id = schedule_response.json()["task_id"]
    run_response = client.post(f"/api/scheduler/tasks/{geofence_task_id}/run")
    assert run_response.status_code == 200

    alerts_response = client.get("/api/alerts", params={"geofence_id": geofence_id})
    assert alerts_response.status_code == 200
    alerts = alerts_response.json()
    assert len(alerts) == 2
    alert_id = alerts[0]["alert_id"]
    alert_update = client.patch(
        f"/api/alerts/{alert_id}",
        json={
            "status": "closed",
            "disposition_note": "Bundle export review complete",
        },
    )
    assert alert_update.status_code == 200

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
    assert entity_resolution.json()["created_entity_count"] == 1

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
    event_id = fused.json()["event_results"][0]["event_id"]

    bundle = client.get(f"/api/events/{event_id}/export")
    assert bundle.status_code == 200
    payload = bundle.json()
    assert payload["event"]["event_id"] == event_id
    assert len(payload["observation_links"]) == 2
    assert len(payload["observations"]) == 2
    assert len(payload["entities"]) == 1
    assert payload["entities"][0]["canonical_name"] == "MV Bundle"
    assert len(payload["entity_observation_links"]) == 2
    assert len(payload["alerts"]) == 2
    assert len(payload["import_runs"]) >= 1
    assert len(payload["source_runs"]) == 1
    assert len(payload["source_definitions"]) == 1
    assert len(payload["scheduled_tasks"]) == 1
    assert payload["scheduled_tasks"][0]["task_id"] == geofence_task_id
    assert len(payload["scheduled_task_runs"]) == 1
    assert payload["scheduled_task_runs"][0]["task_id"] == geofence_task_id
    assert len(payload["products"]) == 2
    assert payload["citations_json"]
    custody_object_types = {row["object_type"] for row in payload["custody_logs"]}
    assert "event" in custody_object_types
    assert "alert" in custody_object_types
    assert "entity" in custody_object_types
    assert "entity_resolution" in custody_object_types
    assert "entity_observation_link" in custody_object_types
    assert "event_fusion" in custody_object_types
    assert "event_observation_link" in custody_object_types
    assert "scheduled_task" in custody_object_types
    assert "scheduled_task_run" in custody_object_types
    assert "geofence_scan" in custody_object_types
    assert "situation_product" in custody_object_types
    assert "source_definition" in custody_object_types
    assert "source_run" in custody_object_types
    assert "event_export" in custody_object_types
    assert any(
        row["action"] == "bundle_exported" and row["object_type"] == "event_export"
        for row in payload["custody_logs"]
    )


def test_event_export_bundle_honors_requested_redaction_level(
    client: TestClient,
    tmp_path: Path,
) -> None:
    fixture = tmp_path / "restricted-bundle.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Restricted bundle source",
                    "url": "https://restricted.example.com/1",
                    "lat": 29.76,
                    "lon": -95.36,
                    "vessel_name": "MV Restricted",
                    "mmsi": "987654321",
                }
            ]
        ),
        encoding="utf-8",
    )
    corroboration = tmp_path / "restricted-bundle-2.json"
    corroboration.write_text(
        json.dumps(
            [
                {
                    "title": "Restricted corroboration",
                    "url": "https://restricted-two.example.com/1",
                    "lat": 29.77,
                    "lon": -95.35,
                    "vessel_name": "MV Restricted",
                    "mmsi": "987654321",
                }
            ]
        ),
        encoding="utf-8",
    )

    client.post(
        "/api/imports/local", json={"source_path": str(fixture), "layer_key": "marine-track"}
    )
    client.post(
        "/api/imports/local", json={"source_path": str(corroboration), "layer_key": "news-track"}
    )

    entity_resolution = client.post(
        "/api/entities/resolve",
        json={
            "min_lon": -96.0,
            "min_lat": 29.0,
            "max_lon": -94.0,
            "max_lat": 31.0,
            "min_observations": 2,
            "redaction_level": "confidential",
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

    session = get_session_factory()()
    try:
        report_product = session.scalar(
            select(SituationProductORM).where(
                SituationProductORM.event_id == event_id,
                SituationProductORM.product_type == "report",
            )
        )
        assert report_product is not None
        report_product.redaction_level = "confidential"
        entity = session.scalar(select(EntityORM).order_by(EntityORM.entity_id.asc()))
        assert entity is not None
        entity.redaction_level = "confidential"
        session.commit()
    finally:
        session.close()

    public_bundle = client.get(
        f"/api/events/{event_id}/export",
        params={"max_redaction_level": "public"},
    )
    assert public_bundle.status_code == 200
    public_payload = public_bundle.json()
    assert len(public_payload["products"]) == 1
    assert public_payload["products"][0]["product_type"] == "cited_summary"
    assert not public_payload["entities"]
    assert not public_payload["entity_observation_links"]
    assert all(citation["source_domain"] for citation in public_payload["citations_json"])

    products_response = client.get(
        f"/api/events/{event_id}/products",
        params={"max_redaction_level": "public"},
    )
    assert products_response.status_code == 200
    assert len(products_response.json()) == 1

    restricted_product_export = client.get(
        f"/api/events/{event_id}/export",
        params={"max_redaction_level": "confidential"},
    )
    assert restricted_product_export.status_code == 200
    confidential_payload = restricted_product_export.json()
    assert len(confidential_payload["products"]) == 2
    assert len(confidential_payload["entities"]) == 1


def test_event_export_bundle_rejects_lower_redaction_level_than_event(
    client: TestClient,
    tmp_path: Path,
) -> None:
    fixture_a = tmp_path / "confidential-event-a.json"
    fixture_a.write_text(
        json.dumps(
            [
                {
                    "title": "Confidential event source",
                    "url": "https://confidential.example.com/1",
                    "lat": 29.76,
                    "lon": -95.36,
                }
            ]
        ),
        encoding="utf-8",
    )
    fixture_b = tmp_path / "confidential-event-b.json"
    fixture_b.write_text(
        json.dumps(
            [
                {
                    "title": "Confidential corroboration",
                    "url": "https://confidential-two.example.com/1",
                    "lat": 29.77,
                    "lon": -95.35,
                },
            ]
        ),
        encoding="utf-8",
    )
    client.post(
        "/api/imports/local", json={"source_path": str(fixture_a), "layer_key": "confidential-feed"}
    )
    client.post(
        "/api/imports/local", json={"source_path": str(fixture_b), "layer_key": "confidential-news"}
    )

    fused = client.post(
        "/api/events/fuse",
        json={
            "min_lon": -96.0,
            "min_lat": 29.0,
            "max_lon": -94.0,
            "max_lat": 31.0,
            "distance_km": 10,
            "time_window_minutes": 120,
            "redaction_level": "confidential",
        },
    )
    assert fused.status_code == 200
    event_id = fused.json()["event_results"][0]["event_id"]

    export_response = client.get(
        f"/api/events/{event_id}/export",
        params={"max_redaction_level": "public"},
    )
    assert export_response.status_code == 403
    assert "cannot be exported" in export_response.json()["detail"]
