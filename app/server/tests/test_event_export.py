from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient


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
    client.post("/api/imports/local", json={"source_path": str(second_fixture), "layer_key": "news-track"})

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
    assert len(payload["import_runs"]) >= 1
    assert len(payload["source_runs"]) == 1
    assert len(payload["source_definitions"]) == 1
    assert len(payload["products"]) == 2
    assert payload["citations_json"]
    custody_object_types = {row["object_type"] for row in payload["custody_logs"]}
    assert "event" in custody_object_types
    assert "entity" in custody_object_types
    assert "entity_resolution" in custody_object_types
    assert "entity_observation_link" in custody_object_types
    assert "event_fusion" in custody_object_types
    assert "event_observation_link" in custody_object_types
    assert "situation_product" in custody_object_types
    assert "source_definition" in custody_object_types
    assert "source_run" in custody_object_types
    assert "event_export" in custody_object_types
    assert any(
        row["action"] == "bundle_exported" and row["object_type"] == "event_export"
        for row in payload["custody_logs"]
    )
