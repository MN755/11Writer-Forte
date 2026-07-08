from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient


def test_entity_resolution_materializes_entity_links_and_custody(
    client: TestClient,
    tmp_path: Path,
) -> None:
    fixture_a = tmp_path / "entity-a.json"
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
    fixture_b = tmp_path / "entity-b.json"
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
    fixture_c = tmp_path / "entity-c.json"
    fixture_c.write_text(
        json.dumps(
            [
                {
                    "title": "Port webcam follow-up",
                    "url": "https://gamma.example.com/vessel",
                    "observed_at": "2026-07-06T20:03:00Z",
                    "lat": 29.761,
                    "lon": -95.359,
                    "vessel_name": "MV Example",
                }
            ]
        ),
        encoding="utf-8",
    )

    client.post("/api/imports/local", json={"source_path": str(fixture_a), "layer_key": "marine-track"})
    client.post("/api/imports/local", json={"source_path": str(fixture_b), "layer_key": "news-track"})
    client.post("/api/imports/local", json={"source_path": str(fixture_c), "layer_key": "camera-feed"})

    response = client.post(
        "/api/entities/resolve",
        json={
            "min_lon": -96.0,
            "min_lat": 29.0,
            "max_lon": -94.0,
            "max_lat": 31.0,
            "min_observations": 2,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["created_entity_count"] == 1
    entity_id = payload["entity_results"][0]["entity_id"]
    assert payload["entity_results"][0]["entity_type"] == "vessel"
    assert payload["entity_results"][0]["canonical_name"] == "MV Example"
    assert payload["entity_results"][0]["observation_count"] == 3
    assert payload["entity_results"][0]["signal_count"] == 2

    entities_response = client.get("/api/entities")
    assert entities_response.status_code == 200
    entities = entities_response.json()
    assert len(entities) == 1
    assert entities[0]["entity_id"] == entity_id
    assert entities[0]["metadata_json"]["signal_keys"] == ["mmsi", "vessel_name"]
    assert entities[0]["metadata_json"]["hard_signal_keys"] == ["mmsi"]
    assert entities[0]["metadata_json"]["observation_count"] == 3
    assert entities[0]["metadata_json"]["evidence_strength"] == "anchored"
    assert entities[0]["metadata_json"]["primary_signal"]["signal_key"] == "mmsi"

    links_response = client.get(f"/api/entities/{entity_id}/observations")
    assert links_response.status_code == 200
    links = links_response.json()
    assert len(links) == 3

    rerun_response = client.post(
        "/api/entities/resolve",
        json={
            "min_lon": -96.0,
            "min_lat": 29.0,
            "max_lon": -94.0,
            "max_lat": 31.0,
            "min_observations": 2,
        },
    )
    assert rerun_response.status_code == 200
    assert rerun_response.json()["created_entity_count"] == 0

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    actions = {row["action"] for row in custody_response.json()}
    assert "entity_created_from_resolution" in actions
    assert "entity_updated_from_resolution" in actions
    assert "resolution_materialized" in actions
    assert "entity_link_created" in actions
