from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient


def test_observation_bbox_query_filters_results(client: TestClient, tmp_path: Path) -> None:
    inside = tmp_path / "inside.json"
    inside.write_text(
        json.dumps(
            [
                {
                    "title": "Inside bbox",
                    "url": "https://alpha.example.com/1",
                    "lat": 29.76,
                    "lon": -95.36,
                }
            ]
        ),
        encoding="utf-8",
    )
    outside = tmp_path / "outside.json"
    outside.write_text(
        json.dumps(
            [
                {
                    "title": "Outside bbox",
                    "url": "https://beta.example.com/1",
                    "lat": 35.22,
                    "lon": -80.84,
                }
            ]
        ),
        encoding="utf-8",
    )

    client.post("/api/imports/local", json={"source_path": str(inside), "layer_key": "marine-track"})
    client.post("/api/imports/local", json={"source_path": str(outside), "layer_key": "air-track"})

    response = client.get(
        "/api/observations",
        params={
            "min_lon": -96.0,
            "min_lat": 29.0,
            "max_lon": -94.0,
            "max_lat": 31.0,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["content_text"] == "Inside bbox"


def test_cross_verification_clusters_independent_observations(
    client: TestClient,
    tmp_path: Path,
) -> None:
    fixture_a = tmp_path / "alpha.json"
    fixture_a.write_text(
        json.dumps(
            [
                {
                    "title": "Port departure",
                    "url": "https://alpha.example.com/departure",
                    "lat": 29.76,
                    "lon": -95.36,
                }
            ]
        ),
        encoding="utf-8",
    )
    fixture_b = tmp_path / "beta.json"
    fixture_b.write_text(
        json.dumps(
            [
                {
                    "title": "Port departure confirmed",
                    "url": "https://beta.example.com/departure",
                    "lat": 29.77,
                    "lon": -95.35,
                }
            ]
        ),
        encoding="utf-8",
    )

    client.post("/api/imports/local", json={"source_path": str(fixture_a), "layer_key": "marine-track"})
    client.post("/api/imports/local", json={"source_path": str(fixture_b), "layer_key": "news-track"})

    response = client.get(
        "/api/observations/cross-verify",
        params={
            "min_lon": -96.0,
            "min_lat": 29.0,
            "max_lon": -94.0,
            "max_lat": 31.0,
            "distance_km": 10,
            "time_window_minutes": 120,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    cluster = payload[0]
    assert cluster["observation_count"] == 2
    assert cluster["source_domain_count"] == 2
    assert cluster["layer_count"] == 2
    assert cluster["verification_score"] > 0.5
