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
                }
            ]
        ),
        encoding="utf-8",
    )
    client.post("/api/imports/local", json={"source_path": str(second_fixture), "layer_key": "news-track"})

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
    assert len(payload["import_runs"]) >= 1
    assert len(payload["source_runs"]) == 1
    assert len(payload["source_definitions"]) == 1
    assert len(payload["products"]) == 2
    assert payload["citations_json"]
