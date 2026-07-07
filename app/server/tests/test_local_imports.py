from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient


def test_local_json_import_applies_trust_profile(client: TestClient, tmp_path: Path) -> None:
    client.post(
        "/api/source-trust/profiles",
        json={
            "domain": "example.com",
            "trust_level": "trusted",
            "approval_policy": "auto_approve_stable",
            "integrity_source": True,
            "notes": "fixture",
        },
    )

    fixture = tmp_path / "input.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Port departure",
                    "url": "https://example.com/port/1",
                    "lat": 29.76,
                    "lon": -95.36,
                }
            ]
        ),
        encoding="utf-8",
    )

    response = client.post(
        "/api/imports/local",
        json={"source_path": str(fixture), "layer_key": "marine-track"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["records_imported"] == 1
    assert payload["records_skipped"] == 0
    assert payload["observations"][0]["trust_level"] == "trusted"
    assert payload["observations"][0]["approval_policy"] == "auto_approve_stable"
    assert payload["observations"][0]["location_geojson"]["type"] == "Point"

    layers_response = client.get("/api/layers")
    assert layers_response.status_code == 200
    layers = layers_response.json()
    assert layers[0]["key"] == "marine-track"
    assert layers[0]["metadata_json"]["auto_created"] is True


def test_local_import_skips_duplicate_observations(client: TestClient, tmp_path: Path) -> None:
    fixture = tmp_path / "duplicate.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Duplicate record",
                    "url": "https://example.com/dup/1",
                    "lat": 29.76,
                    "lon": -95.36,
                },
                {
                    "title": "Duplicate record",
                    "url": "https://example.com/dup/1",
                    "lat": 29.76,
                    "lon": -95.36,
                },
            ]
        ),
        encoding="utf-8",
    )

    first_response = client.post(
        "/api/imports/local",
        json={"source_path": str(fixture), "layer_key": "marine-track"},
    )
    assert first_response.status_code == 200
    first_payload = first_response.json()
    assert first_payload["records_seen"] == 2
    assert first_payload["records_imported"] == 1
    assert first_payload["records_skipped"] == 1
    assert len(first_payload["observations"]) == 1

    second_response = client.post(
        "/api/imports/local",
        json={"source_path": str(fixture), "layer_key": "marine-track"},
    )
    assert second_response.status_code == 200
    second_payload = second_response.json()
    assert second_payload["records_seen"] == 2
    assert second_payload["records_imported"] == 0
    assert second_payload["records_skipped"] == 2
    assert len(second_payload["observations"]) == 0

    observations_response = client.get("/api/observations")
    assert observations_response.status_code == 200
    observations = observations_response.json()
    assert len(observations) == 1

    imports_response = client.get("/api/imports/runs")
    assert imports_response.status_code == 200
    import_runs = imports_response.json()
    assert import_runs[0]["records_skipped"] == 2
    assert import_runs[1]["records_skipped"] == 1

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    assert any(
        row["object_type"] == "local_import_run"
        and row["details_json"]["records_skipped"] == 2
        for row in custody_response.json()
    )
