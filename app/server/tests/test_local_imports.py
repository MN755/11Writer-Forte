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
    assert payload["observations"][0]["trust_level"] == "trusted"
    assert payload["observations"][0]["approval_policy"] == "auto_approve_stable"
    assert payload["observations"][0]["location_geojson"]["type"] == "Point"
