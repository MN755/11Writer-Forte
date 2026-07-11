from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient


def test_event_fusion_creates_event_links_and_products(
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

    fixture_a = tmp_path / "fusion-a.json"
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
    fixture_b = tmp_path / "fusion-b.json"
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

    client.post(
        "/api/imports/local", json={"source_path": str(fixture_a), "layer_key": "marine-track"}
    )
    client.post(
        "/api/imports/local", json={"source_path": str(fixture_b), "layer_key": "news-track"}
    )

    response = client.post(
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
    assert response.status_code == 200
    payload = response.json()
    assert payload["created_event_count"] == 1
    event_id = payload["event_results"][0]["event_id"]

    events_response = client.get("/api/events")
    assert events_response.status_code == 200
    assert events_response.json()[0]["event_id"] == event_id
    assert events_response.json()[0]["occurred_at"].startswith("2026-07-06T20:00:00")

    links_response = client.get(f"/api/events/{event_id}/observations")
    assert links_response.status_code == 200
    links = links_response.json()
    assert len(links) == 2

    products_response = client.get(f"/api/events/{event_id}/products")
    assert products_response.status_code == 200
    products = products_response.json()
    assert len(products) == 2
    product_types = {product["product_type"] for product in products}
    assert product_types == {"cited_summary", "report"}
    assert all(product["citations_json"] for product in products)
    assert any("verification score" in product["body_text"].lower() for product in products)
    assert any("integrity sources" in product["body_text"].lower() for product in products)
    assert any("ground-truth hits" in product["body_text"].lower() for product in products)
    assert all("trust_level" in product["citations_json"][0] for product in products)

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    custody_rows = custody_response.json()
    actions = {row["action"] for row in custody_rows}
    assert "event_created_from_fusion" in actions
    assert "fusion_materialized" in actions
    assert "link_created" in actions
    assert "product_generated" in actions
