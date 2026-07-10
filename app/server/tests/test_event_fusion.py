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
                    "vessel_name": "MV Fusion",
                    "mmsi": "555666777",
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
                    "vessel_name": "MV Fusion",
                    "mmsi": "555666777",
                    "lat": 29.77,
                    "lon": -95.35,
                }
            ]
        ),
        encoding="utf-8",
    )

    client.post("/api/imports/local", json={"source_path": str(fixture_a), "layer_key": "marine-track"})
    client.post("/api/imports/local", json={"source_path": str(fixture_b), "layer_key": "news-track"})
    observations_response = client.get("/api/observations")
    assert observations_response.status_code == 200
    observations = observations_response.json()
    trigger_observation_id = observations[0]["observation_id"]
    alert_response = client.post(
        "/api/alerts",
        json={
            "severity": "warning",
            "status": "open",
            "message": "Departure alert fired from corroborating observation",
            "trigger_basis_json": {"observation_id": trigger_observation_id},
        },
    )
    assert alert_response.status_code == 200
    alert_id = alert_response.json()["alert_id"]
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
    assert events_response.json()[0]["metadata_json"]["linked_entity_count"] == 1
    assert events_response.json()[0]["metadata_json"]["related_alert_count"] == 1
    assert events_response.json()[0]["metadata_json"]["open_alert_count"] == 1
    assert events_response.json()[0]["metadata_json"]["linked_entity_conflict_count"] == 0
    assert events_response.json()[0]["metadata_json"]["confidence_band"] in {"moderate", "high"}
    assert "MV Fusion" in events_response.json()[0]["title"]
    assert events_response.json()[0]["metadata_json"]["analytic_assessment"]["drivers"]
    assert events_response.json()[0]["metadata_json"]["analytic_assessment"]["cautions"]
    assert "anchored" in events_response.json()[0]["metadata_json"]["linked_entity_strengths"]

    summary_response = client.get("/api/events/summary")
    assert summary_response.status_code == 200
    summary_payload = summary_response.json()
    assert summary_payload["total_count"] == 1
    assert summary_payload["open_count"] == 1
    assert summary_payload["alert_scoped_count"] == 1
    assert summary_payload["product_covered_count"] == 1

    report_response = client.get("/api/events/report-index")
    assert report_response.status_code == 200
    report_payload = report_response.json()
    assert report_payload["inventory_summary"]["total_count"] == 1
    assert len(report_payload["recent_events"]) == 1
    assert len(report_payload["recent_products"]) == 2
    assert len(report_payload["recent_alerts"]) == 1

    export_summary_response = client.get("/api/events/export/summary")
    assert export_summary_response.status_code == 200
    export_summary_payload = export_summary_response.json()
    assert export_summary_payload["report_index"]["inventory_summary"]["total_count"] == 1
    assert len(export_summary_payload["events"]) == 1

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
    assert any("executive assessment" in product["body_text"].lower() for product in products)
    assert any("confidence assessment" in product["body_text"].lower() for product in products)
    assert any("confidence drivers" in product["body_text"].lower() for product in products)
    assert any("weakening factors" in product["body_text"].lower() for product in products)
    assert any("timeline" in product["body_text"].lower() for product in products)
    assert any("source reliability breakdown" in product["body_text"].lower() for product in products)
    assert any("linked entities" in product["body_text"].lower() for product in products)
    assert any("related alerts" in product["body_text"].lower() for product in products)
    assert any("collection gaps and follow-up" in product["body_text"].lower() for product in products)
    assert all("trust_level" in product["citations_json"][0] for product in products)

    alerts_response = client.get("/api/alerts", params={"event_id": event_id})
    assert alerts_response.status_code == 200
    related_alerts = alerts_response.json()
    assert len(related_alerts) == 1
    assert related_alerts[0]["alert_id"] == alert_id

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    custody_rows = custody_response.json()
    actions = {row["action"] for row in custody_rows}
    assert "event_created_from_fusion" in actions
    assert "fusion_materialized" in actions
    assert "link_created" in actions
    assert "product_generated" in actions
    assert "alert_linked_to_event" in actions


def test_event_fusion_surfaces_linked_entity_conflicts(
    client: TestClient,
    tmp_path: Path,
) -> None:
    fixture_a = tmp_path / "fusion-conflict-a.json"
    fixture_a.write_text(
        json.dumps(
                [
                    {
                        "title": "Person registry confirmation",
                        "url": "https://alpha.example.com/person",
                        "observed_at": "2026-07-06T20:00:00Z",
                        "email": "person@example.com",
                        "full_name": "John Q. Example",
                        "lat": 44.98,
                        "lon": -93.26,
                }
            ]
        ),
        encoding="utf-8",
    )
    fixture_b = tmp_path / "fusion-conflict-b.json"
    fixture_b.write_text(
        json.dumps(
                [
                    {
                        "title": "Witness person confirmation",
                        "url": "https://beta.example.com/person",
                        "observed_at": "2026-07-06T20:05:00Z",
                        "email": "person@example.com",
                        "full_name": "Jonathan Example",
                        "lat": 44.981,
                        "lon": -93.259,
                }
            ]
        ),
        encoding="utf-8",
    )

    client.post("/api/imports/local", json={"source_path": str(fixture_a), "layer_key": "aviation-track"})
    client.post("/api/imports/local", json={"source_path": str(fixture_b), "layer_key": "witness-track"})
    entity_resolution = client.post(
        "/api/entities/resolve",
        json={
            "min_lon": -94.0,
            "min_lat": 44.0,
            "max_lon": -93.0,
            "max_lat": 45.5,
            "min_observations": 2,
        },
    )
    assert entity_resolution.status_code == 200
    assert entity_resolution.json()["created_entity_count"] == 1

    response = client.post(
        "/api/events/fuse",
        json={
            "min_lon": -94.0,
            "min_lat": 44.0,
            "max_lon": -93.0,
            "max_lat": 45.5,
            "distance_km": 10,
            "time_window_minutes": 120,
            "redaction_level": "public",
        },
    )
    assert response.status_code == 200
    event_id = response.json()["event_results"][0]["event_id"]

    event_response = client.get("/api/events")
    assert event_response.status_code == 200
    event = next(row for row in event_response.json() if row["event_id"] == event_id)
    assert event["metadata_json"]["linked_entity_conflict_count"] == 1
    assert "Linked entities carry 1 conflicting identifier or naming signal groups." in " ".join(
        event["metadata_json"]["analytic_assessment"]["cautions"]
    )

    products_response = client.get(f"/api/events/{event_id}/products")
    assert products_response.status_code == 200
    products = products_response.json()
    assert any("signal_conflicts=1" in product["body_text"] for product in products)
