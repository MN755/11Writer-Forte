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

    fixture_a = tmp_path / "alpha.json"
    fixture_a.write_text(
        json.dumps(
            [
                {
                    "title": "Port departure",
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
    fixture_b = tmp_path / "beta.json"
    fixture_b.write_text(
        json.dumps(
            [
                {
                    "title": "Port departure confirmed",
                    "url": "https://beta.example.com/departure",
                    "observed_at": "2026-07-06T20:05:00Z",
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
    assert cluster["source_domains"] == ["alpha.example.com", "beta.example.com"]
    assert cluster["layer_keys"] == ["marine-track", "news-track"]
    assert cluster["trusted_observation_count"] == 1
    assert cluster["integrity_source_count"] == 1
    assert cluster["ground_truth_count"] == 1
    assert cluster["time_span_minutes"] == 5.0
    assert cluster["started_at"] == "2026-07-06T20:00:00Z"
    assert cluster["ended_at"] == "2026-07-06T20:05:00Z"
    assert cluster["verification_score"] > 0.5


def test_observation_since_filter_uses_observed_at_not_import_time(client: TestClient, tmp_path: Path) -> None:
    fixture = tmp_path / "observed-time.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Old but newly imported",
                    "url": "https://time.example.com/old",
                    "observed_at": "2020-01-01T00:00:00Z",
                    "lat": 44.97,
                    "lon": -93.26,
                },
                {
                    "title": "Recent observation",
                    "url": "https://time.example.com/recent",
                    "observed_at": "2026-07-07T20:00:00Z",
                    "lat": 44.98,
                    "lon": -93.25,
                },
            ]
        ),
        encoding="utf-8",
    )

    import_response = client.post("/api/imports/local", json={"source_path": str(fixture), "layer_key": "timed-feed"})
    assert import_response.status_code == 200

    all_rows = client.get("/api/observations", params={"layer_key": "timed-feed"})
    assert all_rows.status_code == 200
    payload = all_rows.json()
    assert len(payload) == 2
    assert payload[0]["observed_at"] is not None

    filtered = client.get(
        "/api/observations",
        params={
            "layer_key": "timed-feed",
            "since": "2026-01-01T00:00:00Z",
        },
    )
    assert filtered.status_code == 200
    filtered_payload = filtered.json()
    assert len(filtered_payload) == 1
    assert filtered_payload[0]["content_text"] == "Recent observation"
    assert filtered_payload[0]["observed_at"] == "2026-07-07T20:00:00Z"


def test_observation_search_ranks_local_corpus_matches(client: TestClient, tmp_path: Path) -> None:
    fixture = tmp_path / "searchable-observations.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Harbor Manifest Notes",
                    "summary": "Manifest notes supporting the harbor movement and tug escort.",
                    "url": "https://osint.example.com/harbor-manifest",
                    "lat": 44.9784,
                    "lon": -93.2641,
                },
                {
                    "title": "Harbor Departure Confirmed",
                    "summary": "Departure confirmed by local observers after dawn.",
                    "url": "https://osint.example.com/harbor-departure",
                    "lat": 44.9778,
                    "lon": -93.2650,
                },
                {
                    "title": "Rail Delay Bulletin",
                    "summary": "Dispatch notes describing a junction slowdown.",
                    "url": "https://osint.example.com/rail-delay",
                    "lat": 44.9867,
                    "lon": -93.2581,
                },
            ]
        ),
        encoding="utf-8",
    )

    import_response = client.post(
        "/api/imports/local",
        json={"source_path": str(fixture), "layer_key": "searchable-web"},
    )
    assert import_response.status_code == 200

    response = client.get(
        "/api/observations/search",
        params={"q": "harbor manifest", "layer_key": "searchable-web", "limit": 5},
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 2
    assert payload[0]["title"] == "Harbor Manifest Notes"
    assert payload[0]["search_score"] >= payload[1]["search_score"]
    assert payload[0]["matched_terms"] == ["harbor", "manifest"]
    assert payload[0]["source_url"] == "https://osint.example.com/harbor-manifest"
    assert "harbor movement" in (payload[0]["snippet"] or "")


def test_observation_route_returns_structured_error_for_unconfigured_clickhouse_backend(
    client: TestClient,
) -> None:
    response = client.get("/api/observations", params={"backend": "clickhouse"})
    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["action"] == "list_observations"
    assert detail["backend"] == "clickhouse"
    assert detail["error_type"] == "ValueError"
    assert "ClickHouse integration is not enabled" in detail["message"]
