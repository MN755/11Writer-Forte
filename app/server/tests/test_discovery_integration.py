from __future__ import annotations

import json
import re
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Iterator
from urllib.parse import parse_qs, urlsplit

from fastapi.testclient import TestClient
from typer.testing import CliRunner

from src.cli import app as cli_app
from src.config import get_settings, reset_settings_cache
from src.db import get_session_factory
from src.models import (
    CandidateSuppressionORM,
    DiscoveryCampaignORM,
    DiscoveryFrontierEntryORM,
    DiscoveryRunORM,
    ScheduledTaskORM,
    SourceCandidateORM,
    SourceDefinitionORM,
)
from src.services.discovery_analysis import canonical_url_hash


@contextmanager
def discovery_site(
    *,
    robots_mode: str = "allow",
    large_bytes: int = 4096,
) -> Iterator[tuple[str, dict[str, Any]]]:
    """Serve deterministic discovery fixtures without touching the public internet."""

    state: dict[str, Any] = {
        "base_url": "",
        "counts": {},
        "json_version": 1,
        "robots_mode": robots_mode,
        "large_bytes": large_bytes,
        "requests": [],
        "slow_starts": [],
    }

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
            state["requests"].append(self.path)
            request_url = urlsplit(self.path)
            path = request_url.path
            counts = state["counts"]
            counts[path] = counts.get(path, 0) + 1
            base_url = state["base_url"]

            if path == "/robots.txt":
                mode = state["robots_mode"]
                if mode == "error":
                    self._send(503, "text/plain", b"robots temporarily unavailable")
                    return
                disallow = "/" if mode == "deny_all" else "/blocked" if mode == "deny_blocked" else ""
                body = (
                    "User-agent: *\n"
                    f"Disallow: {disallow}\n"
                    f"Sitemap: {base_url}/sitemap.xml\n"
                ).encode()
                self._send(200, "text/plain", body)
                return
            if path == "/seed":
                body = """
                <html><head>
                  <title>Metro source inventory</title>
                  <meta name="geo.position" content="44.98;-93.27">
                  <link rel="alternate" type="application/rss+xml" href="/feed.xml">
                </head><body>
                  <a href="/news">Official road bulletin</a>
                  <a href="/data.json?utm_source=fixture">JSON observations</a>
                  <a href="/map.geojson">GeoJSON map</a>
                  <a href="/archive.csv">Historical archive</a>
                  <a href="/camera/snapshot.jpg">Traffic camera</a>
                  <p>Minneapolis I-94 operational sources.</p>
                </body></html>
                """.encode()
                self._send(200, "text/html; charset=utf-8", body, etag='"seed-v1"')
                return
            if path == "/sitemap.xml":
                body = f"""<?xml version="1.0"?>
                <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                  <url><loc>{base_url}/sitemap-data.json</loc></url>
                  <url><loc>{base_url}/news</loc></url>
                </urlset>""".encode()
                self._send(200, "application/xml", body)
                return
            if path == "/news":
                body = b"""
                <html><head><title>Official I-94 road bulletin</title>
                <meta property="article:modified_time" content="2026-07-09T12:00:00Z">
                </head><body><article>Road conditions for Minneapolis and I-94.</article></body></html>
                """
                self._send(200, "text/html", body)
                return
            if path == "/data.json":
                payload: dict[str, Any] = {
                    "name": "Metro incidents",
                    "latitude": 44.98,
                    "longitude": -93.27,
                    "city": "Minneapolis",
                    "route": "I-94",
                    "updated_at": "2026-07-09T12:00:00Z",
                    "download_url": f"{base_url}/map.geojson",
                    "version": state["json_version"],
                }
                if state["json_version"] > 1:
                    payload["schema_added"] = "health-check-change"
                self._send(
                    200,
                    "application/json",
                    json.dumps(payload).encode(),
                    etag=f'"data-v{state["json_version"]}"',
                )
                return
            if path == "/sitemap-data.json":
                self._send(
                    200,
                    "application/json",
                    json.dumps(
                        {
                            "name": "Sitemap-only dataset",
                            "jurisdiction": "Minnesota",
                            "records": [{"id": 1, "status": "open"}],
                        }
                    ).encode(),
                )
                return
            if path == "/map.geojson":
                self._send(
                    200,
                    "application/geo+json",
                    json.dumps(
                        {
                            "type": "FeatureCollection",
                            "features": [
                                {
                                    "type": "Feature",
                                    "geometry": {
                                        "type": "Point",
                                        "coordinates": [-93.27, 44.98],
                                    },
                                    "properties": {"route": "I-94"},
                                }
                            ],
                        }
                    ).encode(),
                )
                return
            if path == "/feed.xml":
                body = f"""<rss version="2.0"><channel><title>Metro alerts</title>
                <link>{base_url}/news</link><item><title>Road alert</title></item>
                </channel></rss>""".encode()
                self._send(200, "application/rss+xml", body)
                return
            if path == "/archive.csv":
                self._send(
                    200,
                    "text/csv",
                    b"name,latitude,longitude,route\nPast incident,44.97,-93.26,I-94\n",
                )
                return
            if path == "/camera/snapshot.jpg":
                self._send(200, "image/jpeg", b"\xff\xd8\xff\xe0fixture-camera\xff\xd9")
                return
            if path == "/search":
                query = parse_qs(request_url.query).get("q", [""])[0]
                body = f"""<html><head><title>Search results for {query}</title></head>
                <body><a href="/query-result.json">Structured result</a>
                <a href="/query-result.xml">XML result</a></body></html>""".encode()
                self._send(200, "text/html", body)
                return
            if path == "/query-result.json":
                self._send(
                    200,
                    "application/json",
                    json.dumps(
                        {
                            "name": "Provider-neutral query result",
                            "jurisdiction": "Minnesota",
                            "latitude": 44.98,
                            "longitude": -93.27,
                        }
                    ).encode(),
                )
                return
            if path == "/query-result.xml":
                self._send(
                    200,
                    "application/xml",
                    b"<notices><notice><title>Transit notice</title></notice></notices>",
                )
                return
            if path == "/parallel-seed":
                body = """
                <html><head><title>Parallel seed</title></head><body>
                  <a href="/slow-a.json">Slow A</a>
                  <a href="/slow-b.json">Slow B</a>
                </body></html>
                """.encode()
                self._send(200, "text/html; charset=utf-8", body)
                return
            if path in {"/slow-a.json", "/slow-b.json"}:
                state["slow_starts"].append((path, time.monotonic()))
                time.sleep(0.5)
                self._send(
                    200,
                    "application/json",
                    json.dumps(
                        {
                            "name": path.strip("/"),
                            "latitude": 44.98,
                            "longitude": -93.27,
                        }
                    ).encode(),
                )
                return
            if path == "/blocked":
                self._send(200, "text/html", b"<html><title>Should not be fetched</title></html>")
                return
            if path == "/large":
                self._send(200, "application/octet-stream", b"x" * int(state["large_bytes"]))
                return
            self._send(404, "text/plain", b"not found")

        def _send(
            self,
            status: int,
            content_type: str,
            body: bytes,
            *,
            etag: str | None = None,
        ) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            if etag:
                self.send_header("ETag", etag)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    host, port = server.server_address
    state["base_url"] = f"http://{host}:{port}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield state["base_url"], state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def create_campaign(
    client: TestClient,
    *,
    name: str,
    seeds: list[str],
    modes: list[str] | None = None,
    allow_private_networks: bool = True,
    robots_aware: bool = True,
    max_depth: int = 2,
    max_pages: int = 50,
    max_response_bytes: int = 1_000_000,
    store_artifacts: bool = True,
) -> dict[str, Any]:
    selected_modes = modes or ["seed_url"]
    response = client.post(
        "/api/discovery/campaigns",
        json={
            "name": name,
            "description": "integration discovery fixture",
            "mode": selected_modes[0],
            "status": "active",
            "modes_json": selected_modes,
            "seed_urls_json": seeds,
            "target_geography_json": {
                "bbox": [-94.0, 44.5, -93.0, 45.5],
                "place_names": ["Minneapolis"],
                "jurisdictions": ["Minnesota"],
                "routes": ["I-94"],
            },
            "format_targets_json": ["json", "geojson", "rss", "csv", "image"],
            "max_depth": max_depth,
            "max_pages": max_pages,
            "max_candidates": 200,
            "crawl_policy_json": {
                "allow_private_networks": allow_private_networks,
                "robots_aware": robots_aware,
                "crawl_delay_seconds": 0,
                "max_pages_per_domain": 100,
                "retry_attempts": 1,
                "retry_backoff_seconds": 0,
                "request_timeout_seconds": 3,
                "max_response_bytes": max_response_bytes,
                "store_artifacts": store_artifacts,
            },
        },
    )
    assert response.status_code == 200, response.text
    return response.json()


def run_campaign(
    client: TestClient,
    campaign_id: int,
    **overrides: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"resume": False, **overrides}
    response = client.post(f"/api/discovery/campaigns/{campaign_id}/run", json=payload)
    assert response.status_code == 200, response.text
    return response.json()


def candidates_by_path(client: TestClient) -> dict[str, dict[str, Any]]:
    response = client.get("/api/discovery/candidates", params={"limit": 500})
    assert response.status_code == 200
    return {
        urlsplit(candidate["canonical_url"]).path: candidate
        for candidate in response.json()
    }


def make_candidate_promotable(candidate_id: int) -> None:
    """Local HTTP fixtures are intentionally scored private; promotion tests waive that state."""

    session = get_session_factory()()
    try:
        candidate = session.get(SourceCandidateORM, candidate_id)
        assert candidate is not None
        candidate.status = "candidate"
        candidate.score_bucket = "keep_candidate"
        candidate.score = max(candidate.score, 60.0)
        session.commit()
    finally:
        session.close()


def test_discovery_api_runs_multiformat_pipeline_and_exposes_provenance(
    client: TestClient,
) -> None:
    with discovery_site() as (base_url, state):
        campaign = create_campaign(
            client,
            name="metro-multiformat",
            seeds=[f"{base_url}/seed"],
            modes=["seed_url", "sitemap", "neighborhood", "format_targeted", "geospatial"],
        )
        campaign_id = campaign["campaign_id"]
        result = run_campaign(client, campaign_id, max_pages=50)

        assert result["run"]["status"] == "completed"
        assert result["run"]["pages_fetched"] >= 8
        assert result["frontier_dead_letter_count"] == 0
        assert state["counts"]["/robots.txt"] == 1

        listed_campaigns = client.get("/api/discovery/campaigns").json()
        assert [row["campaign_id"] for row in listed_campaigns] == [campaign_id]
        campaign_detail = client.get(f"/api/discovery/campaigns/{campaign_id}")
        assert campaign_detail.status_code == 200
        assert campaign_detail.json()["candidate_count"] >= 8
        assert set(campaign_detail.json()["modes"]) >= {
            "seed_url",
            "sitemap",
            "neighborhood",
            "format_targeted",
        }

        run_id = result["run"]["discovery_run_id"]
        listed_runs = client.get("/api/discovery/runs", params={"campaign_id": campaign_id})
        assert listed_runs.status_code == 200
        assert listed_runs.json()[0]["discovery_run_id"] == run_id
        run_detail = client.get(f"/api/discovery/runs/{run_id}")
        assert run_detail.status_code == 200
        assert len(run_detail.json()["frontier"]) >= 8
        assert len(run_detail.json()["revisions"]) >= 8

        by_path = candidates_by_path(client)
        assert {
            "/seed",
            "/sitemap.xml",
            "/news",
            "/data.json",
            "/map.geojson",
            "/feed.xml",
            "/archive.csv",
            "/camera/snapshot.jpg",
            "/sitemap-data.json",
        }.issubset(by_path)
        assert by_path["/sitemap.xml"]["candidate_type"] == "sitemap"
        assert by_path["/data.json"]["format_hint"] == "json"
        assert by_path["/map.geojson"]["format_hint"] == "geojson"
        assert by_path["/feed.xml"]["candidate_type"] == "rss_feed"
        assert by_path["/archive.csv"]["format_hint"] == "csv"
        assert by_path["/camera/snapshot.jpg"]["candidate_type"] == "camera_image"
        assert by_path["/map.geojson"]["geo_hints_json"]["coordinate_count"] == 1

        data_id = by_path["/data.json"]["candidate_id"]
        detail = client.get(f"/api/discovery/candidates/{data_id}")
        assert detail.status_code == 200
        assert detail.json()["revisions"]
        assert detail.json()["health_checks"][0]["reachable"] is True
        assert detail.json()["artifacts"][0]["object_uri"].startswith("file:")

        score = client.get(f"/api/discovery/candidates/{data_id}/score")
        assert score.status_code == 200
        assert score.json()["score_breakdown_json"]["components"]["geo"] == 100
        assert score.json()["reasons"]

        lineage = client.get(f"/api/discovery/candidates/{data_id}/lineage")
        assert lineage.status_code == 200
        lineage_payload = lineage.json()
        assert lineage_payload["campaigns"][0]["campaign_id"] == campaign_id
        assert lineage_payload["runs"][0]["discovery_run_id"] == run_id
        assert lineage_payload["incoming_edges"]
        assert by_path["/seed"]["candidate_id"] in lineage_payload["ancestor_candidate_ids"]

        ops = client.get("/api/discovery/ops")
        export = client.get("/api/discovery/export/summary")
        health = client.get("/api/discovery/health")
        assert ops.status_code == export.status_code == health.status_code == 200
        assert ops.json()["inventory_summary"]["total_count"] >= 8
        assert export.json()["candidates"]
        assert health.json()["candidate_count"] >= 8


def test_repeated_runs_global_dedupe_and_append_revisions(client: TestClient) -> None:
    with discovery_site() as (base_url, state):
        campaign = create_campaign(
            client,
            name="repeat-dedupe",
            seeds=[f"{base_url}/data.json?utm_source=first"],
            max_depth=0,
            store_artifacts=False,
        )
        first = run_campaign(client, campaign["campaign_id"], max_pages=1)
        candidate = candidates_by_path(client)["/data.json"]
        candidate_id = candidate["candidate_id"]

        state["json_version"] = 2
        second = run_campaign(client, campaign["campaign_id"], max_pages=1)

        assert first["run"]["discovery_run_id"] != second["run"]["discovery_run_id"]
        inventory = client.get("/api/discovery/candidates").json()
        assert len(inventory) == 1
        assert inventory[0]["candidate_id"] == candidate_id
        assert inventory[0]["last_run_id"] == second["run"]["discovery_run_id"]
        detail = client.get(f"/api/discovery/candidates/{candidate_id}").json()
        assert [revision["revision_number"] for revision in detail["revisions"]] == [2, 1]
        assert detail["revisions"][0]["revision_kind"] == "rediscovered"
        assert detail["revisions"][0]["changed"] is True
        assert len(detail["health_checks"]) == 2

        diff = client.get(
            "/api/discovery/inventory-diff",
            params={
                "from_run_id": first["run"]["discovery_run_id"],
                "to_run_id": second["run"]["discovery_run_id"],
                "campaign_id": campaign["campaign_id"],
            },
        )
        assert diff.status_code == 200
        assert candidate_id in diff.json()["changed_candidate_ids"]


def test_query_seeded_templates_expand_variants_and_preserve_query_lineage(
    client: TestClient,
) -> None:
    with discovery_site() as (base_url, state):
        response = client.post(
            "/api/discovery/campaigns",
            json={
                "name": "provider-neutral-query-seeds",
                "status": "active",
                "mode": "query_seeded",
                "modes_json": ["query_seeded"],
                "query_strings_json": ["road closure"],
                "search_templates_json": [
                    f"{base_url}/search?q={{query}}&language={{language}}&locale={{locale}}"
                ],
                "language_variants_json": ["en", "es"],
                "locale_variants_json": ["en-US", "es-US"],
                "target_geography_json": {
                    "place_names": ["Minneapolis"],
                    "jurisdictions": ["Minnesota"],
                },
                "entity_seeds_json": [
                    {
                        "type": "agency",
                        "name": "Metro Transit",
                        "aliases": ["MT"],
                    }
                ],
                "max_depth": 1,
                "max_pages": 30,
                "max_candidates": 100,
                "crawl_policy_json": {
                    "allow_private_networks": True,
                    "robots_aware": False,
                    "crawl_delay_seconds": 0,
                    "max_pages_per_domain": 100,
                    "retry_attempts": 1,
                    "store_artifacts": False,
                },
            },
        )
        assert response.status_code == 200, response.text
        result = run_campaign(client, response.json()["campaign_id"], max_pages=30)
        assert result["run"]["status"] == "completed"

        search_requests = [
            request for request in state["requests"] if urlsplit(request).path == "/search"
        ]
        assert len(search_requests) == 20
        observed = {
            (
                parse_qs(urlsplit(request).query)["q"][0],
                parse_qs(urlsplit(request).query)["language"][0],
                parse_qs(urlsplit(request).query)["locale"][0],
            )
            for request in search_requests
        }
        assert {value[0] for value in observed} == {
            "road closure",
            "agency Metro Transit",
            "MT",
            "Minneapolis",
            "Minnesota",
        }
        assert {value[1] for value in observed} == {"en", "es"}
        assert {value[2] for value in observed} == {"en-US", "es-US"}

        candidates = client.get("/api/discovery/candidates", params={"limit": 100}).json()
        search_candidates = [
            candidate for candidate in candidates if candidate["candidate_type"] == "search_results"
        ]
        assert len(search_candidates) == 20
        assert all(
            candidate["promotion_json"]["recommended_source_kind"] == "web_search"
            for candidate in search_candidates
        )
        result_candidate = next(
            candidate
            for candidate in candidates
            if urlsplit(candidate["canonical_url"]).path == "/query-result.json"
        )
        assert result_candidate["discovery_method"] == "query_result"
        assert result_candidate["parent_url"].startswith(f"{base_url}/search?")

        lineage = client.get(
            f"/api/discovery/candidates/{result_candidate['candidate_id']}/lineage"
        )
        assert lineage.status_code == 200, lineage.text
        incoming = lineage.json()["incoming_edges"]
        assert incoming
        query_context = incoming[0]["metadata_json"]["query_context"]
        assert query_context["query"] in {
            "road closure",
            "agency Metro Transit",
            "MT",
            "Minneapolis",
            "Minnesota",
        }
        assert query_context["language"] in {"en", "es"}
        assert query_context["locale"] in {"en-US", "es-US"}
        assert query_context["search_template"].startswith(f"{base_url}/search?")


def test_frontier_checkpoint_resumes_the_same_run(client: TestClient) -> None:
    with discovery_site() as (base_url, _state):
        campaign = create_campaign(
            client,
            name="checkpoint-resume",
            seeds=[f"{base_url}/seed"],
            modes=["seed_url", "neighborhood", "format_targeted"],
            max_pages=30,
        )
        first = run_campaign(client, campaign["campaign_id"], max_pages=1)
        run_id = first["run"]["discovery_run_id"]
        assert first["run"]["status"] == "checkpointed"
        assert first["frontier_queued_count"] >= 5
        assert first["run"]["frontier_checkpoint_json"]["remaining_entries"] >= 5

        response = client.post(
            f"/api/discovery/campaigns/{campaign['campaign_id']}/run",
            json={"resume": True, "resume_run_id": run_id, "max_pages": 30},
        )
        assert response.status_code == 200, response.text
        resumed = response.json()
        assert resumed["run"]["discovery_run_id"] == run_id
        assert resumed["run"]["resumed_from_run_id"] is None
        assert resumed["run"]["metadata_json"]["resume_count"] == 1
        assert resumed["run"]["status"] == "completed"
        assert resumed["frontier_queued_count"] == 0
        assert len(client.get("/api/discovery/runs").json()) == 1


def test_campaign_run_lease_blocks_overlap_and_recovers_stale_holder(
    client: TestClient,
) -> None:
    with discovery_site() as (base_url, _state):
        campaign = create_campaign(
            client,
            name="lease-recovery",
            seeds=[f"{base_url}/seed"],
            modes=["seed_url", "neighborhood", "format_targeted"],
            max_pages=30,
        )
        first = run_campaign(client, campaign["campaign_id"], max_pages=1)
        run_id = first["run"]["discovery_run_id"]
        session = get_session_factory()()
        try:
            campaign_row = session.get(DiscoveryCampaignORM, campaign["campaign_id"])
            run_row = session.get(DiscoveryRunORM, run_id)
            assert campaign_row is not None
            assert run_row is not None
            now = datetime.now(timezone.utc)
            lease = {
                "campaign_id": campaign["campaign_id"],
                "run_id": run_id,
                "actor": "other-worker",
                "lease_token": "lease-token",
                "acquired_at": now.isoformat(),
                "heartbeat_at": now.isoformat(),
                "expires_at": (now + timedelta(minutes=10)).isoformat(),
                "lease_timeout_seconds": 600.0,
                "stale_takeover_count": 0,
            }
            campaign_row.status = "running"
            campaign_row.metadata_json = {
                **(campaign_row.metadata_json or {}),
                "active_run_lease": lease,
            }
            run_row.metadata_json = {
                **(run_row.metadata_json or {}),
                "active_lease": lease,
            }
            session.commit()
        finally:
            session.close()

        blocked = client.post(
            f"/api/discovery/campaigns/{campaign['campaign_id']}/run",
            json={"resume": True, "resume_run_id": run_id, "max_pages": 30},
        )
        assert blocked.status_code == 409, blocked.text
        assert "already leased" in blocked.json()["detail"]

        session = get_session_factory()()
        try:
            campaign_row = session.get(DiscoveryCampaignORM, campaign["campaign_id"])
            run_row = session.get(DiscoveryRunORM, run_id)
            assert campaign_row is not None
            assert run_row is not None
            stale_at = datetime.now(timezone.utc) - timedelta(hours=1)
            stale_lease = dict((campaign_row.metadata_json or {}).get("active_run_lease") or {})
            stale_lease["heartbeat_at"] = stale_at.isoformat()
            stale_lease["expires_at"] = (stale_at + timedelta(minutes=5)).isoformat()
            campaign_row.metadata_json = {
                **(campaign_row.metadata_json or {}),
                "active_run_lease": stale_lease,
            }
            run_row.metadata_json = {
                **(run_row.metadata_json or {}),
                "active_lease": stale_lease,
            }
            session.commit()
        finally:
            session.close()

        resumed = client.post(
            f"/api/discovery/campaigns/{campaign['campaign_id']}/run",
            json={"resume": True, "resume_run_id": run_id, "max_pages": 30},
        )
        assert resumed.status_code == 200, resumed.text
        payload = resumed.json()
        assert payload["run"]["status"] == "completed"
        assert "active_lease" not in payload["run"]["metadata_json"]
        assert payload["run"]["metadata_json"]["last_released_lease"]["release_reason"] == "run_finished"

        campaign_detail = client.get(f"/api/discovery/campaigns/{campaign['campaign_id']}")
        assert campaign_detail.status_code == 200
        assert "active_run_lease" not in campaign_detail.json()["campaign"]["metadata_json"]

        custody_rows = client.get("/api/custody/logs").json()
        assert any(row["action"] == "discovery_run_lease_recovered" for row in custody_rows)


def test_campaign_honors_bounded_fetch_concurrency(client: TestClient) -> None:
    with discovery_site() as (base_url, state):
        campaign = create_campaign(
            client,
            name="bounded-fetch-concurrency",
            seeds=[f"{base_url}/parallel-seed"],
            modes=["seed_url", "neighborhood", "format_targeted"],
            robots_aware=False,
            max_pages=10,
            store_artifacts=False,
        )
        patch_response = client.patch(
            f"/api/discovery/campaigns/{campaign['campaign_id']}",
            json={"crawl_policy_json": {"robots_aware": False, "crawl_delay_seconds": 0, "max_concurrency": 2, "store_artifacts": False}},
        )
        assert patch_response.status_code == 200, patch_response.text

        result = run_campaign(client, campaign["campaign_id"], max_pages=10)
        assert result["run"]["status"] == "completed"
        slow_starts = sorted(state["slow_starts"], key=lambda item: item[1])
        assert len(slow_starts) == 2
        assert abs(slow_starts[0][1] - slow_starts[1][1]) < 0.35


def test_resume_preserves_run_page_and_domain_budgets(client: TestClient) -> None:
    with discovery_site() as (base_url, state):
        campaign = create_campaign(
            client,
            name="persisted-run-page-budget",
            seeds=[f"{base_url}/seed"],
            modes=["seed_url", "neighborhood", "format_targeted"],
            robots_aware=False,
            max_depth=1,
            max_pages=2,
            store_artifacts=False,
        )
        first = run_campaign(client, campaign["campaign_id"], max_pages=1)
        assert first["run"]["status"] == "checkpointed"

        resumed_response = client.post(
            f"/api/discovery/campaigns/{campaign['campaign_id']}/run",
            json={
                "resume": True,
                "resume_run_id": first["run"]["discovery_run_id"],
                "max_pages": 50,
            },
        )
        assert resumed_response.status_code == 200, resumed_response.text
        resumed = resumed_response.json()
        assert resumed["run"]["pages_fetched"] == 2
        assert resumed["run"]["status"] == "completed"
        detail = client.get(
            f"/api/discovery/runs/{resumed['run']['discovery_run_id']}"
        ).json()
        assert any(
            entry["state"] == "deferred"
            and entry["last_error_text"] == "run_page_limit"
            for entry in detail["frontier"]
        )
        fetched_paths = [
            path for path in state["requests"] if urlsplit(path).path != "/robots.txt"
        ]
        assert len(fetched_paths) == 2

        domain_limited = client.post(
            "/api/discovery/campaigns",
            json={
                "name": "persisted-domain-attempt-budget",
                "status": "active",
                "mode": "seed_url",
                "modes_json": ["seed_url", "neighborhood"],
                "seed_urls_json": [f"{base_url}/seed"],
                "max_depth": 1,
                "max_pages": 20,
                "max_candidates": 20,
                "crawl_policy_json": {
                    "allow_private_networks": True,
                    "robots_aware": False,
                    "crawl_delay_seconds": 0,
                    "max_pages_per_domain": 2,
                    "retry_attempts": 1,
                    "store_artifacts": False,
                },
            },
        )
        assert domain_limited.status_code == 200, domain_limited.text
        domain_campaign_id = domain_limited.json()["campaign_id"]
        domain_first = run_campaign(client, domain_campaign_id, max_pages=1)
        domain_resumed = client.post(
            f"/api/discovery/campaigns/{domain_campaign_id}/run",
            json={
                "resume": True,
                "resume_run_id": domain_first["run"]["discovery_run_id"],
                "max_pages": 20,
            },
        )
        assert domain_resumed.status_code == 200, domain_resumed.text
        domain_detail = client.get(
            f"/api/discovery/runs/{domain_first['run']['discovery_run_id']}"
        ).json()
        assert domain_detail["run"]["pages_fetched"] == 2
        assert any(
            entry["state"] == "deferred"
            and entry["last_error_text"] == "per_domain_page_limit"
            for entry in domain_detail["frontier"]
        )


def test_resume_recovers_stale_fetch_claim_and_honors_candidate_cap(
    client: TestClient,
) -> None:
    with discovery_site() as (base_url, state):
        campaign = create_campaign(
            client,
            name="stale-claim-recovery",
            seeds=[f"{base_url}/seed"],
            modes=["seed_url", "neighborhood"],
            robots_aware=False,
            max_depth=1,
            max_pages=20,
            store_artifacts=False,
        )
        first = run_campaign(client, campaign["campaign_id"], max_pages=1)
        run_id = first["run"]["discovery_run_id"]
        session = get_session_factory()()
        try:
            queued = list(
                session.query(DiscoveryFrontierEntryORM)
                .filter(
                    DiscoveryFrontierEntryORM.discovery_run_id == run_id,
                    DiscoveryFrontierEntryORM.state == "queued",
                )
                .order_by(DiscoveryFrontierEntryORM.frontier_entry_id.asc())
            )
            assert queued
            stale = queued[0]
            stale.state = "fetching"
            stale.claimed_at = datetime.now(timezone.utc) - timedelta(hours=1)
            stale.attempt_count = 1
            stale.max_attempts = 2
            for entry in queued[1:]:
                entry.state = "deferred"
                entry.completed_at = datetime.now(timezone.utc)
            session.commit()
            stale_id = stale.frontier_entry_id
        finally:
            session.close()

        resumed = client.post(
            f"/api/discovery/campaigns/{campaign['campaign_id']}/run",
            json={"resume": True, "resume_run_id": run_id, "max_pages": 20},
        )
        assert resumed.status_code == 200, resumed.text
        detail = client.get(f"/api/discovery/runs/{run_id}").json()
        recovered = next(
            entry for entry in detail["frontier"] if entry["frontier_entry_id"] == stale_id
        )
        assert recovered["state"] == "completed"
        assert recovered["attempt_count"] == 2
        assert detail["run"]["error_count"] == 1

        capped = create_campaign(
            client,
            name="persisted-candidate-budget",
            seeds=[f"{base_url}/seed"],
            modes=["seed_url", "neighborhood"],
            robots_aware=False,
            max_depth=1,
            max_pages=20,
            store_artifacts=False,
        )
        patch_response = client.patch(
            f"/api/discovery/campaigns/{capped['campaign_id']}",
            json={"max_candidates": 1},
        )
        assert patch_response.status_code == 200, patch_response.text
        capped_run = run_campaign(client, capped["campaign_id"], max_pages=1)
        capped_run_id = capped_run["run"]["discovery_run_id"]
        session = get_session_factory()()
        try:
            run = session.get(DiscoveryRunORM, capped_run_id)
            assert run is not None
            deferred = session.query(DiscoveryFrontierEntryORM).filter(
                DiscoveryFrontierEntryORM.discovery_run_id == capped_run_id,
                DiscoveryFrontierEntryORM.state == "deferred",
            ).first()
            assert deferred is not None
            deferred.state = "queued"
            deferred.completed_at = None
            deferred.last_error_text = None
            run.status = "checkpointed"
            session.commit()
            deferred_url = deferred.canonical_url
        finally:
            session.close()
        request_count = len(state["requests"])
        capped_resume = client.post(
            f"/api/discovery/campaigns/{capped['campaign_id']}/run",
            json={"resume": True, "resume_run_id": capped_run_id, "max_pages": 20},
        )
        assert capped_resume.status_code == 200, capped_resume.text
        assert len(state["requests"]) == request_count
        capped_detail = client.get(f"/api/discovery/runs/{capped_run_id}").json()
        deferred_again = next(
            entry for entry in capped_detail["frontier"] if entry["canonical_url"] == deferred_url
        )
        assert deferred_again["state"] == "deferred"
        assert deferred_again["last_error_text"] == "run_candidate_limit"


def test_robots_denial_blocks_target_and_is_visible_in_health(client: TestClient) -> None:
    with discovery_site(robots_mode="deny_blocked") as (base_url, state):
        campaign = create_campaign(
            client,
            name="robots-denied",
            seeds=[f"{base_url}/blocked"],
            max_depth=0,
            store_artifacts=False,
        )
        result = run_campaign(client, campaign["campaign_id"], max_pages=1)

        assert result["run"]["status"] == "completed_with_errors"
        assert result["run"]["pages_fetched"] == 0
        assert result["candidate_ids"] == []
        assert state["counts"].get("/blocked", 0) == 0
        detail = client.get(
            f"/api/discovery/runs/{result['run']['discovery_run_id']}"
        ).json()
        assert detail["frontier"][0]["state"] == "robots_blocked"
        health = client.get("/api/discovery/health").json()
        assert health["robots_block_count"] == 1


def test_robots_server_failure_fails_closed_and_persists_observation(
    client: TestClient,
) -> None:
    with discovery_site(robots_mode="error") as (base_url, state):
        campaign = create_campaign(
            client,
            name="robots-server-failure",
            seeds=[f"{base_url}/data.json"],
            max_depth=0,
            store_artifacts=False,
        )
        result = run_campaign(client, campaign["campaign_id"], max_pages=1)

        assert result["run"]["status"] == "completed_with_errors"
        assert result["run"]["pages_fetched"] == 0
        assert state["counts"]["/robots.txt"] == 1
        assert state["counts"].get("/data.json", 0) == 0
        detail = client.get(
            f"/api/discovery/runs/{result['run']['discovery_run_id']}"
        ).json()
        assert detail["frontier"][0]["state"] == "robots_blocked"
        snapshot = client.get("/api/operations/runtime/export").json()
        observation = snapshot["robots_observations"][0]
        assert observation["status"] == "unavailable"
        assert observation["allowed"] is False
        assert observation["http_status"] == 503


def test_private_network_default_quarantines_without_fetching(client: TestClient) -> None:
    with discovery_site() as (base_url, state):
        campaign = create_campaign(
            client,
            name="private-default-block",
            seeds=[f"{base_url}/data.json"],
            allow_private_networks=False,
            max_depth=0,
            store_artifacts=False,
        )
        result = run_campaign(client, campaign["campaign_id"], max_pages=1)

        assert result["frontier_dead_letter_count"] == 1
        assert state["counts"] == {}
        candidate = candidates_by_path(client)["/data.json"]
        assert candidate["status"] == "quarantined"
        assert candidate["candidate_type"] == "unsafe_target"
        assert candidate["score_bucket"] == "quarantine"
        assert candidate["operational_hints_json"]["unsafe_target"] is True
        detail = client.get(f"/api/discovery/candidates/{candidate['candidate_id']}").json()
        assert detail["health_checks"][0]["reachable"] is False
        assert detail["health_checks"][0]["status"] == "quarantined"


def test_private_network_request_requires_deployment_gate(
    client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setenv("ELEVENWRITER_DISCOVERY_ALLOW_PRIVATE_NETWORKS", "false")
    reset_settings_cache()
    try:
        with discovery_site() as (base_url, state):
            campaign = create_campaign(
                client,
                name="private-override-server-gate",
                seeds=[f"{base_url}/data.json"],
                allow_private_networks=True,
                robots_aware=False,
                max_depth=0,
                store_artifacts=False,
            )
            result = run_campaign(client, campaign["campaign_id"], max_pages=1)

            assert result["frontier_dead_letter_count"] == 1
            assert state["counts"] == {}
            snapshot = result["run"]["policy_snapshot_json"]
            assert snapshot["allow_private_networks_requested"] is True
            assert snapshot["private_network_override_enabled"] is False
            assert snapshot["allow_private_networks"] is False
    finally:
        monkeypatch.setenv("ELEVENWRITER_DISCOVERY_ALLOW_PRIVATE_NETWORKS", "true")
        reset_settings_cache()


def test_artifact_file_is_removed_when_candidate_transaction_fails(
    client: TestClient,
    monkeypatch,
) -> None:
    import src.services.discovery_service as discovery_service

    def fail_after_artifact(*_args, **_kwargs):
        raise RuntimeError("forced graph persistence failure")

    monkeypatch.setattr(discovery_service, "create_discovery_edge", fail_after_artifact)
    with discovery_site() as (base_url, _state):
        campaign = create_campaign(
            client,
            name="artifact-rollback-cleanup",
            seeds=[f"{base_url}/data.json"],
            robots_aware=False,
            max_depth=0,
            store_artifacts=True,
        )
        result = run_campaign(client, campaign["campaign_id"], max_pages=1)

    assert result["run"]["status"] == "completed_with_errors"
    artifact_root = get_settings().data_dir / "discovery_artifacts"
    assert not artifact_root.exists() or not any(
        path.is_file() for path in artifact_root.rglob("*")
    )
    snapshot = client.get("/api/operations/runtime/export").json()
    assert snapshot["discovery_artifacts"] == []
    assert snapshot["storage_objects"] == []


def test_response_size_limit_dead_letters_oversized_document(client: TestClient) -> None:
    with discovery_site(large_bytes=4096) as (base_url, state):
        campaign = create_campaign(
            client,
            name="bounded-response",
            seeds=[f"{base_url}/large"],
            robots_aware=False,
            max_depth=0,
            max_response_bytes=1024,
            store_artifacts=False,
        )
        result = run_campaign(client, campaign["campaign_id"], max_pages=1)

        assert result["frontier_dead_letter_count"] == 1
        assert result["run"]["error_count"] == 1
        assert result["candidate_ids"] == []
        assert state["counts"]["/large"] == 1
        detail = client.get(
            f"/api/discovery/runs/{result['run']['discovery_run_id']}"
        ).json()
        frontier = detail["frontier"][0]
        assert frontier["state"] == "dead_letter"
        assert "exceeds limit 1024" in frontier["last_error_text"]


def test_promotion_is_idempotent_and_reference_sources_never_schedule(
    client: TestClient,
) -> None:
    with discovery_site() as (base_url, _state):
        campaign = create_campaign(
            client,
            name="promotion-contract",
            seeds=[f"{base_url}/data.json", f"{base_url}/camera/snapshot.jpg"],
            robots_aware=False,
            max_depth=0,
            store_artifacts=False,
        )
        run_campaign(client, campaign["campaign_id"], max_pages=2)
        by_path = candidates_by_path(client)
        json_id = by_path["/data.json"]["candidate_id"]
        camera_id = by_path["/camera/snapshot.jpg"]["candidate_id"]
        make_candidate_promotable(json_id)
        make_candidate_promotable(camera_id)

        promotion_payload = {
            "source_kind": "http_json",
            "source_name": "promoted-metro-json",
            "layer_key": "discovered-metro",
            "create_schedule": True,
            "schedule_interval_seconds": 120,
            "reason": "Verified structured endpoint",
            "metadata_json": {
                "allow_private_networks": True,
                "block_private_networks": False,
                "max_response_bytes": 1,
                "headers": {"Authorization": "should-not-be-runtime-config"},
            },
        }
        first = client.post(
            f"/api/discovery/candidates/{json_id}/promote",
            json=promotion_payload,
        )
        assert first.status_code == 200, first.text
        first_payload = first.json()
        assert first_payload["source"]["source_kind"] == "http_json"
        assert first_payload["source"]["enabled"] is True
        promoted_metadata = first_payload["source"]["metadata_json"]
        assert promoted_metadata["block_private_networks"] is True
        assert promoted_metadata["allow_private_networks"] is True
        assert promoted_metadata["max_response_bytes"] == 20 * 1024 * 1024
        assert "headers" not in promoted_metadata
        assert promoted_metadata["operator_metadata"]["headers"]
        assert first_payload["scheduled_task"]["task_type"] == "source_sync"
        assert first_payload["scheduled_task"]["interval_seconds"] == 120
        source_run = client.post(
            f"/api/sources/{first_payload['source']['source_id']}/run"
        )
        assert source_run.status_code == 200, source_run.text
        assert source_run.json()["status"] == "completed"
        assert source_run.json()["records_imported"] == 1

        second = client.post(
            f"/api/discovery/candidates/{json_id}/promote",
            json=promotion_payload,
        )
        assert second.status_code == 200, second.text
        second_payload = second.json()
        assert second_payload["source"]["source_id"] == first_payload["source"]["source_id"]
        assert second_payload["scheduled_task"]["task_id"] == (
            first_payload["scheduled_task"]["task_id"]
        )

        reference = client.post(
            f"/api/discovery/candidates/{camera_id}/promote",
            json={
                "source_kind": "camera_image",
                "source_name": "promoted-camera-reference",
                "layer_key": "traffic-camera-feed",
                "enabled": True,
                "create_schedule": True,
                "reason": "Reachable image endpoint reference",
            },
        )
        assert reference.status_code == 200, reference.text
        reference_payload = reference.json()
        assert reference_payload["source"]["source_kind"] == "camera_image"
        assert reference_payload["source"]["enabled"] is False
        assert reference_payload["source"]["metadata_json"]["runtime_support"] == "reference_only"
        assert reference_payload["decision"]["decision"] == "promoted_reference_only"
        assert reference_payload["scheduled_task"] is None

        sources = client.get("/api/sources").json()
        tasks = client.get("/api/scheduler/tasks").json()
        assert len([source for source in sources if source["source_id"] == first_payload["source"]["source_id"]]) == 1
        assert len([task for task in tasks if task["source_id"] == first_payload["source"]["source_id"]]) == 1
        custody = client.get("/api/custody/logs").json()
        assert any(
            row["object_id"] == str(json_id) and row["action"] == "candidate_promoted"
            for row in custody
        )
        assert any(
            row["object_id"] == str(reference_payload["source"]["source_id"])
            and row["action"] == "source_promoted_from_discovery"
            for row in custody
        )


def test_promotion_rejects_existing_source_kind_conflict_without_disabling(
    client: TestClient,
) -> None:
    with discovery_site() as (base_url, _state):
        campaign = create_campaign(
            client,
            name="promotion-kind-conflict",
            seeds=[f"{base_url}/data.json"],
            robots_aware=False,
            max_depth=0,
            store_artifacts=False,
        )
        run_campaign(client, campaign["campaign_id"], max_pages=1)
        candidate = candidates_by_path(client)["/data.json"]
        make_candidate_promotable(candidate["candidate_id"])
        source_response = client.post(
            "/api/sources",
            json={
                "name": "preexisting-json-source",
                "source_kind": "http_json",
                "layer_key": "existing-source",
                "target_uri": candidate["canonical_url"],
                "enabled": True,
            },
        )
        assert source_response.status_code == 200, source_response.text
        source_id = source_response.json()["source_id"]

        conflict = client.post(
            f"/api/discovery/candidates/{candidate['candidate_id']}/promote",
            json={
                "source_kind": "camera_image",
                "create_schedule": True,
                "reason": "Deliberate conflicting mapping regression",
            },
        )
        assert conflict.status_code == 409, conflict.text
        existing = next(
            source
            for source in client.get("/api/sources").json()
            if source["source_id"] == source_id
        )
        assert existing["source_kind"] == "http_json"
        assert existing["enabled"] is True
        assert not [
            task
            for task in client.get("/api/scheduler/tasks").json()
            if task["source_id"] == source_id
        ]


def test_suppression_blocks_rediscovery_and_promotion(client: TestClient) -> None:
    with discovery_site() as (base_url, state):
        campaign = create_campaign(
            client,
            name="suppression-contract",
            seeds=[f"{base_url}/data.json"],
            robots_aware=False,
            max_depth=0,
            store_artifacts=False,
        )
        run_campaign(client, campaign["campaign_id"], max_pages=1)
        candidate = candidates_by_path(client)["/data.json"]
        requests_before = state["counts"]["/data.json"]

        suppression = client.post(
            f"/api/discovery/candidates/{candidate['candidate_id']}/suppress",
            json={
                "scope": "candidate",
                "reason_code": "operator_irrelevant",
                "reason": "Known mirror with no additional coverage",
            },
        )
        assert suppression.status_code == 200
        assert suppression.json()["status"] == "active"

        second = run_campaign(client, campaign["campaign_id"], max_pages=1)
        assert state["counts"]["/data.json"] == requests_before
        detail = client.get(
            f"/api/discovery/runs/{second['run']['discovery_run_id']}"
        ).json()
        assert detail["frontier"][0]["state"] == "blocked"
        candidate_detail = client.get(
            f"/api/discovery/candidates/{candidate['candidate_id']}"
        ).json()
        assert candidate_detail["candidate"]["status"] == "suppressed"
        assert candidate_detail["suppressions"][0]["reason_code"] == "operator_irrelevant"
        assert candidate_detail["revisions"][0]["revision_kind"] == "suppression"

        promotion = client.post(
            f"/api/discovery/candidates/{candidate['candidate_id']}/promote",
            json={"source_kind": "http_json", "reason": "Should be rejected"},
        )
        assert promotion.status_code == 409
        assert "suppressed" in promotion.json()["detail"]


def test_domain_suppression_covers_subdomains_and_expiry_releases_candidate(
    client: TestClient,
) -> None:
    session = get_session_factory()()
    try:
        root_source = SourceDefinitionORM(
            name="root-domain-source",
            source_kind="http_json",
            layer_key="domain-suppression",
            target_uri="https://example.com/root.json",
            enabled=True,
        )
        subdomain_source = SourceDefinitionORM(
            name="subdomain-source",
            source_kind="http_json",
            layer_key="domain-suppression",
            target_uri="https://api.example.com/data.json",
            enabled=True,
        )
        session.add_all([root_source, subdomain_source])
        session.flush()
        root_candidate = SourceCandidateORM(
            canonical_url_hash=canonical_url_hash(root_source.target_uri),
            canonical_url=root_source.target_uri,
            discovered_url=root_source.target_uri,
            normalized_domain="example.com",
            status="promoted",
            score=70.0,
            score_bucket="keep_candidate",
            promoted_source_id=root_source.source_id,
        )
        subdomain_candidate = SourceCandidateORM(
            canonical_url_hash=canonical_url_hash(subdomain_source.target_uri),
            canonical_url=subdomain_source.target_uri,
            discovered_url=subdomain_source.target_uri,
            normalized_domain="api.example.com",
            status="promoted",
            score=70.0,
            score_bucket="keep_candidate",
            promoted_source_id=subdomain_source.source_id,
        )
        expiring_candidate = SourceCandidateORM(
            canonical_url_hash=canonical_url_hash("https://feeds.example.net/data.json"),
            canonical_url="https://feeds.example.net/data.json",
            discovered_url="https://feeds.example.net/data.json",
            normalized_domain="feeds.example.net",
            status="suppressed",
            score=65.0,
            score_bucket="keep_candidate",
            promotion_json={"recommended_source_kind": "http_json"},
            trust_hints_json={"trust_level": "neutral"},
        )
        session.add_all([root_candidate, subdomain_candidate, expiring_candidate])
        session.flush()
        session.add(
            CandidateSuppressionORM(
                candidate_id=expiring_candidate.candidate_id,
                scope="candidate",
                status="active",
                reason="Temporary hold",
                expires_at=datetime.now(timezone.utc) - timedelta(minutes=1),
            )
        )
        session.commit()
        root_candidate_id = root_candidate.candidate_id
        subdomain_candidate_id = subdomain_candidate.candidate_id
        expiring_candidate_id = expiring_candidate.candidate_id
        root_source_id = root_source.source_id
        subdomain_source_id = subdomain_source.source_id
    finally:
        session.close()

    suppressed = client.post(
        f"/api/discovery/candidates/{root_candidate_id}/suppress",
        json={
            "scope": "domain",
            "normalized_domain": "example.com",
            "reason": "Suppress root and child domains",
        },
    )
    assert suppressed.status_code == 200, suppressed.text
    child_detail = client.get(
        f"/api/discovery/candidates/{subdomain_candidate_id}"
    ).json()
    assert child_detail["suppressions"][0]["suppression_id"] == suppressed.json()[
        "suppression_id"
    ]
    session = get_session_factory()()
    try:
        root = session.get(SourceCandidateORM, root_candidate_id)
        child = session.get(SourceCandidateORM, subdomain_candidate_id)
        assert root is not None and root.status == "suppressed"
        assert child is not None and child.status == "suppressed"
        assert session.get(SourceDefinitionORM, root_source_id).enabled is False
        assert session.get(SourceDefinitionORM, subdomain_source_id).enabled is False
    finally:
        session.close()

    promoted = client.post(
        f"/api/discovery/candidates/{expiring_candidate_id}/promote",
        json={
            "source_kind": "http_json",
            "source_name": "expired-suppression-release",
            "create_schedule": False,
        },
    )
    assert promoted.status_code == 200, promoted.text
    assert promoted.json()["candidate"]["status"] == "promoted"
    detail = client.get(f"/api/discovery/candidates/{expiring_candidate_id}").json()
    assert detail["suppressions"][0]["status"] == "expired"
    assert any(
        revision["revision_kind"] == "suppression_expired"
        for revision in detail["revisions"]
    )


def test_unrelated_campaign_preserves_global_best_score_and_revision_observation(
    client: TestClient,
) -> None:
    with discovery_site() as (base_url, _state):
        target = f"{base_url}/data.json"
        local_campaign = create_campaign(
            client,
            name="best-score-local-campaign",
            seeds=[target],
            robots_aware=False,
            max_depth=0,
            store_artifacts=False,
        )
        first_run = run_campaign(client, local_campaign["campaign_id"], max_pages=1)
        candidate = candidates_by_path(client)["/data.json"]
        best_score = candidate["score"]
        best_breakdown = candidate["score_breakdown_json"]

        distant_campaign = create_campaign(
            client,
            name="best-score-distant-campaign",
            seeds=[target],
            robots_aware=False,
            max_depth=0,
            store_artifacts=False,
        )
        patched = client.patch(
            f"/api/discovery/campaigns/{distant_campaign['campaign_id']}",
            json={
                "target_geography_json": {
                    "bbox": [120.0, -40.0, 121.0, -39.0],
                    "place_names": ["Unrelated Place"],
                    "jurisdictions": ["Unrelated Jurisdiction"],
                }
            },
        )
        assert patched.status_code == 200, patched.text
        second_run = run_campaign(client, distant_campaign["campaign_id"], max_pages=1)

        refreshed = client.get(
            f"/api/discovery/candidates/{candidate['candidate_id']}"
        ).json()
        current = refreshed["candidate"]
        assert current["score"] == best_score
        assert current["score_breakdown_json"]["components"] == best_breakdown["components"]
        assert current["metadata_json"]["best_score_campaign_id"] == local_campaign["campaign_id"]
        assert current["metadata_json"]["best_score_run_id"] == first_run["run"]["discovery_run_id"]
        newest_revision = refreshed["revisions"][0]
        assert newest_revision["discovery_run_id"] == second_run["run"]["discovery_run_id"]
        assert newest_revision["campaign_id"] == distant_campaign["campaign_id"]
        assert newest_revision["score"] < best_score
        assert client.get(
            f"/api/discovery/campaigns/{local_campaign['campaign_id']}"
        ).json()["candidate_count"] == 1
        assert client.get(
            f"/api/discovery/campaigns/{distant_campaign['campaign_id']}"
        ).json()["candidate_count"] == 1


def test_scheduler_runs_discovery_campaign_and_persists_output(client: TestClient) -> None:
    with discovery_site() as (base_url, _state):
        campaign = create_campaign(
            client,
            name="scheduled-discovery",
            seeds=[f"{base_url}/data.json"],
            robots_aware=False,
            max_depth=0,
            store_artifacts=False,
        )
        task = client.post(
            "/api/scheduler/tasks",
            json={
                "name": "scheduled-discovery-task",
                "task_type": "discovery_campaign",
                "interval_seconds": 300,
                "retry_attempts": 1,
                "payload_json": {
                    "campaign_id": campaign["campaign_id"],
                    "resume": False,
                    "max_pages": 1,
                    "max_candidates": 10,
                },
            },
        )
        assert task.status_code == 200, task.text
        task_run = client.post(f"/api/scheduler/tasks/{task.json()['task_id']}/run")
        assert task_run.status_code == 200, task_run.text
        output = task_run.json()["output_json"]
        assert task_run.json()["status"] == "completed"
        assert task_run.json()["records_affected"] == 1
        assert output["campaign_id"] == campaign["campaign_id"]
        assert output["status"] == "completed"
        assert output["pages_fetched"] == 1
        assert len(output["candidate_ids"]) == 1
        run_detail = client.get(f"/api/discovery/runs/{output['discovery_run_id']}")
        assert run_detail.status_code == 200
        custody = client.get("/api/custody/logs").json()
        assert any(
            row["object_type"] == "scheduled_task_run"
            and row["action"] == "task_run_completed"
            for row in custody
        )


def test_scheduler_runs_discovery_health_scan_and_persists_output(
    client: TestClient,
) -> None:
    with discovery_site() as (base_url, state):
        campaign = create_campaign(
            client,
            name="scheduled-discovery-health",
            seeds=[f"{base_url}/data.json"],
            robots_aware=False,
            max_depth=0,
            store_artifacts=False,
        )
        run_campaign(client, campaign["campaign_id"], max_pages=1)
        candidate = candidates_by_path(client)["/data.json"]
        state["json_version"] = 2

        task = client.post(
            "/api/scheduler/tasks",
            json={
                "name": "scheduled-discovery-health-task",
                "task_type": "discovery_health_scan",
                "interval_seconds": 300,
                "retry_attempts": 1,
                "payload_json": {
                    "campaign_id": campaign["campaign_id"],
                    "limit": 10,
                },
            },
        )
        assert task.status_code == 200, task.text
        task_run = client.post(f"/api/scheduler/tasks/{task.json()['task_id']}/run")
        assert task_run.status_code == 200, task_run.text
        output = task_run.json()["output_json"]
        assert task_run.json()["status"] == "completed"
        assert task_run.json()["records_affected"] == 1
        assert output["checked_count"] == 1
        assert output["reachable_count"] == 1
        assert output["changed_count"] == 1
        assert len(output["health_check_ids"]) == 1

        detail = client.get(f"/api/discovery/candidates/{candidate['candidate_id']}")
        assert detail.status_code == 200, detail.text
        assert len(detail.json()["health_checks"]) == 2


def test_scheduler_runs_discovery_revisit_and_persists_output(client: TestClient) -> None:
    with discovery_site() as (base_url, _state):
        campaign = create_campaign(
            client,
            name="scheduled-discovery-revisit",
            seeds=[f"{base_url}/data.json"],
            robots_aware=False,
            max_depth=0,
            store_artifacts=False,
        )
        run_campaign(client, campaign["campaign_id"], max_pages=1)
        candidate = candidates_by_path(client)["/data.json"]

        task = client.post(
            "/api/scheduler/tasks",
            json={
                "name": "scheduled-discovery-revisit-task",
                "task_type": "discovery_revisit",
                "interval_seconds": 300,
                "retry_attempts": 1,
                "payload_json": {
                    "campaign_id": campaign["campaign_id"],
                    "force": True,
                    "priority": 88.0,
                },
            },
        )
        assert task.status_code == 200, task.text
        task_run = client.post(f"/api/scheduler/tasks/{task.json()['task_id']}/run")
        assert task_run.status_code == 200, task_run.text
        output = task_run.json()["output_json"]
        assert task_run.json()["status"] == "completed"
        assert task_run.json()["records_affected"] == 1
        assert output["queued_count"] == 1
        assert output["candidate_ids"] == [candidate["candidate_id"]]
        assert len(output["frontier_entry_ids"]) == 1

        custody = client.get("/api/custody/logs")
        assert custody.status_code == 200
        assert any(
            row["object_type"] == "discovery_revisit"
            and row["action"] == "discovery_revisit_queued"
            for row in custody.json()
        )


def test_health_check_and_forced_revisit_persist_history(client: TestClient) -> None:
    with discovery_site() as (base_url, state):
        campaign = create_campaign(
            client,
            name="health-and-revisit",
            seeds=[f"{base_url}/data.json"],
            robots_aware=False,
            max_depth=0,
            store_artifacts=False,
        )
        run_campaign(client, campaign["campaign_id"], max_pages=1)
        candidate = candidates_by_path(client)["/data.json"]
        candidate_id = candidate["candidate_id"]
        state["json_version"] = 2

        health = client.post(f"/api/discovery/candidates/{candidate_id}/health")
        assert health.status_code == 200, health.text
        assert health.json()["reachable"] is True
        assert health.json()["changed"] is True
        assert health.json()["status"] == "schema_changed"

        detail = client.get(f"/api/discovery/candidates/{candidate_id}").json()
        assert detail["candidate"]["revisit_count"] == 1
        assert detail["candidate"]["last_revisited_at"] is not None
        assert detail["candidate"]["next_revisit_at"] is not None
        assert len(detail["health_checks"]) == 2
        assert detail["revisions"][0]["revision_kind"] == "health_revisit"

        revisit = client.post(
            f"/api/discovery/candidates/{candidate_id}/revisit",
            json={"force": True, "priority": 99},
        )
        assert revisit.status_code == 200, revisit.text
        assert revisit.json()["queued_count"] == 1
        resumed = client.post(
            f"/api/discovery/campaigns/{campaign['campaign_id']}/run",
            json={"resume": True, "max_pages": 1},
        )
        assert resumed.status_code == 200, resumed.text
        assert resumed.json()["run"]["trigger_kind"] == "revisit"
        assert resumed.json()["run"]["status"] == "completed"
        final_detail = client.get(f"/api/discovery/candidates/{candidate_id}").json()
        assert len(final_detail["revisions"]) == 3


def test_health_check_honors_new_robots_denial(client: TestClient) -> None:
    with discovery_site() as (base_url, state):
        campaign = create_campaign(
            client,
            name="health-robots-contract",
            seeds=[f"{base_url}/blocked"],
            robots_aware=False,
            max_depth=0,
            store_artifacts=False,
        )
        run_campaign(client, campaign["campaign_id"], max_pages=1)
        candidate = candidates_by_path(client)["/blocked"]
        target_request_count = state["counts"]["/blocked"]
        state["robots_mode"] = "deny_blocked"
        patched = client.patch(
            f"/api/discovery/campaigns/{campaign['campaign_id']}",
            json={
                "crawl_policy_json": {
                    **campaign["crawl_policy_json"],
                    "robots_aware": True,
                }
            },
        )
        assert patched.status_code == 200, patched.text
        policy = client.put(
            "/api/discovery/domain-policies",
            json={
                "normalized_domain": candidate["normalized_domain"],
                "robots_mode": "respect",
            },
        )
        assert policy.status_code == 200, policy.text

        health = client.post(
            f"/api/discovery/candidates/{candidate['candidate_id']}/health"
        )
        assert health.status_code == 200, health.text
        assert health.json()["status"] == "robots_blocked"
        assert health.json()["robots_allowed"] is False
        assert state["counts"]["/blocked"] == target_request_count
        assert state["counts"]["/robots.txt"] == 1


def test_runtime_snapshot_round_trips_discovery_state(client: TestClient) -> None:
    with discovery_site() as (base_url, _state):
        campaign = create_campaign(
            client,
            name="snapshot-discovery",
            seeds=[f"{base_url}/data.json"],
            robots_aware=True,
            max_depth=0,
        )
        run_campaign(client, campaign["campaign_id"], max_pages=1)
        candidate = candidates_by_path(client)["/data.json"]
        client.post(
            f"/api/discovery/candidates/{candidate['candidate_id']}/suppress",
            json={"reason": "snapshot suppression"},
        )

        exported = client.get("/api/operations/runtime/export")
        assert exported.status_code == 200
        snapshot = exported.json()
        assert len(snapshot["discovery_campaigns"]) == 1
        assert len(snapshot["discovery_runs"]) == 1
        assert len(snapshot["source_candidates"]) == 1
        assert len(snapshot["source_candidate_revisions"]) == 2
        assert snapshot["discovery_graph_edges"]
        assert snapshot["candidate_health_checks"]
        assert snapshot["candidate_suppressions"]
        assert snapshot["robots_observations"]
        assert snapshot["discovery_artifacts"]

        restored = client.post(
            "/api/operations/runtime/restore",
            params={"replace_existing": "true"},
            json=snapshot,
        )
        assert restored.status_code == 200, restored.text
        row_counts = {row["table_name"]: row["row_count"] for row in restored.json()["row_counts"]}
        assert row_counts["discovery_campaigns"] == 1
        assert row_counts["source_candidates"] == 1
        assert row_counts["source_candidate_revisions"] == 2
        assert row_counts["candidate_suppressions"] == 1

        verify = client.get("/api/operations/runtime/export")
        assert verify.status_code == 200
        restored_snapshot = verify.json()
        assert restored_snapshot["source_candidates"][0]["candidate_id"] == (
            candidate["candidate_id"]
        )
        assert restored_snapshot["candidate_suppressions"][0]["reason"] == (
            "snapshot suppression"
        )


def test_real_typer_discovery_campaign_dry_run(client: TestClient) -> None:
    runner = CliRunner()
    create = runner.invoke(
        cli_app,
        [
            "create-discovery-campaign",
            "cli-discovery",
            "--mode",
            "seed_url",
            "--discovery-mode",
            "seed_url",
            "--seed",
            "https://example.com/feed.json",
            "--max-depth",
            "0",
            "--max-pages",
            "1",
        ],
    )
    assert create.exit_code == 0, create.output
    match = re.search(r"campaign=(\d+)", create.output)
    assert match is not None
    campaign_id = match.group(1)

    run = runner.invoke(cli_app, ["run-discovery", campaign_id, "--dry-run"])
    assert run.exit_code == 0, run.output
    assert '"status": "dry_run"' in run.output

    listed = runner.invoke(cli_app, ["list-discovery-campaigns"])
    assert listed.exit_code == 0, listed.output
    assert "cli-discovery" in listed.output
    assert f"{campaign_id} |" in listed.output


def test_real_typer_update_discovery_campaign(client: TestClient) -> None:
    runner = CliRunner()
    create = runner.invoke(
        cli_app,
        [
            "create-discovery-campaign",
            "cli-discovery-update",
            "--mode",
            "seed_url",
            "--discovery-mode",
            "seed_url",
            "--seed",
            "https://example.com/feed.json",
            "--max-depth",
            "1",
            "--max-pages",
            "5",
            "--policy-json",
            "{\"allow_private_networks\": true, \"robots_aware\": false, \"max_concurrency\": 1}",
        ],
    )
    assert create.exit_code == 0, create.output
    match = re.search(r"campaign=(\d+)", create.output)
    assert match is not None
    campaign_id = int(match.group(1))

    update = runner.invoke(
        cli_app,
        [
            "update-discovery-campaign",
            str(campaign_id),
            "--status",
            "paused",
            "--enabled",
            "false",
            "--seed-urls-json",
            "[\"https://example.com/updated.json\"]",
            "--crawl-policy-json",
            "{\"max_concurrency\": 4, \"crawl_delay_seconds\": 0}",
            "--metadata-json",
            "{\"owner\": \"cli\"}",
        ],
    )
    assert update.exit_code == 0, update.output
    assert "\"status\": \"paused\"" in update.output
    assert "\"enabled\": false" in update.output

    detail = client.get(f"/api/discovery/campaigns/{campaign_id}")
    assert detail.status_code == 200, detail.text
    campaign = detail.json()["campaign"]
    assert campaign["status"] == "paused"
    assert campaign["enabled"] is False
    assert campaign["seed_urls_json"] == ["https://example.com/updated.json"]
    assert campaign["crawl_policy_json"]["max_concurrency"] == 4
    assert campaign["crawl_policy_json"]["crawl_delay_seconds"] == 0
    assert campaign["crawl_policy_json"]["allow_private_networks"] is True
    assert campaign["metadata_json"]["owner"] == "cli"


def test_real_typer_discovery_maintenance_schedules(client: TestClient) -> None:
    runner = CliRunner()
    create = runner.invoke(
        cli_app,
        [
            "create-discovery-campaign",
            "cli-discovery-maintenance",
            "--mode",
            "seed_url",
            "--discovery-mode",
            "seed_url",
            "--seed",
            "https://example.com/feed.json",
            "--max-depth",
            "0",
            "--max-pages",
            "1",
        ],
    )
    assert create.exit_code == 0, create.output
    match = re.search(r"campaign=(\d+)", create.output)
    assert match is not None
    campaign_id = int(match.group(1))

    health_schedule = runner.invoke(
        cli_app,
        [
            "add-discovery-health-scan-schedule",
            "cli-discovery-health",
            "3600",
            "--campaign-id",
            str(campaign_id),
            "--limit",
            "25",
        ],
    )
    assert health_schedule.exit_code == 0, health_schedule.output
    assert "created for discovery health scans" in health_schedule.output

    revisit_schedule = runner.invoke(
        cli_app,
        [
            "add-discovery-revisit-schedule",
            "cli-discovery-revisit",
            "7200",
            "--campaign-id",
            str(campaign_id),
            "--force",
            "--priority",
            "42",
        ],
    )
    assert revisit_schedule.exit_code == 0, revisit_schedule.output
    assert "created for discovery revisits" in revisit_schedule.output

    session = get_session_factory()()
    try:
        tasks = list(
            session.query(ScheduledTaskORM)
            .filter(ScheduledTaskORM.name.in_(["cli-discovery-health", "cli-discovery-revisit"]))
            .order_by(ScheduledTaskORM.name.asc())
        )
        assert [task.task_type for task in tasks] == [
            "discovery_health_scan",
            "discovery_revisit",
        ]
        assert tasks[0].payload_json["campaign_id"] == campaign_id
        assert tasks[0].payload_json["limit"] == 25
        assert tasks[1].payload_json["campaign_id"] == campaign_id
        assert tasks[1].payload_json["force"] is True
        assert tasks[1].payload_json["priority"] == 42.0
    finally:
        session.close()


def test_discovery_domain_policy_patch_updates_fields_and_custody(
    client: TestClient,
) -> None:
    created = client.put(
        "/api/discovery/domain-policies",
        json={
            "normalized_domain": "policy.example.org",
            "robots_mode": "respect",
            "policy": "allow",
        },
    )
    assert created.status_code == 200, created.text

    patched = client.patch(
        "/api/discovery/domain-policies/policy.example.org",
        json={
            "policy": "deny",
            "robots_mode": "ignore",
            "enabled": False,
            "allow_subdomains": False,
            "max_concurrency": 3,
            "allowed_content_types_json": ["application/json"],
            "notes": "Escalated domain restriction",
        },
    )
    assert patched.status_code == 200, patched.text
    payload = patched.json()
    assert payload["policy"] == "deny"
    assert payload["robots_mode"] == "ignore"
    assert payload["enabled"] is False
    assert payload["allow_subdomains"] is False
    assert payload["max_concurrency"] == 3
    assert payload["allowed_content_types_json"] == ["application/json"]
    assert payload["notes"] == "Escalated domain restriction"

    listed = client.get(
        "/api/discovery/domain-policies",
        params={"domain": "policy.example.org", "limit": 5},
    )
    assert listed.status_code == 200
    rows = listed.json()
    assert len(rows) == 1
    assert rows[0]["normalized_domain"] == "policy.example.org"

    custody = client.get("/api/custody/logs")
    assert custody.status_code == 200
    assert any(
        row["object_type"] == "discovery_domain_policy"
        and row["action"] == "discovery_domain_policy_updated"
        and row["details_json"]["changes"]["policy"]["new"] == "deny"
        for row in custody.json()
    )


def test_real_typer_discovery_domain_policy_commands(client: TestClient) -> None:
    runner = CliRunner()
    upsert = runner.invoke(
        cli_app,
        [
            "upsert-discovery-domain-policy",
            "cli-policy.example.org",
            "--policy",
            "allow",
            "--robots-mode",
            "respect",
            "--max-concurrency",
            "2",
            "--allowed-content-types-json",
            "[\"application/json\"]",
            "--notes",
            "cli-created",
        ],
    )
    assert upsert.exit_code == 0, upsert.output
    assert "cli-policy.example.org" in upsert.output

    update = runner.invoke(
        cli_app,
        [
            "update-discovery-domain-policy",
            "cli-policy.example.org",
            "--policy",
            "deny",
            "--robots-mode",
            "ignore",
            "--enabled",
            "false",
            "--allow-subdomains",
            "false",
            "--notes",
            "cli-updated",
        ],
    )
    assert update.exit_code == 0, update.output
    assert "\"policy\": \"deny\"" in update.output
    assert "\"robots_mode\": \"ignore\"" in update.output

    listed = runner.invoke(
        cli_app,
        [
            "list-discovery-domain-policies",
            "--domain",
            "cli-policy.example.org",
            "--limit",
            "5",
        ],
    )
    assert listed.exit_code == 0, listed.output
    assert "cli-policy.example.org" in listed.output
    assert "deny" in listed.output

    verify = client.get(
        "/api/discovery/domain-policies",
        params={"domain": "cli-policy.example.org"},
    )
    assert verify.status_code == 200
    rows = verify.json()
    assert len(rows) == 1
    assert rows[0]["policy"] == "deny"
    assert rows[0]["robots_mode"] == "ignore"
    assert rows[0]["enabled"] is False
    assert rows[0]["allow_subdomains"] is False
