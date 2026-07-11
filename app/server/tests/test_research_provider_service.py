from __future__ import annotations

from fastapi.testclient import TestClient

from src.services.discovery_fetch import FetchResult
from src.services import research_provider_service


def test_provider_run_is_idempotent_leased_and_collected_through_safe_boundary(
    client: TestClient, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    provider = client.post(
        "/api/research-fleet/configured-providers",
        json={
            "provider_key": "public_records_test",
            "name": "Public records test",
            "source_kind": "official_public_record",
            "capabilities_json": ["structured_api"],
            "metadata_json": {"endpoint_url": "https://public.example/api"},
        },
    )
    assert provider.status_code == 200, provider.text
    provider_id = provider.json()["research_provider_id"]
    run_payload = {
        "research_provider_id": provider_id,
        "idempotency_key": "research-provider:test:one",
        "normalized_query": "public records",
        "budget_json": {"max_requests": 1},
    }
    queued = client.post("/api/research-fleet/runs", json=run_payload)
    duplicate = client.post("/api/research-fleet/runs", json=run_payload)
    assert queued.status_code == duplicate.status_code == 200
    assert queued.json()["research_provider_run_id"] == duplicate.json()["research_provider_run_id"]

    monkeypatch.setattr(
        research_provider_service,
        "fetch_url",
        lambda _url, policy: FetchResult(
            requested_url="https://public.example/api",
            final_url="https://public.example/api",
            status_code=200,
            headers={"content-type": "application/json"},
            payload=b'{"results":["https://agency.example/record"]}',
            elapsed_ms=1.0,
            attempt_count=1,
        ),
    )
    claim = client.post("/api/research-fleet/runs/claim", json={"worker_id": "worker-a"})
    assert claim.status_code == 200, claim.text
    run_id = claim.json()["research_provider_run_id"]
    heartbeat = client.post(
        f"/api/research-fleet/runs/{run_id}/heartbeat", json={"worker_id": "worker-a"}
    )
    assert heartbeat.status_code == 200
    completed = client.post(
        f"/api/research-fleet/runs/{run_id}/execute", json={"worker_id": "worker-a"}
    )
    assert completed.status_code == 200, completed.text
    assert completed.json()["status"] == "completed"
    assert completed.json()["candidate_urls_json"] == ["https://agency.example/record"]
    assert completed.json()["response_hash"]


def test_official_bootstrap_is_idempotent_and_never_enrolls_keyed_govinfo(
    client: TestClient,
) -> None:
    first = client.post("/api/research-fleet/configured-providers/bootstrap-official")
    assert first.status_code == 200, first.text
    first_payload = first.json()
    assert [row["provider_key"] for row in first_payload["created"]] == ["federal_register_api"]
    provider = first_payload["created"][0]
    assert provider["access_requirement"] == "none"
    assert provider["metadata_json"]["endpoint_url"].startswith(
        "https://www.federalregister.gov/api/v1/"
    )
    assert first_payload["skipped"] == [
        {
            "provider_key": "govinfo_api",
            "reason": "api_key_required",
            "detail": "GovInfo API requires an operator-supplied api.data.gov key; no secret-free bootstrap is permitted.",
            "documentation_url": "https://www.govinfo.gov/developers",
        }
    ]

    second = client.post("/api/research-fleet/configured-providers/bootstrap-official")
    assert second.status_code == 200, second.text
    assert second.json()["created"] == []
    assert [row["research_provider_id"] for row in second.json()["existing"]] == [
        provider["research_provider_id"]
    ]

    providers = client.get("/api/research-fleet/configured-providers")
    assert providers.status_code == 200
    assert [row["provider_key"] for row in providers.json()] == ["federal_register_api"]
