from __future__ import annotations

import socket

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.routes.research_fleet import router
from src.services.discovery_fetch import UnsafeTargetError
from src.services.research_fleet_service import (
    DEFAULT_PUBLIC_PROVIDER_REGISTRY,
    ProviderDefinition,
    admit_static_collection_url,
    deduplicate_candidate_urls,
    plan_research_fleet,
    source_health_and_gaps,
)


def _resolver(ip: str):
    def resolve(host: str, port: object, type: int):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, 0))]

    return resolve


def test_plan_is_deterministic_and_records_unavailable_source_class_gaps() -> None:
    one = plan_research_fleet(
        "  Harbor  logistics ", aliases=["North Harbor", "north harbor"], languages=["en"]
    )
    two = plan_research_fleet(
        "Harbor logistics", aliases=["north harbor", "North Harbor"], languages=["en"]
    )

    assert [task.task_id for task in one.tasks] == [task.task_id for task in two.tasks]
    assert one.normalized_query == "harbor logistics north harbor"
    assert one.coverage_ledger["searches"][0]["source_ids"] == sorted(
        task.provider_id for task in one.tasks
    )


def test_candidate_urls_are_canonicalized_and_globally_deduplicated() -> None:
    assert deduplicate_candidate_urls(
        ["https://EXAMPLE.com/a?b=2&a=1#fragment", "https://example.com/a?a=1&b=2"]
    ) == [{"canonical_url": "https://example.com/a?a=1&b=2", "domain": "example.com"}]


def test_static_collection_rejects_private_dns_and_allows_public_https() -> None:
    provider = DEFAULT_PUBLIC_PROVIDER_REGISTRY[0]
    with pytest.raises(UnsafeTargetError):
        admit_static_collection_url("https://public.example/article", provider=provider, resolver=_resolver("127.0.0.1"))

    decision = admit_static_collection_url(
        "https://PUBLIC.example/article#ignore", provider=provider, resolver=_resolver("93.184.216.34")
    )
    assert decision["canonical_url"] == "https://public.example/article"
    assert decision["max_response_bytes"] > 0


def test_health_reports_recorded_gaps_and_degraded_provider() -> None:
    plan = plan_research_fleet("public records")
    ledger = plan.coverage_ledger
    ledger["attempts"].append(
        {
            "source_id": "federal_register_api",
            "status": "failed",
            "source_kind": "official_public_record",
            "domain": "example.gov",
            "domain_class": "official",
        }
    )
    health = source_health_and_gaps(ledger_payload=ledger)

    assert next(item for item in health["providers"] if item["provider_id"] == "federal_register_api")["state"] == "degraded"
    assert health["coverage"]["attempt_count"] == 1


def test_provider_contract_rejects_login_required_source() -> None:
    with pytest.raises(ValueError, match="login"):
        ProviderDefinition(
            provider_id="nope",
            name="Nope",
            source_kind="other",
            capabilities=("static_html",),
            license_notes="n/a",
            jurisdiction="n/a",
            languages=("en",),
            freshness_hours=1,
            cost="free",
            access_requirement="login",
            robots_supported=True,
            evidence_capture_method="manifest",
        )


def test_routes_expose_plan_and_health_without_database_dependency() -> None:
    app = FastAPI()
    app.include_router(router, prefix="/api")
    client = TestClient(app)

    plan = client.post("/api/research-fleet/plan", json={"question": "Harbor records", "languages": ["en"]})
    assert plan.status_code == 200
    assert plan.json()["tasks"]

    providers = client.get("/api/research-fleet/providers")
    assert providers.status_code == 200
    assert providers.json()[0]["provider_id"] == "federal_register_api"
