from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from src.db import get_session_factory
from src.services.provider_registry_service import evaluate_provider_request


PROVIDER_PATH = "/api/discovery/providers"


def _headers(scope: str) -> dict[str, str]:
    return {
        "X-ElevenWriter-Scopes": scope,
        "X-ElevenWriter-Actor": "provider-registry-test",
    }


def _payload(provider_key: str = "synthetic-rss") -> dict[str, object]:
    return {
        "provider_key": provider_key,
        "display_name": "Synthetic public RSS",
        "provider_kind": "rss_atom",
        "capabilities_json": ["fetch_feed", "source_health"],
        "base_urls_json": ["https://feeds.example.test/public"],
        "access_mode": "public_no_login",
        "terms_url": "https://feeds.example.test/terms",
        "license_note": "Synthetic fixture data; no network access is performed.",
        "robots_mode": "required",
        "jurisdictions_json": ["test-jurisdiction"],
        "languages_json": ["en"],
        "request_budget_json": {
            "max_requests_per_run": 12,
            "max_requests_per_day": 60,
            "max_concurrency": 1,
            "max_response_bytes": 4096,
            "request_timeout_seconds": 2.0,
            "retry_ceiling": 1,
        },
        "artifact_capture_mode": "metadata_only",
        "health_status": "healthy",
    }


def _create_provider(client: TestClient, provider_key: str = "synthetic-rss") -> dict[str, object]:
    response = client.post(PROVIDER_PATH, json=_payload(provider_key), headers=_headers("operate"))
    assert response.status_code == 200, response.text
    return response.json()


@contextmanager
def _session() -> Iterator[Session]:
    session = get_session_factory()()
    try:
        yield session
    finally:
        session.close()


def test_provider_creation_is_disabled_by_default(client: TestClient) -> None:
    created = _create_provider(client)

    assert created["enabled"] is False
    assert created["approved_by"] is None
    assert created["disabled_reason"] == "awaiting_explicit_admin_enable"
    assert created["created_by"] == "provider-registry-test"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("capabilities_json", ["not-a-capability"]),
        ("base_urls_json", ["https://*.example.test/public"]),
        ("base_urls_json", ["http://user:pass@example.test/public"]),
        ("base_urls_json", ["file:///synthetic-public-feed.xml"]),
        ("base_urls_json", ["http://127.0.0.1/public"]),
        ("terms_url", "file:///synthetic-terms.txt"),
    ],
)
def test_provider_creation_rejects_invalid_capabilities_and_public_origins(
    client: TestClient, field: str, value: object
) -> None:
    payload = _payload("invalid-provider")
    payload[field] = value

    response = client.post(PROVIDER_PATH, json=payload, headers=_headers("operate"))

    assert response.status_code == 422


@pytest.mark.parametrize("missing_field", ["terms_url", "license_note", "base_urls_json"])
def test_provider_creation_requires_terms_license_and_origin(
    client: TestClient, missing_field: str
) -> None:
    payload = _payload("incomplete-provider")
    del payload[missing_field]

    response = client.post(PROVIDER_PATH, json=payload, headers=_headers("operate"))

    assert response.status_code == 422


@pytest.mark.parametrize("secret_field", ["api_key", "authorization", "password"])
def test_provider_creation_rejects_secret_like_configuration_fields(
    client: TestClient, secret_field: str
) -> None:
    payload = _payload("secret-field-provider")
    payload[secret_field] = "synthetic-secret-must-not-persist"

    response = client.post(PROVIDER_PATH, json=payload, headers=_headers("operate"))

    assert response.status_code == 422


def test_provider_routes_enforce_read_operate_and_admin_scopes(client: TestClient) -> None:
    payload = _payload("scoped-provider")

    assert client.get(PROVIDER_PATH).status_code == 401
    assert client.post(PROVIDER_PATH, json=payload, headers=_headers("read")).status_code == 403

    created = _create_provider(client, "scoped-provider")
    provider_id = created["provider_id"]

    assert client.get(PROVIDER_PATH, headers=_headers("read")).status_code == 200
    assert (
        client.patch(
            f"{PROVIDER_PATH}/{provider_id}",
            json={"display_name": "Mutation attempt"},
            headers=_headers("read"),
        ).status_code
        == 403
    )
    assert (
        client.post(f"{PROVIDER_PATH}/{provider_id}/enable", headers=_headers("operate")).status_code
        == 403
    )

    enabled = client.post(
        f"{PROVIDER_PATH}/{provider_id}/enable", headers=_headers("admin")
    )
    assert enabled.status_code == 200, enabled.text
    assert enabled.json()["enabled"] is True
    assert enabled.json()["approved_by"] == "provider-registry-test"


def test_pause_immediately_blocks_an_enabled_provider(client: TestClient) -> None:
    created = _create_provider(client, "pausable-provider")
    provider_id = created["provider_id"]
    assert client.post(f"{PROVIDER_PATH}/{provider_id}/enable", headers=_headers("admin")).status_code == 200

    paused = client.post(
        f"{PROVIDER_PATH}/{provider_id}/pause",
        json={"reason": "synthetic maintenance window"},
        headers=_headers("operate"),
    )

    assert paused.status_code == 200, paused.text
    assert paused.json()["enabled"] is False
    assert paused.json()["paused_at"] is not None
    assert paused.json()["disabled_reason"] == "synthetic maintenance window"


def test_policy_decisions_report_disabled_paused_and_url_capability_reasons(
    client: TestClient,
) -> None:
    created = _create_provider(client, "policy-provider")
    provider_id = int(created["provider_id"])

    with _session() as session:
        disabled = evaluate_provider_request(
            session,
            provider_id=provider_id,
            url="https://feeds.example.test/public/feed.xml",
            capability="fetch_feed",
        )
    assert disabled.allowed is False
    assert disabled.reason_code == "provider_disabled"

    assert client.post(f"{PROVIDER_PATH}/{provider_id}/enable", headers=_headers("admin")).status_code == 200
    with _session() as session:
        unsupported = evaluate_provider_request(
            session,
            provider_id=provider_id,
            url="https://feeds.example.test/public/feed.xml",
            capability="search",
        )
        unsafe = evaluate_provider_request(
            session,
            provider_id=provider_id,
            url="http://127.0.0.1/private-feed.xml",
            capability="fetch_feed",
        )
        disallowed_origin = evaluate_provider_request(
            session,
            provider_id=provider_id,
            url="https://other.example.test/public/feed.xml",
            capability="fetch_feed",
        )
        allowed = evaluate_provider_request(
            session,
            provider_id=provider_id,
            url="https://feeds.example.test/public/feed.xml?synthetic=true",
            capability="fetch_feed",
        )

    assert unsupported.reason_code == "unsupported_capability"
    assert unsafe.reason_code == "unsafe_target"
    assert disallowed_origin.reason_code == "origin_not_allowed"
    assert allowed.allowed is True
    assert allowed.reason_code == "allowed"
    assert allowed.applied_budget["max_requests_per_run"] == 12

    paused = client.post(
        f"{PROVIDER_PATH}/{provider_id}/pause",
        json={"reason": "test pause"},
        headers=_headers("operate"),
    )
    assert paused.status_code == 200, paused.text
    with _session() as session:
        decision = evaluate_provider_request(
            session,
            provider_id=provider_id,
            url="https://feeds.example.test/public/feed.xml",
            capability="fetch_feed",
        )
    assert decision.allowed is False
    assert decision.reason_code == "domain_paused"


def test_provider_snapshot_round_trip(client: TestClient) -> None:
    created = _create_provider(client, "snapshot-provider")

    exported = client.get("/api/operations/runtime/export")
    assert exported.status_code == 200, exported.text
    snapshot = exported.json()
    assert [item["provider_key"] for item in snapshot["research_providers"]] == ["snapshot-provider"]

    restored = client.post(
        "/api/operations/runtime/restore",
        params={"replace_existing": "true"},
        json=snapshot,
    )
    assert restored.status_code == 200, restored.text

    fetched = client.get(
        f"{PROVIDER_PATH}/{created['provider_id']}", headers=_headers("read")
    )
    assert fetched.status_code == 200, fetched.text
    assert fetched.json()["provider_key"] == "snapshot-provider"
    assert fetched.json()["enabled"] is False
