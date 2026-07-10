from fastapi.testclient import TestClient
from typer.testing import CliRunner

from src.cli import app as cli_app
from src.services.trust_service import normalize_domain


def test_seed_default_integrity_sources(client: TestClient) -> None:
    seed_response = client.post("/api/source-trust/seed-defaults")
    assert seed_response.status_code == 200
    assert seed_response.json()["created"] >= 4

    list_response = client.get("/api/source-trust/profiles")
    assert list_response.status_code == 200
    domains = [item["domain"] for item in list_response.json()]
    assert "nytimes.com" in domains
    assert "npr.org" in domains

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    custody_rows = custody_response.json()
    assert any(row["action"] == "integrity_sources_seeded" for row in custody_rows)
    assert any(
        row["object_type"] == "source_trust_profile"
        and row["action"] == "source_trust_profile_created"
        for row in custody_rows
    )


def test_create_source_trust_profile_writes_custody_and_rejects_duplicate(
    client: TestClient,
) -> None:
    create_response = client.post(
        "/api/source-trust/profiles",
        json={
            "domain": "WWW.Example.GOV:443",
            "trust_level": "trusted",
            "approval_policy": "auto_approve_stable",
            "integrity_source": True,
            "notes": "operator-entered profile",
        },
    )
    assert create_response.status_code == 200
    profile = create_response.json()
    assert profile["domain"] == "example.gov"

    duplicate_response = client.post(
        "/api/source-trust/profiles",
        json={
            "domain": "https://example.gov/feed.json",
            "trust_level": "trusted",
            "approval_policy": "auto_approve_stable",
        },
    )
    assert duplicate_response.status_code == 409
    assert "already exists" in duplicate_response.json()["detail"]

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    assert any(
        row["object_type"] == "source_trust_profile"
        and row["action"] == "source_trust_profile_created"
        and row["details_json"]["domain"] == "example.gov"
        for row in custody_response.json()
    )


def test_real_typer_add_trust_profile(client: TestClient) -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli_app,
        [
            "add-trust-profile",
            "newsroom.example.org",
            "--trust-level",
            "trusted",
            "--approval-policy",
            "auto_approve_stable",
            "--integrity-source",
            "--notes",
            "cli-managed",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "newsroom.example.org" in result.output


def test_update_source_trust_profile_writes_custody_and_rejects_duplicate(
    client: TestClient,
) -> None:
    first = client.post(
        "/api/source-trust/profiles",
        json={
            "domain": "alpha.example.org",
            "trust_level": "neutral",
            "approval_policy": "manual_review",
        },
    )
    assert first.status_code == 200
    second = client.post(
        "/api/source-trust/profiles",
        json={
            "domain": "beta.example.org",
            "trust_level": "neutral",
            "approval_policy": "manual_review",
        },
    )
    assert second.status_code == 200

    patch = client.patch(
        f"/api/source-trust/profiles/{first.json()['trust_profile_id']}",
        json={
            "domain": "https://WWW.Alpha-Updated.example.org/feed",
            "trust_level": "trusted",
            "approval_policy": "auto_approve_stable",
            "integrity_source": True,
            "notes": "updated by operator",
        },
    )
    assert patch.status_code == 200, patch.text
    payload = patch.json()
    assert payload["domain"] == "alpha-updated.example.org"
    assert payload["trust_level"] == "trusted"
    assert payload["approval_policy"] == "auto_approve_stable"
    assert payload["integrity_source"] is True

    duplicate = client.patch(
        f"/api/source-trust/profiles/{first.json()['trust_profile_id']}",
        json={"domain": "beta.example.org"},
    )
    assert duplicate.status_code == 409
    assert "already exists" in duplicate.json()["detail"]

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    assert any(
        row["object_type"] == "source_trust_profile"
        and row["action"] == "source_trust_profile_updated"
        and row["details_json"]["changes"]["domain"]["new"] == "alpha-updated.example.org"
        for row in custody_response.json()
    )


def test_real_typer_update_trust_profile(client: TestClient) -> None:
    created = client.post(
        "/api/source-trust/profiles",
        json={
            "domain": "cli-update.example.org",
            "trust_level": "neutral",
            "approval_policy": "manual_review",
        },
    )
    assert created.status_code == 200
    trust_profile_id = created.json()["trust_profile_id"]

    runner = CliRunner()
    result = runner.invoke(
        cli_app,
        [
            "update-trust-profile",
            str(trust_profile_id),
            "--trust-level",
            "blocked",
            "--approval-policy",
            "always_review",
            "--notes",
            "cli-updated",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "blocked" in result.output
    assert "always_review" in result.output

    list_response = client.get("/api/source-trust/profiles")
    assert list_response.status_code == 200
    updated = next(
        row for row in list_response.json() if row["trust_profile_id"] == trust_profile_id
    )
    assert updated["trust_level"] == "blocked"
    assert updated["approval_policy"] == "always_review"
    assert updated["notes"] == "cli-updated"


def test_normalize_domain_strips_ports_and_credentials() -> None:
    assert normalize_domain("https://user:pass@127.0.0.1:54837/feed.xml") == "127.0.0.1"
    assert normalize_domain("example.com:8443") == "example.com"
