from __future__ import annotations

from fastapi.testclient import TestClient
from typer.testing import CliRunner

from src.cli import app as cli_app


def test_custody_api_supports_filters(client: TestClient) -> None:
    created = client.post(
        "/api/source-trust/profiles",
        json={
            "domain": "custody-filter.example.org",
            "trust_level": "trusted",
            "approval_policy": "auto_approve_stable",
        },
    )
    assert created.status_code == 200
    trust_profile_id = created.json()["trust_profile_id"]

    seeded = client.post("/api/source-trust/seed-defaults")
    assert seeded.status_code == 200

    filtered = client.get(
        "/api/custody/logs",
        params={
            "object_type": "source_trust_profile",
            "action": "source_trust_profile_created",
            "actor": "api_source_trust",
            "limit": 10,
        },
    )
    assert filtered.status_code == 200
    rows = filtered.json()
    assert rows
    assert all(row["object_type"] == "source_trust_profile" for row in rows)
    assert all(row["action"] == "source_trust_profile_created" for row in rows)
    assert all(row["actor"] == "api_source_trust" for row in rows)
    assert any(row["object_id"] == str(trust_profile_id) for row in rows)

    exact = client.get(
        "/api/custody/logs",
        params={
            "object_type": "source_trust_profile",
            "object_id": str(trust_profile_id),
            "limit": 5,
        },
    )
    assert exact.status_code == 200
    exact_rows = exact.json()
    assert len(exact_rows) == 1
    assert exact_rows[0]["action"] == "source_trust_profile_created"

    seed_logs = client.get(
        "/api/custody/logs",
        params={
            "object_type": "source_trust_seed",
            "action": "integrity_sources_seeded",
            "limit": 5,
        },
    )
    assert seed_logs.status_code == 200
    assert len(seed_logs.json()) == 1


def test_real_typer_list_custody_supports_filters(client: TestClient) -> None:
    created = client.post(
        "/api/source-trust/profiles",
        json={
            "domain": "cli-custody.example.org",
            "trust_level": "trusted",
            "approval_policy": "auto_approve_stable",
        },
    )
    assert created.status_code == 200
    trust_profile_id = created.json()["trust_profile_id"]

    runner = CliRunner()
    result = runner.invoke(
        cli_app,
        [
            "list-custody",
            "--object-type",
            "source_trust_profile",
            "--object-id",
            str(trust_profile_id),
            "--action",
            "source_trust_profile_created",
            "--limit",
            "5",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "source_trust_profile_created" in result.output
    assert f"| {trust_profile_id} |" in result.output
