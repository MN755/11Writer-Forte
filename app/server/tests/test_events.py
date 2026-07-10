from fastapi.testclient import TestClient
from typer.testing import CliRunner

from src.cli import app as cli_app


def test_create_event_writes_custody_and_rejects_duplicate_slug(
    client: TestClient,
) -> None:
    create = client.post(
        "/api/events",
        json={
            "slug": "manual-harbor-incident",
            "title": "Manual Harbor Incident",
            "summary": "Operator-created event for follow-up.",
            "occurred_at": "2026-07-09T12:00:00Z",
            "status": "open",
            "redaction_level": "confidential",
            "metadata_json": {"case_id": "HF-1"},
        },
    )
    assert create.status_code == 200, create.text
    payload = create.json()
    assert payload["slug"] == "manual-harbor-incident"

    duplicate = client.post(
        "/api/events",
        json={
            "slug": "manual-harbor-incident",
            "title": "Duplicate Harbor Incident",
        },
    )
    assert duplicate.status_code == 409
    assert "already exists" in duplicate.json()["detail"]

    custody = client.get(
        "/api/custody/logs",
        params={
            "object_type": "event",
            "object_id": str(payload["event_id"]),
            "action": "event_created",
            "actor": "api_event",
            "limit": 5,
        },
    )
    assert custody.status_code == 200
    rows = custody.json()
    assert len(rows) == 1
    assert rows[0]["details_json"]["slug"] == "manual-harbor-incident"
    assert rows[0]["details_json"]["metadata_json"]["case_id"] == "HF-1"


def test_real_typer_create_and_list_events(client: TestClient) -> None:
    runner = CliRunner()
    create = runner.invoke(
        cli_app,
        [
            "create-event",
            "cli-delta-watch",
            "CLI Delta Watch",
            "--summary",
            "CLI-managed event",
            "--occurred-at",
            "2026-07-10T08:30:00Z",
            "--status",
            "review",
            "--redaction-level",
            "restricted",
            "--metadata-json",
            '{"owner":"cli"}',
        ],
    )
    assert create.exit_code == 0, create.output
    assert "cli-delta-watch" in create.output

    listed = runner.invoke(cli_app, ["list-events"])
    assert listed.exit_code == 0, listed.output
    assert "cli-delta-watch" in listed.output
    assert "CLI Delta Watch" in listed.output

    events = client.get("/api/events")
    assert events.status_code == 200
    assert any(
        row["slug"] == "cli-delta-watch"
        and row["status"] == "review"
        and row["redaction_level"] == "restricted"
        and row["metadata_json"]["owner"] == "cli"
        for row in events.json()
    )
