from fastapi.testclient import TestClient
from typer.testing import CliRunner

from src.cli import app as cli_app


def test_manual_alert_api_writes_custody_and_supports_filters(client: TestClient) -> None:
    created = client.post(
        "/api/alerts",
        json={
            "event_id": 42,
            "severity": "warning",
            "status": "open",
            "dedupe_key": "manual:42",
            "message": "Manual analyst alert",
            "trigger_basis_json": {"source": "analyst"},
        },
    )
    assert created.status_code == 200, created.text
    payload = created.json()
    assert payload["message"] == "Manual analyst alert"

    updated = client.patch(
        f"/api/alerts/{payload['alert_id']}",
        json={
            "status": "acknowledged",
            "severity": "critical",
            "disposition_note": "Escalated by operator",
        },
    )
    assert updated.status_code == 200, updated.text
    assert updated.json()["status"] == "acknowledged"
    assert updated.json()["severity"] == "critical"

    filtered = client.get(
        "/api/alerts",
        params={"status": "acknowledged", "event_id": 42},
    )
    assert filtered.status_code == 200
    rows = filtered.json()
    assert len(rows) == 1
    assert rows[0]["alert_id"] == payload["alert_id"]

    custody = client.get(
        "/api/custody/logs",
        params={
            "object_type": "alert",
            "object_id": str(payload["alert_id"]),
            "limit": 10,
        },
    )
    assert custody.status_code == 200
    actions = {(row["action"], row["actor"]) for row in custody.json()}
    assert ("alert_created", "api_alert") in actions
    assert ("alert_updated", "api_alert") in actions


def test_real_typer_create_list_and_update_alerts(client: TestClient) -> None:
    runner = CliRunner()
    created = runner.invoke(
        cli_app,
        [
            "create-alert",
            "CLI alert",
            "--severity",
            "warning",
            "--status",
            "open",
            "--event-id",
            "7",
            "--dedupe-key",
            "cli:7",
            "--trigger-basis-json",
            '{"origin":"cli"}',
        ],
    )
    assert created.exit_code == 0, created.output
    assert "severity=warning" in created.output

    listed = runner.invoke(
        cli_app,
        ["list-alerts", "--status", "open", "--event-id", "7"],
    )
    assert listed.exit_code == 0, listed.output
    assert "CLI alert" in listed.output

    alerts = client.get("/api/alerts", params={"event_id": 7})
    assert alerts.status_code == 200
    alert_id = alerts.json()[0]["alert_id"]

    updated = runner.invoke(
        cli_app,
        [
            "update-alert",
            str(alert_id),
            "closed",
            "--disposition-note",
            "Resolved in CLI",
            "--severity",
            "info",
        ],
    )
    assert updated.exit_code == 0, updated.output
    assert "status=closed" in updated.output

    custody = client.get(
        "/api/custody/logs",
        params={
            "object_type": "alert",
            "object_id": str(alert_id),
            "actor": "cli",
            "limit": 10,
        },
    )
    assert custody.status_code == 200
    cli_actions = {row["action"] for row in custody.json()}
    assert "alert_created" in cli_actions
    assert "alert_updated" in cli_actions
