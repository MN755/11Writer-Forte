from __future__ import annotations

import json
from subprocess import CompletedProcess

from fastapi.testclient import TestClient
from sqlalchemy import select
from typer.testing import CliRunner

from src.cli import app as cli_app
from src.codex_mcp_server import handle_message
from src.db import get_session_factory
from src.models import CustodyLogORM, StorageObjectORM
from src.services.codex_agent_service import run_codex_research


def test_codex_research_is_read_only_and_tracks_local_briefing(client: TestClient) -> None:
    captured: list[str] = []

    def fake_runner(command: list[str], **_: object) -> CompletedProcess[str]:
        captured.extend(command)
        return CompletedProcess(command, 0, stdout="# Briefing\n\nEvidence-backed summary.", stderr="")

    session = get_session_factory()()
    try:
        result = run_codex_research(
            session,
            "Summarize current local alerts.",
            runner=fake_runner,
        )
    finally:
        session.close()

    assert "--sandbox" in captured
    assert captured[captured.index("--sandbox") + 1] == "read-only"
    assert "--ask-for-approval" in captured
    assert captured[captured.index("--ask-for-approval") + 1] == "never"
    assert 'model_reasoning_effort="medium"' in captured
    assert result.report_path.exists()
    assert result.report_path.read_text(encoding="utf-8").startswith("# Briefing")

    session = get_session_factory()()
    try:
        stored = session.get(StorageObjectORM, result.storage_object_id)
        assert stored is not None
        assert stored.object_kind == "codex_research_briefing"
        custody = list(
            session.scalars(
                select(CustodyLogORM).where(CustodyLogORM.object_type == "codex_research_report")
            )
        )
    finally:
        session.close()
    assert len(custody) == 1
    assert custody[0].action == "codex_research_completed"


def test_mcp_server_exposes_read_only_runtime_evidence(client: TestClient) -> None:
    created = client.post(
        "/api/alerts",
        json={"message": "MCP test alert", "severity": "warning", "trigger_basis_json": {}},
    )
    assert created.status_code == 200

    listed = handle_message({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    assert listed is not None
    tool_names = {tool["name"] for tool in listed["result"]["tools"]}
    assert "forte_list_alerts" in tool_names
    assert all("Read-only" in tool["description"] for tool in listed["result"]["tools"])

    response = handle_message(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "forte_list_alerts", "arguments": {"status": "open"}},
        }
    )
    assert response is not None
    rows = json.loads(response["result"]["content"][0]["text"])
    assert rows[0]["message"] == "MCP test alert"


def test_cli_exposes_the_bounded_codex_command(client: TestClient) -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli_app,
        [
            "show-codex-agent-command",
            "Summarize local evidence",
            "--model",
            "gpt-5.4-mini",
            "--reasoning-effort",
            "medium",
        ],
    )
    assert result.exit_code == 0, result.output
    command = json.loads(result.output)
    assert command[1] == "exec"
    assert command[command.index("--sandbox") + 1] == "read-only"
    assert command[command.index("--model") + 1] == "gpt-5.4-mini"
