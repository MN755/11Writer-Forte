from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from uuid import uuid4

from sqlalchemy.orm import Session

from src.config import Settings, get_settings
from src.models import CustodyLogORM
from src.services.storage_service import register_storage_object


class CodexAgentError(ValueError):
    pass


@dataclass(frozen=True)
class CodexAgentResult:
    model: str
    reasoning_effort: str
    report_path: Path
    storage_object_id: int
    command: list[str]
    output_text: str


def resolve_codex_cli_path(settings: Settings) -> str:
    if settings.codex_cli_path is not None:
        return str(settings.codex_cli_path)
    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        bundled_cli = Path(local_app_data) / "OpenAI" / "Codex" / "bin" / "codex.exe"
        if bundled_cli.is_file():
            return str(bundled_cli)
    return "codex"


def build_research_prompt(objective: str) -> str:
    normalized_objective = objective.strip()
    if not normalized_objective:
        raise CodexAgentError("Research objective must not be empty.")
    if len(normalized_objective) > 12000:
        raise CodexAgentError("Research objective must be 12,000 characters or fewer.")
    return f"""You are 11Writer Forte's headless research analyst.

Objective:
{normalized_objective}

Operating contract:
- Use the elevenwriter-forte MCP tools to inspect local Forte evidence before drawing conclusions.
- Treat Forte's rule-based records, source metadata, custody records, and retained artifacts as the system of record.
- Do not create, modify, or delete sources, schedules, alerts, files, or external resources.
- Do not present unsupported claims as facts. Separate verified evidence, informed assessment, and unknowns.
- Return a compact analyst briefing with: answer, evidence used, uncertainty, and recommended deterministic next actions.
- Cite Forte record IDs and source URIs whenever available.
"""


def build_codex_exec_command(
    objective: str,
    *,
    settings: Settings | None = None,
    model: str | None = None,
    reasoning_effort: str | None = None,
) -> list[str]:
    resolved_settings = settings or get_settings()
    selected_model = (model or resolved_settings.codex_model).strip()
    selected_effort = (reasoning_effort or resolved_settings.codex_reasoning_effort).strip()
    if not selected_model:
        raise CodexAgentError("A Codex model must be configured.")
    if selected_effort not in {"low", "medium", "high"}:
        raise CodexAgentError("Codex reasoning effort must be low, medium, or high.")

    cli_path = resolve_codex_cli_path(resolved_settings)
    server_root = Path(__file__).resolve().parents[2]
    return [
        cli_path,
        "exec",
        "--model",
        selected_model,
        "--config",
        f'model_reasoning_effort="{selected_effort}"',
        "--sandbox",
        "read-only",
        "--ask-for-approval",
        "never",
        "--cd",
        str(server_root),
        build_research_prompt(objective),
    ]


def build_codex_mcp_add_command(settings: Settings | None = None) -> list[str]:
    resolved_settings = settings or get_settings()
    server_root = Path(__file__).resolve().parents[2]
    return [
        resolve_codex_cli_path(resolved_settings),
        "mcp",
        "add",
        "elevenwriter-forte",
        "--env",
        f"PYTHONPATH={server_root}",
        "--",
        sys.executable,
        "-m",
        "src.codex_mcp_server",
    ]


def register_codex_mcp(settings: Settings | None = None) -> str:
    command = build_codex_mcp_add_command(settings)
    try:
        completed = subprocess.run(command, capture_output=True, text=True, check=False)
    except FileNotFoundError as exc:
        raise CodexAgentError(
            "Codex CLI was not found. Set ELEVENWRITER_CODEX_CLI_PATH to the local codex.exe."
        ) from exc
    if completed.returncode != 0:
        error_text = (completed.stderr or completed.stdout or "Codex MCP registration failed.").strip()
        raise CodexAgentError(error_text[:2000])
    return (completed.stdout or "MCP server registered.").strip()


def run_codex_research(
    session: Session,
    objective: str,
    *,
    settings: Settings | None = None,
    model: str | None = None,
    reasoning_effort: str | None = None,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> CodexAgentResult:
    resolved_settings = settings or get_settings()
    command = build_codex_exec_command(
        objective,
        settings=resolved_settings,
        model=model,
        reasoning_effort=reasoning_effort,
    )
    try:
        completed = runner(
            command,
            capture_output=True,
            text=True,
            timeout=resolved_settings.codex_timeout_seconds,
            check=False,
        )
    except FileNotFoundError as exc:
        raise CodexAgentError(
            "Codex CLI was not found. Set ELEVENWRITER_CODEX_CLI_PATH to the local codex.exe."
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise CodexAgentError(
            f"Codex research exceeded {resolved_settings.codex_timeout_seconds:g} seconds."
        ) from exc

    if completed.returncode != 0:
        error_text = (completed.stderr or completed.stdout or "Codex exited without an error message.").strip()
        raise CodexAgentError(f"Codex research failed: {error_text[:2000]}")

    output_text = (completed.stdout or "").strip()
    if not output_text:
        raise CodexAgentError("Codex completed without a research briefing.")
    output_text = output_text[: resolved_settings.codex_report_max_chars]
    report_path = write_report(output_text, resolved_settings.data_dir)
    digest = hashlib.sha256(report_path.read_bytes()).hexdigest()
    object_key = f"agent_reports/{report_path.name}"
    storage_record = register_storage_object(
        session,
        object_key=object_key,
        object_kind="codex_research_briefing",
        owner_type="codex_agent",
        owner_id=report_path.stem,
        object_uri=report_path.resolve().as_uri(),
        content_hash=digest,
        media_type="text/markdown",
        retention_class="investigative",
        byte_size=report_path.stat().st_size,
        metadata_json={
            "model": model or resolved_settings.codex_model,
            "reasoning_effort": reasoning_effort or resolved_settings.codex_reasoning_effort,
            "objective": objective.strip(),
            "command_mode": "read_only",
        },
        actor="codex_agent",
    )
    session.add(
        CustodyLogORM(
            object_type="codex_research_report",
            object_id=report_path.stem,
            action="codex_research_completed",
            actor="codex_agent",
            details_json={
                "storage_object_id": storage_record.storage_object_id,
                "model": model or resolved_settings.codex_model,
                "reasoning_effort": reasoning_effort or resolved_settings.codex_reasoning_effort,
            },
        )
    )
    session.commit()
    session.refresh(storage_record)
    return CodexAgentResult(
        model=model or resolved_settings.codex_model,
        reasoning_effort=reasoning_effort or resolved_settings.codex_reasoning_effort,
        report_path=report_path,
        storage_object_id=storage_record.storage_object_id,
        command=command,
        output_text=output_text,
    )


def write_report(output_text: str, data_dir: Path) -> Path:
    report_dir = data_dir / "agent_reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_path = report_dir / f"codex-research-{timestamp}-{uuid4().hex[:8]}.md"
    report_path.write_text(output_text + "\n", encoding="utf-8")
    return report_path
