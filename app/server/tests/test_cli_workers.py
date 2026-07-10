from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from typer.testing import CliRunner

from src.cli import app
from src.config import reset_settings_cache
from src.db import get_session_factory, init_db, reset_db_state
from src.services.worker_status_service import mark_worker_started


@pytest.fixture()
def cli_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> CliRunner:
    database_path = tmp_path / "test.db"
    data_path = tmp_path / "var"
    monkeypatch.setenv("ELEVENWRITER_DATABASE_URL", f"sqlite:///{database_path}")
    monkeypatch.setenv("ELEVENWRITER_DATA_DIR", str(data_path))
    reset_settings_cache()
    reset_db_state()
    init_db()
    runner = CliRunner()
    yield runner
    reset_db_state()
    reset_settings_cache()


def test_cli_check_worker_health_passes_for_recent_active_worker(cli_env: CliRunner) -> None:
    session = get_session_factory()()
    try:
        mark_worker_started(
            session,
            worker_key="platform_runtime_worker:test:all",
            worker_type="platform_runtime_worker",
            actor="test-worker",
            process_token="token-1",
            metadata_json={"include_stream_runtime": True, "include_enabled_schedules": True},
        )
    finally:
        session.close()

    result = cli_env.invoke(
        app,
        [
            "check-worker-health",
            "--worker-type",
            "platform_runtime_worker",
            "--actor",
            "test-worker",
            "--require-active",
        ],
    )
    assert result.exit_code == 0
    assert "healthy=True" in result.stdout


def test_cli_check_worker_health_fails_for_stale_worker(cli_env: CliRunner) -> None:
    session = get_session_factory()()
    try:
        worker = mark_worker_started(
            session,
            worker_key="platform_runtime_worker:test:all",
            worker_type="platform_runtime_worker",
            actor="test-worker",
            process_token="token-2",
            metadata_json={"include_stream_runtime": True, "include_enabled_schedules": True},
        )
        worker.last_seen_at = worker.last_seen_at - timedelta(hours=2)
        session.commit()
    finally:
        session.close()

    result = cli_env.invoke(
        app,
        [
            "check-worker-health",
            "--worker-type",
            "platform_runtime_worker",
            "--actor",
            "test-worker",
            "--stale-after-seconds",
            "30",
            "--require-active",
        ],
    )
    assert result.exit_code == 1
    assert "healthy=False" in result.stdout
