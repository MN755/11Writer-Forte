from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from src.cli import app
from src.config import reset_settings_cache
from src.db import get_session_factory, init_db, reset_db_state
from src.schemas import ScheduledTaskCreate
from src.services.scheduler_service import create_scheduled_task


@pytest.fixture()
def cli_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> CliRunner:
    database_path = tmp_path / "runtime-status.db"
    data_path = tmp_path / "runtime-status"
    monkeypatch.setenv("ELEVENWRITER_DATABASE_URL", f"sqlite:///{database_path.as_posix()}")
    monkeypatch.setenv("ELEVENWRITER_DATA_DIR", str(data_path))
    reset_settings_cache()
    reset_db_state()
    runner = CliRunner()
    yield runner
    reset_db_state()
    reset_settings_cache()


def test_cli_status_reports_effective_runtime_paths(cli_env: CliRunner, tmp_path: Path) -> None:
    runtime_root = (tmp_path / "runtime-status").resolve()
    database_path = (tmp_path / "runtime-status.db").resolve()

    result = cli_env.invoke(app, ["status"])

    assert result.exit_code == 0
    assert f"database: sqlite:///{database_path.as_posix()}" in result.stdout
    assert f"data dir: {runtime_root}" in result.stdout
    assert f"storage archive dir: {runtime_root / 'artifacts' / 'archive'}" in result.stdout
    assert f"storage rehydrate dir: {runtime_root / 'artifacts' / 'rehydrated'}" in result.stdout


def test_cli_doctor_and_readiness_initialize_clean_local_runtime(cli_env: CliRunner) -> None:
    doctor_result = cli_env.invoke(app, ["doctor"])
    readiness_result = cli_env.invoke(app, ["show-runtime-readiness"])

    assert doctor_result.exit_code == 0
    assert "status=ok backend=sqlite connected=True spatial=python" in doctor_result.stdout
    assert "table_counts:" in doctor_result.stdout

    assert readiness_result.exit_code == 0
    assert "status=not_ready ready=False" in readiness_result.stdout
    assert "checks:" in readiness_result.stdout


def test_cli_readiness_reports_broken_local_import_targets(cli_env: CliRunner, tmp_path: Path) -> None:
    missing_fixture = tmp_path / "missing-runtime-import.json"
    init_db()
    session = get_session_factory()()
    try:
        create_scheduled_task(
            session,
            ScheduledTaskCreate(
                name="cli-readiness-broken-local-import",
                task_type="local_import",
                interval_seconds=300,
                target_path=str(missing_fixture),
                layer_key="cli-runtime-layer",
            ),
        )
    finally:
        session.close()

    readiness_result = cli_env.invoke(app, ["show-runtime-readiness"])

    assert readiness_result.exit_code == 0
    assert "local_import_target_health: action_required" in readiness_result.stdout
