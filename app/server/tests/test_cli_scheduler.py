from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from src.cli import app
from src.config import reset_settings_cache
from src.db import get_session_factory, reset_db_state
from src.models import ScheduledTaskORM


@pytest.fixture()
def cli_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> CliRunner:
    database_path = tmp_path / "test.db"
    data_path = tmp_path / "var"
    monkeypatch.setenv("ELEVENWRITER_DATABASE_URL", f"sqlite:///{database_path}")
    monkeypatch.setenv("ELEVENWRITER_DATA_DIR", str(data_path))
    reset_settings_cache()
    reset_db_state()
    runner = CliRunner()
    yield runner
    reset_db_state()
    reset_settings_cache()


def test_cli_add_observation_watch_schedule_persists_payload(cli_env: CliRunner) -> None:
    result = cli_env.invoke(
        app,
        [
            "add-observation-watch-schedule",
            "cli-observation-watch",
            "harbor manifest",
            "300",
            "--layer",
            "cli-watch-layer",
            "--source-domain",
            "osint.example.com",
            "--trust-level",
            "trusted",
            "--severity",
            "warning",
            "--limit",
            "15",
            "--candidate-limit",
            "150",
            "--lookback-hours",
            "48",
        ],
    )
    assert result.exit_code == 0
    assert "created for observation watch scan" in result.stdout

    session = get_session_factory()()
    try:
        task = session.query(ScheduledTaskORM).filter_by(name="cli-observation-watch").one()
        assert task.task_type == "observation_watch_scan"
        assert task.layer_key == "cli-watch-layer"
        assert task.payload_json["query"] == "harbor manifest"
        assert task.payload_json["source_domain"] == "osint.example.com"
        assert task.payload_json["trust_level"] == "trusted"
        assert task.payload_json["severity"] == "warning"
        assert task.payload_json["limit"] == 15
        assert task.payload_json["candidate_limit"] == 150
        assert task.payload_json["lookback_hours"] == 48.0
    finally:
        session.close()
