from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from src.cli import app
from src.config import reset_settings_cache
from src.db import get_session_factory, reset_db_state
from src.models import SourceDefinitionORM, WatchORM


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


def test_cli_watch_lifecycle_run_schedule_lists_and_feed(
    cli_env: CliRunner,
    tmp_path: Path,
) -> None:
    fixture = tmp_path / "cli-watch-source.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "CLI watch baseline",
                    "url": "https://cli-watch.example/baseline",
                }
            ]
        ),
        encoding="utf-8",
    )
    source_result = cli_env.invoke(
        app,
        [
            "add-source-file",
            "cli-watch-source",
            str(fixture),
            "cli-watch-layer",
        ],
    )
    assert source_result.exit_code == 0, source_result.stdout

    session = get_session_factory()()
    try:
        source = session.query(SourceDefinitionORM).filter_by(name="cli-watch-source").one()
        source_id = source.source_id
    finally:
        session.close()

    add_result = cli_env.invoke(
        app,
        [
            "add-watch",
            "CLI Harbor Delta",
            "source_delta",
            "Watch the controlled local source for deterministic changes.",
            "--source-id",
            str(source_id),
            "--severity",
            "warning",
        ],
    )
    assert add_result.exit_code == 0, add_result.stdout
    assert "slug=cli-harbor-delta" in add_result.stdout
    assert "type=source_delta" in add_result.stdout

    session = get_session_factory()()
    try:
        watch = session.query(WatchORM).filter_by(slug="cli-harbor-delta").one()
        watch_id = watch.watch_id
        assert watch.notification_policy_json == {
            "api_enabled": True,
            "rss_enabled": True,
            "analysis_on_change": False,
        }
    finally:
        session.close()

    list_result = cli_env.invoke(app, ["list-watches", "--state", "enabled"])
    assert list_result.exit_code == 0, list_result.stdout
    assert "cli-harbor-delta" in list_result.stdout

    show_result = cli_env.invoke(app, ["show-watch", str(watch_id)])
    assert show_result.exit_code == 0, show_result.stdout
    assert "objective=Watch the controlled local source" in show_result.stdout
    assert 'rule_json={"alert_on_initial": false' in show_result.stdout

    update_result = cli_env.invoke(
        app,
        [
            "update-watch",
            str(watch_id),
            "--description",
            "Updated from the terminal",
            "--severity",
            "critical",
        ],
    )
    assert update_result.exit_code == 0, update_result.stdout
    assert "severity=critical" in update_result.stdout

    pause_result = cli_env.invoke(app, ["pause-watch", str(watch_id)])
    assert pause_result.exit_code == 0, pause_result.stdout
    assert "state=paused" in pause_result.stdout

    resume_result = cli_env.invoke(app, ["resume-watch", str(watch_id)])
    assert resume_result.exit_code == 0, resume_result.stdout
    assert "state=enabled" in resume_result.stdout

    schedule_result = cli_env.invoke(
        app,
        [
            "add-watch-schedule",
            str(watch_id),
            "cli-watch-schedule",
            "60",
            "--retry-attempts",
            "2",
        ],
    )
    assert schedule_result.exit_code == 0, schedule_result.stdout
    assert f"watch={watch_id} | scheduled_task=" in schedule_result.stdout
    assert "every=60s" in schedule_result.stdout

    baseline_result = cli_env.invoke(app, ["run-watch", str(watch_id)])
    assert baseline_result.exit_code == 0, baseline_result.stdout
    assert "outcome=baseline" in baseline_result.stdout
    assert "changed=False" in baseline_result.stdout

    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "CLI watch changed",
                    "url": "https://cli-watch.example/changed",
                }
            ]
        ),
        encoding="utf-8",
    )
    change_result = cli_env.invoke(app, ["run-watch", str(watch_id)])
    assert change_result.exit_code == 0, change_result.stdout
    assert "outcome=change" in change_result.stdout
    assert "changed=True" in change_result.stdout

    runs_result = cli_env.invoke(
        app,
        ["list-watch-runs", "--watch-id", str(watch_id), "--status", "completed"],
    )
    assert runs_result.exit_code == 0, runs_result.stdout
    assert "| baseline |" in runs_result.stdout
    assert "| change |" in runs_result.stdout

    alerts_result = cli_env.invoke(
        app,
        ["list-watch-alerts", "--watch-id", str(watch_id), "--status", "open"],
    )
    assert alerts_result.exit_code == 0, alerts_result.stdout
    assert "| critical | open |" in alerts_result.stdout

    evidence_result = cli_env.invoke(app, ["show-watch-evidence", str(watch_id)])
    assert evidence_result.exit_code == 0, evidence_result.stdout

    feed_url_result = cli_env.invoke(
        app,
        [
            "show-watch-feed",
            "--base-url",
            "http://127.0.0.1:8765",
            "--watch-id",
            str(watch_id),
        ],
    )
    assert feed_url_result.exit_code == 0, feed_url_result.stdout
    assert (
        f"http://127.0.0.1:8765/api/watches/feed.rss?status=open&watch_id={watch_id}"
        in feed_url_result.stdout
    )

    feed_preview_result = cli_env.invoke(
        app,
        ["show-watch-feed", "--watch-id", str(watch_id), "--preview"],
    )
    assert feed_preview_result.exit_code == 0, feed_preview_result.stdout
    assert "<rss" in feed_preview_result.stdout
    assert "CLI Harbor Delta" in feed_preview_result.stdout


def test_cli_runtime_bundle_export_and_restore_commands(
    cli_env: CliRunner,
    tmp_path: Path,
) -> None:
    assert cli_env.invoke(app, ["init-db"]).exit_code == 0
    bundle_path = tmp_path / "exports" / "runtime-bundle.zip"

    export_result = cli_env.invoke(
        app,
        ["export-runtime-bundle", str(bundle_path)],
    )
    assert export_result.exit_code == 0, export_result.stdout
    assert bundle_path.exists()
    assert f"path={bundle_path.resolve()}" in export_result.stdout
    assert "sha256=" in export_result.stdout
    assert "evidence_count=0" in export_result.stdout

    restore_result = cli_env.invoke(
        app,
        ["restore-runtime-bundle", str(bundle_path), "--replace-existing"],
    )
    assert restore_result.exit_code == 0, restore_result.stdout
    assert f"path={bundle_path.resolve()}" in restore_result.stdout
    assert "replaced_existing=True" in restore_result.stdout
    assert "total_records=" in restore_result.stdout
