from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from src.cli import app
from src.config import reset_settings_cache
from src.db import get_session_factory, init_db, reset_db_state
from src.models import ScheduledTaskORM, StorageObjectORM
from src.schemas import SourceDefinitionCreate
from src.services.source_service import create_source_definition


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


def test_cli_show_operations_report_surfaces_readiness_events_entities_and_backup(
    cli_env: CliRunner,
    tmp_path: Path,
) -> None:
    fixture_a = tmp_path / "ops-report-a.json"
    fixture_a.write_text(
        json.dumps(
            [
                {
                    "title": "Ops report source A",
                    "url": "https://ops-report.example.com/a",
                    "observed_at": "2026-07-06T20:00:00Z",
                    "vessel_name": "MV Ops Report",
                    "mmsi": "999111222",
                    "lat": 29.76,
                    "lon": -95.36,
                }
            ]
        ),
        encoding="utf-8",
    )
    fixture_b = tmp_path / "ops-report-b.json"
    fixture_b.write_text(
        json.dumps(
            [
                {
                    "title": "Ops report source B",
                    "url": "https://ops-report.example.com/b",
                    "observed_at": "2026-07-06T20:05:00Z",
                    "vessel_name": "MV Ops Report",
                    "mmsi": "999111222",
                    "lat": 29.77,
                    "lon": -95.35,
                }
            ]
        ),
        encoding="utf-8",
    )

    assert cli_env.invoke(app, ["import-local", str(fixture_a), "--layer", "ops-report-marine"]).exit_code == 0
    assert cli_env.invoke(app, ["import-local", str(fixture_b), "--layer", "ops-report-news"]).exit_code == 0

    session = get_session_factory()()
    try:
        source = create_source_definition(
            session,
            SourceDefinitionCreate(
                name="ops-report-source",
                source_kind="local_file",
                layer_key="ops-report-marine",
                target_uri=str(fixture_a),
            ),
        )
        session.add_all(
            [
                ScheduledTaskORM(
                    name="ops-report-source-maintenance",
                    task_type="source_maintenance",
                    interval_seconds=600,
                    enabled=True,
                    payload_json={"stale_after_hours": 24.0, "source_limit": 25, "dead_letter_limit": 50},
                ),
                ScheduledTaskORM(
                    name="ops-report-source-health",
                    task_type="source_health_scan",
                    interval_seconds=600,
                    enabled=True,
                    payload_json={"stale_after_hours": 24.0, "source_limit": 50},
                ),
                ScheduledTaskORM(
                    name="ops-report-source-sync",
                    task_type="source_sync",
                    interval_seconds=300,
                    enabled=True,
                    source_id=source.source_id,
                    payload_json={},
                ),
                ScheduledTaskORM(
                    name="ops-report-storage",
                    task_type="storage_lifecycle",
                    interval_seconds=3600,
                    enabled=True,
                    payload_json={"limit": 100},
                ),
                ScheduledTaskORM(
                    name="ops-report-entities",
                    task_type="entity_resolution_refresh",
                    interval_seconds=900,
                    enabled=True,
                    payload_json={"limit": 100, "min_observations": 2},
                ),
                ScheduledTaskORM(
                    name="ops-report-events",
                    task_type="event_fusion_refresh",
                    interval_seconds=900,
                    enabled=True,
                    payload_json={
                        "limit": 100,
                        "time_window_minutes": 120,
                        "distance_km": 10.0,
                        "min_independent_signals": 2,
                    },
                ),
                ScheduledTaskORM(
                    name="ops-report-backup",
                    task_type="runtime_snapshot_export",
                    interval_seconds=21600,
                    enabled=True,
                    payload_json={"file_prefix": "ops-report-runtime-snapshot"},
                ),
            ]
        )
        session.commit()
    finally:
        session.close()

    assert cli_env.invoke(
        app,
        [
            "resolve-entities",
            "--bbox",
            "-96,29,-94,31",
            "--min-observations",
            "2",
        ],
    ).exit_code == 0
    assert cli_env.invoke(
        app,
        [
            "fuse-events",
            "--bbox",
            "-96,29,-94,31",
            "--distance-km",
            "10",
            "--time-window-minutes",
            "120",
        ],
    ).exit_code == 0
    assert cli_env.invoke(
        app,
        [
            "export-runtime-snapshot",
            str(tmp_path / "exports" / "ops-report-runtime-snapshot.json"),
        ],
    ).exit_code == 0

    result = cli_env.invoke(app, ["show-operations-report", "--hours", "24", "--limit", "10"])
    assert result.exit_code == 0
    assert "readiness=" in result.stdout
    assert "backup_coverage=" in result.stdout
    assert "event_inventory=1 open=1 closed=0" in result.stdout
    assert "event_fusion_tasks=1" in result.stdout
    assert "entity_inventory=1" in result.stdout
    assert "entity_resolution_tasks=1" in result.stdout

    export_path = tmp_path / "exports" / "operations-report.json"
    export_result = cli_env.invoke(
        app,
        ["export-operations-report", str(export_path), "--hours", "24", "--limit", "10"],
    )
    assert export_result.exit_code == 0
    assert export_path.exists()
    assert "exported operations report to" in export_result.stdout

    session = get_session_factory()()
    try:
        rows = (
            session.query(StorageObjectORM)
            .filter_by(owner_type="operations_report", object_kind="operations_report_export")
            .all()
        )
        assert len(rows) == 1
        assert rows[0].metadata_json["hours"] == 24.0
        assert rows[0].metadata_json["limit"] == 10
    finally:
        session.close()
