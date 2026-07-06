from __future__ import annotations

import json
from pathlib import Path

from src import cli
from src.config.settings import Settings


def test_build_doctor_report_marks_frontend_removed(tmp_path: Path) -> None:
    report = cli.build_doctor_report(
        repo_root=tmp_path,
        settings=Settings(_env_file=None, APP_RUNTIME_MODE="backend-only", APP_CORS_ORIGINS=""),
    )

    assert report["frontend_runtime_removed"] is True
    assert report["frontend_runtime_paths"] == {
        "app_client": False,
        "sevenpo8_frontend": False,
        "code_oss_reference": False,
    }


def test_build_doctor_report_detects_frontend_runtime_paths(tmp_path: Path) -> None:
    (tmp_path / "app" / "client").mkdir(parents=True)
    (tmp_path / "7Po8" / "apps" / "frontend").mkdir(parents=True)

    report = cli.build_doctor_report(
        repo_root=tmp_path,
        settings=Settings(_env_file=None, APP_RUNTIME_MODE="backend-only", APP_CORS_ORIGINS=""),
    )

    assert report["frontend_runtime_removed"] is False
    assert report["frontend_runtime_paths"]["app_client"] is True
    assert report["frontend_runtime_paths"]["sevenpo8_frontend"] is True


def test_main_dispatches_worker_command(monkeypatch) -> None:
    captured: dict[str, list[str]] = {}

    def fake_run_cli(argv: list[str]) -> None:
        captured["argv"] = argv

    monkeypatch.setattr(cli.runtime_worker, "run_cli", fake_run_cli)

    exit_code = cli.main(["worker", "--worker", "wave_monitor", "--loop"])

    assert exit_code == 0
    assert captured["argv"] == ["--worker", "wave_monitor", "--loop"]


def test_doctor_json_output(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "build_doctor_report",
        lambda: {
            "runtime_mode": "backend-only",
            "platform": {"python": "3.11.0", "system": "TestOS", "release": "1"},
            "api": {
                "reference_database_url": "sqlite:///./data/reference.db",
                "wave_monitor_database_url": "sqlite:///./data/wave_monitor.db",
                "source_discovery_database_url": "sqlite:///./data/source_discovery.db",
                "cors_origins": [],
            },
            "storage": {
                "storageMode": "persistent-sqlite",
                "primaryDatabaseUrl": None,
                "primaryDatabaseBackend": None,
                "primaryDatabasePostgisEnabled": False,
                "sharedStorage": False,
                "distinctDatabaseCount": 3,
                "bootstrapped": False,
                "components": [],
                "caveats": [],
            },
            "workers": {
                "webcam_worker_enabled": False,
                "source_discovery_scheduler_enabled": False,
                "wave_monitor_scheduler_enabled": False,
            },
            "frontend_runtime_removed": True,
            "frontend_runtime_paths": {
                "app_client": False,
                "sevenpo8_frontend": False,
                "code_oss_reference": False,
            },
            "repo_root": "/tmp/repo",
        },
    )

    exit_code = cli.main(["doctor", "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["frontend_runtime_removed"] is True
    assert payload["runtime_mode"] == "backend-only"


def test_db_status_json_output(monkeypatch, capsys) -> None:
    class _FakeReport:
        def model_dump(self, *, mode: str, by_alias: bool) -> dict[str, object]:
            assert mode == "json"
            assert by_alias is True
            return {
                "storageMode": "persistent-postgis",
                "primaryDatabaseUrl": "postgresql+psycopg://user:pass@db/11writer",
                "primaryDatabaseBackend": "postgresql+postgis",
                "primaryDatabasePostgisEnabled": True,
                "sharedStorage": True,
                "distinctDatabaseCount": 1,
                "bootstrapped": False,
                "components": [
                    {
                        "component": "reference",
                        "backend": "postgresql+postgis",
                        "reachable": True,
                        "initialized": True,
                    }
                ],
                "caveats": [],
            }

    monkeypatch.setattr(cli, "build_storage_status", lambda settings: _FakeReport())
    monkeypatch.setattr(cli, "get_settings", lambda: Settings(_env_file=None))

    exit_code = cli.main(["db-status", "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["storageMode"] == "persistent-postgis"
    assert payload["sharedStorage"] is True


def test_alerts_json_output(monkeypatch, capsys) -> None:
    class _FakeReport:
        def model_dump(self, *, mode: str, by_alias: bool) -> dict[str, object]:
            assert mode == "json"
            assert by_alias is True
            return {
                "count": 1,
                "alerts": [
                    {
                        "alertId": "alert:test",
                        "dedupeKey": "runtime-worker-failure:wave_monitor",
                        "subsystem": "runtime_scheduler",
                        "alertType": "runtime_worker_failure",
                        "severity": "high",
                        "status": "open",
                        "title": "Runtime worker failed: wave_monitor",
                        "summary": "boom",
                        "subjectType": "runtime_worker",
                        "subjectId": "wave_monitor",
                        "sourceEventId": "prov:test",
                        "firstObservedAt": "2026-01-01T00:00:00Z",
                        "lastObservedAt": "2026-01-01T00:00:00Z",
                        "occurrenceCount": 1,
                        "evidenceRefs": ["runtime_scheduler_run:test"],
                        "metadata": {},
                        "caveats": [],
                    }
                ],
            }

    monkeypatch.setattr(cli, "list_alert_records", lambda *args, **kwargs: _FakeReport())
    monkeypatch.setattr(cli, "get_settings", lambda: Settings(_env_file=None))

    exit_code = cli.main(["alerts", "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["count"] == 1
    assert payload["alerts"][0]["alertType"] == "runtime_worker_failure"
