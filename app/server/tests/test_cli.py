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
