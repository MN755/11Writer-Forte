from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from src.cli import app
from src.config import reset_settings_cache
from src.db import init_db, reset_db_state


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


def test_cli_search_observations_prints_ranked_results(cli_env: CliRunner, tmp_path: Path) -> None:
    init_db()
    fixture = tmp_path / "cli-searchable-observations.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Harbor Manifest Notes",
                    "summary": "Manifest notes supporting the harbor movement and tug escort.",
                    "url": "https://osint.example.com/harbor-manifest",
                    "lat": 44.9784,
                    "lon": -93.2641,
                },
                {
                    "title": "Rail Delay Bulletin",
                    "summary": "Dispatch notes describing a junction slowdown.",
                    "url": "https://osint.example.com/rail-delay",
                    "lat": 44.9867,
                    "lon": -93.2581,
                },
            ]
        ),
        encoding="utf-8",
    )

    import_result = cli_env.invoke(
        app,
        [
            "import-local",
            str(fixture),
            "--layer",
            "cli-searchable-web",
        ],
    )
    assert import_result.exit_code == 0

    result = cli_env.invoke(
        app,
        [
            "search-observations",
            "harbor manifest",
            "--layer",
            "cli-searchable-web",
            "--limit",
            "5",
        ],
    )
    assert result.exit_code == 0
    assert "Harbor Manifest Notes" in result.stdout
    assert "score=" in result.stdout
    assert "harbor movement" in result.stdout


def test_cli_search_observations_can_find_imported_source_code(cli_env: CliRunner, tmp_path: Path) -> None:
    init_db()
    fixture = tmp_path / "harbor_watch.go"
    fixture.write_text(
        "package main\n\nfunc main() {\n    println(\"harbor watch signal\")\n}\n",
        encoding="utf-8",
    )

    import_result = cli_env.invoke(
        app,
        [
            "import-local",
            str(fixture),
            "--layer",
            "cli-source-code",
        ],
    )
    assert import_result.exit_code == 0
    assert "format=source_code" in import_result.stdout

    result = cli_env.invoke(
        app,
        [
            "search-observations",
            "harbor watch signal",
            "--layer",
            "cli-source-code",
            "--limit",
            "5",
        ],
    )
    assert result.exit_code == 0
    assert "harbor_watch.go" in result.stdout
    assert "harbor watch signal" in result.stdout


def test_cli_import_local_directory_supports_mixed_bundle_search(cli_env: CliRunner, tmp_path: Path) -> None:
    init_db()
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "signals.txt").write_text("https://example.com/harbor-watch\n", encoding="utf-8")
    (bundle / "watch.go").write_text(
        "package main\nfunc main() { println(\"bundle watch target\") }\n",
        encoding="utf-8",
    )

    import_result = cli_env.invoke(
        app,
        [
            "import-local",
            str(bundle),
            "--layer",
            "cli-bundle",
        ],
    )
    assert import_result.exit_code == 0
    assert "format=directory" in import_result.stdout
    assert "imported=2" in import_result.stdout

    result = cli_env.invoke(
        app,
        [
            "search-observations",
            "bundle watch target",
            "--layer",
            "cli-bundle",
            "--limit",
            "5",
        ],
    )
    assert result.exit_code == 0
    assert "watch.go" in result.stdout
