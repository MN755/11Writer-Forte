from __future__ import annotations

from pathlib import Path

import pytest

from src.config import APP_SERVER_ROOT, get_settings, reset_settings_cache


@pytest.fixture(autouse=True)
def reset_config_cache() -> None:
    reset_settings_cache()
    yield
    reset_settings_cache()


def test_settings_resolve_default_runtime_paths_to_app_server_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ELEVENWRITER_DATA_DIR", raising=False)
    monkeypatch.delenv("ELEVENWRITER_DATABASE_URL", raising=False)
    monkeypatch.delenv("ELEVENWRITER_STORAGE_ARCHIVE_DIR", raising=False)
    monkeypatch.delenv("ELEVENWRITER_STORAGE_REHYDRATE_DIR", raising=False)

    settings = get_settings()

    expected_data_dir = (APP_SERVER_ROOT / "var").resolve()
    assert settings.data_dir_effective == expected_data_dir
    assert settings.storage_archive_dir_effective == (expected_data_dir / "artifacts" / "archive").resolve()
    assert settings.storage_rehydrate_dir_effective == (
        expected_data_dir / "artifacts" / "rehydrated"
    ).resolve()
    assert settings.database_url_effective == f"sqlite:///{(expected_data_dir / '11writer_forte.db').as_posix()}"


def test_settings_resolve_relative_runtime_overrides_from_app_server_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ELEVENWRITER_DATA_DIR", "runtime-data")
    monkeypatch.setenv("ELEVENWRITER_STORAGE_ARCHIVE_DIR", "archive-root")
    monkeypatch.setenv("ELEVENWRITER_STORAGE_REHYDRATE_DIR", "rehydrate-root")
    monkeypatch.setenv("ELEVENWRITER_DATABASE_URL", "sqlite:///runtime-data/custom.db")

    settings = get_settings()

    assert settings.data_dir_effective == (APP_SERVER_ROOT / "runtime-data").resolve()
    assert settings.storage_archive_dir_effective == (APP_SERVER_ROOT / "runtime-data" / "archive-root").resolve()
    assert settings.storage_rehydrate_dir_effective == (
        APP_SERVER_ROOT / "runtime-data" / "rehydrate-root"
    ).resolve()
    assert settings.database_url_effective == (
        f"sqlite:///{(APP_SERVER_ROOT / 'runtime-data' / 'custom.db').resolve().as_posix()}"
    )


def test_ensure_runtime_dirs_creates_effective_directories_for_absolute_runtime_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_root = tmp_path / "runtime-root"
    monkeypatch.setenv("ELEVENWRITER_DATA_DIR", str(runtime_root))
    monkeypatch.setenv("ELEVENWRITER_DATABASE_URL", f"sqlite:///{(runtime_root / '11writer_forte.db').as_posix()}")

    settings = get_settings()
    settings.ensure_runtime_dirs()

    assert settings.data_dir_effective == runtime_root.resolve()
    assert settings.data_dir_effective.is_dir()
    assert settings.storage_archive_dir_effective.is_dir()
    assert settings.storage_rehydrate_dir_effective.is_dir()


def test_settings_preserve_in_memory_sqlite_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ELEVENWRITER_DATABASE_URL", "sqlite:///:memory:")

    settings = get_settings()

    assert settings.database_url_effective == "sqlite:///:memory:"
