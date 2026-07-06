from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import inspect

from src.app import create_application
from src.config.settings import Settings, get_settings
from src.reference.db import get_engine
from src.services.storage_profile_service import bootstrap_storage, build_storage_status


def _primary_sqlite_settings(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,
        APP_ENV="test",
        APP_RUNTIME_MODE="backend-only",
        PRIMARY_DATABASE_URL=f"sqlite:///{(tmp_path / '11writer.db').as_posix()}",
        APP_CORS_ORIGINS="",
        WEBCAM_WORKER_ENABLED=False,
        WEBCAM_WORKER_RUN_ON_STARTUP=False,
    )


def test_primary_database_url_fans_out_to_backend_modules(tmp_path: Path) -> None:
    settings = _primary_sqlite_settings(tmp_path)

    assert settings.reference_database_url == settings.primary_database_url
    assert settings.wave_monitor_database_url == settings.primary_database_url
    assert settings.source_discovery_database_url == settings.primary_database_url
    assert settings.camera_database_url == settings.primary_database_url
    assert settings.marine_database_url == settings.primary_database_url


def test_bootstrap_storage_initializes_shared_primary_database(tmp_path: Path) -> None:
    settings = _primary_sqlite_settings(tmp_path)

    report = bootstrap_storage(settings)
    table_names = set(inspect(get_engine(settings.primary_database_url)).get_table_names())

    assert report.storage_mode == "persistent-sqlite"
    assert report.shared_storage is True
    assert report.distinct_database_count == 1
    assert report.bootstrapped is True
    assert all(component.reachable for component in report.components)
    assert all(component.initialized for component in report.components)
    assert all(component.uses_primary_database for component in report.components)
    assert "reference_spatial_index" in table_names


def test_storage_status_route_reports_shared_primary_storage(tmp_path: Path) -> None:
    settings = _primary_sqlite_settings(tmp_path)
    bootstrap_storage(settings)
    app = create_application()
    app.dependency_overrides[get_settings] = lambda: settings

    response = TestClient(app).get("/api/status/storage")
    payload = response.json()

    assert response.status_code == 200
    assert payload["storageMode"] == "persistent-sqlite"
    assert payload["primaryDatabaseUrl"] == settings.primary_database_url
    assert payload["sharedStorage"] is True
    assert payload["distinctDatabaseCount"] == 1
    assert all(component["usesPrimaryDatabase"] for component in payload["components"])


def test_build_storage_status_marks_postgis_primary_without_connecting() -> None:
    settings = Settings(
        _env_file=None,
        APP_ENV="test",
        APP_RUNTIME_MODE="backend-only",
        PRIMARY_DATABASE_URL="postgresql+psycopg://user:pass@db/11writer",
        PRIMARY_DATABASE_ENABLE_POSTGIS=True,
        APP_CORS_ORIGINS="",
    )

    report = build_storage_status(settings)

    assert report.storage_mode == "persistent-postgis"
    assert report.primary_database_backend == "postgresql+postgis"
    assert report.primary_database_postgis_enabled is True
