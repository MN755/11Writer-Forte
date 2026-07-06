from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from src.app import create_application
from src.config.settings import Settings, get_settings
from src.services.storage_profile_service import bootstrap_storage


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,
        APP_ENV="test",
        APP_RUNTIME_MODE="backend-only",
        PRIMARY_DATABASE_URL=f"sqlite:///{(tmp_path / '11writer.db').as_posix()}",
        APP_CORS_ORIGINS="",
        WEBCAM_WORKER_ENABLED=False,
        WEBCAM_WORKER_RUN_ON_STARTUP=False,
    )


def _client(settings: Settings) -> TestClient:
    app = create_application()
    app.dependency_overrides[get_settings] = lambda: settings
    return TestClient(app)


def test_health_live_and_root_endpoints_report_ok(tmp_path: Path) -> None:
    client = _client(_settings(tmp_path))

    root_response = client.get("/health")
    live_response = client.get("/health/live")

    assert root_response.status_code == 200
    assert root_response.json() == {"status": "ok"}
    assert live_response.status_code == 200
    assert live_response.json() == {"status": "ok"}


def test_health_ready_reports_unavailable_until_storage_is_bootstrapped(tmp_path: Path) -> None:
    client = _client(_settings(tmp_path))

    response = client.get("/health/ready")
    payload = response.json()

    assert response.status_code == 503
    assert payload["status"] == "degraded"
    assert payload["ready"] is False
    assert any(check["name"] == "storage" and check["ready"] is False for check in payload["checks"])


def test_health_ready_reports_ok_after_bootstrap(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    bootstrap_storage(settings)
    client = _client(settings)

    response = client.get("/health/ready")
    payload = response.json()

    assert response.status_code == 200
    assert payload["status"] == "ok"
    assert payload["ready"] is True
    assert all(check["ready"] is True for check in payload["checks"])
