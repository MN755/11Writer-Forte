from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from src.config import reset_settings_cache
from src.db import reset_db_state
from src.metrics import reset_metrics_registry


def build_secured_client(monkeypatch, tmp_path: Path) -> TestClient:  # type: ignore[no-untyped-def]
    database_path = tmp_path / "secured.db"
    data_path = tmp_path / "var"
    monkeypatch.setenv("ELEVENWRITER_DATABASE_URL", f"sqlite:///{database_path}")
    monkeypatch.setenv("ELEVENWRITER_DATA_DIR", str(data_path))
    monkeypatch.setenv("ELEVENWRITER_API_AUTH_MODE", "required")
    monkeypatch.setenv("ELEVENWRITER_API_KEY", "forte-test-key")
    reset_settings_cache()
    reset_db_state()
    reset_metrics_registry()

    from src.main import app

    return TestClient(app)


def test_api_key_auth_protects_api_and_metrics_routes(monkeypatch, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    with build_secured_client(monkeypatch, tmp_path) as client:
        health_response = client.get("/health")
        assert health_response.status_code == 200
        assert health_response.json()["api_auth_enabled"] is True

        unauthorized_api = client.get("/api/operations/database")
        assert unauthorized_api.status_code == 401
        assert unauthorized_api.json()["detail"] == "Invalid or missing API key."

        unauthorized_metrics = client.get("/metrics")
        assert unauthorized_metrics.status_code == 401

        headers = {"X-API-Key": "forte-test-key"}
        authorized_api = client.get("/api/operations/database", headers=headers)
        assert authorized_api.status_code == 200
        assert authorized_api.headers["X-Request-ID"]
        assert authorized_api.json()["database_url"] == "sqlite:///secured.db"

        authorized_metrics = client.get("/metrics", headers=headers)
        assert authorized_metrics.status_code == 200
        assert "elevenwriter_auth_enabled 1" in authorized_metrics.text


def test_runtime_diagnostics_and_metrics_expose_operator_state(monkeypatch, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    with build_secured_client(monkeypatch, tmp_path) as client:
        headers = {"X-API-Key": "forte-test-key"}

        task_response = client.post(
            "/api/scheduler/tasks",
            headers=headers,
            json={
                "name": "ops-heartbeat",
                "task_type": "integrity_seed",
                "interval_seconds": 300,
            },
        )
        assert task_response.status_code == 200

        diagnostics_response = client.get("/api/operations/diagnostics", headers=headers)
        assert diagnostics_response.status_code == 200
        diagnostics = diagnostics_response.json()
        assert diagnostics["api_auth"]["enabled"] is True
        assert diagnostics["api_auth"]["mode"] == "required"
        assert diagnostics["api_auth"]["header_name"] == "X-API-Key"
        assert diagnostics["observability"]["metrics_path"] == "/metrics"
        assert diagnostics["scheduler"]["total_count"] == 1
        assert diagnostics["database"]["database_url"] == "sqlite:///secured.db"

        metrics_response = client.get("/metrics", headers=headers)
        assert metrics_response.status_code == 200
        metrics_text = metrics_response.text
        assert 'elevenwriter_scheduler_tasks_total{state="enabled"} 1' in metrics_text
        assert 'elevenwriter_scheduler_tasks_total{state="disabled"} 0' in metrics_text
        assert 'elevenwriter_http_requests_total{method="GET",path="/api/operations/diagnostics",status_code="200"} 1' in metrics_text
        assert 'elevenwriter_http_request_duration_seconds_bucket{le="10",method="GET",path="/api/operations/diagnostics",status_code="200"}' in metrics_text
