from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.app import create_application
from src.config import reset_settings_cache
from src.db import reset_db_state


def configure_token_app(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, scopes: str = '["read"]'
) -> TestClient:
    monkeypatch.setenv("ELEVENWRITER_DATABASE_URL", f"sqlite:///{tmp_path / 'auth.db'}")
    monkeypatch.setenv("ELEVENWRITER_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("ELEVENWRITER_APP_ENV", "docker")
    monkeypatch.setenv("ELEVENWRITER_AUTH_MODE", "token")
    monkeypatch.setenv(
        "ELEVENWRITER_OPERATOR_API_TOKENS",
        '{"reader":{"token":"a-very-long-test-token-that-has-more-than-thirty-two-characters","scopes":'
        + scopes
        + "}}",
    )
    reset_settings_cache()
    reset_db_state()
    return TestClient(create_application())


def test_token_mode_rejects_missing_and_invalid_bearer_tokens(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with configure_token_app(tmp_path, monkeypatch) as client:
        missing = client.get("/api/events")
        assert missing.status_code == 401
        assert missing.headers["www-authenticate"] == "Bearer"
        invalid = client.get("/api/events", headers={"Authorization": "Bearer not-the-token"})
        assert invalid.status_code == 401
        assert client.get("/health").status_code == 200


def test_token_scope_allows_reads_and_blocks_mutations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with configure_token_app(tmp_path, monkeypatch) as client:
        headers = {
            "Authorization": "Bearer a-very-long-test-token-that-has-more-than-thirty-two-characters"
        }
        allowed = client.get("/api/events", headers=headers)
        assert allowed.status_code == 200
        assert allowed.headers["x-11writer-operator"] == "reader"
        denied = client.post("/api/events", headers=headers, json={})
        assert denied.status_code == 403


def test_nonlocal_unauthenticated_deployment_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ELEVENWRITER_APP_ENV", "docker")
    monkeypatch.setenv("ELEVENWRITER_AUTH_MODE", "disabled")
    reset_settings_cache()
    with pytest.raises(ValueError, match="Unauthenticated mode"):
        create_application()
    reset_settings_cache()
