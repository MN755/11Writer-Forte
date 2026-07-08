from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.config import reset_settings_cache
from src.db import reset_db_state


@pytest.fixture()
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    database_path = tmp_path / "test.db"
    data_path = tmp_path / "var"
    monkeypatch.setenv("ELEVENWRITER_DATABASE_URL", f"sqlite:///{database_path}")
    monkeypatch.setenv("ELEVENWRITER_DATABASE_AUTO_MIGRATE", "true")
    monkeypatch.setenv("ELEVENWRITER_DATA_DIR", str(data_path))
    reset_settings_cache()
    reset_db_state()

    from src.main import app

    with TestClient(app) as test_client:
        yield test_client

    reset_db_state()
    reset_settings_cache()

