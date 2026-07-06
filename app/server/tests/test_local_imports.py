from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from src.app import create_application
from src.config.settings import Settings, get_settings
from src.services.ops_audit_service import list_provenance_events


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


def test_local_json_import_persists_memory_and_provenance(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    client = _client(settings)
    dataset_path = tmp_path / "austin-events.json"
    dataset_path.write_text(
        json.dumps(
            [
                {"id": "evt-1", "title": "Port departure", "lat": 30.2672, "lon": -97.7431},
                {"id": "evt-2", "title": "Dock arrival", "lat": 29.7604, "lon": -95.3698},
            ]
        ),
        encoding="utf-8",
    )

    response = client.post(
        "/api/source-discovery/imports/local",
        json={
            "filePath": str(dataset_path),
            "requestedBy": "test-suite",
            "sourceKind": "historical_source",
            "metadata": {"campaign": "fixture"},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["run"]["fileFormat"] == "json"
    assert payload["run"]["importedRecordCount"] == 2
    assert payload["run"]["snapshotCount"] == 2
    assert payload["run"]["geospatialRecordCount"] == 2
    assert payload["memory"]["sourceType"] == "historical_source"
    assert payload["run"]["provenanceEventId"] is not None

    runs_response = client.get("/api/source-discovery/imports/local/runs")
    assert runs_response.status_code == 200
    assert runs_response.json()["count"] == 1

    provenance = list_provenance_events(settings, subsystem="local_dataset_import")
    assert provenance.count == 1
    assert provenance.events[0].event_kind == "local_dataset_import"


def test_local_sqlite_import_reads_selected_tables(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    client = _client(settings)
    dataset_path = tmp_path / "harbor.sqlite"
    connection = sqlite3.connect(dataset_path)
    try:
        connection.execute("CREATE TABLE harbor_events (event_id TEXT, title TEXT, latitude REAL, longitude REAL)")
        connection.execute("INSERT INTO harbor_events VALUES ('evt-1', 'Departure', 30.1, -97.7)")
        connection.execute("INSERT INTO harbor_events VALUES ('evt-2', 'Arrival', 30.2, -97.8)")
        connection.execute("CREATE TABLE ignored_table (id TEXT)")
        connection.execute("INSERT INTO ignored_table VALUES ('ignore-me')")
        connection.commit()
    finally:
        connection.close()

    response = client.post(
        "/api/source-discovery/imports/local",
        json={
            "filePath": str(dataset_path),
            "format": "sqlite",
            "tableNames": ["harbor_events"],
            "requestedBy": "test-suite",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["run"]["fileFormat"] == "sqlite"
    assert payload["run"]["importedRecordCount"] == 2
    assert payload["run"]["snapshotCount"] == 2
    assert payload["run"]["tables"][0]["tableName"] == "harbor_events"
    assert payload["run"]["tables"][0]["importedRowCount"] == 2
