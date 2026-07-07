from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from src.config import reset_settings_cache
from src.db import init_db, reset_db_state
from src.db import get_session_factory
from src.models import GeofenceORM, ObservationORM
from src.services.geospatial_service import (
    build_bbox_sql_filter,
    build_contains_geometry_sql_filter,
)


def test_spatial_wkt_is_persisted_for_observations_and_geofences(
    client: TestClient,
    tmp_path: Path,
) -> None:
    fixture = tmp_path / "point.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Port position",
                    "url": "https://alpha.example.com/port",
                    "lat": 29.76,
                    "lon": -95.36,
                }
            ]
        ),
        encoding="utf-8",
    )

    client.post("/api/imports/local", json={"source_path": str(fixture), "layer_key": "marine-track"})
    geofence_response = client.post(
        "/api/geofences",
        json={
            "name": "Port polygon",
            "geometry_geojson": {
                "type": "Polygon",
                "coordinates": [[[-96.0, 29.0], [-94.0, 29.0], [-94.0, 31.0], [-96.0, 31.0], [-96.0, 29.0]]],
            },
        },
    )
    assert geofence_response.status_code == 200

    session = get_session_factory()()
    try:
        observation = session.scalar(select(ObservationORM).order_by(ObservationORM.observation_id.asc()))
        geofence = session.scalar(select(GeofenceORM).order_by(GeofenceORM.geofence_id.asc()))
        assert observation is not None
        assert geofence is not None
        assert observation.location_wkt == "POINT (-95.36 29.76)"
        assert geofence.geometry_wkt == "POLYGON ((-96 29, -94 29, -94 31, -96 31, -96 29))"
    finally:
        session.close()


def test_postgis_bbox_filter_compiles_to_spatial_sql() -> None:
    statement = select(ObservationORM).where(
        build_bbox_sql_filter(
            ObservationORM.location_wkt,
            min_lon=-96.0,
            min_lat=29.0,
            max_lon=-94.0,
            max_lat=31.0,
        )
    )
    compiled = str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "ST_Intersects" in compiled
    assert "ST_MakeEnvelope(-96.0, 29.0, -94.0, 31.0, 4326)" in compiled
    assert "ST_GeomFromText(observations.location_wkt, 4326)" in compiled


def test_postgis_contains_filter_compiles_to_spatial_sql() -> None:
    statement = select(ObservationORM).where(
        build_contains_geometry_sql_filter(
            "POLYGON ((-96 29, -94 29, -94 31, -96 31, -96 29))",
            ObservationORM.location_wkt,
        )
    )
    compiled = str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "ST_Contains" in compiled
    assert "POLYGON ((-96 29, -94 29, -94 31, -96 31, -96 29))" in compiled
    assert "ST_GeomFromText(observations.location_wkt, 4326)" in compiled


def test_init_db_reconciles_additive_columns(tmp_path: Path, monkeypatch) -> None:
    database_path = tmp_path / "legacy.db"
    connection = sqlite3.connect(database_path)
    try:
        connection.executescript(
            """
            CREATE TABLE geofences (
                geofence_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(160) NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                geometry_geojson JSON NOT NULL,
                rule_expression TEXT NOT NULL DEFAULT '',
                enabled BOOLEAN NOT NULL DEFAULT 1,
                created_at DATETIME,
                updated_at DATETIME
            );
            CREATE TABLE observations (
                observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                import_run_id INTEGER,
                event_id INTEGER,
                layer_key VARCHAR(80) NOT NULL,
                source_domain VARCHAR(255),
                source_type VARCHAR(40) NOT NULL DEFAULT 'local_import',
                record_format VARCHAR(30) NOT NULL DEFAULT 'json',
                trust_level VARCHAR(30) NOT NULL DEFAULT 'neutral',
                approval_policy VARCHAR(40) NOT NULL DEFAULT 'manual_review',
                confidence_score FLOAT NOT NULL DEFAULT 0.5,
                location_geojson JSON,
                content_text TEXT NOT NULL DEFAULT '',
                content_json JSON NOT NULL,
                raw_hash VARCHAR(64) NOT NULL,
                created_at DATETIME,
                updated_at DATETIME
            );
            CREATE TABLE local_import_runs (
                import_run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_path TEXT NOT NULL,
                source_format VARCHAR(30) NOT NULL,
                layer_key VARCHAR(80) NOT NULL DEFAULT 'unassigned',
                status VARCHAR(30) NOT NULL DEFAULT 'queued',
                records_seen INTEGER NOT NULL DEFAULT 0,
                records_imported INTEGER NOT NULL DEFAULT 0,
                notes TEXT NOT NULL DEFAULT '',
                chain_of_custody_json JSON NOT NULL,
                created_at DATETIME,
                updated_at DATETIME
            );
            CREATE TABLE scheduled_tasks (
                task_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(160) NOT NULL,
                task_type VARCHAR(40) NOT NULL,
                enabled BOOLEAN NOT NULL DEFAULT 1,
                interval_seconds INTEGER NOT NULL,
                source_id INTEGER,
                target_path TEXT,
                layer_key VARCHAR(80),
                geofence_id INTEGER,
                notes TEXT NOT NULL DEFAULT '',
                payload_json JSON NOT NULL DEFAULT '{}',
                last_run_at DATETIME,
                next_run_at DATETIME,
                created_at DATETIME,
                updated_at DATETIME
            );
            """
        )
        connection.commit()
    finally:
        connection.close()

    monkeypatch.setenv("ELEVENWRITER_DATABASE_URL", f"sqlite:///{database_path}")
    monkeypatch.setenv("ELEVENWRITER_DATA_DIR", str(tmp_path / "var"))
    reset_settings_cache()
    reset_db_state()

    init_db()

    check_connection = sqlite3.connect(database_path)
    try:
        geofence_columns = {
            row[1] for row in check_connection.execute("PRAGMA table_info(geofences)").fetchall()
        }
        observation_columns = {
            row[1] for row in check_connection.execute("PRAGMA table_info(observations)").fetchall()
        }
        import_columns = {
            row[1] for row in check_connection.execute("PRAGMA table_info(local_import_runs)").fetchall()
        }
        scheduled_task_columns = {
            row[1] for row in check_connection.execute("PRAGMA table_info(scheduled_tasks)").fetchall()
        }
        assert "geometry_wkt" in geofence_columns
        assert "location_wkt" in observation_columns
        assert "records_skipped" in import_columns
        assert "retry_attempts" in scheduled_task_columns
        assert "retry_backoff_seconds" in scheduled_task_columns
    finally:
        check_connection.close()
        reset_db_state()
        reset_settings_cache()
