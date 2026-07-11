from __future__ import annotations

from sqlalchemy import create_engine, inspect, text

from src.db import SCHEMA_MIGRATIONS_TABLE, apply_schema_migrations


def test_versioned_migration_upgrades_production_shaped_sqlite_schema() -> None:
    engine = create_engine("sqlite:///:memory:")
    legacy_tables = {
        "geofences": "geofence_id INTEGER PRIMARY KEY",
        "observations": "observation_id INTEGER PRIMARY KEY",
        "local_import_runs": "local_import_run_id INTEGER PRIMARY KEY",
        "scheduled_tasks": "task_id INTEGER PRIMARY KEY",
        "alerts": "alert_id INTEGER PRIMARY KEY",
        "watches": "watch_id INTEGER PRIMARY KEY",
    }
    with engine.begin() as connection:
        for table_name, columns in legacy_tables.items():
            connection.execute(text(f"CREATE TABLE {table_name} ({columns})"))

    assert apply_schema_migrations(engine) == [1, 2]
    assert apply_schema_migrations(engine) == []

    inspector = inspect(engine)
    assert {column["name"] for column in inspector.get_columns("geofences")} >= {
        "geometry_wkt"
    }
    assert {column["name"] for column in inspector.get_columns("observations")} >= {
        "location_wkt"
    }
    assert {column["name"] for column in inspector.get_columns("scheduled_tasks")} >= {
        "retry_attempts",
        "retry_backoff_seconds",
    }
    assert {"research_providers", "research_provider_runs"}.issubset(
        set(inspector.get_table_names())
    )
    with engine.connect() as connection:
        rows = connection.execute(
            text(f"SELECT version, name FROM {SCHEMA_MIGRATIONS_TABLE}")
        ).all()
    assert rows == [
        (1, "baseline_additive_columns"),
        (2, "research_fleet_durable_tables"),
    ]
