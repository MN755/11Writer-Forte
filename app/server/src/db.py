from __future__ import annotations

from collections.abc import Generator

from sqlalchemy import inspect
from sqlalchemy import text
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.config import get_settings

_engine: Engine | None = None
_session_factory: sessionmaker[Session] | None = None

REQUIRED_ADDITIVE_COLUMNS: dict[str, dict[str, str]] = {
    "geofences": {
        "geometry_wkt": "TEXT",
    },
    "observations": {
        "location_wkt": "TEXT",
    },
    "local_import_runs": {
        "records_skipped": "INTEGER NOT NULL DEFAULT 0",
    },
    "scheduled_tasks": {
        "retry_attempts": "INTEGER NOT NULL DEFAULT 1",
        "retry_backoff_seconds": "FLOAT NOT NULL DEFAULT 0.0",
    },
    "alerts": {
        "disposition_note": "TEXT NOT NULL DEFAULT ''",
    },
}

REQUIRED_POSTGIS_INDEX_DDLS: dict[str, str] = {
    "idx_observations_location_wkt_gist": (
        "CREATE INDEX IF NOT EXISTS idx_observations_location_wkt_gist "
        "ON observations USING GIST (ST_GeomFromText(location_wkt, 4326)) "
        "WHERE location_wkt IS NOT NULL"
    ),
    "idx_geofences_geometry_wkt_gist": (
        "CREATE INDEX IF NOT EXISTS idx_geofences_geometry_wkt_gist "
        "ON geofences USING GIST (ST_GeomFromText(geometry_wkt, 4326)) "
        "WHERE geometry_wkt IS NOT NULL"
    ),
    "idx_camera_inventory_location_wkt_gist": (
        "CREATE INDEX IF NOT EXISTS idx_camera_inventory_location_wkt_gist "
        "ON camera_inventory USING GIST (ST_GeomFromText(location_wkt, 4326)) "
        "WHERE location_wkt IS NOT NULL"
    ),
}


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        settings = get_settings()
        settings.ensure_runtime_dirs()
        _engine = create_engine(
            settings.database_url,
            future=True,
            connect_args=settings.sqlalchemy_connect_args,
        )
    return _engine


def get_session_factory() -> sessionmaker[Session]:
    global _session_factory
    if _session_factory is None:
        _session_factory = sessionmaker(
            bind=get_engine(),
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
            class_=Session,
        )
    return _session_factory


def get_db() -> Generator[Session, None, None]:
    session = get_session_factory()()
    try:
        yield session
    finally:
        session.close()


def init_db() -> None:
    from src.models import Base

    engine = get_engine()
    Base.metadata.create_all(engine)
    reconcile_additive_schema(engine)
    initialize_spatial_backend(engine)


def reconcile_additive_schema(engine: Engine) -> None:
    inspector = inspect(engine)
    with engine.begin() as connection:
        for table_name, required_columns in REQUIRED_ADDITIVE_COLUMNS.items():
            existing = {column["name"] for column in inspector.get_columns(table_name)}
            for column_name, ddl_type in required_columns.items():
                if column_name in existing:
                    continue
                connection.execute(
                    text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl_type}")
                )


def initialize_spatial_backend(engine: Engine) -> None:
    settings = get_settings()
    if not settings.uses_postgres:
        return
    with engine.begin() as connection:
        connection.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
        for ddl in REQUIRED_POSTGIS_INDEX_DDLS.values():
            connection.execute(text(ddl))


def reset_db_state() -> None:
    global _engine, _session_factory
    if _engine is not None:
        _engine.dispose()
    _engine = None
    _session_factory = None
