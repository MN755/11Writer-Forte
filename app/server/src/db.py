from __future__ import annotations

from collections.abc import Generator

from sqlalchemy import text
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.config import get_settings

_engine: Engine | None = None
_session_factory: sessionmaker[Session] | None = None


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
    initialize_spatial_backend(engine)


def initialize_spatial_backend(engine: Engine) -> None:
    settings = get_settings()
    if not settings.uses_postgres:
        return
    with engine.begin() as connection:
        connection.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
        connection.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_observations_location_wkt_gist "
                "ON observations USING GIST (ST_GeomFromText(location_wkt, 4326)) "
                "WHERE location_wkt IS NOT NULL"
            )
        )
        connection.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_geofences_geometry_wkt_gist "
                "ON geofences USING GIST (ST_GeomFromText(geometry_wkt, 4326)) "
                "WHERE geometry_wkt IS NOT NULL"
            )
        )


def reset_db_state() -> None:
    global _engine, _session_factory
    if _engine is not None:
        _engine.dispose()
    _engine = None
    _session_factory = None
