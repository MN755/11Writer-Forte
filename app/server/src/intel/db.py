from __future__ import annotations

from sqlalchemy import text
from sqlmodel import Session

from src.config.settings import get_settings
from src.forte.db.session import DatabaseManager

from . import models  # noqa: F401
from .models import IntelDatabaseStatus


db = DatabaseManager(get_settings().database_url)

_POSTGIS_MANAGED_COLUMNS = {
    "intel_entities": ["geom"],
    "intel_events": ["geom"],
    "intel_observations": ["geom"],
    "intel_geofences": ["bbox_geom", "geometry_geom"],
}
_POSTGIS_MANAGED_INDEXES = {
    "idx_intel_entities_geom",
    "idx_intel_events_geom",
    "idx_intel_observations_geom",
    "idx_intel_geofences_bbox_geom",
    "idx_intel_geofences_geometry_geom",
}


def ensure_current_database() -> None:
    current_url = get_settings().database_url
    if getattr(db, "_url", None) != current_url:
        db.reconfigure(current_url)


def init_db(database: DatabaseManager) -> None:
    from . import models as _models  # noqa: F401

    ensure_current_database()
    database.create_all()
    _ensure_postgis_artifacts(database)


def get_database_status(session: Session) -> IntelDatabaseStatus:
    bind = session.get_bind()
    dialect = getattr(bind.dialect, "name", "unknown") if bind is not None else "unknown"
    driver = getattr(bind.dialect, "driver", None) if bind is not None else None
    server_version: str | None = None
    postgis_version: str | None = None
    managed_spatial_columns: list[str] = []
    managed_spatial_indexes: list[str] = []
    caveats: list[str] = []
    spatial_backend = f"{dialect}-fallback"

    if dialect == "sqlite":
        server_version = session.execute(text("SELECT sqlite_version()")).scalar_one_or_none()
        spatial_backend = "sqlite-python-fallback"
        caveats.append(
            "SQLite keeps local development and tests alive, but geospatial queries fall back to Python instead of PostGIS operators and indexes."
        )
    elif dialect == "postgresql":
        server_version = session.execute(text("SHOW server_version")).scalar_one_or_none()
        try:
            postgis_version = session.execute(text("SELECT PostGIS_Version()")).scalar_one_or_none()
        except Exception as exc:  # noqa: BLE001
            caveats.append(f"PostGIS extension was not readable: {exc.__class__.__name__}.")
        if postgis_version:
            spatial_backend = "postgis"
            managed_spatial_columns = _postgres_managed_columns(session)
            managed_spatial_indexes = _postgres_managed_indexes(session)
            missing_columns = sorted(_expected_spatial_columns() - set(managed_spatial_columns))
            missing_indexes = sorted(_POSTGIS_MANAGED_INDEXES - set(managed_spatial_indexes))
            if missing_columns:
                caveats.append(f"Missing managed PostGIS columns: {', '.join(missing_columns)}.")
            if missing_indexes:
                caveats.append(f"Missing managed PostGIS indexes: {', '.join(missing_indexes)}.")
        else:
            spatial_backend = "postgresql-no-postgis"
            caveats.append("PostgreSQL is available, but PostGIS is not installed or not readable yet.")
    else:
        caveats.append(f"Dialect {dialect} does not currently receive managed PostGIS artifacts.")

    return IntelDatabaseStatus(
        dialect=dialect,
        driver=driver,
        server_version=server_version,
        postgis_available=bool(postgis_version),
        postgis_version=postgis_version,
        spatial_backend=spatial_backend,
        managed_spatial_columns=managed_spatial_columns,
        managed_spatial_indexes=managed_spatial_indexes,
        caveats=caveats,
    )


def _ensure_postgis_artifacts(database: DatabaseManager) -> None:
    engine = database.engine
    if engine.dialect.name != "postgresql":
        return

    statements = [
        "CREATE EXTENSION IF NOT EXISTS postgis",
        "ALTER TABLE intel_entities ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326)",
        "ALTER TABLE intel_events ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326)",
        "ALTER TABLE intel_observations ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326)",
        "ALTER TABLE intel_geofences ADD COLUMN IF NOT EXISTS bbox_geom geometry(Polygon, 4326)",
        "ALTER TABLE intel_geofences ADD COLUMN IF NOT EXISTS geometry_geom geometry(Geometry, 4326)",
        """
        CREATE OR REPLACE FUNCTION intel_sync_point_geom() RETURNS trigger AS $$
        BEGIN
            NEW.geom := CASE
                WHEN NEW.longitude IS NOT NULL AND NEW.latitude IS NOT NULL
                THEN ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326)
                ELSE NULL
            END;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        """,
        """
        CREATE OR REPLACE FUNCTION intel_sync_geofence_geom() RETURNS trigger AS $$
        BEGIN
            NEW.bbox_geom := CASE
                WHEN NEW.min_longitude IS NOT NULL
                    AND NEW.min_latitude IS NOT NULL
                    AND NEW.max_longitude IS NOT NULL
                    AND NEW.max_latitude IS NOT NULL
                THEN ST_MakeEnvelope(NEW.min_longitude, NEW.min_latitude, NEW.max_longitude, NEW.max_latitude, 4326)
                ELSE NULL
            END;
            NEW.geometry_geom := CASE
                WHEN NEW.geometry_geojson IS NOT NULL AND NEW.geometry_geojson::text <> '{}'
                THEN ST_SetSRID(ST_GeomFromGeoJSON(NEW.geometry_geojson::text), 4326)
                ELSE NEW.bbox_geom
            END;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        """,
        "DROP TRIGGER IF EXISTS trg_intel_entities_sync_geom ON intel_entities",
        "DROP TRIGGER IF EXISTS trg_intel_events_sync_geom ON intel_events",
        "DROP TRIGGER IF EXISTS trg_intel_observations_sync_geom ON intel_observations",
        "DROP TRIGGER IF EXISTS trg_intel_geofences_sync_geom ON intel_geofences",
        """
        CREATE TRIGGER trg_intel_entities_sync_geom
        BEFORE INSERT OR UPDATE OF latitude, longitude ON intel_entities
        FOR EACH ROW EXECUTE FUNCTION intel_sync_point_geom()
        """,
        """
        CREATE TRIGGER trg_intel_events_sync_geom
        BEFORE INSERT OR UPDATE OF latitude, longitude ON intel_events
        FOR EACH ROW EXECUTE FUNCTION intel_sync_point_geom()
        """,
        """
        CREATE TRIGGER trg_intel_observations_sync_geom
        BEFORE INSERT OR UPDATE OF latitude, longitude ON intel_observations
        FOR EACH ROW EXECUTE FUNCTION intel_sync_point_geom()
        """,
        """
        CREATE TRIGGER trg_intel_geofences_sync_geom
        BEFORE INSERT OR UPDATE OF min_latitude, min_longitude, max_latitude, max_longitude, geometry_geojson ON intel_geofences
        FOR EACH ROW EXECUTE FUNCTION intel_sync_geofence_geom()
        """,
        """
        UPDATE intel_entities
        SET geom = CASE
            WHEN longitude IS NOT NULL AND latitude IS NOT NULL
            THEN ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
            ELSE NULL
        END
        """,
        """
        UPDATE intel_events
        SET geom = CASE
            WHEN longitude IS NOT NULL AND latitude IS NOT NULL
            THEN ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
            ELSE NULL
        END
        """,
        """
        UPDATE intel_observations
        SET geom = CASE
            WHEN longitude IS NOT NULL AND latitude IS NOT NULL
            THEN ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
            ELSE NULL
        END
        """,
        """
        UPDATE intel_geofences
        SET bbox_geom = CASE
                WHEN min_longitude IS NOT NULL
                    AND min_latitude IS NOT NULL
                    AND max_longitude IS NOT NULL
                    AND max_latitude IS NOT NULL
                THEN ST_MakeEnvelope(min_longitude, min_latitude, max_longitude, max_latitude, 4326)
                ELSE NULL
            END,
            geometry_geom = CASE
                WHEN geometry_geojson IS NOT NULL AND geometry_geojson::text <> '{}'
                THEN ST_SetSRID(ST_GeomFromGeoJSON(geometry_geojson::text), 4326)
                WHEN min_longitude IS NOT NULL
                    AND min_latitude IS NOT NULL
                    AND max_longitude IS NOT NULL
                    AND max_latitude IS NOT NULL
                THEN ST_MakeEnvelope(min_longitude, min_latitude, max_longitude, max_latitude, 4326)
                ELSE NULL
            END
        """,
        "CREATE INDEX IF NOT EXISTS idx_intel_entities_geom ON intel_entities USING GIST (geom)",
        "CREATE INDEX IF NOT EXISTS idx_intel_events_geom ON intel_events USING GIST (geom)",
        "CREATE INDEX IF NOT EXISTS idx_intel_observations_geom ON intel_observations USING GIST (geom)",
        "CREATE INDEX IF NOT EXISTS idx_intel_geofences_bbox_geom ON intel_geofences USING GIST (bbox_geom)",
        "CREATE INDEX IF NOT EXISTS idx_intel_geofences_geometry_geom ON intel_geofences USING GIST (geometry_geom)",
    ]

    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))


def _postgres_managed_columns(session: Session) -> list[str]:
    rows = session.execute(
        text(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND (
                (table_name = 'intel_entities' AND column_name IN ('geom'))
                OR (table_name = 'intel_events' AND column_name IN ('geom'))
                OR (table_name = 'intel_observations' AND column_name IN ('geom'))
                OR (table_name = 'intel_geofences' AND column_name IN ('bbox_geom', 'geometry_geom'))
              )
            ORDER BY table_name, column_name
            """
        )
    ).all()
    return [f"{table_name}.{column_name}" for table_name, column_name in rows]


def _postgres_managed_indexes(session: Session) -> list[str]:
    rows = session.execute(
        text(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND indexname IN (
                'idx_intel_entities_geom',
                'idx_intel_events_geom',
                'idx_intel_observations_geom',
                'idx_intel_geofences_bbox_geom',
                'idx_intel_geofences_geometry_geom'
              )
            ORDER BY indexname
            """
        )
    ).all()
    return [row[0] for row in rows]


def _expected_spatial_columns() -> set[str]:
    return {
        f"{table_name}.{column_name}"
        for table_name, columns in _POSTGIS_MANAGED_COLUMNS.items()
        for column_name in columns
    }
