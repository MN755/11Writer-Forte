from __future__ import annotations

from sqlalchemy import func, inspect, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.config import get_settings
from src.migrations import inspect_database_revision
from src.models import (
    AlertORM,
    CameraInventoryORM,
    CameraSourceInventoryORM,
    CustodyLogORM,
    DataLayerORM,
    EntityObservationLinkORM,
    EntityORM,
    EventObservationLinkORM,
    EventORM,
    GeofenceORM,
    LocalImportRunORM,
    ObservationORM,
    ScheduledTaskORM,
    ScheduledTaskRunORM,
    SituationProductORM,
    StorageObjectORM,
    SourceDefinitionORM,
    SourceRunORM,
    SourceTrustProfileORM,
)

REQUIRED_POSTGIS_INDEX_NAMES: tuple[str, ...] = (
    "idx_observations_location_wkt_gist",
    "idx_geofences_geometry_wkt_gist",
    "idx_camera_inventory_location_wkt_gist",
)

TABLE_COUNT_MODELS: tuple[tuple[str, object], ...] = (
    ("data_layers", DataLayerORM),
    ("source_trust_profiles", SourceTrustProfileORM),
    ("source_definitions", SourceDefinitionORM),
    ("local_import_runs", LocalImportRunORM),
    ("observations", ObservationORM),
    ("camera_inventory", CameraInventoryORM),
    ("camera_source_inventory", CameraSourceInventoryORM),
    ("storage_objects", StorageObjectORM),
    ("events", EventORM),
    ("entities", EntityORM),
    ("event_observation_links", EventObservationLinkORM),
    ("entity_observation_links", EntityObservationLinkORM),
    ("geofences", GeofenceORM),
    ("alerts", AlertORM),
    ("scheduled_tasks", ScheduledTaskORM),
    ("scheduled_task_runs", ScheduledTaskRunORM),
    ("source_runs", SourceRunORM),
    ("situation_products", SituationProductORM),
    ("custody_logs", CustodyLogORM),
)


def build_database_diagnostics(session: Session) -> dict[str, object]:
    settings = get_settings()
    engine = session.get_bind()
    backend = engine.dialect.name
    warnings: list[str] = []
    notes: list[str] = []

    connected = check_database_connectivity(session)
    migration_status = inspect_database_revision(engine)
    if not migration_status.version_table_present:
        warnings.append(
            "Alembic revision state is missing. Run `elevenwriter migrate-db` before using the runtime."
        )
    elif not migration_status.schema_up_to_date:
        warnings.append(
            "Database schema is not at the Alembic head revision. Run `elevenwriter migrate-db`."
        )

    postgis_expected = settings.uses_postgres
    postgis_version: str | None = None
    spatial_indexes: list[dict[str, object]] = []
    postgis_extension_installed: bool | None = None

    if postgis_expected:
        postgis_version = fetch_postgis_extension_version(session)
        postgis_extension_installed = postgis_version is not None
        if not postgis_extension_installed:
            warnings.append("PostGIS extension is not installed on the PostgreSQL backend.")
        spatial_indexes = collect_spatial_index_statuses(session)
        missing_indexes = [item["index_name"] for item in spatial_indexes if not item["present"]]
        if missing_indexes:
            warnings.append(
                "Required PostGIS spatial indexes are missing: " + ", ".join(missing_indexes)
            )
    else:
        postgis_extension_installed = None
        notes.append("Running the SQLite/Python spatial fallback path.")
    notes.append(
        "Alembic revision status: "
        f"current={migration_status.current_revision} head={migration_status.head_revision}"
    )

    if not connected:
        warnings.append("Database connectivity probe failed.")

    status = "ok" if not warnings else "degraded"
    return {
        "status": status,
        "database_backend": backend,
        "database_url": settings.database_url,
        "database_connected": connected,
        "spatial_backend": settings.spatial_backend,
        "scheduler_poll_seconds": settings.scheduler_poll_seconds,
        "postgis_expected": postgis_expected,
        "postgis_extension_installed": postgis_extension_installed,
        "postgis_version": postgis_version,
        "warning_count": len(warnings),
        "warnings": warnings,
        "notes": notes,
        "migration": {
            "current_revision": migration_status.current_revision,
            "head_revision": migration_status.head_revision,
            "version_table_present": migration_status.version_table_present,
            "has_application_tables": migration_status.has_application_tables,
            "schema_up_to_date": migration_status.schema_up_to_date,
        },
        "spatial_indexes": spatial_indexes,
        "table_counts": collect_table_counts(session),
    }


def check_database_connectivity(session: Session) -> bool:
    try:
        return int(session.execute(text("SELECT 1")).scalar_one()) == 1
    except SQLAlchemyError:
        return False


def fetch_postgis_extension_version(session: Session) -> str | None:
    version = session.execute(
        text("SELECT extversion FROM pg_extension WHERE extname = 'postgis'")
    ).scalar_one_or_none()
    if version is None:
        return None
    return str(version)


def collect_spatial_index_statuses(session: Session) -> list[dict[str, object]]:
    existing = fetch_postgres_index_names(session)
    return [
        {"index_name": index_name, "present": index_name in existing}
        for index_name in REQUIRED_POSTGIS_INDEX_NAMES
    ]


def fetch_postgres_index_names(session: Session) -> set[str]:
    rows = session.execute(
        text(
            "SELECT indexname "
            "FROM pg_indexes "
            "WHERE schemaname = ANY(current_schemas(false))"
        )
    ).all()
    return {str(row[0]) for row in rows}


def collect_table_counts(session: Session) -> list[dict[str, object]]:
    existing_tables = set(inspect(session.get_bind()).get_table_names())
    return [
        {
            "table_name": table_name,
            "row_count": (
                int(session.scalar(select(func.count()).select_from(model)) or 0)
                if table_name in existing_tables
                else 0
            ),
        }
        for table_name, model in TABLE_COUNT_MODELS
    ]
