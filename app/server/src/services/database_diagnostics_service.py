from __future__ import annotations

from sqlalchemy import func, inspect, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.config import get_settings
from src.db import REQUIRED_ADDITIVE_COLUMNS, REQUIRED_POSTGIS_INDEX_DDLS
from src.models import (
    AlertORM,
    CandidateHealthCheckORM,
    CandidatePromotionDecisionORM,
    CandidateSuppressionORM,
    CameraInventoryORM,
    CameraSourceInventoryORM,
    CustodyLogORM,
    DataLayerORM,
    DiscoveryArtifactORM,
    DiscoveryCampaignORM,
    DiscoveryDomainPolicyORM,
    DiscoveryFrontierEntryORM,
    DiscoveryGraphEdgeORM,
    DiscoveryRunORM,
    EntityObservationLinkORM,
    EntityORM,
    EventObservationLinkORM,
    EventORM,
    GeofenceORM,
    LocalImportRunORM,
    ObservationORM,
    RobotsObservationORM,
    ScheduledTaskORM,
    ScheduledTaskRunORM,
    SituationProductORM,
    StorageObjectORM,
    SourceDefinitionORM,
    SourceCandidateORM,
    SourceCandidateRevisionORM,
    SourceRunORM,
    SourceTrustProfileORM,
)

TABLE_COUNT_MODELS: tuple[tuple[str, object], ...] = (
    ("data_layers", DataLayerORM),
    ("source_trust_profiles", SourceTrustProfileORM),
    ("discovery_domain_policies", DiscoveryDomainPolicyORM),
    ("discovery_campaigns", DiscoveryCampaignORM),
    ("discovery_runs", DiscoveryRunORM),
    ("source_definitions", SourceDefinitionORM),
    ("source_candidates", SourceCandidateORM),
    ("discovery_frontier_entries", DiscoveryFrontierEntryORM),
    ("source_candidate_revisions", SourceCandidateRevisionORM),
    ("discovery_graph_edges", DiscoveryGraphEdgeORM),
    ("candidate_health_checks", CandidateHealthCheckORM),
    ("candidate_suppressions", CandidateSuppressionORM),
    ("candidate_promotion_decisions", CandidatePromotionDecisionORM),
    ("robots_observations", RobotsObservationORM),
    ("discovery_artifacts", DiscoveryArtifactORM),
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
    required_columns = collect_required_column_statuses(engine)
    missing_columns = [item for item in required_columns if not item["present"]]
    if missing_columns:
        warnings.append(
            "Required additive schema columns are missing: "
            + ", ".join(f"{item['table_name']}.{item['column_name']}" for item in missing_columns)
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
        "required_columns": required_columns,
        "spatial_indexes": spatial_indexes,
        "table_counts": collect_table_counts(session),
    }


def check_database_connectivity(session: Session) -> bool:
    try:
        return int(session.execute(text("SELECT 1")).scalar_one()) == 1
    except SQLAlchemyError:
        return False


def collect_required_column_statuses(engine: Engine) -> list[dict[str, object]]:
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())
    statuses: list[dict[str, object]] = []
    for table_name, required_columns in REQUIRED_ADDITIVE_COLUMNS.items():
        existing_columns = set()
        if table_name in table_names:
            existing_columns = {column["name"] for column in inspector.get_columns(table_name)}
        for column_name in required_columns:
            statuses.append(
                {
                    "table_name": table_name,
                    "column_name": column_name,
                    "present": column_name in existing_columns,
                }
            )
    return statuses


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
        for index_name in REQUIRED_POSTGIS_INDEX_DDLS
    ]


def fetch_postgres_index_names(session: Session) -> set[str]:
    rows = session.execute(
        text("SELECT indexname FROM pg_indexes WHERE schemaname = ANY(current_schemas(false))")
    ).all()
    return {str(row[0]) for row in rows}


def collect_table_counts(session: Session) -> list[dict[str, object]]:
    return [
        {
            "table_name": table_name,
            "row_count": int(session.scalar(select(func.count()).select_from(model)) or 0),
        }
        for table_name, model in TABLE_COUNT_MODELS
    ]
