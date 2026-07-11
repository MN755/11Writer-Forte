from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime, timezone

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
    "watches": {
        "coverage_json": "JSON NOT NULL DEFAULT '{}'",
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

SCHEMA_MIGRATIONS_TABLE = "forte_schema_migrations"


@dataclass(frozen=True)
class SchemaMigration:
    """A small, ordered, replay-safe schema migration.

    Forte supports SQLite for local operation without taking a dependency on an
    external migration runner.  Keeping the migration list in code makes upgrades
    explicit and testable while the migration ledger prevents an old deployment
    from silently relying on ``create_all`` or ad-hoc reconciliation.
    """

    version: int
    name: str


SCHEMA_MIGRATIONS: tuple[SchemaMigration, ...] = (
    SchemaMigration(1, "baseline_additive_columns"),
    SchemaMigration(2, "research_fleet_durable_tables"),
)


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
    apply_schema_migrations(engine)
    initialize_spatial_backend(engine)


def apply_schema_migrations(engine: Engine) -> list[int]:
    """Apply every known schema migration exactly once and return versions applied."""
    _ensure_migration_ledger(engine)
    with engine.connect() as connection:
        applied = {
            int(row[0])
            for row in connection.execute(text(f"SELECT version FROM {SCHEMA_MIGRATIONS_TABLE}"))
        }

    newly_applied: list[int] = []
    for migration in SCHEMA_MIGRATIONS:
        if migration.version in applied:
            continue
        with engine.begin() as connection:
            # Recheck inside the transaction so two local processes cannot record
            # the same migration independently.
            exists = connection.execute(
                text(
                    f"SELECT 1 FROM {SCHEMA_MIGRATIONS_TABLE} WHERE version = :version"
                ),
                {"version": migration.version},
            ).first()
            if exists is not None:
                continue
            _apply_migration(connection, migration)
            connection.execute(
                text(
                    f"INSERT INTO {SCHEMA_MIGRATIONS_TABLE} (version, name, applied_at) "
                    "VALUES (:version, :name, :applied_at)"
                ),
                {
                    "version": migration.version,
                    "name": migration.name,
                    "applied_at": datetime.now(timezone.utc).isoformat(),
                },
            )
        newly_applied.append(migration.version)
    return newly_applied


def _ensure_migration_ledger(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                f"CREATE TABLE IF NOT EXISTS {SCHEMA_MIGRATIONS_TABLE} ("
                "version INTEGER PRIMARY KEY, name VARCHAR(200) NOT NULL, "
                "applied_at VARCHAR(64) NOT NULL)"
            )
        )


def _apply_migration(connection, migration: SchemaMigration) -> None:  # type: ignore[no-untyped-def]
    if migration.version == 1:
        _apply_baseline_additive_columns(connection)
        return
    if migration.version == 2:
        _apply_research_fleet_durable_tables(connection)
        return
    raise RuntimeError(f"No implementation registered for schema migration {migration.version}.")


def _apply_baseline_additive_columns(connection) -> None:  # type: ignore[no-untyped-def]
    """Upgrade the production-shaped pre-migration schema without data loss."""
    inspector = inspect(connection)
    for table_name, required_columns in REQUIRED_ADDITIVE_COLUMNS.items():
        existing = {column["name"] for column in inspector.get_columns(table_name)}
        for column_name, ddl_type in required_columns.items():
            if column_name in existing:
                continue
            connection.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl_type}"))


def _apply_research_fleet_durable_tables(connection) -> None:  # type: ignore[no-untyped-def]
    """Create the durable provider/run tables for deployments predating Phase 1."""
    primary_key = (
        "INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY"
        if connection.dialect.name == "postgresql"
        else "INTEGER PRIMARY KEY"
    )
    connection.execute(
        text(
            "CREATE TABLE IF NOT EXISTS research_providers ("
            f"research_provider_id {primary_key}, "
            "provider_key VARCHAR(120) NOT NULL UNIQUE, name VARCHAR(200) NOT NULL, "
            "source_kind VARCHAR(80) NOT NULL, enabled BOOLEAN NOT NULL DEFAULT 1, "
            "health_state VARCHAR(30) NOT NULL DEFAULT 'unknown', "
            "schema_version VARCHAR(80) NOT NULL DEFAULT '1', license_notes TEXT NOT NULL DEFAULT '', "
            "jurisdiction VARCHAR(160) NOT NULL DEFAULT 'operator-configured', languages_json JSON NOT NULL DEFAULT '[]', "
            "freshness_hours INTEGER NOT NULL DEFAULT 24, cost VARCHAR(30) NOT NULL DEFAULT 'free', "
            "access_requirement VARCHAR(40) NOT NULL DEFAULT 'none', robots_supported BOOLEAN NOT NULL DEFAULT 1, "
            "evidence_capture_method VARCHAR(120) NOT NULL DEFAULT 'response_manifest', capabilities_json JSON NOT NULL DEFAULT '[]', "
            "default_budget_json JSON NOT NULL DEFAULT '{}', coverage_gaps_json JSON NOT NULL DEFAULT '{}', "
            "last_health_at TIMESTAMP, last_success_at TIMESTAMP, last_error_text TEXT, metadata_json JSON NOT NULL DEFAULT '{}', "
            "created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL)"
        )
    )
    connection.execute(
        text(
            "CREATE TABLE IF NOT EXISTS research_provider_runs ("
            f"research_provider_run_id {primary_key}, research_provider_id INTEGER NOT NULL, "
            "investigation_id INTEGER, idempotency_key VARCHAR(160) NOT NULL UNIQUE, status VARCHAR(30) NOT NULL DEFAULT 'queued', "
            "worker_class VARCHAR(60) NOT NULL DEFAULT 'search', priority INTEGER NOT NULL DEFAULT 0, "
            "cancellation_requested BOOLEAN NOT NULL DEFAULT 0, attempt_count INTEGER NOT NULL DEFAULT 0, "
            "max_attempts INTEGER NOT NULL DEFAULT 2, retry_class VARCHAR(50) NOT NULL DEFAULT 'transient', retry_at TIMESTAMP, "
            "lease_owner VARCHAR(120), lease_acquired_at TIMESTAMP, lease_expires_at TIMESTAMP, heartbeat_at TIMESTAMP, "
            "started_at TIMESTAMP, finished_at TIMESTAMP, normalized_query TEXT NOT NULL DEFAULT '', request_snapshot_json JSON NOT NULL DEFAULT '{}', "
            "response_hash VARCHAR(128), candidate_urls_json JSON NOT NULL DEFAULT '[]', coverage_gaps_json JSON NOT NULL DEFAULT '{}', "
            "budget_json JSON NOT NULL DEFAULT '{}', bytes_collected INTEGER NOT NULL DEFAULT 0, request_count INTEGER NOT NULL DEFAULT 0, "
            "error_text TEXT, output_json JSON NOT NULL DEFAULT '{}', created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL, "
            "FOREIGN KEY(research_provider_id) REFERENCES research_providers(research_provider_id), "
            "FOREIGN KEY(investigation_id) REFERENCES investigations(investigation_id))"
        )
    )
    for statement in (
        "CREATE INDEX IF NOT EXISTS ix_research_providers_provider_key ON research_providers(provider_key)",
        "CREATE INDEX IF NOT EXISTS ix_research_provider_runs_idempotency_key ON research_provider_runs(idempotency_key)",
        "CREATE INDEX IF NOT EXISTS ix_research_provider_run_provider_status ON research_provider_runs(research_provider_id, status)",
        "CREATE INDEX IF NOT EXISTS ix_research_provider_run_lease ON research_provider_runs(status, lease_expires_at)",
    ):
        connection.execute(text(statement))


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
