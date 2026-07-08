from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import inspect
from sqlalchemy.engine import Engine

APPLICATION_TABLES: tuple[str, ...] = (
    "data_layers",
    "source_trust_profiles",
    "source_definitions",
    "local_import_runs",
    "observations",
    "camera_inventory",
    "camera_source_inventory",
    "storage_objects",
    "events",
    "entities",
    "event_observation_links",
    "entity_observation_links",
    "geofences",
    "alerts",
    "scheduled_tasks",
    "scheduled_task_runs",
    "source_runs",
    "situation_products",
    "custody_logs",
)


class DatabaseMigrationRequiredError(RuntimeError):
    pass


@dataclass(frozen=True)
class DatabaseRevisionStatus:
    current_revision: str | None
    head_revision: str
    head_revisions: tuple[str, ...]
    version_table_present: bool
    has_application_tables: bool
    schema_up_to_date: bool


def get_migrations_path() -> Path:
    return Path(__file__).resolve().parent / "db_migrations"


def build_alembic_config(database_url: str) -> Config:
    config = Config()
    config.set_main_option("script_location", str(get_migrations_path()))
    config.set_main_option("sqlalchemy.url", database_url)
    return config


def get_head_revisions(database_url: str) -> tuple[str, ...]:
    script = ScriptDirectory.from_config(build_alembic_config(database_url))
    return tuple(script.get_heads())


def inspect_database_revision(engine: Engine) -> DatabaseRevisionStatus:
    head_revisions = get_head_revisions(str(engine.url))
    head_revision = head_revisions[0] if head_revisions else "base"
    with engine.connect() as connection:
        inspector = inspect(connection)
        table_names = set(inspector.get_table_names())
        version_table_present = "alembic_version" in table_names
        has_application_tables = any(table_name in table_names for table_name in APPLICATION_TABLES)
        context = MigrationContext.configure(connection)
        current_heads = tuple(context.get_current_heads())
    schema_up_to_date = bool(current_heads) and set(current_heads) == set(head_revisions)
    current_revision = current_heads[0] if len(current_heads) == 1 else None
    return DatabaseRevisionStatus(
        current_revision=current_revision,
        head_revision=head_revision,
        head_revisions=head_revisions,
        version_table_present=version_table_present,
        has_application_tables=has_application_tables,
        schema_up_to_date=schema_up_to_date,
    )


def upgrade_database(engine: Engine, revision: str = "head") -> DatabaseRevisionStatus:
    config = build_alembic_config(str(engine.url))
    with engine.begin() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, revision)
    return inspect_database_revision(engine)


def ensure_database_revision(
    engine: Engine,
    *,
    auto_upgrade: bool = False,
) -> DatabaseRevisionStatus:
    if auto_upgrade:
        return upgrade_database(engine)

    status = inspect_database_revision(engine)
    if not status.has_application_tables and not status.version_table_present:
        raise DatabaseMigrationRequiredError(
            "Database is not initialized. Run `elevenwriter init-db` before starting the runtime."
        )
    if not status.version_table_present:
        raise DatabaseMigrationRequiredError(
            "Legacy database detected without Alembic revision state. Run `elevenwriter migrate-db`."
        )
    if not status.schema_up_to_date:
        raise DatabaseMigrationRequiredError(
            "Database schema is not at the Alembic head revision. Run `elevenwriter migrate-db`."
        )
    return status
