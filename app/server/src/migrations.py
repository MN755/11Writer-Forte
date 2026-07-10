from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

HEAD_REVISION = "20260710_0001"
VERSION_TABLE = "runtime_schema_version"

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
    "watches",
    "watch_runs",
    "worker_statuses",
    "source_runs",
    "source_checkpoints",
    "source_dead_letters",
    "situation_products",
    "custody_logs",
)


@dataclass(frozen=True)
class DatabaseRevisionStatus:
    current_revision: str | None
    head_revision: str
    head_revisions: tuple[str, ...]
    version_table_present: bool
    has_application_tables: bool
    schema_up_to_date: bool


def inspect_database_revision(engine: Engine) -> DatabaseRevisionStatus:
    with engine.connect() as connection:
        inspector = inspect(connection)
        table_names = set(inspector.get_table_names())
        version_table_present = VERSION_TABLE in table_names
        has_application_tables = any(table_name in table_names for table_name in APPLICATION_TABLES)
        current_revision: str | None = None
        if version_table_present:
            current_revision = connection.execute(
                text(f"SELECT version_num FROM {VERSION_TABLE} LIMIT 1")
            ).scalar_one_or_none()
    schema_up_to_date = current_revision == HEAD_REVISION
    return DatabaseRevisionStatus(
        current_revision=current_revision,
        head_revision=HEAD_REVISION,
        head_revisions=(HEAD_REVISION,),
        version_table_present=version_table_present,
        has_application_tables=has_application_tables,
        schema_up_to_date=schema_up_to_date,
    )


def upgrade_database(engine: Engine, revision: str = "head") -> DatabaseRevisionStatus:
    target_revision = HEAD_REVISION if revision == "head" else revision
    with engine.begin() as connection:
        connection.execute(
            text(
                f"CREATE TABLE IF NOT EXISTS {VERSION_TABLE} "
                "(version_num VARCHAR(64) NOT NULL PRIMARY KEY)"
            )
        )
        connection.execute(text(f"DELETE FROM {VERSION_TABLE}"))
        connection.execute(
            text(f"INSERT INTO {VERSION_TABLE} (version_num) VALUES (:revision)"),
            {"revision": target_revision},
        )
    return inspect_database_revision(engine)
