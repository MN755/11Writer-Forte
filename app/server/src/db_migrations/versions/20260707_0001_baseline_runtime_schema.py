"""Baseline runtime schema with legacy upgrade guards.

Revision ID: 20260707_0001
Revises:
Create Date: 2026-07-07 00:00:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260707_0001"
down_revision = None
branch_labels = None
depends_on = None


class SchemaAudit:
    def __init__(self) -> None:
        self.bind = op.get_bind()
        self.refresh()

    def refresh(self) -> None:
        self.inspector = sa.inspect(self.bind)
        self.table_names = set(self.inspector.get_table_names())

    def has_table(self, table_name: str) -> bool:
        return table_name in self.table_names

    def has_column(self, table_name: str, column_name: str) -> bool:
        if not self.has_table(table_name):
            return False
        return column_name in {column["name"] for column in self.inspector.get_columns(table_name)}

    def has_index(self, table_name: str, index_name: str) -> bool:
        if not self.has_table(table_name):
            return False
        return index_name in {index["name"] for index in self.inspector.get_indexes(table_name)}


def ensure_table(audit: SchemaAudit, table_name: str, create_table: callable) -> None:
    if audit.has_table(table_name):
        return
    create_table()
    audit.refresh()


def ensure_column(audit: SchemaAudit, table_name: str, column: sa.Column) -> None:
    if not audit.has_table(table_name) or audit.has_column(table_name, column.name):
        return
    op.add_column(table_name, column)
    audit.refresh()


def ensure_index(
    audit: SchemaAudit,
    index_name: str,
    table_name: str,
    columns: list[str],
    *,
    unique: bool = False,
) -> None:
    if not audit.has_table(table_name) or audit.has_index(table_name, index_name):
        return
    op.create_index(index_name, table_name, columns, unique=unique)
    audit.refresh()


def upgrade() -> None:
    audit = SchemaAudit()
    metadata_json = sa.JSON()
    now = sa.DateTime()

    ensure_table(
        audit,
        "data_layers",
        lambda: op.create_table(
            "data_layers",
            sa.Column("layer_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("key", sa.String(length=80), nullable=False),
            sa.Column("name", sa.String(length=200), nullable=False),
            sa.Column("description", sa.Text(), nullable=False, server_default=""),
            sa.Column("temporal_resolution", sa.String(length=80), nullable=False, server_default="unknown"),
            sa.Column("data_latency", sa.String(length=80), nullable=False, server_default="unknown"),
            sa.Column("metadata_json", metadata_json, nullable=False, server_default=sa.text("'{}'")),
            sa.Column("created_at", now, nullable=False),
            sa.Column("updated_at", now, nullable=False),
        ),
    )
    ensure_table(
        audit,
        "source_trust_profiles",
        lambda: op.create_table(
            "source_trust_profiles",
            sa.Column("trust_profile_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("domain", sa.String(length=255), nullable=False),
            sa.Column("trust_level", sa.String(length=30), nullable=False, server_default="neutral"),
            sa.Column("approval_policy", sa.String(length=40), nullable=False, server_default="manual_review"),
            sa.Column("integrity_source", sa.Boolean(), nullable=False, server_default=sa.text("0")),
            sa.Column("notes", sa.Text(), nullable=False, server_default=""),
            sa.Column("created_at", now, nullable=False),
            sa.Column("updated_at", now, nullable=False),
        ),
    )
    ensure_table(
        audit,
        "events",
        lambda: op.create_table(
            "events",
            sa.Column("event_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("slug", sa.String(length=120), nullable=False),
            sa.Column("title", sa.String(length=200), nullable=False),
            sa.Column("summary", sa.Text(), nullable=False, server_default=""),
            sa.Column("occurred_at", now, nullable=True),
            sa.Column("status", sa.String(length=50), nullable=False, server_default="open"),
            sa.Column("redaction_level", sa.String(length=50), nullable=False, server_default="public"),
            sa.Column("metadata_json", metadata_json, nullable=False, server_default=sa.text("'{}'")),
            sa.Column("created_at", now, nullable=False),
            sa.Column("updated_at", now, nullable=False),
        ),
    )
    ensure_table(
        audit,
        "entities",
        lambda: op.create_table(
            "entities",
            sa.Column("entity_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("slug", sa.String(length=120), nullable=False),
            sa.Column("entity_type", sa.String(length=40), nullable=False),
            sa.Column("canonical_name", sa.String(length=200), nullable=False),
            sa.Column("resolution_basis", sa.String(length=80), nullable=False, server_default="rule_based"),
            sa.Column("confidence_score", sa.Float(), nullable=False, server_default="0.5"),
            sa.Column("redaction_level", sa.String(length=50), nullable=False, server_default="public"),
            sa.Column("metadata_json", metadata_json, nullable=False, server_default=sa.text("'{}'")),
            sa.Column("created_at", now, nullable=False),
            sa.Column("updated_at", now, nullable=False),
        ),
    )
    ensure_table(
        audit,
        "geofences",
        lambda: op.create_table(
            "geofences",
            sa.Column("geofence_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("name", sa.String(length=160), nullable=False),
            sa.Column("description", sa.Text(), nullable=False, server_default=""),
            sa.Column("geometry_geojson", metadata_json, nullable=False),
            sa.Column("geometry_wkt", sa.Text(), nullable=True),
            sa.Column("rule_expression", sa.Text(), nullable=False, server_default=""),
            sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("1")),
            sa.Column("created_at", now, nullable=False),
            sa.Column("updated_at", now, nullable=False),
        ),
    )
    ensure_table(
        audit,
        "local_import_runs",
        lambda: op.create_table(
            "local_import_runs",
            sa.Column("import_run_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("source_path", sa.Text(), nullable=False),
            sa.Column("source_format", sa.String(length=30), nullable=False),
            sa.Column("layer_key", sa.String(length=80), nullable=False, server_default="unassigned"),
            sa.Column("status", sa.String(length=30), nullable=False, server_default="queued"),
            sa.Column("records_seen", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("records_imported", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("records_skipped", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("notes", sa.Text(), nullable=False, server_default=""),
            sa.Column("chain_of_custody_json", metadata_json, nullable=False, server_default=sa.text("'[]'")),
            sa.Column("created_at", now, nullable=False),
            sa.Column("updated_at", now, nullable=False),
        ),
    )
    ensure_table(
        audit,
        "observations",
        lambda: op.create_table(
            "observations",
            sa.Column("observation_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("import_run_id", sa.Integer(), sa.ForeignKey("local_import_runs.import_run_id"), nullable=True),
            sa.Column("event_id", sa.Integer(), sa.ForeignKey("events.event_id"), nullable=True),
            sa.Column("layer_key", sa.String(length=80), nullable=False),
            sa.Column("source_domain", sa.String(length=255), nullable=True),
            sa.Column("source_type", sa.String(length=40), nullable=False, server_default="local_import"),
            sa.Column("record_format", sa.String(length=30), nullable=False, server_default="json"),
            sa.Column("trust_level", sa.String(length=30), nullable=False, server_default="neutral"),
            sa.Column("approval_policy", sa.String(length=40), nullable=False, server_default="manual_review"),
            sa.Column("confidence_score", sa.Float(), nullable=False, server_default="0.5"),
            sa.Column("location_geojson", metadata_json, nullable=True),
            sa.Column("location_wkt", sa.Text(), nullable=True),
            sa.Column("content_text", sa.Text(), nullable=False, server_default=""),
            sa.Column("content_json", metadata_json, nullable=False, server_default=sa.text("'{}'")),
            sa.Column("raw_hash", sa.String(length=64), nullable=False),
            sa.Column("created_at", now, nullable=False),
            sa.Column("updated_at", now, nullable=False),
        ),
    )
    ensure_table(
        audit,
        "camera_inventory",
        lambda: op.create_table(
            "camera_inventory",
            sa.Column("camera_inventory_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("camera_key", sa.String(length=160), nullable=False),
            sa.Column("observation_id", sa.Integer(), sa.ForeignKey("observations.observation_id"), nullable=True),
            sa.Column("external_id", sa.String(length=120), nullable=True),
            sa.Column("name", sa.String(length=200), nullable=False),
            sa.Column("source_domain", sa.String(length=255), nullable=True),
            sa.Column("layer_key", sa.String(length=80), nullable=False),
            sa.Column("provider", sa.String(length=120), nullable=False, server_default=""),
            sa.Column("road_name", sa.String(length=160), nullable=False, server_default=""),
            sa.Column("status", sa.String(length=40), nullable=False, server_default="unknown"),
            sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.text("1")),
            sa.Column("image_url", sa.Text(), nullable=True),
            sa.Column("stream_url", sa.Text(), nullable=True),
            sa.Column("page_url", sa.Text(), nullable=True),
            sa.Column("location_geojson", metadata_json, nullable=True),
            sa.Column("location_wkt", sa.Text(), nullable=True),
            sa.Column("last_observed_at", now, nullable=True),
            sa.Column("confidence_score", sa.Float(), nullable=False, server_default="0.5"),
            sa.Column("metadata_json", metadata_json, nullable=False, server_default=sa.text("'{}'")),
            sa.Column("created_at", now, nullable=False),
            sa.Column("updated_at", now, nullable=False),
        ),
    )
    ensure_table(
        audit,
        "camera_source_inventory",
        lambda: op.create_table(
            "camera_source_inventory",
            sa.Column("camera_source_inventory_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("candidate_key", sa.String(length=200), nullable=False),
            sa.Column(
                "camera_inventory_id",
                sa.Integer(),
                sa.ForeignKey("camera_inventory.camera_inventory_id"),
                nullable=True,
            ),
            sa.Column("observation_id", sa.Integer(), sa.ForeignKey("observations.observation_id"), nullable=True),
            sa.Column("external_id", sa.String(length=120), nullable=True),
            sa.Column("name", sa.String(length=200), nullable=False),
            sa.Column("source_domain", sa.String(length=255), nullable=True),
            sa.Column("layer_key", sa.String(length=80), nullable=False),
            sa.Column("provider", sa.String(length=120), nullable=False, server_default=""),
            sa.Column("endpoint_kind", sa.String(length=40), nullable=False),
            sa.Column("endpoint_url", sa.Text(), nullable=False),
            sa.Column("status", sa.String(length=40), nullable=False, server_default="candidate"),
            sa.Column("verification_state", sa.String(length=40), nullable=False, server_default="observed"),
            sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.text("1")),
            sa.Column("last_observed_at", now, nullable=True),
            sa.Column("last_checked_at", now, nullable=True),
            sa.Column("confidence_score", sa.Float(), nullable=False, server_default="0.5"),
            sa.Column("graduation_score", sa.Float(), nullable=False, server_default="0.0"),
            sa.Column("metadata_json", metadata_json, nullable=False, server_default=sa.text("'{}'")),
            sa.Column("created_at", now, nullable=False),
            sa.Column("updated_at", now, nullable=False),
        ),
    )
    ensure_table(
        audit,
        "storage_objects",
        lambda: op.create_table(
            "storage_objects",
            sa.Column("storage_object_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("object_key", sa.String(length=200), nullable=False),
            sa.Column("object_kind", sa.String(length=60), nullable=False),
            sa.Column("owner_type", sa.String(length=60), nullable=False),
            sa.Column("owner_id", sa.String(length=120), nullable=False),
            sa.Column("content_hash", sa.String(length=64), nullable=True),
            sa.Column("media_type", sa.String(length=120), nullable=True),
            sa.Column("storage_tier", sa.String(length=30), nullable=False, server_default="hot"),
            sa.Column("retention_class", sa.String(length=30), nullable=False, server_default="operational"),
            sa.Column("lifecycle_status", sa.String(length=30), nullable=False, server_default="active"),
            sa.Column("source_uri", sa.Text(), nullable=True),
            sa.Column("object_uri", sa.Text(), nullable=False),
            sa.Column("byte_size", sa.Integer(), nullable=True),
            sa.Column("observed_at", now, nullable=True),
            sa.Column("expires_at", now, nullable=True),
            sa.Column("promoted_by_type", sa.String(length=60), nullable=True),
            sa.Column("promoted_by_id", sa.String(length=120), nullable=True),
            sa.Column("degraded_from_storage_object_id", sa.Integer(), nullable=True),
            sa.Column("metadata_json", metadata_json, nullable=False, server_default=sa.text("'{}'")),
            sa.Column("created_at", now, nullable=False),
            sa.Column("updated_at", now, nullable=False),
        ),
    )
    ensure_table(
        audit,
        "event_observation_links",
        lambda: op.create_table(
            "event_observation_links",
            sa.Column("event_observation_link_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("event_id", sa.Integer(), sa.ForeignKey("events.event_id"), nullable=False),
            sa.Column("observation_id", sa.Integer(), sa.ForeignKey("observations.observation_id"), nullable=False),
            sa.Column("relationship_type", sa.String(length=40), nullable=False, server_default="supporting"),
            sa.Column("confidence_contribution", sa.Float(), nullable=False, server_default="0.5"),
            sa.Column("created_at", now, nullable=False),
        ),
    )
    ensure_table(
        audit,
        "entity_observation_links",
        lambda: op.create_table(
            "entity_observation_links",
            sa.Column("entity_observation_link_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("entity_id", sa.Integer(), sa.ForeignKey("entities.entity_id"), nullable=False),
            sa.Column("observation_id", sa.Integer(), sa.ForeignKey("observations.observation_id"), nullable=False),
            sa.Column("match_basis", sa.String(length=80), nullable=False, server_default="rule_based"),
            sa.Column("confidence_contribution", sa.Float(), nullable=False, server_default="0.5"),
            sa.Column("created_at", now, nullable=False),
        ),
    )
    ensure_table(
        audit,
        "alerts",
        lambda: op.create_table(
            "alerts",
            sa.Column("alert_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("event_id", sa.Integer(), sa.ForeignKey("events.event_id"), nullable=True),
            sa.Column("geofence_id", sa.Integer(), sa.ForeignKey("geofences.geofence_id"), nullable=True),
            sa.Column("severity", sa.String(length=30), nullable=False, server_default="info"),
            sa.Column("status", sa.String(length=30), nullable=False, server_default="open"),
            sa.Column("dedupe_key", sa.String(length=160), nullable=True),
            sa.Column("message", sa.Text(), nullable=False),
            sa.Column("disposition_note", sa.Text(), nullable=False, server_default=""),
            sa.Column("trigger_basis_json", metadata_json, nullable=False, server_default=sa.text("'{}'")),
            sa.Column("created_at", now, nullable=False),
            sa.Column("updated_at", now, nullable=False),
        ),
    )
    ensure_table(
        audit,
        "scheduled_tasks",
        lambda: op.create_table(
            "scheduled_tasks",
            sa.Column("task_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("name", sa.String(length=160), nullable=False),
            sa.Column("task_type", sa.String(length=40), nullable=False),
            sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("1")),
            sa.Column("interval_seconds", sa.Integer(), nullable=False),
            sa.Column("retry_attempts", sa.Integer(), nullable=False, server_default="1"),
            sa.Column("retry_backoff_seconds", sa.Float(), nullable=False, server_default="0.0"),
            sa.Column("source_id", sa.Integer(), nullable=True),
            sa.Column("target_path", sa.Text(), nullable=True),
            sa.Column("layer_key", sa.String(length=80), nullable=True),
            sa.Column("geofence_id", sa.Integer(), sa.ForeignKey("geofences.geofence_id"), nullable=True),
            sa.Column("notes", sa.Text(), nullable=False, server_default=""),
            sa.Column("payload_json", metadata_json, nullable=False, server_default=sa.text("'{}'")),
            sa.Column("last_run_at", now, nullable=True),
            sa.Column("next_run_at", now, nullable=True),
            sa.Column("created_at", now, nullable=False),
            sa.Column("updated_at", now, nullable=False),
        ),
    )
    ensure_table(
        audit,
        "scheduled_task_runs",
        lambda: op.create_table(
            "scheduled_task_runs",
            sa.Column("task_run_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("task_id", sa.Integer(), sa.ForeignKey("scheduled_tasks.task_id"), nullable=False),
            sa.Column("status", sa.String(length=30), nullable=False, server_default="queued"),
            sa.Column("started_at", now, nullable=False),
            sa.Column("finished_at", now, nullable=True),
            sa.Column("records_affected", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("error_text", sa.Text(), nullable=True),
            sa.Column("output_json", metadata_json, nullable=False, server_default=sa.text("'{}'")),
        ),
    )
    ensure_table(
        audit,
        "source_definitions",
        lambda: op.create_table(
            "source_definitions",
            sa.Column("source_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("name", sa.String(length=160), nullable=False),
            sa.Column("source_kind", sa.String(length=40), nullable=False),
            sa.Column("layer_key", sa.String(length=80), nullable=False),
            sa.Column("target_uri", sa.Text(), nullable=False),
            sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("1")),
            sa.Column("integrity_source", sa.Boolean(), nullable=False, server_default=sa.text("0")),
            sa.Column("notes", sa.Text(), nullable=False, server_default=""),
            sa.Column("metadata_json", metadata_json, nullable=False, server_default=sa.text("'{}'")),
            sa.Column("created_at", now, nullable=False),
            sa.Column("updated_at", now, nullable=False),
        ),
    )
    ensure_table(
        audit,
        "source_runs",
        lambda: op.create_table(
            "source_runs",
            sa.Column("source_run_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("source_id", sa.Integer(), sa.ForeignKey("source_definitions.source_id"), nullable=False),
            sa.Column("import_run_id", sa.Integer(), sa.ForeignKey("local_import_runs.import_run_id"), nullable=True),
            sa.Column("status", sa.String(length=30), nullable=False, server_default="queued"),
            sa.Column("started_at", now, nullable=False),
            sa.Column("finished_at", now, nullable=True),
            sa.Column("records_imported", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("error_text", sa.Text(), nullable=True),
            sa.Column("output_json", metadata_json, nullable=False, server_default=sa.text("'{}'")),
        ),
    )
    ensure_table(
        audit,
        "situation_products",
        lambda: op.create_table(
            "situation_products",
            sa.Column("product_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("event_id", sa.Integer(), sa.ForeignKey("events.event_id"), nullable=False),
            sa.Column("product_type", sa.String(length=40), nullable=False),
            sa.Column("redaction_level", sa.String(length=50), nullable=False, server_default="public"),
            sa.Column("title", sa.String(length=200), nullable=False),
            sa.Column("body_text", sa.Text(), nullable=False),
            sa.Column("citations_json", metadata_json, nullable=False, server_default=sa.text("'[]'")),
            sa.Column("generated_by", sa.String(length=80), nullable=False, server_default="rule_based"),
            sa.Column("created_at", now, nullable=False),
            sa.Column("updated_at", now, nullable=False),
        ),
    )
    ensure_table(
        audit,
        "custody_logs",
        lambda: op.create_table(
            "custody_logs",
            sa.Column("custody_log_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("object_type", sa.String(length=50), nullable=False),
            sa.Column("object_id", sa.String(length=120), nullable=False),
            sa.Column("action", sa.String(length=80), nullable=False),
            sa.Column("actor", sa.String(length=80), nullable=False, server_default="system"),
            sa.Column("details_json", metadata_json, nullable=False, server_default=sa.text("'{}'")),
            sa.Column("created_at", now, nullable=False),
        ),
    )

    for table_name, columns in {
        "geofences": [sa.Column("geometry_wkt", sa.Text(), nullable=True)],
        "observations": [sa.Column("location_wkt", sa.Text(), nullable=True)],
        "local_import_runs": [sa.Column("records_skipped", sa.Integer(), nullable=False, server_default="0")],
        "scheduled_tasks": [
            sa.Column("retry_attempts", sa.Integer(), nullable=False, server_default="1"),
            sa.Column("retry_backoff_seconds", sa.Float(), nullable=False, server_default="0.0"),
        ],
        "alerts": [sa.Column("disposition_note", sa.Text(), nullable=False, server_default="")],
    }.items():
        for column in columns:
            ensure_column(audit, table_name, column)

    for index_name, table_name, columns, unique in (
        ("ix_data_layers_key", "data_layers", ["key"], True),
        ("ix_source_trust_profiles_domain", "source_trust_profiles", ["domain"], True),
        ("ix_events_slug", "events", ["slug"], True),
        ("ix_entities_slug", "entities", ["slug"], True),
        ("ix_entities_entity_type", "entities", ["entity_type"], False),
        ("ix_geofences_name", "geofences", ["name"], True),
        ("ix_observations_layer_key", "observations", ["layer_key"], False),
        ("ix_observations_raw_hash", "observations", ["raw_hash"], False),
        ("ix_camera_inventory_camera_key", "camera_inventory", ["camera_key"], True),
        ("ix_camera_inventory_observation_id", "camera_inventory", ["observation_id"], False),
        ("ix_camera_inventory_external_id", "camera_inventory", ["external_id"], False),
        ("ix_camera_inventory_source_domain", "camera_inventory", ["source_domain"], False),
        ("ix_camera_inventory_layer_key", "camera_inventory", ["layer_key"], False),
        ("ix_camera_inventory_status", "camera_inventory", ["status"], False),
        ("ix_camera_inventory_active", "camera_inventory", ["active"], False),
        ("ix_camera_source_inventory_candidate_key", "camera_source_inventory", ["candidate_key"], True),
        ("ix_camera_source_inventory_camera_inventory_id", "camera_source_inventory", ["camera_inventory_id"], False),
        ("ix_camera_source_inventory_observation_id", "camera_source_inventory", ["observation_id"], False),
        ("ix_camera_source_inventory_external_id", "camera_source_inventory", ["external_id"], False),
        ("ix_camera_source_inventory_source_domain", "camera_source_inventory", ["source_domain"], False),
        ("ix_camera_source_inventory_layer_key", "camera_source_inventory", ["layer_key"], False),
        ("ix_camera_source_inventory_endpoint_kind", "camera_source_inventory", ["endpoint_kind"], False),
        ("ix_camera_source_inventory_status", "camera_source_inventory", ["status"], False),
        ("ix_camera_source_inventory_verification_state", "camera_source_inventory", ["verification_state"], False),
        ("ix_camera_source_inventory_active", "camera_source_inventory", ["active"], False),
        ("ix_camera_source_inventory_last_observed_at", "camera_source_inventory", ["last_observed_at"], False),
        ("ix_storage_objects_object_key", "storage_objects", ["object_key"], True),
        ("ix_storage_objects_object_kind", "storage_objects", ["object_kind"], False),
        ("ix_storage_objects_owner_type", "storage_objects", ["owner_type"], False),
        ("ix_storage_objects_owner_id", "storage_objects", ["owner_id"], False),
        ("ix_storage_objects_content_hash", "storage_objects", ["content_hash"], False),
        ("ix_storage_objects_storage_tier", "storage_objects", ["storage_tier"], False),
        ("ix_storage_objects_retention_class", "storage_objects", ["retention_class"], False),
        ("ix_storage_objects_lifecycle_status", "storage_objects", ["lifecycle_status"], False),
        ("ix_storage_objects_observed_at", "storage_objects", ["observed_at"], False),
        ("ix_storage_objects_expires_at", "storage_objects", ["expires_at"], False),
        ("ix_event_observation_links_event_id", "event_observation_links", ["event_id"], False),
        ("ix_event_observation_links_observation_id", "event_observation_links", ["observation_id"], False),
        ("ix_entity_observation_links_entity_id", "entity_observation_links", ["entity_id"], False),
        ("ix_entity_observation_links_observation_id", "entity_observation_links", ["observation_id"], False),
        ("ix_alerts_dedupe_key", "alerts", ["dedupe_key"], False),
        ("ix_scheduled_tasks_name", "scheduled_tasks", ["name"], True),
        ("ix_scheduled_tasks_task_type", "scheduled_tasks", ["task_type"], False),
        ("ix_scheduled_tasks_source_id", "scheduled_tasks", ["source_id"], False),
        ("ix_scheduled_tasks_layer_key", "scheduled_tasks", ["layer_key"], False),
        ("ix_scheduled_tasks_geofence_id", "scheduled_tasks", ["geofence_id"], False),
        ("ix_scheduled_task_runs_task_id", "scheduled_task_runs", ["task_id"], False),
        ("ix_source_definitions_name", "source_definitions", ["name"], True),
        ("ix_source_definitions_source_kind", "source_definitions", ["source_kind"], False),
        ("ix_source_definitions_layer_key", "source_definitions", ["layer_key"], False),
        ("ix_source_runs_source_id", "source_runs", ["source_id"], False),
        ("ix_source_runs_import_run_id", "source_runs", ["import_run_id"], False),
        ("ix_situation_products_event_id", "situation_products", ["event_id"], False),
        ("ix_situation_products_product_type", "situation_products", ["product_type"], False),
        ("ix_custody_logs_object_type", "custody_logs", ["object_type"], False),
        ("ix_custody_logs_object_id", "custody_logs", ["object_id"], False),
    ):
        ensure_index(audit, index_name, table_name, columns, unique=unique)

    if audit.bind.dialect.name == "postgresql":
        op.execute(sa.text("CREATE EXTENSION IF NOT EXISTS postgis"))
        op.execute(
            sa.text(
                "CREATE INDEX IF NOT EXISTS idx_observations_location_wkt_gist "
                "ON observations USING GIST (ST_GeomFromText(location_wkt, 4326)) "
                "WHERE location_wkt IS NOT NULL"
            )
        )
        op.execute(
            sa.text(
                "CREATE INDEX IF NOT EXISTS idx_geofences_geometry_wkt_gist "
                "ON geofences USING GIST (ST_GeomFromText(geometry_wkt, 4326)) "
                "WHERE geometry_wkt IS NOT NULL"
            )
        )
        op.execute(
            sa.text(
                "CREATE INDEX IF NOT EXISTS idx_camera_inventory_location_wkt_gist "
                "ON camera_inventory USING GIST (ST_GeomFromText(location_wkt, 4326)) "
                "WHERE location_wkt IS NOT NULL"
            )
        )


def downgrade() -> None:
    raise NotImplementedError("Downgrades are intentionally not implemented for the baseline runtime schema.")
