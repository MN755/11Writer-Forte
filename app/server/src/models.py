from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import JSON, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(default=utcnow, onupdate=utcnow)


class DataLayerORM(TimestampMixin, Base):
    __tablename__ = "data_layers"

    layer_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(String(80), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(200))
    description: Mapped[str] = mapped_column(Text, default="")
    temporal_resolution: Mapped[str] = mapped_column(String(80), default="unknown")
    data_latency: Mapped[str] = mapped_column(String(80), default="unknown")
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class EventORM(TimestampMixin, Base):
    __tablename__ = "events"

    event_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    slug: Mapped[str] = mapped_column(String(120), unique=True, index=True)
    title: Mapped[str] = mapped_column(String(200))
    summary: Mapped[str] = mapped_column(Text, default="")
    occurred_at: Mapped[datetime | None] = mapped_column(default=None)
    status: Mapped[str] = mapped_column(String(50), default="open")
    redaction_level: Mapped[str] = mapped_column(String(50), default="public")
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

    observation_links: Mapped[list["EventObservationLinkORM"]] = relationship(back_populates="event")
    products: Mapped[list["SituationProductORM"]] = relationship(back_populates="event")


class EntityORM(TimestampMixin, Base):
    __tablename__ = "entities"

    entity_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    slug: Mapped[str] = mapped_column(String(120), unique=True, index=True)
    entity_type: Mapped[str] = mapped_column(String(40), index=True)
    canonical_name: Mapped[str] = mapped_column(String(200))
    resolution_basis: Mapped[str] = mapped_column(String(80), default="rule_based")
    confidence_score: Mapped[float] = mapped_column(Float, default=0.5)
    redaction_level: Mapped[str] = mapped_column(String(50), default="public")
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

    observation_links: Mapped[list["EntityObservationLinkORM"]] = relationship(back_populates="entity")


class GeofenceORM(TimestampMixin, Base):
    __tablename__ = "geofences"

    geofence_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(160), unique=True, index=True)
    description: Mapped[str] = mapped_column(Text, default="")
    geometry_geojson: Mapped[dict[str, Any]] = mapped_column(JSON)
    geometry_wkt: Mapped[str | None] = mapped_column(Text, default=None)
    rule_expression: Mapped[str] = mapped_column(Text, default="")
    enabled: Mapped[bool] = mapped_column(default=True)


class AlertORM(TimestampMixin, Base):
    __tablename__ = "alerts"

    alert_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[int | None] = mapped_column(ForeignKey("events.event_id"), default=None)
    geofence_id: Mapped[int | None] = mapped_column(ForeignKey("geofences.geofence_id"), default=None)
    severity: Mapped[str] = mapped_column(String(30), default="info")
    status: Mapped[str] = mapped_column(String(30), default="open")
    dedupe_key: Mapped[str | None] = mapped_column(String(160), index=True, default=None)
    message: Mapped[str] = mapped_column(Text)
    disposition_note: Mapped[str] = mapped_column(Text, default="")
    trigger_basis_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class SourceTrustProfileORM(TimestampMixin, Base):
    __tablename__ = "source_trust_profiles"

    trust_profile_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    domain: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    trust_level: Mapped[str] = mapped_column(String(30), default="neutral")
    approval_policy: Mapped[str] = mapped_column(String(40), default="manual_review")
    integrity_source: Mapped[bool] = mapped_column(default=False)
    notes: Mapped[str] = mapped_column(Text, default="")


class LocalImportRunORM(TimestampMixin, Base):
    __tablename__ = "local_import_runs"

    import_run_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_path: Mapped[str] = mapped_column(Text)
    source_format: Mapped[str] = mapped_column(String(30))
    layer_key: Mapped[str] = mapped_column(String(80), default="unassigned")
    status: Mapped[str] = mapped_column(String(30), default="queued")
    records_seen: Mapped[int] = mapped_column(Integer, default=0)
    records_imported: Mapped[int] = mapped_column(Integer, default=0)
    records_skipped: Mapped[int] = mapped_column(Integer, default=0)
    notes: Mapped[str] = mapped_column(Text, default="")
    chain_of_custody_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list)

    observations: Mapped[list["ObservationORM"]] = relationship(back_populates="import_run")


class ObservationORM(TimestampMixin, Base):
    __tablename__ = "observations"

    observation_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    import_run_id: Mapped[int | None] = mapped_column(ForeignKey("local_import_runs.import_run_id"))
    event_id: Mapped[int | None] = mapped_column(ForeignKey("events.event_id"), default=None)
    layer_key: Mapped[str] = mapped_column(String(80), index=True)
    source_domain: Mapped[str | None] = mapped_column(String(255), default=None)
    source_type: Mapped[str] = mapped_column(String(40), default="local_import")
    record_format: Mapped[str] = mapped_column(String(30), default="json")
    trust_level: Mapped[str] = mapped_column(String(30), default="neutral")
    approval_policy: Mapped[str] = mapped_column(String(40), default="manual_review")
    confidence_score: Mapped[float] = mapped_column(Float, default=0.5)
    location_geojson: Mapped[dict[str, Any] | None] = mapped_column(JSON, default=None)
    location_wkt: Mapped[str | None] = mapped_column(Text, default=None)
    content_text: Mapped[str] = mapped_column(Text, default="")
    content_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    raw_hash: Mapped[str] = mapped_column(String(64), index=True)

    import_run: Mapped[LocalImportRunORM | None] = relationship(back_populates="observations")
    event_links: Mapped[list["EventObservationLinkORM"]] = relationship(back_populates="observation")
    entity_links: Mapped[list["EntityObservationLinkORM"]] = relationship(back_populates="observation")


class CameraInventoryORM(TimestampMixin, Base):
    __tablename__ = "camera_inventory"

    camera_inventory_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    camera_key: Mapped[str] = mapped_column(String(160), unique=True, index=True)
    observation_id: Mapped[int | None] = mapped_column(ForeignKey("observations.observation_id"), default=None, index=True)
    external_id: Mapped[str | None] = mapped_column(String(120), default=None, index=True)
    name: Mapped[str] = mapped_column(String(200))
    source_domain: Mapped[str | None] = mapped_column(String(255), default=None, index=True)
    layer_key: Mapped[str] = mapped_column(String(80), index=True)
    provider: Mapped[str] = mapped_column(String(120), default="")
    road_name: Mapped[str] = mapped_column(String(160), default="")
    status: Mapped[str] = mapped_column(String(40), default="unknown", index=True)
    active: Mapped[bool] = mapped_column(default=True, index=True)
    image_url: Mapped[str | None] = mapped_column(Text, default=None)
    stream_url: Mapped[str | None] = mapped_column(Text, default=None)
    page_url: Mapped[str | None] = mapped_column(Text, default=None)
    location_geojson: Mapped[dict[str, Any] | None] = mapped_column(JSON, default=None)
    location_wkt: Mapped[str | None] = mapped_column(Text, default=None)
    last_observed_at: Mapped[datetime | None] = mapped_column(default=None)
    confidence_score: Mapped[float] = mapped_column(Float, default=0.5)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class StorageObjectORM(TimestampMixin, Base):
    __tablename__ = "storage_objects"

    storage_object_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    object_key: Mapped[str] = mapped_column(String(200), unique=True, index=True)
    object_kind: Mapped[str] = mapped_column(String(60), index=True)
    owner_type: Mapped[str] = mapped_column(String(60), index=True)
    owner_id: Mapped[str] = mapped_column(String(120), index=True)
    content_hash: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    media_type: Mapped[str | None] = mapped_column(String(120), default=None)
    storage_tier: Mapped[str] = mapped_column(String(30), default="hot", index=True)
    retention_class: Mapped[str] = mapped_column(String(30), default="operational", index=True)
    lifecycle_status: Mapped[str] = mapped_column(String(30), default="active", index=True)
    source_uri: Mapped[str | None] = mapped_column(Text, default=None)
    object_uri: Mapped[str] = mapped_column(Text)
    byte_size: Mapped[int | None] = mapped_column(Integer, default=None)
    observed_at: Mapped[datetime | None] = mapped_column(default=None, index=True)
    expires_at: Mapped[datetime | None] = mapped_column(default=None, index=True)
    promoted_by_type: Mapped[str | None] = mapped_column(String(60), default=None)
    promoted_by_id: Mapped[str | None] = mapped_column(String(120), default=None)
    degraded_from_storage_object_id: Mapped[int | None] = mapped_column(Integer, default=None)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class CustodyLogORM(Base):
    __tablename__ = "custody_logs"

    custody_log_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    object_type: Mapped[str] = mapped_column(String(50), index=True)
    object_id: Mapped[str] = mapped_column(String(120), index=True)
    action: Mapped[str] = mapped_column(String(80))
    actor: Mapped[str] = mapped_column(String(80), default="system")
    details_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)


class ScheduledTaskORM(TimestampMixin, Base):
    __tablename__ = "scheduled_tasks"

    task_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(160), unique=True, index=True)
    task_type: Mapped[str] = mapped_column(String(40), index=True)
    enabled: Mapped[bool] = mapped_column(default=True)
    interval_seconds: Mapped[int] = mapped_column(Integer)
    retry_attempts: Mapped[int] = mapped_column(Integer, default=1)
    retry_backoff_seconds: Mapped[float] = mapped_column(Float, default=0.0)
    source_id: Mapped[int | None] = mapped_column(default=None, index=True)
    target_path: Mapped[str | None] = mapped_column(Text, default=None)
    layer_key: Mapped[str | None] = mapped_column(String(80), default=None)
    geofence_id: Mapped[int | None] = mapped_column(ForeignKey("geofences.geofence_id"), default=None)
    notes: Mapped[str] = mapped_column(Text, default="")
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    last_run_at: Mapped[datetime | None] = mapped_column(default=None)
    next_run_at: Mapped[datetime | None] = mapped_column(default=None)

    runs: Mapped[list["ScheduledTaskRunORM"]] = relationship(back_populates="task")


class ScheduledTaskRunORM(Base):
    __tablename__ = "scheduled_task_runs"

    task_run_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[int] = mapped_column(ForeignKey("scheduled_tasks.task_id"), index=True)
    status: Mapped[str] = mapped_column(String(30), default="queued")
    started_at: Mapped[datetime] = mapped_column(default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(default=None)
    records_affected: Mapped[int] = mapped_column(Integer, default=0)
    error_text: Mapped[str | None] = mapped_column(Text, default=None)
    output_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

    task: Mapped[ScheduledTaskORM] = relationship(back_populates="runs")


class EventObservationLinkORM(Base):
    __tablename__ = "event_observation_links"

    event_observation_link_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("events.event_id"), index=True)
    observation_id: Mapped[int] = mapped_column(ForeignKey("observations.observation_id"), index=True)
    relationship_type: Mapped[str] = mapped_column(String(40), default="supporting")
    confidence_contribution: Mapped[float] = mapped_column(Float, default=0.5)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)

    event: Mapped[EventORM] = relationship(back_populates="observation_links")
    observation: Mapped[ObservationORM] = relationship(back_populates="event_links")


class EntityObservationLinkORM(Base):
    __tablename__ = "entity_observation_links"

    entity_observation_link_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    entity_id: Mapped[int] = mapped_column(ForeignKey("entities.entity_id"), index=True)
    observation_id: Mapped[int] = mapped_column(ForeignKey("observations.observation_id"), index=True)
    match_basis: Mapped[str] = mapped_column(String(80), default="rule_based")
    confidence_contribution: Mapped[float] = mapped_column(Float, default=0.5)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)

    entity: Mapped[EntityORM] = relationship(back_populates="observation_links")
    observation: Mapped[ObservationORM] = relationship(back_populates="entity_links")


class SituationProductORM(TimestampMixin, Base):
    __tablename__ = "situation_products"

    product_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("events.event_id"), index=True)
    product_type: Mapped[str] = mapped_column(String(40), index=True)
    redaction_level: Mapped[str] = mapped_column(String(50), default="public")
    title: Mapped[str] = mapped_column(String(200))
    body_text: Mapped[str] = mapped_column(Text)
    citations_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list)
    generated_by: Mapped[str] = mapped_column(String(80), default="rule_based")

    event: Mapped[EventORM] = relationship(back_populates="products")


class SourceDefinitionORM(TimestampMixin, Base):
    __tablename__ = "source_definitions"

    source_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(160), unique=True, index=True)
    source_kind: Mapped[str] = mapped_column(String(40), index=True)
    layer_key: Mapped[str] = mapped_column(String(80), index=True)
    target_uri: Mapped[str] = mapped_column(Text)
    enabled: Mapped[bool] = mapped_column(default=True)
    integrity_source: Mapped[bool] = mapped_column(default=False)
    notes: Mapped[str] = mapped_column(Text, default="")
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

    runs: Mapped[list["SourceRunORM"]] = relationship(back_populates="source")


class SourceRunORM(Base):
    __tablename__ = "source_runs"

    source_run_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("source_definitions.source_id"), index=True)
    import_run_id: Mapped[int | None] = mapped_column(ForeignKey("local_import_runs.import_run_id"), default=None)
    status: Mapped[str] = mapped_column(String(30), default="queued")
    started_at: Mapped[datetime] = mapped_column(default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(default=None)
    records_imported: Mapped[int] = mapped_column(Integer, default=0)
    error_text: Mapped[str | None] = mapped_column(Text, default=None)
    output_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

    source: Mapped[SourceDefinitionORM] = relationship(back_populates="runs")
