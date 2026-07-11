from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import JSON, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint
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


class CameraSourceInventoryORM(TimestampMixin, Base):
    __tablename__ = "camera_source_inventory"

    camera_source_inventory_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    candidate_key: Mapped[str] = mapped_column(String(200), unique=True, index=True)
    camera_inventory_id: Mapped[int | None] = mapped_column(
        ForeignKey("camera_inventory.camera_inventory_id"),
        default=None,
        index=True,
    )
    observation_id: Mapped[int | None] = mapped_column(
        ForeignKey("observations.observation_id"),
        default=None,
        index=True,
    )
    external_id: Mapped[str | None] = mapped_column(String(120), default=None, index=True)
    name: Mapped[str] = mapped_column(String(200))
    source_domain: Mapped[str | None] = mapped_column(String(255), default=None, index=True)
    layer_key: Mapped[str] = mapped_column(String(80), index=True)
    provider: Mapped[str] = mapped_column(String(120), default="")
    endpoint_kind: Mapped[str] = mapped_column(String(40), index=True)
    endpoint_url: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(40), default="candidate", index=True)
    verification_state: Mapped[str] = mapped_column(String(40), default="observed", index=True)
    active: Mapped[bool] = mapped_column(default=True, index=True)
    last_observed_at: Mapped[datetime | None] = mapped_column(default=None, index=True)
    last_checked_at: Mapped[datetime | None] = mapped_column(default=None)
    confidence_score: Mapped[float] = mapped_column(Float, default=0.5)
    graduation_score: Mapped[float] = mapped_column(Float, default=0.0)
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


class DiscoveryCampaignORM(TimestampMixin, Base):
    __tablename__ = "discovery_campaigns"

    campaign_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(160), unique=True, index=True)
    description: Mapped[str] = mapped_column(Text, default="")
    mode: Mapped[str] = mapped_column(String(40), default="query_seeded", index=True)
    status: Mapped[str] = mapped_column(String(30), default="draft", index=True)
    enabled: Mapped[bool] = mapped_column(default=True, index=True)
    layer_key: Mapped[str | None] = mapped_column(String(80), default=None, index=True)
    query_text: Mapped[str] = mapped_column(Text, default="")
    modes_json: Mapped[list[str]] = mapped_column(JSON, default=list)
    query_strings_json: Mapped[list[str]] = mapped_column(JSON, default=list)
    search_templates_json: Mapped[list[str]] = mapped_column(JSON, default=list)
    format_targets_json: Mapped[list[str]] = mapped_column(JSON, default=list)
    seed_urls_json: Mapped[list[str]] = mapped_column(JSON, default=list)
    locale_variants_json: Mapped[list[str]] = mapped_column(JSON, default=list)
    language_variants_json: Mapped[list[str]] = mapped_column(JSON, default=list)
    domain_allowlist_json: Mapped[list[str]] = mapped_column(JSON, default=list)
    domain_denylist_json: Mapped[list[str]] = mapped_column(JSON, default=list)
    target_geography_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    entity_seeds_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list)
    historical_backfill: Mapped[bool] = mapped_column(default=False, index=True)
    recency_days: Mapped[int | None] = mapped_column(Integer, default=None)
    max_depth: Mapped[int] = mapped_column(Integer, default=2)
    max_pages: Mapped[int] = mapped_column(Integer, default=100)
    max_candidates: Mapped[int] = mapped_column(Integer, default=1000)
    request_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    crawl_policy_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    scoring_weights_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    schedule_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    last_run_at: Mapped[datetime | None] = mapped_column(default=None, index=True)
    last_completed_at: Mapped[datetime | None] = mapped_column(default=None)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class DiscoveryRunORM(TimestampMixin, Base):
    __tablename__ = "discovery_runs"

    discovery_run_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    campaign_id: Mapped[int] = mapped_column(
        ForeignKey("discovery_campaigns.campaign_id"),
        index=True,
    )
    mode: Mapped[str] = mapped_column(String(40), default="query_seeded", index=True)
    status: Mapped[str] = mapped_column(String(30), default="queued", index=True)
    trigger_kind: Mapped[str] = mapped_column(String(30), default="manual", index=True)
    actor: Mapped[str] = mapped_column(String(80), default="discovery_engine")
    resumed_from_run_id: Mapped[int | None] = mapped_column(Integer, default=None, index=True)
    started_at: Mapped[datetime] = mapped_column(default=utcnow, index=True)
    finished_at: Mapped[datetime | None] = mapped_column(default=None)
    pages_queued: Mapped[int] = mapped_column(Integer, default=0)
    pages_fetched: Mapped[int] = mapped_column(Integer, default=0)
    candidates_discovered: Mapped[int] = mapped_column(Integer, default=0)
    candidates_updated: Mapped[int] = mapped_column(Integer, default=0)
    error_count: Mapped[int] = mapped_column(Integer, default=0)
    request_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    policy_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    frontier_checkpoint_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    stats_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    output_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    error_text: Mapped[str | None] = mapped_column(Text, default=None)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class SourceCandidateORM(TimestampMixin, Base):
    __tablename__ = "source_candidates"
    __table_args__ = (
        Index("ix_source_candidates_status_score", "status", "score"),
        Index("ix_source_candidates_domain_status", "normalized_domain", "status"),
    )

    candidate_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    canonical_url_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    canonical_url: Mapped[str] = mapped_column(Text)
    discovered_url: Mapped[str] = mapped_column(Text)
    normalized_domain: Mapped[str] = mapped_column(String(255), index=True)
    path_pattern: Mapped[str | None] = mapped_column(String(500), default=None, index=True)
    first_campaign_id: Mapped[int | None] = mapped_column(
        ForeignKey("discovery_campaigns.campaign_id"),
        default=None,
        index=True,
    )
    last_campaign_id: Mapped[int | None] = mapped_column(
        ForeignKey("discovery_campaigns.campaign_id"),
        default=None,
        index=True,
    )
    first_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("discovery_runs.discovery_run_id"),
        default=None,
        index=True,
    )
    last_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("discovery_runs.discovery_run_id"),
        default=None,
        index=True,
    )
    parent_url: Mapped[str | None] = mapped_column(Text, default=None)
    discovery_method: Mapped[str] = mapped_column(String(50), default="unknown", index=True)
    candidate_type: Mapped[str] = mapped_column(String(60), default="unknown", index=True)
    format_hint: Mapped[str] = mapped_column(String(60), default="unknown", index=True)
    footprint_kind: Mapped[str] = mapped_column(String(40), default="unknown", index=True)
    footprint_geojson: Mapped[dict[str, Any] | None] = mapped_column(JSON, default=None)
    geo_hints_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    temporal_hints_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    format_hints_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    trust_hints_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    operational_hints_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    promotion_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String(40), default="candidate", index=True)
    score: Mapped[float] = mapped_column(Float, default=0.0, index=True)
    score_bucket: Mapped[str] = mapped_column(String(40), default="keep_candidate", index=True)
    score_breakdown_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    content_hash: Mapped[str | None] = mapped_column(String(64), default=None, index=True)
    schema_hash: Mapped[str | None] = mapped_column(String(64), default=None, index=True)
    first_seen_at: Mapped[datetime] = mapped_column(default=utcnow, index=True)
    last_seen_at: Mapped[datetime] = mapped_column(default=utcnow, index=True)
    last_changed_at: Mapped[datetime | None] = mapped_column(default=None)
    last_checked_at: Mapped[datetime | None] = mapped_column(default=None, index=True)
    last_revisited_at: Mapped[datetime | None] = mapped_column(default=None)
    next_revisit_at: Mapped[datetime | None] = mapped_column(default=None, index=True)
    revisit_count: Mapped[int] = mapped_column(Integer, default=0)
    failure_count: Mapped[int] = mapped_column(Integer, default=0)
    last_failure_at: Mapped[datetime | None] = mapped_column(default=None)
    last_error_text: Mapped[str | None] = mapped_column(Text, default=None)
    promoted_source_id: Mapped[int | None] = mapped_column(
        ForeignKey("source_definitions.source_id"),
        default=None,
        index=True,
    )
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class DiscoveryFrontierEntryORM(TimestampMixin, Base):
    __tablename__ = "discovery_frontier_entries"
    __table_args__ = (
        UniqueConstraint(
            "discovery_run_id",
            "canonical_url_hash",
            name="uq_discovery_frontier_run_url_hash",
        ),
        Index("ix_discovery_frontier_run_state_priority", "discovery_run_id", "state", "priority"),
        Index("ix_discovery_frontier_state_next_attempt", "state", "next_attempt_at"),
    )

    frontier_entry_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    discovery_run_id: Mapped[int] = mapped_column(
        ForeignKey("discovery_runs.discovery_run_id"),
        index=True,
    )
    campaign_id: Mapped[int] = mapped_column(
        ForeignKey("discovery_campaigns.campaign_id"),
        index=True,
    )
    candidate_id: Mapped[int | None] = mapped_column(
        ForeignKey("source_candidates.candidate_id"),
        default=None,
        index=True,
    )
    canonical_url_hash: Mapped[str] = mapped_column(String(64), index=True)
    canonical_url: Mapped[str] = mapped_column(Text)
    discovered_url: Mapped[str] = mapped_column(Text)
    priority: Mapped[float] = mapped_column(Float, default=0.0, index=True)
    state: Mapped[str] = mapped_column(String(30), default="queued", index=True)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, default=3)
    depth: Mapped[int] = mapped_column(Integer, default=0)
    parent_url: Mapped[str | None] = mapped_column(Text, default=None)
    parent_candidate_id: Mapped[int | None] = mapped_column(
        ForeignKey("source_candidates.candidate_id"),
        default=None,
        index=True,
    )
    discovery_method: Mapped[str] = mapped_column(String(50), default="unknown", index=True)
    next_attempt_at: Mapped[datetime | None] = mapped_column(default=None, index=True)
    last_attempt_at: Mapped[datetime | None] = mapped_column(default=None)
    claimed_at: Mapped[datetime | None] = mapped_column(default=None)
    fetched_at: Mapped[datetime | None] = mapped_column(default=None)
    completed_at: Mapped[datetime | None] = mapped_column(default=None)
    dead_lettered_at: Mapped[datetime | None] = mapped_column(default=None)
    checkpoint_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    last_error_text: Mapped[str | None] = mapped_column(Text, default=None)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class SourceCandidateRevisionORM(Base):
    __tablename__ = "source_candidate_revisions"
    __table_args__ = (
        UniqueConstraint(
            "candidate_id",
            "revision_number",
            name="uq_source_candidate_revision_number",
        ),
        Index("ix_source_candidate_revision_observed", "candidate_id", "observed_at"),
    )

    candidate_revision_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    candidate_id: Mapped[int] = mapped_column(ForeignKey("source_candidates.candidate_id"), index=True)
    campaign_id: Mapped[int | None] = mapped_column(
        ForeignKey("discovery_campaigns.campaign_id"),
        default=None,
        index=True,
    )
    discovery_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("discovery_runs.discovery_run_id"),
        default=None,
        index=True,
    )
    revision_number: Mapped[int] = mapped_column(Integer)
    revision_kind: Mapped[str] = mapped_column(String(40), default="observed", index=True)
    observed_at: Mapped[datetime] = mapped_column(default=utcnow, index=True)
    status: Mapped[str] = mapped_column(String(40), default="candidate")
    score: Mapped[float] = mapped_column(Float, default=0.0)
    score_bucket: Mapped[str] = mapped_column(String(40), default="keep_candidate")
    score_breakdown_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    content_hash: Mapped[str | None] = mapped_column(String(64), default=None)
    schema_hash: Mapped[str | None] = mapped_column(String(64), default=None)
    changed: Mapped[bool] = mapped_column(default=False)
    reason: Mapped[str] = mapped_column(Text, default="")
    snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)


class DiscoveryGraphEdgeORM(Base):
    __tablename__ = "discovery_graph_edges"
    __table_args__ = (
        Index("ix_discovery_graph_child_run", "child_candidate_id", "discovery_run_id"),
        Index("ix_discovery_graph_parent_run", "parent_candidate_id", "discovery_run_id"),
    )

    graph_edge_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    edge_key: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    campaign_id: Mapped[int] = mapped_column(
        ForeignKey("discovery_campaigns.campaign_id"),
        index=True,
    )
    discovery_run_id: Mapped[int] = mapped_column(
        ForeignKey("discovery_runs.discovery_run_id"),
        index=True,
    )
    parent_candidate_id: Mapped[int | None] = mapped_column(
        ForeignKey("source_candidates.candidate_id"),
        default=None,
        index=True,
    )
    child_candidate_id: Mapped[int] = mapped_column(
        ForeignKey("source_candidates.candidate_id"),
        index=True,
    )
    parent_url: Mapped[str | None] = mapped_column(Text, default=None)
    child_url: Mapped[str] = mapped_column(Text)
    edge_type: Mapped[str] = mapped_column(String(40), default="discovered_from", index=True)
    discovery_method: Mapped[str] = mapped_column(String(50), default="unknown", index=True)
    depth: Mapped[int] = mapped_column(Integer, default=0)
    evidence_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, index=True)


class CandidateHealthCheckORM(Base):
    __tablename__ = "candidate_health_checks"
    __table_args__ = (
        Index("ix_candidate_health_candidate_checked", "candidate_id", "checked_at"),
        Index("ix_candidate_health_status_checked", "status", "checked_at"),
    )

    health_check_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    candidate_id: Mapped[int] = mapped_column(ForeignKey("source_candidates.candidate_id"), index=True)
    discovery_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("discovery_runs.discovery_run_id"),
        default=None,
        index=True,
    )
    checked_at: Mapped[datetime] = mapped_column(default=utcnow, index=True)
    status: Mapped[str] = mapped_column(String(30), default="unknown", index=True)
    reachable: Mapped[bool] = mapped_column(default=False, index=True)
    http_status: Mapped[int | None] = mapped_column(Integer, default=None)
    latency_ms: Mapped[float | None] = mapped_column(Float, default=None)
    content_type: Mapped[str | None] = mapped_column(String(160), default=None)
    content_length: Mapped[int | None] = mapped_column(Integer, default=None)
    content_hash: Mapped[str | None] = mapped_column(String(64), default=None)
    schema_hash: Mapped[str | None] = mapped_column(String(64), default=None)
    changed: Mapped[bool] = mapped_column(default=False)
    redirect_url: Mapped[str | None] = mapped_column(Text, default=None)
    robots_allowed: Mapped[bool | None] = mapped_column(default=None)
    error_type: Mapped[str | None] = mapped_column(String(80), default=None)
    error_text: Mapped[str | None] = mapped_column(Text, default=None)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)


class CandidateSuppressionORM(Base):
    __tablename__ = "candidate_suppressions"
    __table_args__ = (
        Index("ix_candidate_suppression_candidate_status", "candidate_id", "status"),
        Index("ix_candidate_suppression_domain_status", "normalized_domain", "status"),
    )

    suppression_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    candidate_id: Mapped[int | None] = mapped_column(
        ForeignKey("source_candidates.candidate_id"),
        default=None,
        index=True,
    )
    normalized_domain: Mapped[str | None] = mapped_column(String(255), default=None, index=True)
    scope: Mapped[str] = mapped_column(String(30), default="candidate", index=True)
    status: Mapped[str] = mapped_column(String(30), default="active", index=True)
    reason_code: Mapped[str] = mapped_column(String(60), default="operator_suppressed", index=True)
    reason: Mapped[str] = mapped_column(Text, default="")
    actor: Mapped[str] = mapped_column(String(80), default="system")
    created_at: Mapped[datetime] = mapped_column(default=utcnow, index=True)
    expires_at: Mapped[datetime | None] = mapped_column(default=None, index=True)
    revoked_at: Mapped[datetime | None] = mapped_column(default=None)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class CandidatePromotionDecisionORM(Base):
    __tablename__ = "candidate_promotion_decisions"
    __table_args__ = (
        Index("ix_candidate_promotion_candidate_decided", "candidate_id", "decided_at"),
        Index("ix_candidate_promotion_decision_decided", "decision", "decided_at"),
    )

    promotion_decision_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    candidate_id: Mapped[int] = mapped_column(ForeignKey("source_candidates.candidate_id"), index=True)
    source_id: Mapped[int | None] = mapped_column(
        ForeignKey("source_definitions.source_id"),
        default=None,
        index=True,
    )
    discovery_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("discovery_runs.discovery_run_id"),
        default=None,
        index=True,
    )
    decision: Mapped[str] = mapped_column(String(40), default="deferred", index=True)
    recommended_source_kind: Mapped[str | None] = mapped_column(String(40), default=None, index=True)
    recommended_schedule_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    score: Mapped[float] = mapped_column(Float, default=0.0)
    score_bucket: Mapped[str] = mapped_column(String(40), default="keep_candidate")
    score_breakdown_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    reason: Mapped[str] = mapped_column(Text, default="")
    trust_reasoning_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    integrity_reasoning_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    health_risks_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    geo_relevance_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    evidence_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    actor: Mapped[str] = mapped_column(String(80), default="system")
    decided_at: Mapped[datetime] = mapped_column(default=utcnow, index=True)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class DiscoveryDomainPolicyORM(TimestampMixin, Base):
    __tablename__ = "discovery_domain_policies"

    domain_policy_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    normalized_domain: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    policy: Mapped[str] = mapped_column(String(30), default="allow", index=True)
    robots_mode: Mapped[str] = mapped_column(String(30), default="respect", index=True)
    enabled: Mapped[bool] = mapped_column(default=True, index=True)
    allow_subdomains: Mapped[bool] = mapped_column(default=True)
    crawl_delay_seconds: Mapped[float] = mapped_column(Float, default=1.0)
    max_concurrency: Mapped[int] = mapped_column(Integer, default=1)
    max_depth: Mapped[int] = mapped_column(Integer, default=2)
    max_pages_per_run: Mapped[int] = mapped_column(Integer, default=100)
    max_response_bytes: Mapped[int] = mapped_column(Integer, default=5_000_000)
    request_timeout_seconds: Mapped[float] = mapped_column(Float, default=20.0)
    retry_attempts: Mapped[int] = mapped_column(Integer, default=2)
    retry_backoff_seconds: Mapped[float] = mapped_column(Float, default=1.0)
    allowed_path_patterns_json: Mapped[list[str]] = mapped_column(JSON, default=list)
    denied_path_patterns_json: Mapped[list[str]] = mapped_column(JSON, default=list)
    allowed_content_types_json: Mapped[list[str]] = mapped_column(JSON, default=list)
    notes: Mapped[str] = mapped_column(Text, default="")
    last_fetch_at: Mapped[datetime | None] = mapped_column(default=None)
    next_allowed_at: Mapped[datetime | None] = mapped_column(default=None, index=True)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class RobotsObservationORM(Base):
    __tablename__ = "robots_observations"
    __table_args__ = (
        Index("ix_robots_observation_domain_fetched", "normalized_domain", "fetched_at"),
    )

    robots_observation_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    domain_policy_id: Mapped[int | None] = mapped_column(
        ForeignKey("discovery_domain_policies.domain_policy_id"),
        default=None,
        index=True,
    )
    discovery_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("discovery_runs.discovery_run_id"),
        default=None,
        index=True,
    )
    normalized_domain: Mapped[str] = mapped_column(String(255), index=True)
    robots_url: Mapped[str] = mapped_column(Text)
    fetched_at: Mapped[datetime] = mapped_column(default=utcnow, index=True)
    expires_at: Mapped[datetime | None] = mapped_column(default=None, index=True)
    status: Mapped[str] = mapped_column(String(30), default="unknown", index=True)
    http_status: Mapped[int | None] = mapped_column(Integer, default=None)
    allowed: Mapped[bool | None] = mapped_column(default=None)
    crawl_delay_seconds: Mapped[float | None] = mapped_column(Float, default=None)
    sitemap_urls_json: Mapped[list[str]] = mapped_column(JSON, default=list)
    rules_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    content_hash: Mapped[str | None] = mapped_column(String(64), default=None)
    error_text: Mapped[str | None] = mapped_column(Text, default=None)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)


class DiscoveryArtifactORM(Base):
    __tablename__ = "discovery_artifacts"
    __table_args__ = (
        Index("ix_discovery_artifact_candidate_fetched", "candidate_id", "fetched_at"),
        Index("ix_discovery_artifact_run_kind", "discovery_run_id", "artifact_kind"),
    )

    discovery_artifact_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    candidate_id: Mapped[int | None] = mapped_column(
        ForeignKey("source_candidates.candidate_id"),
        default=None,
        index=True,
    )
    discovery_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("discovery_runs.discovery_run_id"),
        default=None,
        index=True,
    )
    frontier_entry_id: Mapped[int | None] = mapped_column(
        ForeignKey("discovery_frontier_entries.frontier_entry_id"),
        default=None,
        index=True,
    )
    storage_object_id: Mapped[int | None] = mapped_column(
        ForeignKey("storage_objects.storage_object_id"),
        default=None,
        index=True,
    )
    artifact_kind: Mapped[str] = mapped_column(String(50), default="fetched_document", index=True)
    source_url: Mapped[str] = mapped_column(Text)
    media_type: Mapped[str | None] = mapped_column(String(160), default=None)
    content_hash: Mapped[str | None] = mapped_column(String(64), default=None, index=True)
    byte_size: Mapped[int | None] = mapped_column(Integer, default=None)
    fetched_at: Mapped[datetime] = mapped_column(default=utcnow, index=True)
    object_uri: Mapped[str | None] = mapped_column(Text, default=None)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)


class WatchORM(TimestampMixin, Base):
    __tablename__ = "watches"

    watch_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(160), unique=True, index=True)
    slug: Mapped[str] = mapped_column(String(160), unique=True, index=True)
    objective: Mapped[str] = mapped_column(Text)
    description: Mapped[str] = mapped_column(Text, default="")
    watch_type: Mapped[str] = mapped_column(String(40), index=True)
    state: Mapped[str] = mapped_column(String(30), default="enabled", index=True)
    rule_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    source_id: Mapped[int | None] = mapped_column(ForeignKey("source_definitions.source_id"), default=None, index=True)
    camera_inventory_id: Mapped[int | None] = mapped_column(ForeignKey("camera_inventory.camera_inventory_id"), default=None, index=True)
    camera_source_inventory_id: Mapped[int | None] = mapped_column(ForeignKey("camera_source_inventory.camera_source_inventory_id"), default=None, index=True)
    layer_key: Mapped[str | None] = mapped_column(String(80), default=None, index=True)
    event_id: Mapped[int | None] = mapped_column(ForeignKey("events.event_id"), default=None, index=True)
    geofence_id: Mapped[int | None] = mapped_column(ForeignKey("geofences.geofence_id"), default=None, index=True)
    scheduled_task_id: Mapped[int | None] = mapped_column(ForeignKey("scheduled_tasks.task_id"), default=None, index=True)
    interval_seconds: Mapped[int | None] = mapped_column(Integer, default=None)
    severity: Mapped[str] = mapped_column(String(30), default="info", index=True)
    notification_policy_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    baseline_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    dedupe_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    coverage_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    last_evaluated_at: Mapped[datetime | None] = mapped_column(default=None, index=True)
    last_changed_at: Mapped[datetime | None] = mapped_column(default=None, index=True)
    next_run_at: Mapped[datetime | None] = mapped_column(default=None, index=True)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    provenance_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

    runs: Mapped[list["WatchRunORM"]] = relationship(back_populates="watch")
    rule_versions: Mapped[list["WatchRuleVersionORM"]] = relationship(
        back_populates="watch",
        cascade="all, delete-orphan",
    )
    reports: Mapped[list["WatchReportORM"]] = relationship(
        back_populates="watch",
        cascade="all, delete-orphan",
    )


class WatchRuleVersionORM(Base):
    """Immutable, inspectable scope revisions for an investigation watch."""

    __tablename__ = "watch_rule_versions"
    __table_args__ = (
        UniqueConstraint("watch_id", "version_number", name="uq_watch_rule_version"),
        Index("ix_watch_rule_version_watch_state", "watch_id", "status"),
        Index("ix_watch_rule_version_hash", "rule_hash"),
    )

    watch_rule_version_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    watch_id: Mapped[int] = mapped_column(ForeignKey("watches.watch_id"), index=True)
    version_number: Mapped[int] = mapped_column(Integer)
    query_version: Mapped[str] = mapped_column(String(80), default="1", index=True)
    status: Mapped[str] = mapped_column(String(30), default="active", index=True)
    compiler_name: Mapped[str] = mapped_column(String(80), default="structured")
    original_instruction: Mapped[str | None] = mapped_column(Text, default=None)
    rule_hash: Mapped[str] = mapped_column(String(64), index=True)
    rule_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    scope_preview_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, index=True)
    activated_at: Mapped[datetime | None] = mapped_column(default=None)
    superseded_at: Mapped[datetime | None] = mapped_column(default=None)
    archived_at: Mapped[datetime | None] = mapped_column(default=None)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

    watch: Mapped[WatchORM] = relationship(back_populates="rule_versions")


class WatchReportORM(Base):
    """A locally retained on-demand summary generated from a watch's run ledger."""

    __tablename__ = "watch_reports"
    __table_args__ = (Index("ix_watch_report_watch_generated", "watch_id", "generated_at"),)

    watch_report_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    watch_id: Mapped[int] = mapped_column(ForeignKey("watches.watch_id"), index=True)
    rule_version: Mapped[int | None] = mapped_column(Integer, default=None)
    report_hash: Mapped[str] = mapped_column(String(64), index=True)
    report_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    generated_at: Mapped[datetime] = mapped_column(default=utcnow, index=True)
    requested_by: Mapped[str] = mapped_column(String(120), default="api")

    watch: Mapped[WatchORM] = relationship(back_populates="reports")


class WatchRunORM(Base):
    __tablename__ = "watch_runs"

    watch_run_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    watch_id: Mapped[int] = mapped_column(ForeignKey("watches.watch_id"), index=True)
    scheduled_task_run_id: Mapped[int | None] = mapped_column(ForeignKey("scheduled_task_runs.task_run_id"), default=None, index=True)
    source_run_id: Mapped[int | None] = mapped_column(ForeignKey("source_runs.source_run_id"), default=None, index=True)
    alert_id: Mapped[int | None] = mapped_column(ForeignKey("alerts.alert_id"), default=None, index=True)
    storage_object_id: Mapped[int | None] = mapped_column(ForeignKey("storage_objects.storage_object_id"), default=None, index=True)
    status: Mapped[str] = mapped_column(String(30), default="running", index=True)
    outcome: Mapped[str] = mapped_column(String(30), default="pending", index=True)
    started_at: Mapped[datetime] = mapped_column(default=utcnow, index=True)
    finished_at: Mapped[datetime | None] = mapped_column(default=None)
    change_detected: Mapped[bool] = mapped_column(default=False)
    baseline_initialized: Mapped[bool] = mapped_column(default=False)
    dedupe_key: Mapped[str | None] = mapped_column(String(200), default=None, index=True)
    evidence_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    checkpoint_before_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    checkpoint_after_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    output_summary: Mapped[str] = mapped_column(Text, default="")
    error_text: Mapped[str | None] = mapped_column(Text, default=None)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

    watch: Mapped[WatchORM] = relationship(back_populates="runs")
