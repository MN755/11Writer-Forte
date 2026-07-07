from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


TrustLevel = Literal["trusted", "neutral", "blocked"]
ApprovalPolicy = Literal["auto_approve_stable", "manual_review", "always_review", "auto_reject"]


class ForteModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class HealthResponse(ForteModel):
    status: str
    app_name: str
    app_version: str
    database_url: str
    spatial_backend: str


class DataLayerCreate(ForteModel):
    key: str
    name: str
    description: str = ""
    temporal_resolution: str = "unknown"
    data_latency: str = "unknown"
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class DataLayerRead(DataLayerCreate):
    layer_id: int
    created_at: datetime
    updated_at: datetime


class EventCreate(ForteModel):
    slug: str
    title: str
    summary: str = ""
    occurred_at: datetime | None = None
    status: str = "open"
    redaction_level: str = "public"
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class EventRead(EventCreate):
    event_id: int
    created_at: datetime
    updated_at: datetime


class EntityRead(ForteModel):
    entity_id: int
    slug: str
    entity_type: str
    canonical_name: str
    resolution_basis: str
    confidence_score: float
    redaction_level: str
    metadata_json: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class EntityObservationLinkRead(ForteModel):
    entity_observation_link_id: int
    entity_id: int
    observation_id: int
    match_basis: str
    confidence_contribution: float
    created_at: datetime


class EventObservationLinkRead(ForteModel):
    event_observation_link_id: int
    event_id: int
    observation_id: int
    relationship_type: str
    confidence_contribution: float
    created_at: datetime


class SituationProductRead(ForteModel):
    product_id: int
    event_id: int
    product_type: str
    redaction_level: str
    title: str
    body_text: str
    citations_json: list[dict[str, Any]]
    generated_by: str
    created_at: datetime
    updated_at: datetime


class GeofenceCreate(ForteModel):
    name: str
    description: str = ""
    geometry_geojson: dict[str, Any]
    rule_expression: str = ""
    enabled: bool = True


class GeofenceRead(GeofenceCreate):
    geofence_id: int
    created_at: datetime
    updated_at: datetime


class AlertCreate(ForteModel):
    event_id: int | None = None
    geofence_id: int | None = None
    severity: str = "info"
    status: str = "open"
    dedupe_key: str | None = None
    message: str
    trigger_basis_json: dict[str, Any] = Field(default_factory=dict)


class AlertRead(AlertCreate):
    alert_id: int
    created_at: datetime
    updated_at: datetime


class SourceTrustProfileCreate(ForteModel):
    domain: str
    trust_level: TrustLevel = "neutral"
    approval_policy: ApprovalPolicy = "manual_review"
    integrity_source: bool = False
    notes: str = ""


class SourceTrustProfileRead(SourceTrustProfileCreate):
    trust_profile_id: int
    created_at: datetime
    updated_at: datetime


class SourceDefinitionCreate(ForteModel):
    name: str
    source_kind: Literal["local_file", "http_json", "http_text"]
    layer_key: str
    target_uri: str
    enabled: bool = True
    integrity_source: bool = False
    notes: str = ""
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class SourceDefinitionRead(SourceDefinitionCreate):
    source_id: int
    created_at: datetime
    updated_at: datetime


class SourceRunRead(ForteModel):
    source_run_id: int
    source_id: int
    import_run_id: int | None
    status: str
    started_at: datetime
    finished_at: datetime | None
    records_imported: int
    error_text: str | None
    output_json: dict[str, Any]


class IntegritySeedResponse(ForteModel):
    created: int
    domains: list[str]


class ObservationRead(ForteModel):
    observation_id: int
    import_run_id: int | None
    event_id: int | None
    layer_key: str
    source_domain: str | None
    source_type: str
    record_format: str
    trust_level: TrustLevel
    approval_policy: ApprovalPolicy
    confidence_score: float
    location_geojson: dict[str, Any] | None
    content_text: str
    content_json: dict[str, Any]
    raw_hash: str
    created_at: datetime
    updated_at: datetime


class CrossVerificationSummaryRead(ForteModel):
    cluster_id: str
    observation_ids: list[int]
    observation_count: int
    source_domain_count: int
    layer_count: int
    independent_signal_count: int
    verification_score: float
    started_at: datetime
    ended_at: datetime
    centroid_geojson: dict[str, Any]


class EventFusionRequest(ForteModel):
    layer_key: str | None = None
    source_domain: str | None = None
    trust_level: str | None = None
    min_lon: float | None = None
    min_lat: float | None = None
    max_lon: float | None = None
    max_lat: float | None = None
    since: datetime | None = None
    until: datetime | None = None
    limit: int = Field(default=500, ge=1, le=2000)
    time_window_minutes: int = Field(default=60, ge=1, le=1440)
    distance_km: float = Field(default=25.0, gt=0.0, le=500.0)
    min_independent_signals: int = Field(default=2, ge=2, le=10)
    redaction_level: str = "public"


class EventFusionResultRead(ForteModel):
    event_id: int
    slug: str
    title: str
    observation_count: int
    product_count: int
    verification_score: float


class EventFusionResponse(ForteModel):
    created_event_count: int
    event_results: list[EventFusionResultRead]


class EntityResolutionRequest(ForteModel):
    layer_key: str | None = None
    source_domain: str | None = None
    trust_level: str | None = None
    min_lon: float | None = None
    min_lat: float | None = None
    max_lon: float | None = None
    max_lat: float | None = None
    since: datetime | None = None
    until: datetime | None = None
    limit: int = Field(default=500, ge=1, le=2000)
    min_observations: int = Field(default=2, ge=1, le=100)
    entity_type: str | None = None
    redaction_level: str = "public"


class EntityResolutionResultRead(ForteModel):
    entity_id: int
    slug: str
    entity_type: str
    canonical_name: str
    observation_count: int
    signal_count: int
    confidence_score: float
    created_new: bool


class EntityResolutionResponse(ForteModel):
    created_entity_count: int
    entity_results: list[EntityResolutionResultRead]


class LocalImportRequest(ForteModel):
    source_path: str
    layer_key: str = "unassigned"
    notes: str = ""


class LocalImportRunRead(ForteModel):
    import_run_id: int
    source_path: str
    source_format: str
    layer_key: str
    status: str
    records_seen: int
    records_imported: int
    notes: str
    chain_of_custody_json: list[dict[str, Any]]
    created_at: datetime
    updated_at: datetime
    observations: list[ObservationRead] = Field(default_factory=list)


class LocalImportRunSummaryRead(ForteModel):
    import_run_id: int
    source_path: str
    source_format: str
    layer_key: str
    status: str
    records_seen: int
    records_imported: int
    notes: str
    chain_of_custody_json: list[dict[str, Any]]
    created_at: datetime
    updated_at: datetime


class CustodyLogRead(ForteModel):
    custody_log_id: int
    object_type: str
    object_id: str
    action: str
    actor: str
    details_json: dict[str, Any]
    created_at: datetime


class ScheduledTaskCreate(ForteModel):
    name: str
    task_type: Literal["local_import", "geofence_scan", "integrity_seed", "source_sync"]
    interval_seconds: int = Field(ge=60)
    enabled: bool = True
    source_id: int | None = None
    target_path: str | None = None
    layer_key: str | None = None
    geofence_id: int | None = None
    notes: str = ""
    payload_json: dict[str, Any] = Field(default_factory=dict)


class ScheduledTaskRead(ScheduledTaskCreate):
    task_id: int
    last_run_at: datetime | None
    next_run_at: datetime | None
    created_at: datetime
    updated_at: datetime


class ScheduledTaskRunRead(ForteModel):
    task_run_id: int
    task_id: int
    status: str
    started_at: datetime
    finished_at: datetime | None
    records_affected: int
    error_text: str | None
    output_json: dict[str, Any]


class SchedulerKickResponse(ForteModel):
    runs_created: int
    task_run_ids: list[int]


class EventExportBundleRead(ForteModel):
    exported_at: datetime
    event: EventRead
    observation_links: list[EventObservationLinkRead]
    observations: list[ObservationRead]
    import_runs: list[LocalImportRunSummaryRead]
    source_runs: list[SourceRunRead]
    source_definitions: list[SourceDefinitionRead]
    products: list[SituationProductRead]
    custody_logs: list[CustodyLogRead]
    citations_json: list[dict[str, Any]]
