from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum

from pydantic import ConfigDict
from sqlalchemy import JSON, UniqueConstraint
from sqlmodel import Field, SQLModel


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def to_camel(value: str) -> str:
    parts = value.split("_")
    return parts[0] + "".join(part.capitalize() for part in parts[1:])


class CamelInputModel(SQLModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class SourceKind(str, Enum):
    DATA_FEED_SOURCE = "data_feed_source"
    DATA_SOURCE = "data_source"
    INTEGRITY_SOURCE = "integrity_source"
    HISTORICAL_SOURCE = "historical_source"


class RedactionLevel(str, Enum):
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


class AlertSeverity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AlertStatus(str, Enum):
    OPEN = "open"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"


class AssessmentKind(str, Enum):
    CROSS_VERIFICATION = "cross_verification"
    CONFIDENCE = "confidence"
    ENTITY_RESOLUTION = "entity_resolution"
    INTEGRITY = "integrity"


class AssessmentStatus(str, Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    SUPERSEDED = "superseded"


class ProductKind(str, Enum):
    CITED_SUMMARY = "cited_summary"
    REPORT = "report"


class CustodySubjectKind(str, Enum):
    SOURCE = "source"
    ENTITY = "entity"
    EVENT = "event"
    OBSERVATION = "observation"
    GEOFENCE = "geofence"
    ALERT = "alert"
    PRODUCT = "product"
    INGEST_JOB = "ingest_job"
    ASSESSMENT = "assessment"
    ENTITY_RESOLUTION = "entity_resolution"


class CustodyAction(str, Enum):
    CREATED = "created"
    UPDATED = "updated"
    INGESTED = "ingested"
    ASSESSED = "assessed"
    ALERTED = "alerted"
    RESOLVED = "resolved"


class IntelSource(SQLModel, table=True):
    __tablename__ = "intel_sources"

    source_id: str = Field(primary_key=True, max_length=160)
    name: str = Field(index=True, max_length=255)
    kind: SourceKind = Field(default=SourceKind.DATA_SOURCE, index=True)
    description: str = Field(default="")
    canonical_uri: str | None = Field(default=None, max_length=2048)
    base_domain: str | None = Field(default=None, index=True, max_length=255)
    trust_tier: str = Field(default="tier_3", index=True, max_length=32)
    integrity_score: float = Field(default=0.5, ge=0.0, le=1.0)
    default_confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    temporal_resolution_seconds: int | None = Field(default=None, ge=0)
    data_latency_seconds: int | None = Field(default=None, ge=0)
    enabled: bool = Field(default=True, index=True)
    formats: list[str] = Field(default_factory=list, sa_type=JSON)
    tags: list[str] = Field(default_factory=list, sa_type=JSON)
    metadata_json: dict[str, object] = Field(default_factory=dict, sa_type=JSON)
    created_at: datetime = Field(default_factory=utc_now, index=True)
    updated_at: datetime = Field(default_factory=utc_now, index=True)


class IntelEntity(SQLModel, table=True):
    __tablename__ = "intel_entities"

    entity_id: str = Field(primary_key=True, max_length=160)
    entity_type: str = Field(index=True, max_length=64)
    name: str = Field(index=True, max_length=255)
    status: str = Field(default="active", index=True, max_length=32)
    description: str = Field(default="")
    primary_source_id: str | None = Field(default=None, foreign_key="intel_sources.source_id", index=True)
    country_code: str | None = Field(default=None, index=True, max_length=16)
    latitude: float | None = Field(default=None)
    longitude: float | None = Field(default=None)
    altitude_meters: float | None = Field(default=None)
    geometry_wkt: str | None = Field(default=None)
    canonical_identifiers: dict[str, str] = Field(default_factory=dict, sa_type=JSON)
    aliases: list[str] = Field(default_factory=list, sa_type=JSON)
    tags: list[str] = Field(default_factory=list, sa_type=JSON)
    metadata_json: dict[str, object] = Field(default_factory=dict, sa_type=JSON)
    created_at: datetime = Field(default_factory=utc_now, index=True)
    updated_at: datetime = Field(default_factory=utc_now, index=True)


class IntelEvent(SQLModel, table=True):
    __tablename__ = "intel_events"

    event_id: str = Field(primary_key=True, max_length=160)
    event_type: str = Field(index=True, max_length=64)
    title: str = Field(index=True, max_length=255)
    status: str = Field(default="active", index=True, max_length=32)
    summary: str = Field(default="")
    redaction_level: RedactionLevel = Field(default=RedactionLevel.PUBLIC, index=True)
    geofence_id: str | None = Field(default=None, foreign_key="intel_geofences.geofence_id", index=True)
    latitude: float | None = Field(default=None)
    longitude: float | None = Field(default=None)
    altitude_meters: float | None = Field(default=None)
    geometry_wkt: str | None = Field(default=None)
    confidence_score: float = Field(default=0.5, ge=0.0, le=1.0, index=True)
    confidence_rule_version: str = Field(default="forte-baseline-v1", max_length=64)
    started_at: datetime | None = Field(default=None, index=True)
    ended_at: datetime | None = Field(default=None, index=True)
    detected_at: datetime = Field(default_factory=utc_now, index=True)
    tags: list[str] = Field(default_factory=list, sa_type=JSON)
    metadata_json: dict[str, object] = Field(default_factory=dict, sa_type=JSON)
    created_at: datetime = Field(default_factory=utc_now, index=True)
    updated_at: datetime = Field(default_factory=utc_now, index=True)


class IntelObservation(SQLModel, table=True):
    __tablename__ = "intel_observations"

    observation_id: str = Field(primary_key=True, max_length=180)
    source_id: str = Field(foreign_key="intel_sources.source_id", index=True)
    event_id: str | None = Field(default=None, foreign_key="intel_events.event_id", index=True)
    entity_id: str | None = Field(default=None, foreign_key="intel_entities.entity_id", index=True)
    observation_type: str = Field(default="record", index=True, max_length=64)
    title: str = Field(default="", max_length=255)
    summary: str = Field(default="")
    observed_at: datetime | None = Field(default=None, index=True)
    collected_at: datetime = Field(default_factory=utc_now, index=True)
    latitude: float | None = Field(default=None)
    longitude: float | None = Field(default=None)
    altitude_meters: float | None = Field(default=None)
    geometry_wkt: str | None = Field(default=None)
    raw_uri: str | None = Field(default=None, max_length=2048)
    extracted_text: str | None = Field(default=None)
    raw_hash_sha256: str | None = Field(default=None, index=True, max_length=64)
    confidence_score: float = Field(default=0.5, ge=0.0, le=1.0, index=True)
    is_ground_truth: bool = Field(default=False, index=True)
    tags: list[str] = Field(default_factory=list, sa_type=JSON)
    raw_payload_json: dict[str, object] = Field(default_factory=dict, sa_type=JSON)
    metadata_json: dict[str, object] = Field(default_factory=dict, sa_type=JSON)
    created_at: datetime = Field(default_factory=utc_now, index=True)
    updated_at: datetime = Field(default_factory=utc_now, index=True)


class IntelEventEntityLink(SQLModel, table=True):
    __tablename__ = "intel_event_entity_links"
    __table_args__ = (
        UniqueConstraint("event_id", "entity_id", "role", name="uq_intel_event_entity_link"),
    )

    link_id: int | None = Field(default=None, primary_key=True)
    event_id: str = Field(foreign_key="intel_events.event_id", index=True)
    entity_id: str = Field(foreign_key="intel_entities.entity_id", index=True)
    role: str = Field(default="observed", index=True, max_length=64)
    confidence_score: float = Field(default=0.5, ge=0.0, le=1.0)
    basis: list[str] = Field(default_factory=list, sa_type=JSON)
    created_at: datetime = Field(default_factory=utc_now, index=True)


class IntelGeofence(SQLModel, table=True):
    __tablename__ = "intel_geofences"

    geofence_id: str = Field(primary_key=True, max_length=160)
    name: str = Field(index=True, max_length=255)
    description: str = Field(default="")
    redaction_level: RedactionLevel = Field(default=RedactionLevel.PUBLIC, index=True)
    min_latitude: float | None = Field(default=None)
    min_longitude: float | None = Field(default=None)
    max_latitude: float | None = Field(default=None)
    max_longitude: float | None = Field(default=None)
    geometry_geojson: dict[str, object] = Field(default_factory=dict, sa_type=JSON)
    trigger_on_entry: bool = Field(default=True)
    trigger_on_exit: bool = Field(default=False)
    metadata_json: dict[str, object] = Field(default_factory=dict, sa_type=JSON)
    created_at: datetime = Field(default_factory=utc_now, index=True)
    updated_at: datetime = Field(default_factory=utc_now, index=True)


class IntelAlert(SQLModel, table=True):
    __tablename__ = "intel_alerts"
    __table_args__ = (
        UniqueConstraint("dedupe_key", name="uq_intel_alert_dedupe_key"),
    )

    alert_id: str = Field(primary_key=True, max_length=180)
    title: str = Field(index=True, max_length=255)
    status: AlertStatus = Field(default=AlertStatus.OPEN, index=True)
    severity: AlertSeverity = Field(default=AlertSeverity.MEDIUM, index=True)
    alert_type: str = Field(default="geofence", index=True, max_length=64)
    summary: str = Field(default="")
    event_id: str | None = Field(default=None, foreign_key="intel_events.event_id", index=True)
    entity_id: str | None = Field(default=None, foreign_key="intel_entities.entity_id", index=True)
    geofence_id: str | None = Field(default=None, foreign_key="intel_geofences.geofence_id", index=True)
    confidence_score: float = Field(default=0.5, ge=0.0, le=1.0)
    dedupe_key: str = Field(index=True, max_length=255)
    triggered_at: datetime = Field(default_factory=utc_now, index=True)
    resolved_at: datetime | None = Field(default=None, index=True)
    observation_ids: list[str] = Field(default_factory=list, sa_type=JSON)
    metadata_json: dict[str, object] = Field(default_factory=dict, sa_type=JSON)
    created_at: datetime = Field(default_factory=utc_now, index=True)
    updated_at: datetime = Field(default_factory=utc_now, index=True)


class IntelAssessment(SQLModel, table=True):
    __tablename__ = "intel_assessments"

    assessment_id: str = Field(primary_key=True, max_length=180)
    assessment_kind: AssessmentKind = Field(default=AssessmentKind.CONFIDENCE, index=True)
    target_kind: CustodySubjectKind = Field(index=True)
    target_id: str = Field(index=True, max_length=180)
    status: AssessmentStatus = Field(default=AssessmentStatus.ACTIVE, index=True)
    score: float = Field(default=0.5, ge=0.0, le=1.0, index=True)
    rule_version: str = Field(default="forte-baseline-v1", max_length=64)
    rationale_lines: list[str] = Field(default_factory=list, sa_type=JSON)
    supporting_observation_ids: list[str] = Field(default_factory=list, sa_type=JSON)
    contradicting_observation_ids: list[str] = Field(default_factory=list, sa_type=JSON)
    metadata_json: dict[str, object] = Field(default_factory=dict, sa_type=JSON)
    created_at: datetime = Field(default_factory=utc_now, index=True)


class IntelEntityResolution(SQLModel, table=True):
    __tablename__ = "intel_entity_resolutions"
    __table_args__ = (
        UniqueConstraint(
            "entity_id",
            "source_id",
            "external_record_id",
            name="uq_intel_entity_resolution_record",
        ),
    )

    resolution_id: str = Field(primary_key=True, max_length=180)
    entity_id: str = Field(foreign_key="intel_entities.entity_id", index=True)
    source_id: str = Field(foreign_key="intel_sources.source_id", index=True)
    external_record_id: str = Field(index=True, max_length=255)
    external_label: str | None = Field(default=None, max_length=255)
    match_rule: str = Field(default="manual", index=True, max_length=128)
    status: str = Field(default="linked", index=True, max_length=32)
    confidence_score: float = Field(default=0.5, ge=0.0, le=1.0, index=True)
    observed_at: datetime | None = Field(default=None, index=True)
    identifiers: dict[str, str] = Field(default_factory=dict, sa_type=JSON)
    metadata_json: dict[str, object] = Field(default_factory=dict, sa_type=JSON)
    created_at: datetime = Field(default_factory=utc_now, index=True)


class IntelAnalyticProduct(SQLModel, table=True):
    __tablename__ = "intel_analytic_products"

    product_id: str = Field(primary_key=True, max_length=180)
    product_kind: ProductKind = Field(default=ProductKind.CITED_SUMMARY, index=True)
    title: str = Field(index=True, max_length=255)
    event_id: str | None = Field(default=None, foreign_key="intel_events.event_id", index=True)
    redaction_level: RedactionLevel = Field(default=RedactionLevel.PUBLIC, index=True)
    status: str = Field(default="draft", index=True, max_length=32)
    content: str = Field(default="")
    citations: list[dict[str, object]] = Field(default_factory=list, sa_type=JSON)
    metadata_json: dict[str, object] = Field(default_factory=dict, sa_type=JSON)
    created_at: datetime = Field(default_factory=utc_now, index=True)
    updated_at: datetime = Field(default_factory=utc_now, index=True)


class IntelIngestJob(SQLModel, table=True):
    __tablename__ = "intel_ingest_jobs"

    ingest_job_id: str = Field(primary_key=True, max_length=180)
    source_id: str = Field(foreign_key="intel_sources.source_id", index=True)
    input_path: str = Field(index=True, max_length=2048)
    format_detected: str = Field(index=True, max_length=64)
    status: str = Field(default="running", index=True, max_length=32)
    record_count: int = Field(default=0, ge=0)
    error_summary: str | None = Field(default=None)
    metadata_json: dict[str, object] = Field(default_factory=dict, sa_type=JSON)
    started_at: datetime = Field(default_factory=utc_now, index=True)
    finished_at: datetime | None = Field(default=None, index=True)


class IntelCustodyRecord(SQLModel, table=True):
    __tablename__ = "intel_custody_records"

    custody_record_id: str = Field(primary_key=True, max_length=180)
    subject_kind: CustodySubjectKind = Field(index=True)
    subject_id: str = Field(index=True, max_length=180)
    action: CustodyAction = Field(index=True)
    actor: str = Field(default="system", index=True, max_length=160)
    occurred_at: datetime = Field(default_factory=utc_now, index=True)
    parent_record_id: str | None = Field(default=None, index=True, max_length=180)
    input_hash_sha256: str | None = Field(default=None, max_length=64)
    output_hash_sha256: str | None = Field(default=None, max_length=64)
    tool_name: str | None = Field(default=None, max_length=160)
    notes: str | None = Field(default=None)
    metadata_json: dict[str, object] = Field(default_factory=dict, sa_type=JSON)


class SourceCreate(CamelInputModel):
    source_id: str | None = None
    name: str
    kind: SourceKind = SourceKind.DATA_SOURCE
    description: str = ""
    canonical_uri: str | None = None
    base_domain: str | None = None
    trust_tier: str = "tier_3"
    integrity_score: float = 0.5
    default_confidence: float = 0.5
    temporal_resolution_seconds: int | None = None
    data_latency_seconds: int | None = None
    enabled: bool = True
    formats: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    metadata_json: dict[str, object] = Field(default_factory=dict)
    actor: str = "system"


class EntityCreate(CamelInputModel):
    entity_id: str | None = None
    entity_type: str
    name: str
    status: str = "active"
    description: str = ""
    primary_source_id: str | None = None
    country_code: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    altitude_meters: float | None = None
    geometry_wkt: str | None = None
    canonical_identifiers: dict[str, str] = Field(default_factory=dict)
    aliases: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    metadata_json: dict[str, object] = Field(default_factory=dict)
    actor: str = "system"


class EventCreate(CamelInputModel):
    event_id: str | None = None
    event_type: str
    title: str
    status: str = "active"
    summary: str = ""
    redaction_level: RedactionLevel = RedactionLevel.PUBLIC
    geofence_id: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    altitude_meters: float | None = None
    geometry_wkt: str | None = None
    confidence_score: float = 0.5
    confidence_rule_version: str = "forte-baseline-v1"
    started_at: datetime | None = None
    ended_at: datetime | None = None
    detected_at: datetime | None = None
    tags: list[str] = Field(default_factory=list)
    metadata_json: dict[str, object] = Field(default_factory=dict)
    actor: str = "system"


class ObservationCreate(CamelInputModel):
    observation_id: str | None = None
    source_id: str
    event_id: str | None = None
    entity_id: str | None = None
    observation_type: str = "record"
    title: str = ""
    summary: str = ""
    observed_at: datetime | None = None
    collected_at: datetime | None = None
    latitude: float | None = None
    longitude: float | None = None
    altitude_meters: float | None = None
    geometry_wkt: str | None = None
    raw_uri: str | None = None
    extracted_text: str | None = None
    raw_hash_sha256: str | None = None
    confidence_score: float = 0.5
    is_ground_truth: bool = False
    tags: list[str] = Field(default_factory=list)
    raw_payload_json: dict[str, object] = Field(default_factory=dict)
    metadata_json: dict[str, object] = Field(default_factory=dict)
    actor: str = "system"


class GeofenceCreate(CamelInputModel):
    geofence_id: str | None = None
    name: str
    description: str = ""
    redaction_level: RedactionLevel = RedactionLevel.PUBLIC
    min_latitude: float | None = None
    min_longitude: float | None = None
    max_latitude: float | None = None
    max_longitude: float | None = None
    geometry_geojson: dict[str, object] = Field(default_factory=dict)
    trigger_on_entry: bool = True
    trigger_on_exit: bool = False
    metadata_json: dict[str, object] = Field(default_factory=dict)
    actor: str = "system"


class AlertCreate(CamelInputModel):
    alert_id: str | None = None
    title: str
    status: AlertStatus = AlertStatus.OPEN
    severity: AlertSeverity = AlertSeverity.MEDIUM
    alert_type: str = "manual"
    summary: str = ""
    event_id: str | None = None
    entity_id: str | None = None
    geofence_id: str | None = None
    confidence_score: float = 0.5
    dedupe_key: str
    triggered_at: datetime | None = None
    resolved_at: datetime | None = None
    observation_ids: list[str] = Field(default_factory=list)
    metadata_json: dict[str, object] = Field(default_factory=dict)
    actor: str = "system"


class AssessmentCreate(CamelInputModel):
    assessment_id: str | None = None
    assessment_kind: AssessmentKind = AssessmentKind.CONFIDENCE
    target_kind: CustodySubjectKind
    target_id: str
    status: AssessmentStatus = AssessmentStatus.ACTIVE
    score: float = 0.5
    rule_version: str = "forte-baseline-v1"
    rationale_lines: list[str] = Field(default_factory=list)
    supporting_observation_ids: list[str] = Field(default_factory=list)
    contradicting_observation_ids: list[str] = Field(default_factory=list)
    metadata_json: dict[str, object] = Field(default_factory=dict)
    actor: str = "system"


class EntityResolutionCreate(CamelInputModel):
    resolution_id: str | None = None
    entity_id: str
    source_id: str
    external_record_id: str
    external_label: str | None = None
    match_rule: str = "manual"
    status: str = "linked"
    confidence_score: float = 0.5
    observed_at: datetime | None = None
    identifiers: dict[str, str] = Field(default_factory=dict)
    metadata_json: dict[str, object] = Field(default_factory=dict)
    actor: str = "system"


class AnalyticProductCreate(CamelInputModel):
    product_id: str | None = None
    product_kind: ProductKind = ProductKind.CITED_SUMMARY
    title: str
    event_id: str | None = None
    redaction_level: RedactionLevel = RedactionLevel.PUBLIC
    status: str = "draft"
    content: str = ""
    citations: list[dict[str, object]] = Field(default_factory=list)
    metadata_json: dict[str, object] = Field(default_factory=dict)
    actor: str = "system"


class CustodyRecordCreate(CamelInputModel):
    custody_record_id: str | None = None
    subject_kind: CustodySubjectKind
    subject_id: str
    action: CustodyAction
    actor: str = "system"
    occurred_at: datetime | None = None
    parent_record_id: str | None = None
    input_hash_sha256: str | None = None
    output_hash_sha256: str | None = None
    tool_name: str | None = None
    notes: str | None = None
    metadata_json: dict[str, object] = Field(default_factory=dict)


class IngestFileRequest(CamelInputModel):
    path: str
    source_id: str | None = None
    source_name: str | None = None
    source_kind: SourceKind = SourceKind.HISTORICAL_SOURCE
    event_id: str | None = None
    entity_id: str | None = None
    format_hint: str = "auto"
    actor: str = "system"


class OverviewResponse(SQLModel):
    counts: dict[str, int]
    open_alert_count: int
    active_event_count: int
    source_count: int
    last_updated_at: str


class EventFeedSyncRequest(CamelInputModel):
    feeds: list[str] = Field(default_factory=list)
    max_records_per_feed: int = 100
    actor: str = "system"
    evaluate_geofences: bool = True


class EventFeedSyncFeedResult(CamelInputModel):
    feed_key: str
    source_id: str
    status: str
    fetched_count: int = 0
    events_created: int = 0
    events_updated: int = 0
    observations_created: int = 0
    observations_updated: int = 0
    detail: str = ""
    event_ids: list[str] = Field(default_factory=list)
    caveats: list[str] = Field(default_factory=list)


class EventFeedSyncResponse(CamelInputModel):
    started_at: str
    completed_at: str
    feed_count: int
    synced_feed_count: int
    created_alert_count: int = 0
    results: list[EventFeedSyncFeedResult] = Field(default_factory=list)
    caveats: list[str] = Field(default_factory=list)
