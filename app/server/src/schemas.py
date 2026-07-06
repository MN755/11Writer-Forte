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


class IntegritySeedResponse(BaseModel):
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
