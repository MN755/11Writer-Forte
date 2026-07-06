from __future__ import annotations

from typing import Literal

from pydantic import Field

from src.types.entities import CamelModel, ReferenceObjectSummary


GeofenceShapeKind = Literal["bbox", "circle", "polygon"]
GeofenceRedactionLevel = Literal["public", "restricted", "confidential"]


class GeofenceSummary(CamelModel):
    geofence_id: str
    name: str
    shape_kind: GeofenceShapeKind
    redaction_level: GeofenceRedactionLevel
    enabled: bool = True
    created_by: str
    created_at: str
    updated_at: str
    description: str | None = None
    min_lat: float | None = None
    min_lon: float | None = None
    max_lat: float | None = None
    max_lon: float | None = None
    center_lat: float | None = None
    center_lon: float | None = None
    radius_m: float | None = None
    tags: list[str] = Field(default_factory=list)


class GeofenceDetail(GeofenceSummary):
    geometry_json: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)
    caveats: list[str] = Field(default_factory=list)


class GeofenceCreateRequest(CamelModel):
    geofence_id: str
    name: str
    shape_kind: GeofenceShapeKind
    redaction_level: GeofenceRedactionLevel = "public"
    created_by: str = "11writer-api"
    enabled: bool = True
    description: str | None = None
    min_lat: float | None = None
    min_lon: float | None = None
    max_lat: float | None = None
    max_lon: float | None = None
    center_lat: float | None = None
    center_lon: float | None = None
    radius_m: float | None = None
    polygon_points: list[list[float]] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, object] = Field(default_factory=dict)


class GeofenceListResponse(CamelModel):
    count: int
    geofences: list[GeofenceSummary] = Field(default_factory=list)
    caveats: list[str] = Field(default_factory=list)


class GeofenceEvaluationRequest(CamelModel):
    lat: float
    lon: float
    observed_at: str | None = None
    subject_type: str = "observation"
    subject_id: str | None = None
    observation_label: str | None = None
    requested_by: str = "11writer-api"
    reference_object_types: list[str] = Field(default_factory=list)
    reference_limit: int = 10
    metadata: dict[str, object] = Field(default_factory=dict)


class GeofenceEvaluationSummary(CamelModel):
    evaluation_id: str
    geofence_id: str
    subject_type: str
    subject_id: str | None = None
    observation_label: str | None = None
    observed_lat: float
    observed_lon: float
    observed_at: str
    matched: bool
    match_method: str
    distance_to_center_m: float | None = None
    reference_match_count: int = 0
    matched_reference_objects: list[ReferenceObjectSummary] = Field(default_factory=list)
    alert_id: str | None = None
    provenance_event_id: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)
    caveats: list[str] = Field(default_factory=list)


class GeofenceEvaluationResponse(CamelModel):
    geofence: GeofenceSummary
    evaluation: GeofenceEvaluationSummary
    caveats: list[str] = Field(default_factory=list)


class GeofenceEvaluationListResponse(CamelModel):
    count: int
    evaluations: list[GeofenceEvaluationSummary] = Field(default_factory=list)
    caveats: list[str] = Field(default_factory=list)
