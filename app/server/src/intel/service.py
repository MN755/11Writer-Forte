from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import func
from sqlmodel import SQLModel, Session, select

from .models import (
    AlertCreate,
    AlertSeverity,
    AlertStatus,
    AnalyticProductCreate,
    AssessmentCreate,
    AssessmentKind,
    AssessmentStatus,
    CustodyAction,
    CustodyRecordCreate,
    CustodySubjectKind,
    EntityCreate,
    EntityResolutionCreate,
    EventCreate,
    GeofenceCreate,
    IngestFileRequest,
    IntelAlert,
    IntelAnalyticProduct,
    IntelAssessment,
    IntelCustodyRecord,
    IntelEntity,
    IntelEntityResolution,
    IntelEvent,
    IntelEventEntityLink,
    IntelGeofence,
    IntelIngestJob,
    IntelObservation,
    IntelSource,
    ObservationCreate,
    OverviewResponse,
    ProductKind,
    RedactionLevel,
    SourceCreate,
    SourceKind,
    utc_now,
)


def _new_id(prefix: str) -> str:
    return f"{prefix}:{uuid.uuid4().hex}"


def _utc_now_iso() -> str:
    return utc_now().isoformat()


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _parse_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _pick_float(record: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = record.get(key)
        if value is None or value == "":
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _pick_str(record: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = record.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _coerce_jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _coerce_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_coerce_jsonable(item) for item in value]
    return str(value)


def _truncate(value: str, max_length: int = 240) -> str:
    return value if len(value) <= max_length else f"{value[: max_length - 3]}..."


def _extract_bbox(geometry_geojson: dict[str, Any]) -> tuple[float | None, float | None, float | None, float | None]:
    coordinates = geometry_geojson.get("coordinates")
    if not coordinates:
        return None, None, None, None

    points: list[tuple[float, float]] = []

    def _walk(node: Any) -> None:
        if (
            isinstance(node, (list, tuple))
            and len(node) >= 2
            and isinstance(node[0], (int, float))
            and isinstance(node[1], (int, float))
        ):
            points.append((float(node[0]), float(node[1])))
            return
        if isinstance(node, (list, tuple)):
            for child in node:
                _walk(child)

    _walk(coordinates)
    if not points:
        return None, None, None, None

    longitudes = [point[0] for point in points]
    latitudes = [point[1] for point in points]
    return min(latitudes), min(longitudes), max(latitudes), max(longitudes)


class IntelService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def list_sources(self, kind: str | None = None, limit: int = 100) -> list[IntelSource]:
        statement = select(IntelSource).order_by(IntelSource.created_at.desc()).limit(limit)
        if kind:
            statement = statement.where(IntelSource.kind == kind)
        return list(self.session.exec(statement))

    def get_source(self, source_id: str) -> IntelSource | None:
        return self.session.get(IntelSource, source_id)

    def create_source(self, payload: SourceCreate) -> IntelSource:
        source = IntelSource(
            source_id=payload.source_id or _new_id("source"),
            name=payload.name,
            kind=payload.kind,
            description=payload.description,
            canonical_uri=payload.canonical_uri,
            base_domain=payload.base_domain,
            trust_tier=payload.trust_tier,
            integrity_score=payload.integrity_score,
            default_confidence=payload.default_confidence,
            temporal_resolution_seconds=payload.temporal_resolution_seconds,
            data_latency_seconds=payload.data_latency_seconds,
            enabled=payload.enabled,
            formats=list(payload.formats),
            tags=list(payload.tags),
            metadata_json=_coerce_jsonable(payload.metadata_json),
        )
        self.session.add(source)
        self._record_custody(
            CustodyRecordCreate(
                subject_kind=CustodySubjectKind.SOURCE,
                subject_id=source.source_id,
                action=CustodyAction.CREATED,
                actor=payload.actor,
                tool_name="intel.create_source",
            ),
            commit=False,
        )
        self.session.commit()
        self.session.refresh(source)
        return source

    def upsert_source(self, payload: SourceCreate, *, commit: bool = True) -> tuple[IntelSource, bool]:
        source_id = payload.source_id or _new_id("source")
        existing = self.get_source(source_id)
        if existing is None:
            source = IntelSource(
                source_id=source_id,
                name=payload.name,
                kind=payload.kind,
                description=payload.description,
                canonical_uri=payload.canonical_uri,
                base_domain=payload.base_domain,
                trust_tier=payload.trust_tier,
                integrity_score=payload.integrity_score,
                default_confidence=payload.default_confidence,
                temporal_resolution_seconds=payload.temporal_resolution_seconds,
                data_latency_seconds=payload.data_latency_seconds,
                enabled=payload.enabled,
                formats=list(payload.formats),
                tags=list(payload.tags),
                metadata_json=_coerce_jsonable(payload.metadata_json),
            )
            self.session.add(source)
            self._record_custody(
                CustodyRecordCreate(
                    subject_kind=CustodySubjectKind.SOURCE,
                    subject_id=source.source_id,
                    action=CustodyAction.CREATED,
                    actor=payload.actor,
                    tool_name="intel.upsert_source",
                ),
                commit=False,
            )
            if commit:
                self.session.commit()
                self.session.refresh(source)
            return source, True

        existing.name = payload.name
        existing.kind = payload.kind
        existing.description = payload.description
        existing.canonical_uri = payload.canonical_uri
        existing.base_domain = payload.base_domain
        existing.trust_tier = payload.trust_tier
        existing.integrity_score = payload.integrity_score
        existing.default_confidence = payload.default_confidence
        existing.temporal_resolution_seconds = payload.temporal_resolution_seconds
        existing.data_latency_seconds = payload.data_latency_seconds
        existing.enabled = payload.enabled
        existing.formats = sorted({*existing.formats, *payload.formats})
        existing.tags = sorted({*existing.tags, *payload.tags})
        existing.metadata_json = _coerce_jsonable({**existing.metadata_json, **payload.metadata_json})
        existing.updated_at = utc_now()
        self.session.add(existing)
        self._record_custody(
            CustodyRecordCreate(
                subject_kind=CustodySubjectKind.SOURCE,
                subject_id=existing.source_id,
                action=CustodyAction.UPDATED,
                actor=payload.actor,
                tool_name="intel.upsert_source",
            ),
            commit=False,
        )
        if commit:
            self.session.commit()
            self.session.refresh(existing)
        return existing, False

    def list_entities(self, entity_type: str | None = None, limit: int = 100) -> list[IntelEntity]:
        statement = select(IntelEntity).order_by(IntelEntity.updated_at.desc()).limit(limit)
        if entity_type:
            statement = statement.where(IntelEntity.entity_type == entity_type)
        return list(self.session.exec(statement))

    def get_entity(self, entity_id: str) -> IntelEntity | None:
        return self.session.get(IntelEntity, entity_id)

    def create_entity(self, payload: EntityCreate) -> IntelEntity:
        entity = IntelEntity(
            entity_id=payload.entity_id or _new_id("entity"),
            entity_type=payload.entity_type,
            name=payload.name,
            status=payload.status,
            description=payload.description,
            primary_source_id=payload.primary_source_id,
            country_code=payload.country_code,
            latitude=payload.latitude,
            longitude=payload.longitude,
            altitude_meters=payload.altitude_meters,
            geometry_wkt=payload.geometry_wkt,
            canonical_identifiers=dict(payload.canonical_identifiers),
            aliases=list(payload.aliases),
            tags=list(payload.tags),
            metadata_json=_coerce_jsonable(payload.metadata_json),
        )
        self.session.add(entity)
        self._record_custody(
            CustodyRecordCreate(
                subject_kind=CustodySubjectKind.ENTITY,
                subject_id=entity.entity_id,
                action=CustodyAction.CREATED,
                actor=payload.actor,
                tool_name="intel.create_entity",
            ),
            commit=False,
        )
        self.session.commit()
        self.session.refresh(entity)
        return entity

    def list_events(self, status: str | None = None, limit: int = 100) -> list[IntelEvent]:
        statement = select(IntelEvent).order_by(IntelEvent.updated_at.desc()).limit(limit)
        if status:
            statement = statement.where(IntelEvent.status == status)
        return list(self.session.exec(statement))

    def get_event(self, event_id: str) -> IntelEvent | None:
        return self.session.get(IntelEvent, event_id)

    def create_event(self, payload: EventCreate) -> IntelEvent:
        event = IntelEvent(
            event_id=payload.event_id or _new_id("event"),
            event_type=payload.event_type,
            title=payload.title,
            status=payload.status,
            summary=payload.summary,
            redaction_level=payload.redaction_level,
            geofence_id=payload.geofence_id,
            latitude=payload.latitude,
            longitude=payload.longitude,
            altitude_meters=payload.altitude_meters,
            geometry_wkt=payload.geometry_wkt,
            confidence_score=payload.confidence_score,
            confidence_rule_version=payload.confidence_rule_version,
            started_at=payload.started_at,
            ended_at=payload.ended_at,
            detected_at=payload.detected_at or utc_now(),
            tags=list(payload.tags),
            metadata_json=_coerce_jsonable(payload.metadata_json),
        )
        self.session.add(event)
        self._record_custody(
            CustodyRecordCreate(
                subject_kind=CustodySubjectKind.EVENT,
                subject_id=event.event_id,
                action=CustodyAction.CREATED,
                actor=payload.actor,
                tool_name="intel.create_event",
            ),
            commit=False,
        )
        self.session.commit()
        self.session.refresh(event)
        return event

    def upsert_event(self, payload: EventCreate, *, commit: bool = True) -> tuple[IntelEvent, bool]:
        event_id = payload.event_id or _new_id("event")
        existing = self.get_event(event_id)
        if existing is None:
            event = IntelEvent(
                event_id=event_id,
                event_type=payload.event_type,
                title=payload.title,
                status=payload.status,
                summary=payload.summary,
                redaction_level=payload.redaction_level,
                geofence_id=payload.geofence_id,
                latitude=payload.latitude,
                longitude=payload.longitude,
                altitude_meters=payload.altitude_meters,
                geometry_wkt=payload.geometry_wkt,
                confidence_score=payload.confidence_score,
                confidence_rule_version=payload.confidence_rule_version,
                started_at=payload.started_at,
                ended_at=payload.ended_at,
                detected_at=payload.detected_at or utc_now(),
                tags=list(payload.tags),
                metadata_json=_coerce_jsonable(payload.metadata_json),
            )
            self.session.add(event)
            self._record_custody(
                CustodyRecordCreate(
                    subject_kind=CustodySubjectKind.EVENT,
                    subject_id=event.event_id,
                    action=CustodyAction.CREATED,
                    actor=payload.actor,
                    tool_name="intel.upsert_event",
                ),
                commit=False,
            )
            if commit:
                self.session.commit()
                self.session.refresh(event)
            return event, True

        existing.event_type = payload.event_type
        existing.title = payload.title
        existing.status = payload.status
        existing.summary = payload.summary
        existing.redaction_level = payload.redaction_level
        existing.geofence_id = payload.geofence_id
        existing.latitude = payload.latitude
        existing.longitude = payload.longitude
        existing.altitude_meters = payload.altitude_meters
        existing.geometry_wkt = payload.geometry_wkt
        existing.confidence_score = payload.confidence_score
        existing.confidence_rule_version = payload.confidence_rule_version
        existing.started_at = payload.started_at
        existing.ended_at = payload.ended_at
        existing.detected_at = payload.detected_at or existing.detected_at
        existing.tags = sorted({*existing.tags, *payload.tags})
        existing.metadata_json = _coerce_jsonable({**existing.metadata_json, **payload.metadata_json})
        existing.updated_at = utc_now()
        self.session.add(existing)
        self._record_custody(
            CustodyRecordCreate(
                subject_kind=CustodySubjectKind.EVENT,
                subject_id=existing.event_id,
                action=CustodyAction.UPDATED,
                actor=payload.actor,
                tool_name="intel.upsert_event",
            ),
            commit=False,
        )
        if commit:
            self.session.commit()
            self.session.refresh(existing)
        return existing, False

    def list_observations(
        self,
        event_id: str | None = None,
        entity_id: str | None = None,
        source_id: str | None = None,
        limit: int = 200,
    ) -> list[IntelObservation]:
        statement = select(IntelObservation).order_by(IntelObservation.collected_at.desc()).limit(limit)
        if event_id:
            statement = statement.where(IntelObservation.event_id == event_id)
        if entity_id:
            statement = statement.where(IntelObservation.entity_id == entity_id)
        if source_id:
            statement = statement.where(IntelObservation.source_id == source_id)
        return list(self.session.exec(statement))

    def get_observation(self, observation_id: str) -> IntelObservation | None:
        return self.session.get(IntelObservation, observation_id)

    def create_observation(
        self,
        payload: ObservationCreate,
        *,
        recompute_event_confidence: bool = True,
    ) -> IntelObservation:
        if self.get_source(payload.source_id) is None:
            raise ValueError(f"Unknown source_id: {payload.source_id}")
        if payload.event_id and self.get_event(payload.event_id) is None:
            raise ValueError(f"Unknown event_id: {payload.event_id}")
        if payload.entity_id and self.get_entity(payload.entity_id) is None:
            raise ValueError(f"Unknown entity_id: {payload.entity_id}")

        observation = IntelObservation(
            observation_id=payload.observation_id or _new_id("observation"),
            source_id=payload.source_id,
            event_id=payload.event_id,
            entity_id=payload.entity_id,
            observation_type=payload.observation_type,
            title=payload.title,
            summary=payload.summary,
            observed_at=payload.observed_at,
            collected_at=payload.collected_at or utc_now(),
            latitude=payload.latitude,
            longitude=payload.longitude,
            altitude_meters=payload.altitude_meters,
            geometry_wkt=payload.geometry_wkt,
            raw_uri=payload.raw_uri,
            extracted_text=payload.extracted_text,
            raw_hash_sha256=payload.raw_hash_sha256,
            confidence_score=payload.confidence_score,
            is_ground_truth=payload.is_ground_truth,
            tags=list(payload.tags),
            raw_payload_json=_coerce_jsonable(payload.raw_payload_json),
            metadata_json=_coerce_jsonable(payload.metadata_json),
        )
        self.session.add(observation)
        if payload.event_id and payload.entity_id:
            self._upsert_event_entity_link(payload.event_id, payload.entity_id, payload.confidence_score)
        self._record_custody(
            CustodyRecordCreate(
                subject_kind=CustodySubjectKind.OBSERVATION,
                subject_id=observation.observation_id,
                action=CustodyAction.INGESTED,
                actor=payload.actor,
                input_hash_sha256=payload.raw_hash_sha256,
                tool_name="intel.create_observation",
                metadata_json={"source_id": payload.source_id},
            ),
            commit=False,
        )
        self.session.commit()
        self.session.refresh(observation)
        if recompute_event_confidence and payload.event_id:
            self.recompute_event_assessment(payload.event_id, actor=payload.actor)
        return observation

    def upsert_observation(
        self,
        payload: ObservationCreate,
        *,
        recompute_event_confidence: bool = True,
        commit: bool = True,
    ) -> tuple[IntelObservation, bool]:
        observation_id = payload.observation_id or _new_id("observation")
        existing = self.get_observation(observation_id)
        if existing is None:
            observation = self.create_observation(
                ObservationCreate(
                    observation_id=observation_id,
                    source_id=payload.source_id,
                    event_id=payload.event_id,
                    entity_id=payload.entity_id,
                    observation_type=payload.observation_type,
                    title=payload.title,
                    summary=payload.summary,
                    observed_at=payload.observed_at,
                    collected_at=payload.collected_at,
                    latitude=payload.latitude,
                    longitude=payload.longitude,
                    altitude_meters=payload.altitude_meters,
                    geometry_wkt=payload.geometry_wkt,
                    raw_uri=payload.raw_uri,
                    extracted_text=payload.extracted_text,
                    raw_hash_sha256=payload.raw_hash_sha256,
                    confidence_score=payload.confidence_score,
                    is_ground_truth=payload.is_ground_truth,
                    tags=list(payload.tags),
                    raw_payload_json=dict(payload.raw_payload_json),
                    metadata_json=dict(payload.metadata_json),
                    actor=payload.actor,
                ),
                recompute_event_confidence=recompute_event_confidence,
            )
            return observation, True

        if self.get_source(payload.source_id) is None:
            raise ValueError(f"Unknown source_id: {payload.source_id}")
        if payload.event_id and self.get_event(payload.event_id) is None:
            raise ValueError(f"Unknown event_id: {payload.event_id}")
        if payload.entity_id and self.get_entity(payload.entity_id) is None:
            raise ValueError(f"Unknown entity_id: {payload.entity_id}")

        existing.source_id = payload.source_id
        existing.event_id = payload.event_id
        existing.entity_id = payload.entity_id
        existing.observation_type = payload.observation_type
        existing.title = payload.title
        existing.summary = payload.summary
        existing.observed_at = payload.observed_at
        existing.collected_at = payload.collected_at or existing.collected_at
        existing.latitude = payload.latitude
        existing.longitude = payload.longitude
        existing.altitude_meters = payload.altitude_meters
        existing.geometry_wkt = payload.geometry_wkt
        existing.raw_uri = payload.raw_uri
        existing.extracted_text = payload.extracted_text
        existing.raw_hash_sha256 = payload.raw_hash_sha256
        existing.confidence_score = payload.confidence_score
        existing.is_ground_truth = payload.is_ground_truth
        existing.tags = sorted({*existing.tags, *payload.tags})
        existing.raw_payload_json = _coerce_jsonable(payload.raw_payload_json)
        existing.metadata_json = _coerce_jsonable({**existing.metadata_json, **payload.metadata_json})
        existing.updated_at = utc_now()
        self.session.add(existing)
        if payload.event_id and payload.entity_id:
            self._upsert_event_entity_link(payload.event_id, payload.entity_id, payload.confidence_score)
        self._record_custody(
            CustodyRecordCreate(
                subject_kind=CustodySubjectKind.OBSERVATION,
                subject_id=existing.observation_id,
                action=CustodyAction.UPDATED,
                actor=payload.actor,
                input_hash_sha256=payload.raw_hash_sha256,
                tool_name="intel.upsert_observation",
                metadata_json={"source_id": payload.source_id},
            ),
            commit=False,
        )
        if commit:
            self.session.commit()
            self.session.refresh(existing)
            if recompute_event_confidence and payload.event_id:
                self.recompute_event_assessment(payload.event_id, actor=payload.actor)
        return existing, False

    def list_geofences(self, limit: int = 100) -> list[IntelGeofence]:
        return list(
            self.session.exec(
                select(IntelGeofence).order_by(IntelGeofence.updated_at.desc()).limit(limit)
            )
        )

    def get_geofence(self, geofence_id: str) -> IntelGeofence | None:
        return self.session.get(IntelGeofence, geofence_id)

    def create_geofence(self, payload: GeofenceCreate) -> IntelGeofence:
        min_latitude = payload.min_latitude
        min_longitude = payload.min_longitude
        max_latitude = payload.max_latitude
        max_longitude = payload.max_longitude
        if payload.geometry_geojson and None in {min_latitude, min_longitude, max_latitude, max_longitude}:
            derived = _extract_bbox(payload.geometry_geojson)
            if min_latitude is None:
                min_latitude = derived[0]
            if min_longitude is None:
                min_longitude = derived[1]
            if max_latitude is None:
                max_latitude = derived[2]
            if max_longitude is None:
                max_longitude = derived[3]

        geofence = IntelGeofence(
            geofence_id=payload.geofence_id or _new_id("geofence"),
            name=payload.name,
            description=payload.description,
            redaction_level=payload.redaction_level,
            min_latitude=min_latitude,
            min_longitude=min_longitude,
            max_latitude=max_latitude,
            max_longitude=max_longitude,
            geometry_geojson=_coerce_jsonable(payload.geometry_geojson),
            trigger_on_entry=payload.trigger_on_entry,
            trigger_on_exit=payload.trigger_on_exit,
            metadata_json=_coerce_jsonable(payload.metadata_json),
        )
        self.session.add(geofence)
        self._record_custody(
            CustodyRecordCreate(
                subject_kind=CustodySubjectKind.GEOFENCE,
                subject_id=geofence.geofence_id,
                action=CustodyAction.CREATED,
                actor=payload.actor,
                tool_name="intel.create_geofence",
            ),
            commit=False,
        )
        self.session.commit()
        self.session.refresh(geofence)
        return geofence

    def list_alerts(self, status: str | None = None, limit: int = 100) -> list[IntelAlert]:
        statement = select(IntelAlert).order_by(IntelAlert.triggered_at.desc()).limit(limit)
        if status:
            statement = statement.where(IntelAlert.status == status)
        return list(self.session.exec(statement))

    def get_alert(self, alert_id: str) -> IntelAlert | None:
        return self.session.get(IntelAlert, alert_id)

    def create_alert(self, payload: AlertCreate) -> IntelAlert:
        alert = IntelAlert(
            alert_id=payload.alert_id or _new_id("alert"),
            title=payload.title,
            status=payload.status,
            severity=payload.severity,
            alert_type=payload.alert_type,
            summary=payload.summary,
            event_id=payload.event_id,
            entity_id=payload.entity_id,
            geofence_id=payload.geofence_id,
            confidence_score=payload.confidence_score,
            dedupe_key=payload.dedupe_key,
            triggered_at=payload.triggered_at or utc_now(),
            resolved_at=payload.resolved_at,
            observation_ids=list(payload.observation_ids),
            metadata_json=_coerce_jsonable(payload.metadata_json),
        )
        self.session.add(alert)
        self._record_custody(
            CustodyRecordCreate(
                subject_kind=CustodySubjectKind.ALERT,
                subject_id=alert.alert_id,
                action=CustodyAction.ALERTED,
                actor=payload.actor,
                tool_name="intel.create_alert",
                metadata_json={"dedupe_key": payload.dedupe_key},
            ),
            commit=False,
        )
        self.session.commit()
        self.session.refresh(alert)
        return alert

    def list_assessments(
        self,
        target_kind: str | None = None,
        target_id: str | None = None,
        limit: int = 100,
    ) -> list[IntelAssessment]:
        statement = select(IntelAssessment).order_by(IntelAssessment.created_at.desc()).limit(limit)
        if target_kind:
            statement = statement.where(IntelAssessment.target_kind == target_kind)
        if target_id:
            statement = statement.where(IntelAssessment.target_id == target_id)
        return list(self.session.exec(statement))

    def create_assessment(self, payload: AssessmentCreate) -> IntelAssessment:
        assessment = IntelAssessment(
            assessment_id=payload.assessment_id or _new_id("assessment"),
            assessment_kind=payload.assessment_kind,
            target_kind=payload.target_kind,
            target_id=payload.target_id,
            status=payload.status,
            score=payload.score,
            rule_version=payload.rule_version,
            rationale_lines=list(payload.rationale_lines),
            supporting_observation_ids=list(payload.supporting_observation_ids),
            contradicting_observation_ids=list(payload.contradicting_observation_ids),
            metadata_json=_coerce_jsonable(payload.metadata_json),
        )
        self.session.add(assessment)
        self._record_custody(
            CustodyRecordCreate(
                subject_kind=CustodySubjectKind.ASSESSMENT,
                subject_id=assessment.assessment_id,
                action=CustodyAction.ASSESSED,
                actor=payload.actor,
                tool_name="intel.create_assessment",
                metadata_json={
                    "target_kind": payload.target_kind.value,
                    "target_id": payload.target_id,
                },
            ),
            commit=False,
        )
        self.session.commit()
        self.session.refresh(assessment)
        return assessment

    def list_entity_resolutions(self, entity_id: str | None = None, limit: int = 100) -> list[IntelEntityResolution]:
        statement = select(IntelEntityResolution).order_by(IntelEntityResolution.created_at.desc()).limit(limit)
        if entity_id:
            statement = statement.where(IntelEntityResolution.entity_id == entity_id)
        return list(self.session.exec(statement))

    def create_entity_resolution(self, payload: EntityResolutionCreate) -> IntelEntityResolution:
        resolution = IntelEntityResolution(
            resolution_id=payload.resolution_id or _new_id("resolution"),
            entity_id=payload.entity_id,
            source_id=payload.source_id,
            external_record_id=payload.external_record_id,
            external_label=payload.external_label,
            match_rule=payload.match_rule,
            status=payload.status,
            confidence_score=payload.confidence_score,
            observed_at=payload.observed_at,
            identifiers=dict(payload.identifiers),
            metadata_json=_coerce_jsonable(payload.metadata_json),
        )
        self.session.add(resolution)
        self._record_custody(
            CustodyRecordCreate(
                subject_kind=CustodySubjectKind.ENTITY_RESOLUTION,
                subject_id=resolution.resolution_id,
                action=CustodyAction.CREATED,
                actor=payload.actor,
                tool_name="intel.create_entity_resolution",
                metadata_json={"entity_id": payload.entity_id, "source_id": payload.source_id},
            ),
            commit=False,
        )
        self.session.commit()
        self.session.refresh(resolution)
        return resolution

    def list_products(self, event_id: str | None = None, limit: int = 100) -> list[IntelAnalyticProduct]:
        statement = select(IntelAnalyticProduct).order_by(IntelAnalyticProduct.updated_at.desc()).limit(limit)
        if event_id:
            statement = statement.where(IntelAnalyticProduct.event_id == event_id)
        return list(self.session.exec(statement))

    def create_product(self, payload: AnalyticProductCreate) -> IntelAnalyticProduct:
        product = IntelAnalyticProduct(
            product_id=payload.product_id or _new_id("product"),
            product_kind=payload.product_kind,
            title=payload.title,
            event_id=payload.event_id,
            redaction_level=payload.redaction_level,
            status=payload.status,
            content=payload.content,
            citations=_coerce_jsonable(payload.citations),
            metadata_json=_coerce_jsonable(payload.metadata_json),
        )
        self.session.add(product)
        self._record_custody(
            CustodyRecordCreate(
                subject_kind=CustodySubjectKind.PRODUCT,
                subject_id=product.product_id,
                action=CustodyAction.CREATED,
                actor=payload.actor,
                tool_name="intel.create_product",
            ),
            commit=False,
        )
        self.session.commit()
        self.session.refresh(product)
        return product

    def list_custody(
        self,
        subject_kind: str | None = None,
        subject_id: str | None = None,
        limit: int = 200,
    ) -> list[IntelCustodyRecord]:
        statement = select(IntelCustodyRecord).order_by(IntelCustodyRecord.occurred_at.desc()).limit(limit)
        if subject_kind:
            statement = statement.where(IntelCustodyRecord.subject_kind == subject_kind)
        if subject_id:
            statement = statement.where(IntelCustodyRecord.subject_id == subject_id)
        return list(self.session.exec(statement))

    def create_custody_record(self, payload: CustodyRecordCreate) -> IntelCustodyRecord:
        return self._record_custody(payload, commit=True)

    def recompute_event_assessment(self, event_id: str, actor: str = "system") -> IntelAssessment:
        event = self.get_event(event_id)
        if event is None:
            raise ValueError(f"Unknown event_id: {event_id}")

        observations = self.list_observations(event_id=event_id, limit=5000)
        if not observations:
            score = 0.1
            rationale_lines = [
                "No observations are linked to this event yet.",
                "Confidence remains low until at least one observation is attached.",
            ]
            supporting_ids: list[str] = []
            contradicting_ids: list[str] = []
        else:
            source_ids = {observation.source_id for observation in observations}
            sources = {
                source.source_id: source
                for source in self.session.exec(select(IntelSource).where(IntelSource.source_id.in_(source_ids)))
            }
            avg_observation_confidence = sum(observation.confidence_score for observation in observations) / len(observations)
            avg_source_integrity = (
                sum(sources.get(observation.source_id, IntelSource(source_id="", name="")).integrity_score for observation in observations)
                / len(observations)
            )
            distinct_source_count = len(source_ids)
            coverage_factor = min(1.0, distinct_source_count / 3.0)
            ground_truth_count = sum(1 for observation in observations if observation.is_ground_truth)
            ground_truth_factor = min(1.0, ground_truth_count / 2.0)
            score = round(
                min(
                    1.0,
                    0.45 * avg_observation_confidence
                    + 0.35 * avg_source_integrity
                    + 0.1 * coverage_factor
                    + 0.1 * ground_truth_factor,
                ),
                3,
            )
            rationale_lines = [
                f"{len(observations)} supporting observations attached.",
                f"{distinct_source_count} distinct sources participated in the score.",
                f"Average observation confidence: {avg_observation_confidence:.3f}.",
                f"Average source integrity: {avg_source_integrity:.3f}.",
                f"Ground-truth observations: {ground_truth_count}.",
            ]
            supporting_ids = [observation.observation_id for observation in observations]
            contradicting_ids = []

        active_assessments = self.session.exec(
            select(IntelAssessment).where(
                IntelAssessment.target_kind == CustodySubjectKind.EVENT,
                IntelAssessment.target_id == event_id,
                IntelAssessment.status == AssessmentStatus.ACTIVE,
                IntelAssessment.assessment_kind == AssessmentKind.CROSS_VERIFICATION,
            )
        )
        for assessment in active_assessments:
            assessment.status = AssessmentStatus.SUPERSEDED
        event.confidence_score = score
        event.updated_at = utc_now()
        assessment = IntelAssessment(
            assessment_id=_new_id("assessment"),
            assessment_kind=AssessmentKind.CROSS_VERIFICATION,
            target_kind=CustodySubjectKind.EVENT,
            target_id=event_id,
            status=AssessmentStatus.ACTIVE,
            score=score,
            rule_version=event.confidence_rule_version,
            rationale_lines=rationale_lines,
            supporting_observation_ids=supporting_ids,
            contradicting_observation_ids=contradicting_ids,
            metadata_json={"generated_at": _utc_now_iso()},
        )
        self.session.add(assessment)
        self._record_custody(
            CustodyRecordCreate(
                subject_kind=CustodySubjectKind.EVENT,
                subject_id=event_id,
                action=CustodyAction.ASSESSED,
                actor=actor,
                tool_name="intel.recompute_event_assessment",
                notes=f"Confidence recomputed to {score:.3f}.",
            ),
            commit=False,
        )
        self.session.commit()
        self.session.refresh(assessment)
        self.session.refresh(event)
        return assessment

    def evaluate_geofences(self, actor: str = "system") -> list[IntelAlert]:
        geofences = self.list_geofences(limit=1000)
        if not geofences:
            return []
        created: list[IntelAlert] = []
        for geofence in geofences:
            for event in self.list_events(status="active", limit=1000):
                if self._point_inside_geofence(event.latitude, event.longitude, geofence):
                    dedupe_key = f"geofence:{geofence.geofence_id}:event:{event.event_id}"
                    if not self._alert_exists(dedupe_key):
                        created.append(
                            self.create_alert(
                                AlertCreate(
                                    title=f"Event entered geofence: {geofence.name}",
                                    severity=AlertSeverity.HIGH,
                                    alert_type="geofence_event_entry",
                                    summary=f"Event {event.title} is inside geofence {geofence.name}.",
                                    event_id=event.event_id,
                                    geofence_id=geofence.geofence_id,
                                    confidence_score=event.confidence_score,
                                    dedupe_key=dedupe_key,
                                    actor=actor,
                                )
                            )
                        )
            for entity in self.list_entities(limit=1000):
                if self._point_inside_geofence(entity.latitude, entity.longitude, geofence):
                    dedupe_key = f"geofence:{geofence.geofence_id}:entity:{entity.entity_id}"
                    if not self._alert_exists(dedupe_key):
                        created.append(
                            self.create_alert(
                                AlertCreate(
                                    title=f"Entity entered geofence: {geofence.name}",
                                    severity=AlertSeverity.MEDIUM,
                                    alert_type="geofence_entity_entry",
                                    summary=f"Entity {entity.name} is inside geofence {geofence.name}.",
                                    entity_id=entity.entity_id,
                                    geofence_id=geofence.geofence_id,
                                    confidence_score=0.6,
                                    dedupe_key=dedupe_key,
                                    actor=actor,
                                )
                            )
                        )
        return created

    def ingest_file(self, request: IngestFileRequest) -> dict[str, Any]:
        path = Path(request.path).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"Input path does not exist: {path}")

        format_detected = self._detect_format(path, request.format_hint)
        source = self.get_source(request.source_id) if request.source_id else None
        if source is None:
            source = self.create_source(
                SourceCreate(
                    source_id=request.source_id,
                    name=request.source_name or path.stem,
                    kind=request.source_kind,
                    canonical_uri=str(path),
                    formats=[format_detected],
                    metadata_json={"ingest_path": str(path)},
                    actor=request.actor,
                )
            )
        elif format_detected not in source.formats:
            source.formats = sorted({*source.formats, format_detected})
            source.updated_at = utc_now()
            self.session.add(source)
            self.session.commit()

        job = IntelIngestJob(
            ingest_job_id=_new_id("ingest"),
            source_id=source.source_id,
            input_path=str(path),
            format_detected=format_detected,
            status="running",
            metadata_json={"format_hint": request.format_hint},
        )
        self.session.add(job)
        self.session.commit()
        self.session.refresh(job)

        created_observations: list[IntelObservation] = []
        try:
            for observation_create in self._iter_file_observations(path, format_detected, request, source.source_id):
                created_observations.append(
                    self.create_observation(
                        observation_create,
                        recompute_event_confidence=False,
                    )
                )
            if request.event_id:
                self.recompute_event_assessment(request.event_id, actor=request.actor)
            job.status = "completed"
            job.record_count = len(created_observations)
            job.finished_at = utc_now()
            job.metadata_json = {
                "observation_ids": [observation.observation_id for observation in created_observations[:50]],
                "truncated": len(created_observations) > 50,
            }
        except Exception as exc:
            job.status = "failed"
            job.error_summary = f"{exc.__class__.__name__}: {exc}"
            job.finished_at = utc_now()
            self.session.add(job)
            self._record_custody(
                CustodyRecordCreate(
                    subject_kind=CustodySubjectKind.INGEST_JOB,
                    subject_id=job.ingest_job_id,
                    action=CustodyAction.UPDATED,
                    actor=request.actor,
                    tool_name="intel.ingest_file",
                    notes=job.error_summary,
                ),
                commit=False,
            )
            self.session.commit()
            raise

        self.session.add(job)
        self._record_custody(
            CustodyRecordCreate(
                subject_kind=CustodySubjectKind.INGEST_JOB,
                subject_id=job.ingest_job_id,
                action=CustodyAction.INGESTED,
                actor=request.actor,
                tool_name="intel.ingest_file",
                metadata_json={"record_count": len(created_observations), "format": format_detected},
            ),
            commit=False,
        )
        self.session.commit()
        self.session.refresh(job)
        self.session.refresh(source)
        return {
            "job": job,
            "source": source,
            "observations_created": len(created_observations),
            "observation_ids": [observation.observation_id for observation in created_observations[:50]],
            "truncated": len(created_observations) > 50,
        }

    def overview(self) -> OverviewResponse:
        counts = {
            "sources": self._count(IntelSource),
            "entities": self._count(IntelEntity),
            "events": self._count(IntelEvent),
            "observations": self._count(IntelObservation),
            "geofences": self._count(IntelGeofence),
            "alerts": self._count(IntelAlert),
            "assessments": self._count(IntelAssessment),
            "entity_resolutions": self._count(IntelEntityResolution),
            "products": self._count(IntelAnalyticProduct),
            "custody_records": self._count(IntelCustodyRecord),
            "ingest_jobs": self._count(IntelIngestJob),
        }
        return OverviewResponse(
            counts=counts,
            open_alert_count=self._count_where(IntelAlert, IntelAlert.status == AlertStatus.OPEN),
            active_event_count=self._count_where(IntelEvent, IntelEvent.status == "active"),
            source_count=counts["sources"],
            last_updated_at=_utc_now_iso(),
        )

    def _record_custody(self, payload: CustodyRecordCreate, *, commit: bool) -> IntelCustodyRecord:
        record = IntelCustodyRecord(
            custody_record_id=payload.custody_record_id or _new_id("custody"),
            subject_kind=payload.subject_kind,
            subject_id=payload.subject_id,
            action=payload.action,
            actor=payload.actor,
            occurred_at=payload.occurred_at or utc_now(),
            parent_record_id=payload.parent_record_id,
            input_hash_sha256=payload.input_hash_sha256,
            output_hash_sha256=payload.output_hash_sha256,
            tool_name=payload.tool_name,
            notes=payload.notes,
            metadata_json=_coerce_jsonable(payload.metadata_json),
        )
        self.session.add(record)
        if commit:
            self.session.commit()
            self.session.refresh(record)
        return record

    def _upsert_event_entity_link(self, event_id: str, entity_id: str, confidence_score: float) -> None:
        existing = self.session.exec(
            select(IntelEventEntityLink).where(
                IntelEventEntityLink.event_id == event_id,
                IntelEventEntityLink.entity_id == entity_id,
                IntelEventEntityLink.role == "observed",
            )
        ).first()
        if existing is not None:
            existing.confidence_score = max(existing.confidence_score, confidence_score)
            return
        self.session.add(
            IntelEventEntityLink(
                event_id=event_id,
                entity_id=entity_id,
                role="observed",
                confidence_score=confidence_score,
                basis=["linked_by_observation"],
            )
        )

    def _alert_exists(self, dedupe_key: str) -> bool:
        return self.session.exec(select(IntelAlert).where(IntelAlert.dedupe_key == dedupe_key)).first() is not None

    def _point_inside_geofence(
        self,
        latitude: float | None,
        longitude: float | None,
        geofence: IntelGeofence,
    ) -> bool:
        if latitude is None or longitude is None:
            return False
        if None in {
            geofence.min_latitude,
            geofence.min_longitude,
            geofence.max_latitude,
            geofence.max_longitude,
        }:
            return False
        return (
            geofence.min_latitude <= latitude <= geofence.max_latitude
            and geofence.min_longitude <= longitude <= geofence.max_longitude
        )

    def _count(self, model: type[SQLModel]) -> int:
        return int(self.session.exec(select(func.count()).select_from(model)).one())

    def _count_where(self, model: type[SQLModel], condition: Any) -> int:
        return int(self.session.exec(select(func.count()).select_from(model).where(condition)).one())

    def _detect_format(self, path: Path, format_hint: str) -> str:
        hint = format_hint.lower()
        if hint != "auto":
            return hint
        suffix = path.suffix.lower()
        if suffix == ".json":
            return "json"
        if suffix == ".jsonl":
            return "jsonl"
        if suffix in {".sqlite", ".db"}:
            return "sqlite"
        if suffix in {".txt", ".md", ".log", ".c", ".cpp", ".cc", ".go", ".java", ".php", ".r", ".py"}:
            return "text"
        return "text"

    def _iter_file_observations(
        self,
        path: Path,
        format_detected: str,
        request: IngestFileRequest,
        source_id: str,
    ) -> Iterable[ObservationCreate]:
        if format_detected == "json":
            yield from self._iter_json_file(path, request, source_id)
            return
        if format_detected == "jsonl":
            yield from self._iter_jsonl_file(path, request, source_id)
            return
        if format_detected == "sqlite":
            yield from self._iter_sqlite_file(path, request, source_id)
            return
        yield from self._iter_text_file(path, request, source_id)

    def _iter_json_file(
        self,
        path: Path,
        request: IngestFileRequest,
        source_id: str,
    ) -> Iterable[ObservationCreate]:
        loaded = json.loads(path.read_text(encoding="utf-8"))
        records: list[Any]
        if isinstance(loaded, list):
            records = loaded
        elif isinstance(loaded, dict) and isinstance(loaded.get("records"), list):
            records = list(loaded["records"])
        else:
            records = [loaded]
        for index, record in enumerate(records, start=1):
            yield self._observation_from_record(
                record=record,
                request=request,
                source_id=source_id,
                title_hint=f"{path.stem}#{index}",
                metadata={"input_path": str(path), "record_index": index, "format": "json"},
            )

    def _iter_jsonl_file(
        self,
        path: Path,
        request: IngestFileRequest,
        source_id: str,
    ) -> Iterable[ObservationCreate]:
        for index, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            stripped = line.strip()
            if not stripped:
                continue
            record = json.loads(stripped)
            yield self._observation_from_record(
                record=record,
                request=request,
                source_id=source_id,
                title_hint=f"{path.stem}#{index}",
                metadata={"input_path": str(path), "record_index": index, "format": "jsonl"},
            )

    def _iter_text_file(
        self,
        path: Path,
        request: IngestFileRequest,
        source_id: str,
    ) -> Iterable[ObservationCreate]:
        text = path.read_text(encoding="utf-8", errors="replace")
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if len(lines) <= 1:
            payload = {"text": text}
            yield self._observation_from_record(
                record=payload,
                request=request,
                source_id=source_id,
                title_hint=path.stem,
                metadata={"input_path": str(path), "format": "text"},
            )
            return
        for index, line in enumerate(lines, start=1):
            payload = {"text": line}
            yield self._observation_from_record(
                record=payload,
                request=request,
                source_id=source_id,
                title_hint=f"{path.stem}:line:{index}",
                metadata={"input_path": str(path), "line_number": index, "format": "text"},
            )

    def _iter_sqlite_file(
        self,
        path: Path,
        request: IngestFileRequest,
        source_id: str,
    ) -> Iterable[ObservationCreate]:
        connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            table_rows = connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            ).fetchall()
            for table_row in table_rows:
                table_name = str(table_row["name"])
                rows = connection.execute(f'SELECT * FROM "{table_name}"').fetchall()
                for index, row in enumerate(rows, start=1):
                    payload = dict(row)
                    yield self._observation_from_record(
                        record=payload,
                        request=request,
                        source_id=source_id,
                        title_hint=f"{table_name}#{index}",
                        metadata={
                            "input_path": str(path),
                            "table_name": table_name,
                            "row_index": index,
                            "format": "sqlite",
                        },
                    )
        finally:
            connection.close()

    def _observation_from_record(
        self,
        *,
        record: Any,
        request: IngestFileRequest,
        source_id: str,
        title_hint: str,
        metadata: dict[str, Any],
    ) -> ObservationCreate:
        if isinstance(record, dict):
            normalized = {str(key): _coerce_jsonable(value) for key, value in record.items()}
        else:
            normalized = {"value": _coerce_jsonable(record)}
        title = _pick_str(normalized, "title", "name", "id") or title_hint
        extracted_text = _pick_str(normalized, "text", "content", "description", "summary", "message")
        if extracted_text is None:
            extracted_text = json.dumps(normalized, ensure_ascii=True, sort_keys=True)
        summary = _pick_str(normalized, "summary", "description", "message") or _truncate(extracted_text, 200)
        raw_uri = _pick_str(normalized, "url", "uri", "href", "link")
        observed_at = (
            _parse_datetime(_pick_str(normalized, "observed_at", "observedAt", "timestamp", "time", "created_at", "createdAt"))
            or _parse_datetime(normalized.get("observed_at"))
            or _parse_datetime(normalized.get("timestamp"))
        )
        latitude = _pick_float(normalized, "latitude", "lat", "y")
        longitude = _pick_float(normalized, "longitude", "lon", "lng", "x")
        altitude = _pick_float(normalized, "altitude", "altitude_meters", "altitudeMeters")
        record_json = json.dumps(normalized, ensure_ascii=True, sort_keys=True)
        return ObservationCreate(
            source_id=source_id,
            event_id=request.event_id,
            entity_id=request.entity_id,
            observation_type="file_record",
            title=title,
            summary=summary,
            observed_at=observed_at,
            latitude=latitude,
            longitude=longitude,
            altitude_meters=altitude,
            raw_uri=raw_uri,
            extracted_text=extracted_text,
            raw_hash_sha256=_sha256_text(record_json),
            confidence_score=0.55,
            tags=[request.format_hint, metadata["format"]],
            raw_payload_json=normalized,
            metadata_json=metadata,
            actor=request.actor,
        )
