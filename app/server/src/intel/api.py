from __future__ import annotations

import json
from collections.abc import Generator

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel import Session

from .db import db, ensure_current_database
from .models import (
    AlertCreate,
    AnalyticProductCreate,
    AssessmentCreate,
    CustodyRecordCreate,
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
    IntelGeofence,
    IntelObservation,
    IntelSource,
    ObservationCreate,
    OverviewResponse,
    SourceCreate,
)
from .service import IntelService


router = APIRouter(prefix="/api/intel", tags=["intel"])


def get_session() -> Generator[Session, None, None]:
    ensure_current_database()
    yield from db.get_session()


def get_service(session: Session = Depends(get_session)) -> IntelService:
    return IntelService(session)


def _http_400(exc: Exception) -> HTTPException:
    return HTTPException(status_code=400, detail=str(exc))


@router.get("/overview", response_model=OverviewResponse)
def overview(service: IntelService = Depends(get_service)) -> OverviewResponse:
    return service.overview()


@router.post("/sources", response_model=IntelSource, status_code=201)
def create_source(payload: SourceCreate, service: IntelService = Depends(get_service)) -> IntelSource:
    return service.create_source(payload)


@router.get("/sources", response_model=list[IntelSource])
def list_sources(
    kind: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    service: IntelService = Depends(get_service),
) -> list[IntelSource]:
    return service.list_sources(kind=kind, limit=limit)


@router.get("/sources/{source_id}", response_model=IntelSource)
def get_source(source_id: str, service: IntelService = Depends(get_service)) -> IntelSource:
    source = service.get_source(source_id)
    if source is None:
        raise HTTPException(status_code=404, detail="Source not found.")
    return source


@router.post("/entities", response_model=IntelEntity, status_code=201)
def create_entity(payload: EntityCreate, service: IntelService = Depends(get_service)) -> IntelEntity:
    try:
        return service.create_entity(payload)
    except ValueError as exc:
        raise _http_400(exc) from exc


@router.get("/entities", response_model=list[IntelEntity])
def list_entities(
    entity_type: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    service: IntelService = Depends(get_service),
) -> list[IntelEntity]:
    return service.list_entities(entity_type=entity_type, limit=limit)


@router.get("/entities/{entity_id}", response_model=IntelEntity)
def get_entity(entity_id: str, service: IntelService = Depends(get_service)) -> IntelEntity:
    entity = service.get_entity(entity_id)
    if entity is None:
        raise HTTPException(status_code=404, detail="Entity not found.")
    return entity


@router.post("/events", response_model=IntelEvent, status_code=201)
def create_event(payload: EventCreate, service: IntelService = Depends(get_service)) -> IntelEvent:
    try:
        return service.create_event(payload)
    except ValueError as exc:
        raise _http_400(exc) from exc


@router.get("/events", response_model=list[IntelEvent])
def list_events(
    status: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    service: IntelService = Depends(get_service),
) -> list[IntelEvent]:
    return service.list_events(status=status, limit=limit)


@router.get("/events/{event_id}", response_model=IntelEvent)
def get_event(event_id: str, service: IntelService = Depends(get_service)) -> IntelEvent:
    event = service.get_event(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found.")
    return event


@router.post("/events/{event_id}/recompute-confidence", response_model=IntelAssessment)
def recompute_event_confidence(
    event_id: str,
    actor: str = Query(default="system"),
    service: IntelService = Depends(get_service),
) -> IntelAssessment:
    try:
        return service.recompute_event_assessment(event_id, actor=actor)
    except ValueError as exc:
        raise _http_400(exc) from exc


@router.post("/observations", response_model=IntelObservation, status_code=201)
def create_observation(payload: ObservationCreate, service: IntelService = Depends(get_service)) -> IntelObservation:
    try:
        return service.create_observation(payload)
    except ValueError as exc:
        raise _http_400(exc) from exc


@router.get("/observations", response_model=list[IntelObservation])
def list_observations(
    event_id: str | None = Query(default=None),
    entity_id: str | None = Query(default=None),
    source_id: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=5000),
    service: IntelService = Depends(get_service),
) -> list[IntelObservation]:
    return service.list_observations(event_id=event_id, entity_id=entity_id, source_id=source_id, limit=limit)


@router.get("/observations/{observation_id}", response_model=IntelObservation)
def get_observation(observation_id: str, service: IntelService = Depends(get_service)) -> IntelObservation:
    observation = service.get_observation(observation_id)
    if observation is None:
        raise HTTPException(status_code=404, detail="Observation not found.")
    return observation


@router.post("/geofences", response_model=IntelGeofence, status_code=201)
def create_geofence(payload: GeofenceCreate, service: IntelService = Depends(get_service)) -> IntelGeofence:
    return service.create_geofence(payload)


@router.get("/geofences", response_model=list[IntelGeofence])
def list_geofences(
    limit: int = Query(default=100, ge=1, le=1000),
    service: IntelService = Depends(get_service),
) -> list[IntelGeofence]:
    return service.list_geofences(limit=limit)


@router.get("/geofences/{geofence_id}", response_model=IntelGeofence)
def get_geofence(geofence_id: str, service: IntelService = Depends(get_service)) -> IntelGeofence:
    geofence = service.get_geofence(geofence_id)
    if geofence is None:
        raise HTTPException(status_code=404, detail="Geofence not found.")
    return geofence


@router.post("/alerts", response_model=IntelAlert, status_code=201)
def create_alert(payload: AlertCreate, service: IntelService = Depends(get_service)) -> IntelAlert:
    try:
        return service.create_alert(payload)
    except ValueError as exc:
        raise _http_400(exc) from exc


@router.get("/alerts", response_model=list[IntelAlert])
def list_alerts(
    status: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    service: IntelService = Depends(get_service),
) -> list[IntelAlert]:
    return service.list_alerts(status=status, limit=limit)


@router.get("/alerts/{alert_id}", response_model=IntelAlert)
def get_alert(alert_id: str, service: IntelService = Depends(get_service)) -> IntelAlert:
    alert = service.get_alert(alert_id)
    if alert is None:
        raise HTTPException(status_code=404, detail="Alert not found.")
    return alert


@router.post("/alerts/evaluate-geofences", response_model=list[IntelAlert])
def evaluate_geofences(
    actor: str = Query(default="system"),
    service: IntelService = Depends(get_service),
) -> list[IntelAlert]:
    return service.evaluate_geofences(actor=actor)


@router.post("/assessments", response_model=IntelAssessment, status_code=201)
def create_assessment(payload: AssessmentCreate, service: IntelService = Depends(get_service)) -> IntelAssessment:
    return service.create_assessment(payload)


@router.get("/assessments", response_model=list[IntelAssessment])
def list_assessments(
    target_kind: str | None = Query(default=None),
    target_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    service: IntelService = Depends(get_service),
) -> list[IntelAssessment]:
    return service.list_assessments(target_kind=target_kind, target_id=target_id, limit=limit)


@router.post("/entity-resolutions", response_model=IntelEntityResolution, status_code=201)
def create_entity_resolution(
    payload: EntityResolutionCreate,
    service: IntelService = Depends(get_service),
) -> IntelEntityResolution:
    return service.create_entity_resolution(payload)


@router.get("/entity-resolutions", response_model=list[IntelEntityResolution])
def list_entity_resolutions(
    entity_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    service: IntelService = Depends(get_service),
) -> list[IntelEntityResolution]:
    return service.list_entity_resolutions(entity_id=entity_id, limit=limit)


@router.post("/products", response_model=IntelAnalyticProduct, status_code=201)
def create_product(
    payload: AnalyticProductCreate,
    service: IntelService = Depends(get_service),
) -> IntelAnalyticProduct:
    return service.create_product(payload)


@router.get("/products", response_model=list[IntelAnalyticProduct])
def list_products(
    event_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    service: IntelService = Depends(get_service),
) -> list[IntelAnalyticProduct]:
    return service.list_products(event_id=event_id, limit=limit)


@router.post("/custody", response_model=IntelCustodyRecord, status_code=201)
def create_custody_record(
    payload: CustodyRecordCreate,
    service: IntelService = Depends(get_service),
) -> IntelCustodyRecord:
    return service.create_custody_record(payload)


@router.get("/custody", response_model=list[IntelCustodyRecord])
def list_custody(
    subject_kind: str | None = Query(default=None),
    subject_id: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=5000),
    service: IntelService = Depends(get_service),
) -> list[IntelCustodyRecord]:
    return service.list_custody(subject_kind=subject_kind, subject_id=subject_id, limit=limit)


@router.post("/intake/files")
def ingest_file(request: IngestFileRequest, service: IntelService = Depends(get_service)) -> dict[str, object]:
    try:
        return service.ingest_file(request)
    except (FileNotFoundError, ValueError, OSError, json.JSONDecodeError) as exc:
        raise _http_400(exc) from exc
