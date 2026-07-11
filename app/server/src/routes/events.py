from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db import get_db
from src.models import EventORM, EventObservationLinkORM, SituationProductORM
from src.schemas import (
    DataLayerCreate,
    DataLayerRead,
    EventCreate,
    EventExportBundleRead,
    EventFusionRequest,
    EventFusionResponse,
    EventFusionResultRead,
    EventObservationLinkRead,
    EventRead,
    SituationProductRead,
)
from src.services.event_export_service import build_event_export_bundle
from src.services.event_fusion_service import materialize_fused_events
from src.services.event_service import create_event, list_events
from src.services.layer_service import create_data_layer, list_data_layers
from src.services.redaction_service import (
    enforce_export_redaction,
    filter_records_by_redaction_level,
)

router = APIRouter(prefix="/events", tags=["events"])
layer_router = APIRouter(prefix="/layers", tags=["layers"])


@router.get("", response_model=list[EventRead])
def get_events(session: Session = Depends(get_db)) -> list[EventORM]:
    return list_events(session)


@router.post("", response_model=EventRead)
def post_event(payload: EventCreate, session: Session = Depends(get_db)) -> EventORM:
    try:
        return create_event(session, payload, actor="api_event")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/fuse", response_model=EventFusionResponse)
def fuse_events(
    payload: EventFusionRequest, session: Session = Depends(get_db)
) -> EventFusionResponse:
    results = materialize_fused_events(session, payload)
    return EventFusionResponse(
        created_event_count=sum(1 for result in results if result.created_new),
        event_results=[
            EventFusionResultRead(
                event_id=result.event.event_id,
                slug=result.event.slug,
                title=result.event.title,
                observation_count=result.observation_count,
                product_count=result.product_count,
                verification_score=result.verification_score,
            )
            for result in results
        ],
    )


@router.get("/{event_id}/observations", response_model=list[EventObservationLinkRead])
def list_event_observations(
    event_id: int, session: Session = Depends(get_db)
) -> list[EventObservationLinkORM]:
    event = session.get(EventORM, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail=f"Event {event_id} does not exist.")
    statement = select(EventObservationLinkORM).where(EventObservationLinkORM.event_id == event_id)
    return list(
        session.scalars(statement.order_by(EventObservationLinkORM.event_observation_link_id.asc()))
    )


@router.get("/{event_id}/products", response_model=list[SituationProductRead])
def list_event_products(
    event_id: int,
    max_redaction_level: str | None = None,
    session: Session = Depends(get_db),
) -> list[SituationProductORM]:
    event = session.get(EventORM, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail=f"Event {event_id} does not exist.")
    try:
        enforce_export_redaction(event, max_redaction_level)
    except ValueError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    statement = select(SituationProductORM).where(SituationProductORM.event_id == event_id)
    rows = list(session.scalars(statement.order_by(SituationProductORM.product_id.asc())))
    if max_redaction_level is None:
        return rows
    return filter_records_by_redaction_level(rows, max_redaction_level)


@router.get("/{event_id}/export", response_model=EventExportBundleRead)
def export_event_bundle(
    event_id: int,
    max_redaction_level: str | None = None,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        return build_event_export_bundle(session, event_id, max_redaction_level=max_redaction_level)
    except ValueError as exc:
        detail = str(exc)
        status_code = 403 if "cannot be exported" in detail else 404
        raise HTTPException(status_code=status_code, detail=detail) from exc


@layer_router.get("", response_model=list[DataLayerRead])
def list_layers(session: Session = Depends(get_db)) -> list[object]:
    return list_data_layers(session)


@layer_router.post("", response_model=DataLayerRead)
def create_layer(payload: DataLayerCreate, session: Session = Depends(get_db)) -> object:
    try:
        return create_data_layer(session, payload, actor="api")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
