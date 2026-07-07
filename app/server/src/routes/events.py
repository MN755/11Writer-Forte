from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db import get_db
from src.models import DataLayerORM, EventORM, EventObservationLinkORM, SituationProductORM
from src.schemas import (
    DataLayerCreate,
    DataLayerRead,
    EventCreate,
    EventFusionRequest,
    EventFusionResponse,
    EventFusionResultRead,
    EventObservationLinkRead,
    EventRead,
    SituationProductRead,
)
from src.services.event_fusion_service import materialize_fused_events

router = APIRouter(prefix="/events", tags=["events"])
layer_router = APIRouter(prefix="/layers", tags=["layers"])


@router.get("", response_model=list[EventRead])
def list_events(session: Session = Depends(get_db)) -> list[EventORM]:
    return list(session.scalars(select(EventORM).order_by(EventORM.created_at.desc())))


@router.post("", response_model=EventRead)
def create_event(payload: EventCreate, session: Session = Depends(get_db)) -> EventORM:
    record = EventORM(**payload.model_dump())
    session.add(record)
    session.commit()
    session.refresh(record)
    return record


@router.post("/fuse", response_model=EventFusionResponse)
def fuse_events(payload: EventFusionRequest, session: Session = Depends(get_db)) -> EventFusionResponse:
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
def list_event_observations(event_id: int, session: Session = Depends(get_db)) -> list[EventObservationLinkORM]:
    event = session.get(EventORM, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail=f"Event {event_id} does not exist.")
    statement = select(EventObservationLinkORM).where(EventObservationLinkORM.event_id == event_id)
    return list(session.scalars(statement.order_by(EventObservationLinkORM.event_observation_link_id.asc())))


@router.get("/{event_id}/products", response_model=list[SituationProductRead])
def list_event_products(event_id: int, session: Session = Depends(get_db)) -> list[SituationProductORM]:
    event = session.get(EventORM, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail=f"Event {event_id} does not exist.")
    statement = select(SituationProductORM).where(SituationProductORM.event_id == event_id)
    return list(session.scalars(statement.order_by(SituationProductORM.product_id.asc())))


@layer_router.get("", response_model=list[DataLayerRead])
def list_layers(session: Session = Depends(get_db)) -> list[DataLayerORM]:
    return list(session.scalars(select(DataLayerORM).order_by(DataLayerORM.name.asc())))


@layer_router.post("", response_model=DataLayerRead)
def create_layer(payload: DataLayerCreate, session: Session = Depends(get_db)) -> DataLayerORM:
    record = DataLayerORM(**payload.model_dump())
    session.add(record)
    session.commit()
    session.refresh(record)
    return record
