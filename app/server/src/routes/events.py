from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db import get_db
from src.models import DataLayerORM, EventORM
from src.schemas import DataLayerCreate, DataLayerRead, EventCreate, EventRead

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

