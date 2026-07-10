from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import CustodyLogORM, EventORM
from src.schemas import EventCreate


def list_events(session: Session) -> list[EventORM]:
    statement = select(EventORM).order_by(EventORM.created_at.desc(), EventORM.event_id.desc())
    return list(session.scalars(statement))


def create_event(
    session: Session,
    payload: EventCreate,
    *,
    actor: str = "system",
) -> EventORM:
    existing = session.scalar(select(EventORM).where(EventORM.slug == payload.slug))
    if existing is not None:
        raise ValueError(f"Event slug '{payload.slug}' already exists.")

    record = EventORM(**payload.model_dump())
    session.add(record)
    session.flush()
    session.add(
        CustodyLogORM(
            object_type="event",
            object_id=str(record.event_id),
            action="event_created",
            actor=actor,
            details_json={
                **payload.model_dump(mode="json"),
                "event_id": record.event_id,
            },
        )
    )
    session.commit()
    session.refresh(record)
    return record
