from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import AlertORM, CustodyLogORM
from src.schemas import AlertCreate, AlertUpdate


def list_alerts(
    session: Session,
    *,
    status: str | None = None,
    geofence_id: int | None = None,
    event_id: int | None = None,
) -> list[AlertORM]:
    statement = select(AlertORM).order_by(AlertORM.created_at.desc(), AlertORM.alert_id.desc())
    if status is not None:
        statement = statement.where(AlertORM.status == status)
    if geofence_id is not None:
        statement = statement.where(AlertORM.geofence_id == geofence_id)
    if event_id is not None:
        statement = statement.where(AlertORM.event_id == event_id)
    return list(session.scalars(statement))


def create_alert(
    session: Session,
    payload: AlertCreate,
    *,
    actor: str = "system",
) -> AlertORM:
    record = AlertORM(**payload.model_dump())
    session.add(record)
    session.flush()
    session.add(
        CustodyLogORM(
            object_type="alert",
            object_id=str(record.alert_id),
            action="alert_created",
            actor=actor,
            details_json={
                **payload.model_dump(mode="json"),
                "alert_id": record.alert_id,
            },
        )
    )
    session.commit()
    session.refresh(record)
    return record


def update_alert(
    session: Session,
    alert_id: int,
    payload: AlertUpdate,
    *,
    actor: str = "system",
) -> AlertORM:
    record = session.get(AlertORM, alert_id)
    if record is None:
        raise ValueError(f"Alert {alert_id} does not exist.")
    previous_status = record.status
    record.status = payload.status
    if payload.severity is not None:
        record.severity = payload.severity
    record.disposition_note = payload.disposition_note
    session.add(
        CustodyLogORM(
            object_type="alert",
            object_id=str(record.alert_id),
            action="alert_updated",
            actor=actor,
            details_json={
                "previous_status": previous_status,
                "status": record.status,
                "severity": record.severity,
                "disposition_note": record.disposition_note,
            },
        )
    )
    session.commit()
    session.refresh(record)
    return record
