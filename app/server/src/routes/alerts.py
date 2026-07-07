from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db import get_db
from src.models import AlertORM, CustodyLogORM
from src.schemas import AlertCreate, AlertRead, AlertUpdate

router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("", response_model=list[AlertRead])
def list_alerts(
    status: str | None = None,
    geofence_id: int | None = None,
    event_id: int | None = None,
    session: Session = Depends(get_db),
) -> list[AlertORM]:
    statement = select(AlertORM).order_by(AlertORM.created_at.desc())
    if status is not None:
        statement = statement.where(AlertORM.status == status)
    if geofence_id is not None:
        statement = statement.where(AlertORM.geofence_id == geofence_id)
    if event_id is not None:
        statement = statement.where(AlertORM.event_id == event_id)
    return list(session.scalars(statement))


@router.post("", response_model=AlertRead)
def create_alert(payload: AlertCreate, session: Session = Depends(get_db)) -> AlertORM:
    record = AlertORM(**payload.model_dump())
    session.add(record)
    session.flush()
    session.add(
        CustodyLogORM(
            object_type="alert",
            object_id=str(record.alert_id),
            action="alert_created",
            actor="system",
            details_json={
                **payload.model_dump(),
                "alert_id": record.alert_id,
            },
        )
    )
    session.commit()
    session.refresh(record)
    return record


@router.patch("/{alert_id}", response_model=AlertRead)
def update_alert(alert_id: int, payload: AlertUpdate, session: Session = Depends(get_db)) -> AlertORM:
    record = session.get(AlertORM, alert_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"Alert {alert_id} does not exist.")
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
            actor="system",
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
