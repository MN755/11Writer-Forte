from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from src.db import get_db
from src.models import AlertORM
from src.schemas import AlertCreate, AlertRead, AlertUpdate
from src.services.alert_service import (
    create_alert as create_alert_record,
    list_alerts as list_alert_records,
    update_alert as update_alert_record,
)

router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("", response_model=list[AlertRead])
def get_alerts(
    status: str | None = None,
    geofence_id: int | None = None,
    event_id: int | None = None,
    session: Session = Depends(get_db),
) -> list[AlertORM]:
    return list_alert_records(
        session,
        status=status,
        geofence_id=geofence_id,
        event_id=event_id,
    )


@router.post("", response_model=AlertRead)
def post_alert(payload: AlertCreate, session: Session = Depends(get_db)) -> AlertORM:
    return create_alert_record(session, payload, actor="api_alert")


@router.patch("/{alert_id}", response_model=AlertRead)
def patch_alert(
    alert_id: int, payload: AlertUpdate, session: Session = Depends(get_db)
) -> AlertORM:
    try:
        return update_alert_record(session, alert_id, payload, actor="api_alert")
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
