from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from src.db import get_db
from src.models import AlertORM, CustodyLogORM
from src.schemas import (
    AlertCreate,
    AlertInventorySummaryRead,
    AlertOpsExportSummaryRead,
    AlertOpsReportIndexRead,
    AlertRead,
    AlertUpdate,
)
from src.services.alert_service import (
    build_alert_inventory_summary,
    build_alert_ops_export_summary,
    build_alert_ops_report_index,
    list_alert_records,
)

router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("", response_model=list[AlertRead])
def list_alerts(
    status: str | None = None,
    geofence_id: int | None = None,
    event_id: int | None = None,
    severity: str | None = None,
    limit: int = 200,
    session: Session = Depends(get_db),
) -> list[AlertORM]:
    return list_alert_records(
        session,
        status=status,
        geofence_id=geofence_id,
        event_id=event_id,
        severity=severity,
        limit=limit,
    )


@router.get("/summary", response_model=AlertInventorySummaryRead)
def alert_summary(
    status: str | None = None,
    geofence_id: int | None = None,
    event_id: int | None = None,
    severity: str | None = None,
    stale_after_hours: float = 24.0,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    return build_alert_inventory_summary(
        session,
        status=status,
        geofence_id=geofence_id,
        event_id=event_id,
        severity=severity,
        stale_after_hours=stale_after_hours,
    )


@router.get("/report-index", response_model=AlertOpsReportIndexRead)
def alert_report_index(
    status: str | None = None,
    geofence_id: int | None = None,
    event_id: int | None = None,
    severity: str | None = None,
    stale_after_hours: float = 24.0,
    limit: int = 25,
    stale_alert_limit: int = 25,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    return build_alert_ops_report_index(
        session,
        status=status,
        geofence_id=geofence_id,
        event_id=event_id,
        severity=severity,
        stale_after_hours=stale_after_hours,
        limit=limit,
        stale_alert_limit=stale_alert_limit,
    )


@router.get("/export/summary", response_model=AlertOpsExportSummaryRead)
def alert_export_summary(
    status: str | None = None,
    geofence_id: int | None = None,
    event_id: int | None = None,
    severity: str | None = None,
    stale_after_hours: float = 24.0,
    alert_limit: int = 500,
    report_limit: int = 25,
    stale_alert_limit: int = 25,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    return build_alert_ops_export_summary(
        session,
        status=status,
        geofence_id=geofence_id,
        event_id=event_id,
        severity=severity,
        stale_after_hours=stale_after_hours,
        alert_limit=alert_limit,
        report_limit=report_limit,
        stale_alert_limit=stale_alert_limit,
    )


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
