from __future__ import annotations

from fastapi import APIRouter, Depends

from src.config.settings import Settings, get_settings
from src.services.ops_audit_service import list_alert_records, list_provenance_events
from src.types.api import BackendAlertResponse, BackendProvenanceResponse

router = APIRouter(prefix="/api/ops", tags=["ops"])


@router.get("/provenance", response_model=BackendProvenanceResponse)
def ops_provenance(
    limit: int = 50,
    subsystem: str | None = None,
    subject_type: str | None = None,
    subject_id: str | None = None,
    settings: Settings = Depends(get_settings),
) -> BackendProvenanceResponse:
    return list_provenance_events(
        settings,
        limit=limit,
        subsystem=subsystem,
        subject_type=subject_type,
        subject_id=subject_id,
    )


@router.get("/alerts", response_model=BackendAlertResponse)
def ops_alerts(
    limit: int = 50,
    subsystem: str | None = None,
    status: str | None = None,
    severity: str | None = None,
    settings: Settings = Depends(get_settings),
) -> BackendAlertResponse:
    return list_alert_records(
        settings,
        limit=limit,
        subsystem=subsystem,
        status=status,
        severity=severity,
    )
