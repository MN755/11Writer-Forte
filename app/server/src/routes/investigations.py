from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.db import get_db
from src.schemas import (
    InvestigationArchiveResultRead,
    InvestigationCreate,
    InvestigationDiscoveryAttemptCreate,
    InvestigationDiscoveryAttemptRead,
    InvestigationEvidencePromotionCreate,
    InvestigationEvidencePromotionRead,
    InvestigationRead,
    InvestigationReportVersionCreate,
    InvestigationReportVersionRead,
    InvestigationTransitionRequest,
)
from src.services.investigation_service import (
    add_report_version,
    archive_investigation,
    build_monitor_contract,
    create_investigation,
    list_discovery_attempts,
    list_evidence_promotions,
    list_investigations,
    list_report_versions,
    promote_evidence,
    record_discovery_attempt,
    require_investigation,
    transition_investigation,
)


router = APIRouter(prefix="/investigations", tags=["investigations"])


@router.get("", response_model=list[InvestigationRead])
def list_investigation_records(
    state: str | None = None,
    limit: int = Query(default=200, ge=1, le=2000),
    session: Session = Depends(get_db),
) -> list[object]:
    return list_investigations(session, state=state, limit=limit)


@router.post("", response_model=InvestigationRead)
def create_investigation_record(
    payload: InvestigationCreate, session: Session = Depends(get_db)
) -> object:
    try:
        return create_investigation(session, payload, actor="api")
    except ValueError as exc:
        raise translate_investigation_error(exc) from exc


@router.get("/{investigation_id}", response_model=InvestigationRead)
def get_investigation_record(investigation_id: int, session: Session = Depends(get_db)) -> object:
    try:
        return require_investigation(session, investigation_id)
    except ValueError as exc:
        raise translate_investigation_error(exc) from exc


@router.post("/{investigation_id}/transition", response_model=InvestigationRead)
def transition_investigation_record(
    investigation_id: int,
    payload: InvestigationTransitionRequest,
    session: Session = Depends(get_db),
) -> object:
    try:
        return transition_investigation(session, investigation_id, payload, actor="api")
    except ValueError as exc:
        raise translate_investigation_error(exc) from exc


@router.get("/{investigation_id}/attempts", response_model=list[InvestigationDiscoveryAttemptRead])
def list_investigation_attempts(
    investigation_id: int,
    limit: int = Query(default=200, ge=1, le=2000),
    session: Session = Depends(get_db),
) -> list[object]:
    try:
        return list_discovery_attempts(session, investigation_id, limit=limit)
    except ValueError as exc:
        raise translate_investigation_error(exc) from exc


@router.post("/{investigation_id}/attempts", response_model=InvestigationDiscoveryAttemptRead)
def record_investigation_attempt(
    investigation_id: int,
    payload: InvestigationDiscoveryAttemptCreate,
    session: Session = Depends(get_db),
) -> object:
    try:
        return record_discovery_attempt(session, investigation_id, payload, actor="api")
    except ValueError as exc:
        raise translate_investigation_error(exc) from exc


@router.get("/{investigation_id}/reports", response_model=list[InvestigationReportVersionRead])
def list_investigation_reports(
    investigation_id: int,
    limit: int = Query(default=200, ge=1, le=2000),
    session: Session = Depends(get_db),
) -> list[object]:
    try:
        return list_report_versions(session, investigation_id, limit=limit)
    except ValueError as exc:
        raise translate_investigation_error(exc) from exc


@router.post("/{investigation_id}/reports", response_model=InvestigationReportVersionRead)
def create_investigation_report(
    investigation_id: int,
    payload: InvestigationReportVersionCreate,
    session: Session = Depends(get_db),
) -> object:
    try:
        return add_report_version(session, investigation_id, payload, actor="api")
    except ValueError as exc:
        raise translate_investigation_error(exc) from exc


@router.get("/{investigation_id}/evidence", response_model=list[InvestigationEvidencePromotionRead])
def list_investigation_evidence(
    investigation_id: int,
    limit: int = Query(default=200, ge=1, le=2000),
    session: Session = Depends(get_db),
) -> list[object]:
    try:
        return list_evidence_promotions(session, investigation_id, limit=limit)
    except ValueError as exc:
        raise translate_investigation_error(exc) from exc


@router.post("/{investigation_id}/evidence", response_model=InvestigationEvidencePromotionRead)
def promote_investigation_evidence(
    investigation_id: int,
    payload: InvestigationEvidencePromotionCreate,
    session: Session = Depends(get_db),
) -> object:
    try:
        return promote_evidence(session, investigation_id, payload, actor="api")
    except ValueError as exc:
        raise translate_investigation_error(exc) from exc


@router.get("/{investigation_id}/monitor-contract")
def get_investigation_monitor_contract(
    investigation_id: int,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        return build_monitor_contract(session, investigation_id)
    except ValueError as exc:
        raise translate_investigation_error(exc) from exc


@router.post("/{investigation_id}/archive", response_model=InvestigationArchiveResultRead)
def archive_investigation_record(
    investigation_id: int,
    stop_reason: str | None = Query(default=None, max_length=10_000),
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        return archive_investigation(
            session, investigation_id, stop_reason=stop_reason, actor="api"
        )
    except ValueError as exc:
        raise translate_investigation_error(exc) from exc


def translate_investigation_error(exc: ValueError) -> HTTPException:
    status_code = 404 if "does not exist" in str(exc) else 409
    return HTTPException(status_code=status_code, detail=str(exc))
