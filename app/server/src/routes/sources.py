from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from src.db import get_db
from src.schemas import (
    SourceDefinitionCreate,
    SourceDefinitionRead,
    SourceInventorySummaryRead,
    SourceOpsExportSummaryRead,
    SourceOpsReportIndexRead,
    SourceDefinitionUpdate,
    SourceOpsDetailRead,
    SourceRunRead,
)
from src.services.source_service import (
    build_source_inventory_summary,
    build_source_ops_detail,
    build_source_ops_export_summary,
    build_source_ops_report_index,
    create_source_definition,
    list_source_definitions,
    list_source_runs,
    run_source_definition,
    update_source_definition,
)

router = APIRouter(prefix="/sources", tags=["sources"])


@router.get("", response_model=list[SourceDefinitionRead])
def list_sources(session: Session = Depends(get_db)) -> list[object]:
    return list_source_definitions(session)


@router.post("", response_model=SourceDefinitionRead)
def create_source(payload: SourceDefinitionCreate, session: Session = Depends(get_db)) -> object:
    try:
        return create_source_definition(session, payload)
    except ValueError as exc:
        raise translate_source_error(exc) from exc


@router.patch("/{source_id}", response_model=SourceDefinitionRead)
def update_source(source_id: int, payload: SourceDefinitionUpdate, session: Session = Depends(get_db)) -> object:
    try:
        return update_source_definition(session, source_id, payload)
    except ValueError as exc:
        raise translate_source_error(exc) from exc


@router.get("/runs", response_model=list[SourceRunRead])
def list_runs(session: Session = Depends(get_db)) -> list[object]:
    return list_source_runs(session)


@router.get("/summary", response_model=SourceInventorySummaryRead)
def source_summary(
    stale_after_hours: float = 24.0,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    return build_source_inventory_summary(session, stale_after_hours=stale_after_hours)


@router.get("/report-index", response_model=SourceOpsReportIndexRead)
def source_report_index(
    stale_after_hours: float = 24.0,
    limit: int = 25,
    stale_source_limit: int = 25,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    return build_source_ops_report_index(
        session,
        stale_after_hours=stale_after_hours,
        limit=limit,
        stale_source_limit=stale_source_limit,
    )


@router.get("/export/summary", response_model=SourceOpsExportSummaryRead)
def source_export_summary(
    stale_after_hours: float = 24.0,
    source_limit: int = 500,
    report_limit: int = 25,
    stale_source_limit: int = 25,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    return build_source_ops_export_summary(
        session,
        stale_after_hours=stale_after_hours,
        source_limit=source_limit,
        report_limit=report_limit,
        stale_source_limit=stale_source_limit,
    )


@router.get("/{source_id}/ops", response_model=SourceOpsDetailRead)
def source_ops(source_id: int, session: Session = Depends(get_db)) -> dict[str, object]:
    try:
        return build_source_ops_detail(session, source_id)
    except ValueError as exc:
        raise translate_source_error(exc) from exc


@router.post("/{source_id}/run", response_model=SourceRunRead)
def run_source(source_id: int, session: Session = Depends(get_db)) -> object:
    try:
        return run_source_definition(session, source_id)
    except ValueError as exc:
        raise translate_source_error(exc) from exc


def translate_source_error(exc: ValueError) -> HTTPException:
    detail = str(exc)
    status_code = 404 if "does not exist" in detail else 409
    return HTTPException(status_code=status_code, detail=detail)
