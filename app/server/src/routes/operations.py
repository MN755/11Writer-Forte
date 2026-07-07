from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.db import get_db
from src.schemas import DatabaseDiagnosticsRead
from src.schemas import OperationsReportRead
from src.schemas import RuntimeRestoreResultRead
from src.schemas import RuntimeSnapshotRead
from src.services.database_diagnostics_service import build_database_diagnostics
from src.services.operations_report_service import build_operations_report
from src.services.runtime_snapshot_service import build_runtime_snapshot
from src.services.runtime_snapshot_service import restore_runtime_snapshot

router = APIRouter(prefix="/operations", tags=["operations"])


@router.get("/report", response_model=OperationsReportRead)
def operations_report(
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = Query(default=25, ge=1, le=200),
    session: Session = Depends(get_db),
) -> dict[str, object]:
    return build_operations_report(session, since=since, until=until, limit=limit)


@router.get("/database", response_model=DatabaseDiagnosticsRead)
def database_diagnostics(session: Session = Depends(get_db)) -> dict[str, object]:
    return build_database_diagnostics(session)


@router.get("/runtime/export", response_model=RuntimeSnapshotRead)
def export_runtime_snapshot(session: Session = Depends(get_db)) -> dict[str, object]:
    return build_runtime_snapshot(session)


@router.post("/runtime/restore", response_model=RuntimeRestoreResultRead)
def restore_runtime(
    payload: RuntimeSnapshotRead,
    replace_existing: bool = Query(default=False),
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        return restore_runtime_snapshot(
            session,
            payload.model_dump(mode="python"),
            replace_existing=replace_existing,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
