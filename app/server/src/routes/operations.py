from datetime import datetime

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from src.db import get_db
from src.schemas import DatabaseDiagnosticsRead
from src.schemas import OperationsReportRead
from src.services.database_diagnostics_service import build_database_diagnostics
from src.services.operations_report_service import build_operations_report

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
