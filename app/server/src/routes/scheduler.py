from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db import get_db
from src.models import ScheduledTaskORM, ScheduledTaskRunORM
from src.schemas import (
    SchedulerOpsExportSummaryRead,
    SchedulerInventorySummaryRead,
    SchedulerOpsReportIndexRead,
    SchedulerKickResponse,
    ScheduledTaskCreate,
    ScheduledTaskRead,
    ScheduledTaskRunRead,
    ScheduledTaskUpdate,
)
from src.services.scheduler_service import (
    build_scheduler_inventory_summary,
    build_scheduler_ops_export_summary,
    build_scheduler_ops_report_index,
    create_scheduled_task,
    run_due_tasks,
    run_task,
    update_scheduled_task,
)

router = APIRouter(prefix="/scheduler", tags=["scheduler"])


@router.get("/tasks", response_model=list[ScheduledTaskRead])
def list_tasks(session: Session = Depends(get_db)) -> list[ScheduledTaskORM]:
    statement = select(ScheduledTaskORM).order_by(ScheduledTaskORM.task_id.asc())
    return list(session.scalars(statement))


@router.get("/summary", response_model=SchedulerInventorySummaryRead)
def scheduler_summary(session: Session = Depends(get_db)) -> dict[str, object]:
    return build_scheduler_inventory_summary(session)


@router.get("/report-index", response_model=SchedulerOpsReportIndexRead)
def scheduler_report_index(
    limit: int = 25,
    overdue_task_limit: int = 25,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    return build_scheduler_ops_report_index(
        session,
        limit=limit,
        overdue_task_limit=overdue_task_limit,
    )


@router.get("/export/summary", response_model=SchedulerOpsExportSummaryRead)
def scheduler_export_summary(
    task_limit: int = 500,
    report_limit: int = 25,
    overdue_task_limit: int = 25,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    return build_scheduler_ops_export_summary(
        session,
        task_limit=task_limit,
        report_limit=report_limit,
        overdue_task_limit=overdue_task_limit,
    )

@router.post("/tasks", response_model=ScheduledTaskRead)
def create_task(payload: ScheduledTaskCreate, session: Session = Depends(get_db)) -> ScheduledTaskORM:
    try:
        return create_scheduled_task(session, payload)
    except ValueError as exc:
        raise translate_task_error(exc) from exc


@router.patch("/tasks/{task_id}", response_model=ScheduledTaskRead)
def patch_task(
    task_id: int,
    payload: ScheduledTaskUpdate,
    session: Session = Depends(get_db),
) -> ScheduledTaskORM:
    try:
        return update_scheduled_task(session, task_id, payload)
    except ValueError as exc:
        raise translate_task_error(exc) from exc


@router.get("/runs", response_model=list[ScheduledTaskRunRead])
def list_task_runs(session: Session = Depends(get_db)) -> list[ScheduledTaskRunORM]:
    statement = select(ScheduledTaskRunORM).order_by(ScheduledTaskRunORM.task_run_id.desc())
    return list(session.scalars(statement))


@router.post("/run-due", response_model=SchedulerKickResponse)
def kick_due_tasks(session: Session = Depends(get_db)) -> SchedulerKickResponse:
    runs = run_due_tasks(session)
    return SchedulerKickResponse(
        runs_created=len(runs),
        task_run_ids=[run.task_run_id for run in runs],
    )


@router.post("/tasks/{task_id}/run", response_model=ScheduledTaskRunRead)
def run_task_now(task_id: int, session: Session = Depends(get_db)) -> ScheduledTaskRunORM:
    try:
        return run_task(session, task_id)
    except ValueError as exc:
        raise translate_task_error(exc) from exc


def translate_task_error(exc: ValueError) -> HTTPException:
    detail = str(exc)
    status_code = 404 if "does not exist" in detail else 409
    return HTTPException(status_code=status_code, detail=detail)
