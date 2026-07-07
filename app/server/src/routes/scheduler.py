from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db import get_db
from src.models import ScheduledTaskORM, ScheduledTaskRunORM
from src.schemas import (
    SchedulerKickResponse,
    ScheduledTaskCreate,
    ScheduledTaskRead,
    ScheduledTaskRunRead,
    ScheduledTaskUpdate,
)
from src.services.scheduler_service import create_scheduled_task, run_due_tasks, run_task, update_scheduled_task

router = APIRouter(prefix="/scheduler", tags=["scheduler"])


@router.get("/tasks", response_model=list[ScheduledTaskRead])
def list_tasks(session: Session = Depends(get_db)) -> list[ScheduledTaskORM]:
    statement = select(ScheduledTaskORM).order_by(ScheduledTaskORM.task_id.asc())
    return list(session.scalars(statement))


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
