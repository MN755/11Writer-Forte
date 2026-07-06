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
)
from src.services.scheduler_service import create_scheduled_task, run_due_tasks, run_task

router = APIRouter(prefix="/scheduler", tags=["scheduler"])


@router.get("/tasks", response_model=list[ScheduledTaskRead])
def list_tasks(session: Session = Depends(get_db)) -> list[ScheduledTaskORM]:
    statement = select(ScheduledTaskORM).order_by(ScheduledTaskORM.task_id.asc())
    return list(session.scalars(statement))


@router.post("/tasks", response_model=ScheduledTaskRead)
def create_task(payload: ScheduledTaskCreate, session: Session = Depends(get_db)) -> ScheduledTaskORM:
    return create_scheduled_task(session, payload)


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
        raise HTTPException(status_code=404, detail=str(exc)) from exc
