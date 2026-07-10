from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db import get_db
from src.models import ScheduledTaskORM, ScheduledTaskRunORM
from src.routes.error_helpers import translate_service_error
from src.schemas import (
    ExportArtifactWriteResultRead,
    SchedulerOpsExportSummaryRead,
    SchedulerSummaryArtifactExportRequest,
    SchedulerInventorySummaryRead,
    SchedulerOpsReportIndexRead,
    SchedulerKickResponse,
    ScheduledTaskCreate,
    ScheduledTaskRead,
    ScheduledTaskRunRead,
    ScheduledTaskUpdate,
)
from src.services.export_artifact_service import persist_typed_json_export_artifact
from src.services.source_service import SourceExecutionError
from src.services.scheduler_service import (
    ScheduledTaskExecutionError,
    build_scheduler_inventory_summary,
    build_scheduler_ops_export_summary,
    build_scheduler_ops_report_index,
    create_scheduled_task,
    run_enabled_tasks,
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


@router.post("/export/summary/artifact", response_model=ExportArtifactWriteResultRead)
def scheduler_export_summary_artifact(
    payload: SchedulerSummaryArtifactExportRequest,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        report = build_scheduler_ops_export_summary(
            session,
            task_limit=payload.task_limit,
            report_limit=payload.report_limit,
            overdue_task_limit=payload.overdue_task_limit,
        )
        result = persist_typed_json_export_artifact(
            session,
            output_path=Path(payload.output_path),
            payload=report,
            response_model=SchedulerOpsExportSummaryRead,
            object_kind="scheduler_summary_export",
            owner_type="scheduler_export",
            owner_id="scoped",
            source_uri="/api/scheduler/export/summary",
            observed_at=report["generated_at"],
            metadata_json=report["filters_json"],
            actor="api_export",
        )
    except ValueError as exc:
        raise translate_service_error(
            exc,
            action="scheduler_export_summary_artifact",
            context={"output_path": payload.output_path},
        ) from exc
    except OSError as exc:
        raise translate_service_error(
            exc,
            action="scheduler_export_summary_artifact",
            context={"output_path": payload.output_path},
        ) from exc
    return {
        "output_path": result["output_path"],
        "storage_object": result["storage_object"],
    }


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


@router.post("/run-enabled", response_model=SchedulerKickResponse)
def kick_enabled_tasks(
    task_type: list[str] | None = None,
    session: Session = Depends(get_db),
) -> SchedulerKickResponse:
    runs = run_enabled_tasks(session, task_types=task_type)
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
    except ScheduledTaskExecutionError as exc:
        raise translate_task_execution_error(exc) from exc


def translate_task_error(exc: ValueError) -> HTTPException:
    detail = str(exc)
    status_code = 404 if "does not exist" in detail else 409
    return HTTPException(status_code=status_code, detail=detail)


def translate_task_execution_error(exc: ScheduledTaskExecutionError) -> HTTPException:
    cause = exc.cause
    detail: dict[str, object] = {
        "message": str(cause),
        "task_id": exc.task_id,
        "task_run_id": exc.task_run_id,
        "task_type": exc.task_type,
        "error_type": cause.__class__.__name__,
    }
    if isinstance(cause, SourceExecutionError):
        source_error = cause
        source_cause = source_error.cause
        cause = source_cause
        detail.update(
            {
                "message": str(source_cause),
                "error_type": source_cause.__class__.__name__,
                "source_id": source_error.source_id,
                "source_run_id": source_error.source_run_id,
                "source_kind": source_error.source_kind,
            }
        )
    if isinstance(cause, FileNotFoundError):
        status_code = 404
    elif isinstance(cause, ValueError):
        status_code = 409
    elif isinstance(cause, (RuntimeError, OSError)):
        status_code = 502
    else:
        status_code = 500
    return HTTPException(status_code=status_code, detail=detail)
