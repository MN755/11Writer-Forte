from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.db import get_db
from src.routes.error_helpers import translate_service_error
from src.schemas import DatabaseDiagnosticsRead
from src.schemas import ExportArtifactWriteResultRead
from src.schemas import OperationsReportArtifactExportRequest
from src.schemas import OperationsReportRead
from src.schemas import PlatformRuntimeCycleRead
from src.schemas import RuntimeReadinessRead
from src.schemas import RuntimeBundleExportRequest
from src.schemas import RuntimeBundleExportResultRead
from src.schemas import RuntimeBundleRestoreRequest
from src.schemas import RuntimeBundleRestoreResultRead
from src.schemas import RuntimeSnapshotArtifactExportRequest
from src.schemas import RuntimeSnapshotArtifactExportResultRead
from src.schemas import RuntimeRestoreResultRead
from src.schemas import RuntimeSnapshotRead
from src.schemas import WorkerStatusRead
from src.schemas import WorkerStatusSummaryRead
from src.services.database_diagnostics_service import build_database_diagnostics
from src.services.operations_report_service import (
    build_operations_report,
    export_operations_report_artifact,
)
from src.services.platform_runtime_service import run_platform_runtime_cycle
from src.services.runtime_readiness_service import build_runtime_readiness
from src.services.runtime_bundle_service import export_runtime_bundle, restore_runtime_bundle
from src.services.runtime_snapshot_service import build_runtime_snapshot
from src.services.runtime_snapshot_service import export_runtime_snapshot_artifacts
from src.services.runtime_snapshot_service import restore_runtime_snapshot
from src.services.source_service import SourceExecutionError
from src.services.worker_status_service import build_worker_status_summary, list_worker_statuses

router = APIRouter(prefix="/operations", tags=["operations"])


@router.get("/report", response_model=OperationsReportRead)
def operations_report(
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = Query(default=25, ge=1, le=200),
    session: Session = Depends(get_db),
) -> dict[str, object]:
    return build_operations_report(session, since=since, until=until, limit=limit)


@router.post("/report/export", response_model=ExportArtifactWriteResultRead)
def export_operations_report_route(
    payload: OperationsReportArtifactExportRequest,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        result = export_operations_report_artifact(
            session,
            output_path=Path(payload.output_path),
            hours=payload.hours,
            limit=payload.limit,
            actor="api_export",
        )
    except ValueError as exc:
        raise translate_service_error(
            exc,
            action="export_operations_report",
            context={"output_path": payload.output_path, "hours": payload.hours, "limit": payload.limit},
        ) from exc
    except OSError as exc:
        raise translate_service_error(
            exc,
            action="export_operations_report",
            context={"output_path": payload.output_path, "hours": payload.hours, "limit": payload.limit},
        ) from exc
    return {
        "output_path": result["output_path"],
        "storage_object": result["storage_object"],
    }


@router.get("/database", response_model=DatabaseDiagnosticsRead)
def database_diagnostics(session: Session = Depends(get_db)) -> dict[str, object]:
    return build_database_diagnostics(session)


@router.get("/readiness", response_model=RuntimeReadinessRead)
def runtime_readiness(session: Session = Depends(get_db)) -> dict[str, object]:
    return build_runtime_readiness(session)


@router.post("/runtime/cycle", response_model=PlatformRuntimeCycleRead)
def execute_runtime_cycle(
    include_stream_runtime: bool = True,
    include_enabled_schedules: bool = True,
    task_type: list[str] | None = Query(default=None),
    source_id: int | None = None,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        return run_platform_runtime_cycle(
            session,
            include_stream_runtime=include_stream_runtime,
            include_enabled_schedules=include_enabled_schedules,
            task_types=task_type,
            source_id=source_id,
            actor="api_platform_runtime_cycle",
        )
    except ValueError as exc:
        raise translate_service_error(
            exc,
            action="platform_runtime_cycle",
            context={
                "source_id": source_id,
                "include_stream_runtime": include_stream_runtime,
                "include_enabled_schedules": include_enabled_schedules,
            },
        ) from exc
    except SourceExecutionError as exc:
        raise translate_platform_runtime_error(exc) from exc


@router.get("/workers", response_model=list[WorkerStatusRead])
def operations_workers(session: Session = Depends(get_db)) -> list[object]:
    return list_worker_statuses(session)


@router.get("/workers/summary", response_model=WorkerStatusSummaryRead)
def operations_worker_summary(
    stale_after_seconds: float | None = Query(default=None, ge=1.0, le=3600.0),
    session: Session = Depends(get_db),
) -> dict[str, object]:
    return build_worker_status_summary(session, stale_after_seconds=stale_after_seconds)


@router.get("/runtime/export", response_model=RuntimeSnapshotRead)
def export_runtime_snapshot(session: Session = Depends(get_db)) -> dict[str, object]:
    return build_runtime_snapshot(session)


@router.post("/runtime/export-artifacts", response_model=RuntimeSnapshotArtifactExportResultRead)
def export_runtime_snapshot_artifact_route(
    payload: RuntimeSnapshotArtifactExportRequest,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        result = export_runtime_snapshot_artifacts(
            session,
            output_path=Path(payload.output_path),
            manifest_path=Path(payload.manifest_path) if payload.manifest_path is not None else None,
            actor="api_export",
        )
    except ValueError as exc:
        raise translate_service_error(
            exc,
            action="export_runtime_snapshot_artifacts",
            context={"output_path": payload.output_path, "manifest_path": payload.manifest_path},
        ) from exc
    except OSError as exc:
        raise translate_service_error(
            exc,
            action="export_runtime_snapshot_artifacts",
            context={"output_path": payload.output_path, "manifest_path": payload.manifest_path},
        ) from exc
    return {
        "output_path": str(result["output_path"]),
        "manifest_path": str(result["manifest_path"]),
        "snapshot_manifest": result["manifest"],
        "snapshot_storage_object": result["snapshot_record"],
        "manifest_storage_object": result["manifest_record"],
    }


@router.post("/runtime/bundle/export", response_model=RuntimeBundleExportResultRead)
def export_runtime_bundle_route(
    payload: RuntimeBundleExportRequest,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        return export_runtime_bundle(
            session,
            output_path=Path(payload.output_path),
            actor="api_export",
        )
    except (ValueError, OSError) as exc:
        raise translate_service_error(
            exc,
            action="export_runtime_bundle",
            context={"output_path": payload.output_path},
        ) from exc


@router.post("/runtime/bundle/restore", response_model=RuntimeBundleRestoreResultRead)
def restore_runtime_bundle_route(
    payload: RuntimeBundleRestoreRequest,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        return restore_runtime_bundle(
            session,
            bundle_path=Path(payload.input_path),
            replace_existing=payload.replace_existing,
            actor="api_restore",
        )
    except (ValueError, OSError) as exc:
        raise translate_service_error(
            exc,
            action="restore_runtime_bundle",
            context={
                "input_path": payload.input_path,
                "replace_existing": payload.replace_existing,
            },
        ) from exc


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
        raise translate_service_error(
            exc,
            action="runtime_restore",
            context={"replace_existing": replace_existing},
        ) from exc


def translate_platform_runtime_error(exc: SourceExecutionError) -> HTTPException:
    cause = exc.cause
    if isinstance(cause, FileNotFoundError):
        status_code = 404
    elif isinstance(cause, ValueError):
        status_code = 409
    elif isinstance(cause, (RuntimeError, OSError)):
        status_code = 502
    else:
        status_code = 500
    return HTTPException(
        status_code=status_code,
        detail={
            "message": str(cause),
            "source_id": exc.source_id,
            "source_run_id": exc.source_run_id,
            "source_kind": exc.source_kind,
            "error_type": cause.__class__.__name__,
        },
    )
