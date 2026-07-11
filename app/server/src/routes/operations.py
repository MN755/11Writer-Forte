from datetime import datetime
import tempfile
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from src.db import get_db
from src.schemas import DatabaseDiagnosticsRead
from src.schemas import OperationsReportRead
from src.schemas import RuntimeBundleRestoreResultRead
from src.schemas import RuntimeRestoreResultRead
from src.schemas import RuntimeSnapshotRead
from src.services.runtime_bundle_service import default_runtime_bundle_output_path
from src.services.runtime_bundle_service import export_runtime_bundle
from src.services.runtime_bundle_service import restore_runtime_bundle
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


@router.get("/runtime/bundle/export")
def export_runtime_bundle_archive(
    session: Session = Depends(get_db),
) -> FileResponse:
    result = export_runtime_bundle(
        session,
        default_runtime_bundle_output_path(),
        actor="api_export",
    )
    output_path = Path(result["output_path"])
    response = FileResponse(
        output_path,
        media_type="application/zip",
        filename=output_path.name,
    )
    response.headers["X-ElevenWriter-Storage-Object-Id"] = str(result["storage_object_id"])
    response.headers["X-ElevenWriter-Bundle-SHA256"] = str(result["bundle_sha256"])
    response.headers["X-ElevenWriter-Bundle-Format-Version"] = str(result["bundle_format_version"])
    return response


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


@router.post("/runtime/bundle/restore", response_model=RuntimeBundleRestoreResultRead)
async def restore_runtime_bundle_archive(
    request: Request,
    replace_existing: bool = Query(default=False),
    session: Session = Depends(get_db),
) -> dict[str, object]:
    payload = await request.body()
    if not payload:
        raise HTTPException(status_code=400, detail="Runtime bundle payload is empty.")

    with tempfile.NamedTemporaryFile(
        prefix="11writer-runtime-bundle-upload-",
        suffix=".zip",
        delete=False,
    ) as staged:
        staged.write(payload)
        staged_path = Path(staged.name)

    try:
        return restore_runtime_bundle(
            session,
            staged_path,
            replace_existing=replace_existing,
            actor="api_restore",
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    finally:
        staged_path.unlink(missing_ok=True)
