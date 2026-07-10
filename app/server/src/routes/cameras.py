from pathlib import Path

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from src.db import get_db
from src.routes.error_helpers import translate_service_error
from src.schemas import (
    CameraSummaryArtifactExportRequest,
    CameraOpsExportSummaryRead,
    CameraOpsReportIndexRead,
    CameraInventoryOpsDetailRead,
    CameraInventoryRead,
    CameraInventorySummaryRead,
    CameraMaterializationRequest,
    CameraMaterializationResponse,
    ExportArtifactWriteResultRead,
)
from src.services.camera_source_service import materialize_camera_source_inventory
from src.services.camera_service import (
    build_camera_ops_export_summary,
    build_camera_ops_report_index,
    build_camera_inventory_ops_detail,
    build_camera_inventory_summary,
    list_cameras,
    materialize_camera_inventory,
)
from src.services.export_artifact_service import persist_typed_json_export_artifact

router = APIRouter(prefix="/cameras", tags=["cameras"])


@router.get("", response_model=list[CameraInventoryRead])
def list_camera_inventory(
    layer_key: str | None = None,
    source_domain: str | None = None,
    status: str | None = None,
    active: bool | None = None,
    min_lon: float | None = None,
    min_lat: float | None = None,
    max_lon: float | None = None,
    max_lat: float | None = None,
    limit: int = 200,
    session: Session = Depends(get_db),
) -> list[object]:
    return list_cameras(
        session,
        layer_key=layer_key,
        source_domain=source_domain,
        status=status,
        active=active,
        min_lon=min_lon,
        min_lat=min_lat,
        max_lon=max_lon,
        max_lat=max_lat,
        limit=limit,
    )


@router.post("/materialize", response_model=CameraMaterializationResponse)
def materialize_cameras(
    payload: CameraMaterializationRequest,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    result = materialize_camera_inventory(
        session,
        layer_key=payload.layer_key,
        source_domain=payload.source_domain,
        limit=payload.limit,
        actor="api_camera_registry",
    )
    source_result = materialize_camera_source_inventory(
        session,
        layer_key=payload.layer_key,
        source_domain=payload.source_domain,
        limit=payload.limit,
        actor="api_camera_source_registry",
    )
    result["source_created_count"] = int(source_result["created_count"])
    result["source_updated_count"] = int(source_result["updated_count"])
    result["source_scanned_endpoint_count"] = int(source_result["scanned_endpoint_count"])
    return result


@router.get("/summary", response_model=CameraInventorySummaryRead)
def summarize_cameras(
    layer_key: str | None = None,
    source_domain: str | None = None,
    status: str | None = None,
    active: bool | None = None,
    min_lon: float | None = None,
    min_lat: float | None = None,
    max_lon: float | None = None,
    max_lat: float | None = None,
    stale_after_hours: float = 24.0,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    return build_camera_inventory_summary(
        session,
        layer_key=layer_key,
        source_domain=source_domain,
        status=status,
        active=active,
        min_lon=min_lon,
        min_lat=min_lat,
        max_lon=max_lon,
        max_lat=max_lat,
        stale_after_hours=stale_after_hours,
    )


@router.get("/report-index", response_model=CameraOpsReportIndexRead)
def camera_report_index(
    layer_key: str | None = None,
    source_domain: str | None = None,
    status: str | None = None,
    active: bool | None = None,
    min_lon: float | None = None,
    min_lat: float | None = None,
    max_lon: float | None = None,
    max_lat: float | None = None,
    stale_after_hours: float = 24.0,
    limit: int = 25,
    stale_camera_limit: int = 25,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    return build_camera_ops_report_index(
        session,
        layer_key=layer_key,
        source_domain=source_domain,
        status=status,
        active=active,
        min_lon=min_lon,
        min_lat=min_lat,
        max_lon=max_lon,
        max_lat=max_lat,
        stale_after_hours=stale_after_hours,
        limit=limit,
        stale_camera_limit=stale_camera_limit,
    )


@router.get("/export/summary", response_model=CameraOpsExportSummaryRead)
def export_camera_summary(
    layer_key: str | None = None,
    source_domain: str | None = None,
    status: str | None = None,
    active: bool | None = None,
    min_lon: float | None = None,
    min_lat: float | None = None,
    max_lon: float | None = None,
    max_lat: float | None = None,
    stale_after_hours: float = 24.0,
    camera_limit: int = 500,
    report_limit: int = 25,
    stale_camera_limit: int = 25,
    session: Session = Depends(get_db),
    ) -> dict[str, object]:
    return build_camera_ops_export_summary(
        session,
        layer_key=layer_key,
        source_domain=source_domain,
        status=status,
        active=active,
        min_lon=min_lon,
        min_lat=min_lat,
        max_lon=max_lon,
        max_lat=max_lat,
        stale_after_hours=stale_after_hours,
        camera_limit=camera_limit,
        report_limit=report_limit,
        stale_camera_limit=stale_camera_limit,
    )


@router.post("/export/summary/artifact", response_model=ExportArtifactWriteResultRead)
def export_camera_summary_artifact(
    payload: CameraSummaryArtifactExportRequest,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        report = build_camera_ops_export_summary(
            session,
            layer_key=payload.layer_key,
            source_domain=payload.source_domain,
            status=payload.status,
            active=payload.active,
            min_lon=payload.min_lon,
            min_lat=payload.min_lat,
            max_lon=payload.max_lon,
            max_lat=payload.max_lat,
            stale_after_hours=payload.stale_after_hours,
            camera_limit=payload.camera_limit,
            report_limit=payload.report_limit,
            stale_camera_limit=payload.stale_camera_limit,
        )
        result = persist_typed_json_export_artifact(
            session,
            output_path=Path(payload.output_path),
            payload=report,
            response_model=CameraOpsExportSummaryRead,
            object_kind="camera_summary_export",
            owner_type="camera_export",
            owner_id=payload.layer_key or payload.source_domain or "scoped",
            source_uri="/api/cameras/export/summary",
            observed_at=report["generated_at"],
            metadata_json=report["filters_json"],
            actor="api_export",
        )
    except ValueError as exc:
        raise translate_service_error(
            exc,
            action="camera_export_summary_artifact",
            context={"output_path": payload.output_path},
        ) from exc
    except OSError as exc:
        raise translate_service_error(
            exc,
            action="camera_export_summary_artifact",
            context={"output_path": payload.output_path},
        ) from exc
    return {
        "output_path": result["output_path"],
        "storage_object": result["storage_object"],
    }


@router.get("/{camera_inventory_id}/ops", response_model=CameraInventoryOpsDetailRead)
def camera_ops_detail(camera_inventory_id: int, session: Session = Depends(get_db)) -> dict[str, object]:
    try:
        return build_camera_inventory_ops_detail(session, camera_inventory_id)
    except ValueError as exc:
        raise translate_service_error(
            exc,
            action="camera_ops_detail",
            context={"camera_inventory_id": camera_inventory_id},
        ) from exc
