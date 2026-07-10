from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from src.db import get_db
from src.routes.error_helpers import translate_service_error
from src.schemas import (
    CameraSourceSummaryArtifactExportRequest,
    CameraSourceOpsExportSummaryRead,
    CameraSourceInventoryRead,
    CameraSourceMaterializationRequest,
    CameraSourceMaterializationResponse,
    CameraSourceVerificationRequest,
    CameraSourceVerificationResponse,
    CameraSourceOpsDetailRead,
    CameraSourceOpsReportIndexRead,
    CameraSourceSummaryRead,
    ExportArtifactWriteResultRead,
)
from src.services.export_artifact_service import persist_typed_json_export_artifact
from src.services.camera_source_service import (
    build_camera_source_ops_export_summary,
    build_camera_source_inventory_ops_detail,
    build_camera_source_ops_report_index,
    build_camera_source_inventory_summary,
    list_camera_sources,
    materialize_camera_source_inventory,
    verify_camera_source_inventory,
)

router = APIRouter(prefix="/camera-sources", tags=["camera-sources"])


@router.get("", response_model=list[CameraSourceInventoryRead])
def list_camera_source_inventory(
    layer_key: str | None = None,
    source_domain: str | None = None,
    endpoint_kind: str | None = None,
    status: str | None = None,
    verification_state: str | None = None,
    active: bool | None = None,
    limit: int = 200,
    session: Session = Depends(get_db),
) -> list[object]:
    return list_camera_sources(
        session,
        layer_key=layer_key,
        source_domain=source_domain,
        endpoint_kind=endpoint_kind,
        status=status,
        verification_state=verification_state,
        active=active,
        limit=limit,
    )


@router.post("/materialize", response_model=CameraSourceMaterializationResponse)
def materialize_camera_sources(
    payload: CameraSourceMaterializationRequest,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    return materialize_camera_source_inventory(
        session,
        layer_key=payload.layer_key,
        source_domain=payload.source_domain,
        active=payload.active,
        limit=payload.limit,
        actor="api_camera_source_registry",
    )


@router.post("/verify", response_model=CameraSourceVerificationResponse)
def verify_camera_sources(
    payload: CameraSourceVerificationRequest,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        return verify_camera_source_inventory(
            session,
            camera_source_inventory_id=payload.camera_source_inventory_id,
            layer_key=payload.layer_key,
            source_domain=payload.source_domain,
            endpoint_kind=payload.endpoint_kind,
            status=payload.status,
            verification_state=payload.verification_state,
            active=payload.active,
            limit=payload.limit,
            timeout_seconds=payload.timeout_seconds,
            max_payload_bytes=payload.max_payload_bytes,
            allow_private_networks=payload.allow_private_networks,
            min_request_interval_seconds=payload.min_request_interval_seconds,
            actor="api_camera_source_verifier",
        )
    except ValueError as exc:
        raise translate_service_error(
            exc,
            action="verify_camera_sources",
            context={
                "camera_source_inventory_id": payload.camera_source_inventory_id,
                "layer_key": payload.layer_key,
            },
        ) from exc
    except RuntimeError as exc:
        raise translate_service_error(
            exc,
            action="verify_camera_sources",
            context={
                "camera_source_inventory_id": payload.camera_source_inventory_id,
                "layer_key": payload.layer_key,
            },
        ) from exc


@router.get("/summary", response_model=CameraSourceSummaryRead)
def summarize_camera_sources(
    layer_key: str | None = None,
    source_domain: str | None = None,
    endpoint_kind: str | None = None,
    status: str | None = None,
    verification_state: str | None = None,
    active: bool | None = None,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    return build_camera_source_inventory_summary(
        session,
        layer_key=layer_key,
        source_domain=source_domain,
        endpoint_kind=endpoint_kind,
        status=status,
        verification_state=verification_state,
        active=active,
    )


@router.get("/report-index", response_model=CameraSourceOpsReportIndexRead)
def camera_source_report_index(
    layer_key: str | None = None,
    source_domain: str | None = None,
    endpoint_kind: str | None = None,
    status: str | None = None,
    verification_state: str | None = None,
    active: bool | None = None,
    stale_after_hours: float = 24.0,
    limit: int = 25,
    stale_source_limit: int = 25,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    return build_camera_source_ops_report_index(
        session,
        layer_key=layer_key,
        source_domain=source_domain,
        endpoint_kind=endpoint_kind,
        status=status,
        verification_state=verification_state,
        active=active,
        stale_after_hours=stale_after_hours,
        limit=limit,
        stale_source_limit=stale_source_limit,
    )


@router.get("/export/summary", response_model=CameraSourceOpsExportSummaryRead)
def camera_source_export_summary(
    layer_key: str | None = None,
    source_domain: str | None = None,
    endpoint_kind: str | None = None,
    status: str | None = None,
    verification_state: str | None = None,
    active: bool | None = None,
    stale_after_hours: float = 24.0,
    source_limit: int = 500,
    report_limit: int = 25,
    stale_source_limit: int = 25,
    session: Session = Depends(get_db),
    ) -> dict[str, object]:
    return build_camera_source_ops_export_summary(
        session,
        layer_key=layer_key,
        source_domain=source_domain,
        endpoint_kind=endpoint_kind,
        status=status,
        verification_state=verification_state,
        active=active,
        stale_after_hours=stale_after_hours,
        source_limit=source_limit,
        report_limit=report_limit,
        stale_source_limit=stale_source_limit,
    )


@router.post("/export/summary/artifact", response_model=ExportArtifactWriteResultRead)
def camera_source_export_summary_artifact(
    payload: CameraSourceSummaryArtifactExportRequest,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        report = build_camera_source_ops_export_summary(
            session,
            layer_key=payload.layer_key,
            source_domain=payload.source_domain,
            endpoint_kind=payload.endpoint_kind,
            status=payload.status,
            verification_state=payload.verification_state,
            active=payload.active,
            stale_after_hours=payload.stale_after_hours,
            source_limit=payload.source_limit,
            report_limit=payload.report_limit,
            stale_source_limit=payload.stale_source_limit,
        )
        result = persist_typed_json_export_artifact(
            session,
            output_path=Path(payload.output_path),
            payload=report,
            response_model=CameraSourceOpsExportSummaryRead,
            object_kind="camera_source_summary_export",
            owner_type="camera_source_export",
            owner_id=payload.layer_key or payload.source_domain or "scoped",
            source_uri="/api/camera-sources/export/summary",
            observed_at=report["generated_at"],
            metadata_json=report["filters_json"],
            actor="api_export",
        )
    except ValueError as exc:
        raise translate_service_error(
            exc,
            action="camera_source_export_summary_artifact",
            context={"output_path": payload.output_path},
        ) from exc
    except OSError as exc:
        raise translate_service_error(
            exc,
            action="camera_source_export_summary_artifact",
            context={"output_path": payload.output_path},
        ) from exc
    return {
        "output_path": result["output_path"],
        "storage_object": result["storage_object"],
    }


@router.get("/{camera_source_inventory_id}/ops", response_model=CameraSourceOpsDetailRead)
def camera_source_ops_detail(
    camera_source_inventory_id: int,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        return build_camera_source_inventory_ops_detail(session, camera_source_inventory_id)
    except ValueError as exc:
        raise translate_service_error(
            exc,
            action="camera_source_ops_detail",
            context={"camera_source_inventory_id": camera_source_inventory_id},
        ) from exc
