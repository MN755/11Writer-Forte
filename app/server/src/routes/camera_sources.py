from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from src.db import get_db
from src.schemas import (
    CameraSourceInventoryRead,
    CameraSourceMaterializationRequest,
    CameraSourceMaterializationResponse,
    CameraSourceOpsDetailRead,
    CameraSourceSummaryRead,
)
from src.services.camera_source_service import (
    build_camera_source_inventory_ops_detail,
    build_camera_source_inventory_summary,
    list_camera_sources,
    materialize_camera_source_inventory,
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


@router.get("/{camera_source_inventory_id}/ops", response_model=CameraSourceOpsDetailRead)
def camera_source_ops_detail(
    camera_source_inventory_id: int,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        return build_camera_source_inventory_ops_detail(session, camera_source_inventory_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
