from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from src.db import get_db
from src.schemas import CameraInventoryRead, CameraMaterializationRequest, CameraMaterializationResponse
from src.services.camera_service import list_cameras, materialize_camera_inventory

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
    return materialize_camera_inventory(
        session,
        layer_key=payload.layer_key,
        source_domain=payload.source_domain,
        limit=payload.limit,
        actor="api_camera_registry",
    )
