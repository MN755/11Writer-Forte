from fastapi import APIRouter, Depends

from src.config.settings import Settings, get_settings
from src.services.storage_profile_service import build_storage_status
from src.services.status_service import build_source_status
from src.types.api import SourceStatusResponse, StorageStatusResponse

router = APIRouter(prefix="/api/status", tags=["status"])


@router.get("/sources", response_model=SourceStatusResponse)
async def source_status(
    settings: Settings = Depends(get_settings),
) -> SourceStatusResponse:
    return build_source_status(settings)


@router.get("/storage", response_model=StorageStatusResponse)
async def storage_status(
    settings: Settings = Depends(get_settings),
) -> StorageStatusResponse:
    return build_storage_status(settings)
