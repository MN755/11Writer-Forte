from fastapi import APIRouter

from src.config import get_settings
from src.schemas import HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    settings = get_settings()
    return HealthResponse(
        status="ok",
        app_name=settings.app_name,
        app_version=settings.app_version,
        database_url=settings.database_url,
        spatial_backend=settings.spatial_backend,
    )
