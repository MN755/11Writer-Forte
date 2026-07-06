from fastapi import APIRouter, Depends, Response, status

from src.config.settings import Settings, get_settings
from src.services.runtime_health_service import build_runtime_readiness_report
from src.types.api import HealthResponse, RuntimeReadinessResponse

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def healthcheck() -> HealthResponse:
    return HealthResponse(status="ok")


@router.get("/health/live", response_model=HealthResponse)
async def livecheck() -> HealthResponse:
    return HealthResponse(status="ok")


@router.get("/health/ready", response_model=RuntimeReadinessResponse)
async def readycheck(
    response: Response,
    settings: Settings = Depends(get_settings),
) -> RuntimeReadinessResponse:
    report = build_runtime_readiness_report(settings)
    if not report.ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return report
