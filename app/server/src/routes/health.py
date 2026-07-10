from fastapi import APIRouter, Depends, Response, status
from sqlalchemy.orm import Session

from src.config import get_settings
from src.db import get_db
from src.schemas import HealthResponse
from src.schemas import ReadinessProbeResponse
from src.services.database_diagnostics_service import build_database_diagnostics
from src.services.runtime_readiness_service import build_runtime_readiness

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
def health(session: Session = Depends(get_db)) -> HealthResponse:
    settings = get_settings()
    diagnostics = build_database_diagnostics(session)
    return HealthResponse(
        status=str(diagnostics["status"]),
        app_name=settings.app_name,
        app_version=settings.app_version,
        database_url=settings.database_url_effective,
        database_backend=str(diagnostics["database_backend"]),
        database_connected=bool(diagnostics["database_connected"]),
        spatial_backend=settings.spatial_backend,
        postgis_ready=not bool(diagnostics["postgis_expected"]) or bool(
            diagnostics["postgis_extension_installed"]
        ),
        warning_count=int(diagnostics["warning_count"]),
    )


@router.get("/ready", response_model=ReadinessProbeResponse)
def ready(
    response: Response,
    session: Session = Depends(get_db),
) -> ReadinessProbeResponse:
    settings = get_settings()
    readiness = build_runtime_readiness(session)
    if not bool(readiness["ready"]):
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return ReadinessProbeResponse(
        status="ok" if bool(readiness["ready"]) else "not_ready",
        ready=bool(readiness["ready"]),
        app_name=settings.app_name,
        app_version=settings.app_version,
        overall_status=str(readiness["overall_status"]),
        action_required_count=int(readiness["action_required_count"]),
        warning_count=int(readiness["warning_count"]),
        check_count=int(readiness["check_count"]),
        operator_actions=list(readiness["operator_actions"]),
    )
