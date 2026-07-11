from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from src.config import get_settings
from src.db import get_db
from src.schemas import HealthResponse
from src.services.database_diagnostics_service import build_database_diagnostics

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
def health(session: Session = Depends(get_db)) -> HealthResponse:
    settings = get_settings()
    diagnostics = build_database_diagnostics(session)
    return HealthResponse(
        status=str(diagnostics["status"]),
        app_name=settings.app_name,
        app_version=settings.app_version,
        database_backend=str(diagnostics["database_backend"]),
        database_connected=bool(diagnostics["database_connected"]),
        spatial_backend=settings.spatial_backend,
        postgis_ready=not bool(diagnostics["postgis_expected"])
        or bool(diagnostics["postgis_extension_installed"]),
        warning_count=int(diagnostics["warning_count"]),
    )
