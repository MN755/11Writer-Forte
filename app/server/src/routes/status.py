from fastapi import APIRouter

from src.config.settings import get_settings
from src.services.backend_database_service import backend_database_status
from src.services.status_service import build_source_status
from src.types.backend_database import BackendDatabaseStatusResponse
from src.types.api import SourceStatusResponse

router = APIRouter(prefix="/api/status", tags=["status"])


@router.get("/sources", response_model=SourceStatusResponse)
async def source_status() -> SourceStatusResponse:
    return build_source_status(get_settings())


@router.get("/databases", response_model=BackendDatabaseStatusResponse)
async def database_status() -> BackendDatabaseStatusResponse:
    return backend_database_status(get_settings())
