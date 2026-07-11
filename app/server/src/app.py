from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.config import get_settings
from src.db import init_db
from src.routes.alerts import router as alerts_router
from src.routes.camera_sources import router as camera_sources_router
from src.routes.cameras import router as cameras_router
from src.routes.clickhouse import router as clickhouse_router
from src.routes.custody import router as custody_router
from src.routes.discovery import router as discovery_router
from src.routes.entities import router as entities_router
from src.routes.events import layer_router
from src.routes.events import router as events_router
from src.routes.geofences import router as geofences_router
from src.routes.health import router as health_router
from src.routes.imports import router as imports_router
from src.routes.investigations import router as investigations_router
from src.routes.observations import router as observations_router
from src.routes.operations import router as operations_router
from src.routes.scheduler import router as scheduler_router
from src.routes.storage import router as storage_router
from src.routes.sources import router as sources_router
from src.routes.source_trust import router as source_trust_router
from src.routes.watches import router as watches_router


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    init_db()
    yield


def create_application() -> FastAPI:
    settings = get_settings()
    settings.ensure_runtime_dirs()
    application = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        lifespan=lifespan,
    )
    application.include_router(health_router)
    application.include_router(events_router, prefix=settings.api_prefix)
    application.include_router(entities_router, prefix=settings.api_prefix)
    application.include_router(layer_router, prefix=settings.api_prefix)
    application.include_router(geofences_router, prefix=settings.api_prefix)
    application.include_router(alerts_router, prefix=settings.api_prefix)
    application.include_router(camera_sources_router, prefix=settings.api_prefix)
    application.include_router(cameras_router, prefix=settings.api_prefix)
    application.include_router(clickhouse_router, prefix=settings.api_prefix)
    application.include_router(custody_router, prefix=settings.api_prefix)
    application.include_router(discovery_router, prefix=settings.api_prefix)
    application.include_router(imports_router, prefix=settings.api_prefix)
    application.include_router(investigations_router, prefix=settings.api_prefix)
    application.include_router(observations_router, prefix=settings.api_prefix)
    application.include_router(operations_router, prefix=settings.api_prefix)
    application.include_router(scheduler_router, prefix=settings.api_prefix)
    application.include_router(storage_router, prefix=settings.api_prefix)
    application.include_router(sources_router, prefix=settings.api_prefix)
    application.include_router(source_trust_router, prefix=settings.api_prefix)
    application.include_router(watches_router, prefix=settings.api_prefix)
    return application
