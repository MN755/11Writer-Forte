from __future__ import annotations

import logging
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from src.auth import authenticate_request
from src.config import get_settings
from src.db import init_db
from src.metrics import inc_counter, observe_histogram
from src.observability import bind_request_id, configure_logging, log_event, reset_request_id
from src.routes.alerts import router as alerts_router
from src.routes.camera_sources import router as camera_sources_router
from src.routes.cameras import router as cameras_router
from src.routes.clickhouse import router as clickhouse_router
from src.routes.custody import router as custody_router
from src.routes.entities import router as entities_router
from src.routes.events import layer_router
from src.routes.events import router as events_router
from src.routes.geofences import router as geofences_router
from src.routes.health import router as health_router
from src.routes.imports import router as imports_router
from src.routes.observations import router as observations_router
from src.routes.operations import router as operations_router
from src.routes.scheduler import router as scheduler_router
from src.routes.storage import router as storage_router
from src.routes.sources import router as sources_router
from src.routes.source_trust import router as source_trust_router
from src.services.redaction_service import sanitize_url

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    configure_logging(settings)
    init_db()
    log_event(
        logger,
        logging.INFO,
        "app_started",
        app_env=settings.app_env,
        database_url=sanitize_url(settings.database_url),
        api_auth_enabled=settings.api_auth_enabled,
        metrics_enabled=settings.metrics_enabled,
    )
    yield
    log_event(logger, logging.INFO, "app_stopped")


def create_application() -> FastAPI:
    settings = get_settings()
    settings.ensure_runtime_dirs()
    application = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        lifespan=lifespan,
    )

    @application.middleware("http")
    async def request_runtime_middleware(request, call_next):  # type: ignore[no-untyped-def]
        runtime_settings = get_settings()
        request_id = request.headers.get(runtime_settings.request_id_header) or str(uuid.uuid4())
        request.state.request_id = request_id
        token = bind_request_id(request_id)
        started = time.perf_counter()
        auth_result = authenticate_request(request)
        path_template = request.url.path
        try:
            if not auth_result.allowed:
                duration = time.perf_counter() - started
                inc_counter(
                    "elevenwriter_http_requests_total",
                    method=request.method,
                    path=path_template,
                    status_code=str(auth_result.status_code or 401),
                )
                observe_histogram(
                    "elevenwriter_http_request_duration_seconds",
                    duration,
                    method=request.method,
                    path=path_template,
                    status_code=str(auth_result.status_code or 401),
                )
                log_event(
                    logger,
                    logging.WARNING,
                    "request_auth_failed",
                    method=request.method,
                    path=path_template,
                    status_code=auth_result.status_code,
                    detail=auth_result.detail,
                    request_id=request_id,
                )
                return JSONResponse(
                    status_code=auth_result.status_code or 401,
                    content={
                        "detail": auth_result.detail or "Unauthorized.",
                        "request_id": request_id,
                    },
                    headers={runtime_settings.request_id_header: request_id},
                )

            response = await call_next(request)
            route = request.scope.get("route")
            if route is not None and getattr(route, "path", None):
                path_template = str(route.path)
            duration = time.perf_counter() - started
            status_code = str(response.status_code)
            inc_counter(
                "elevenwriter_http_requests_total",
                method=request.method,
                path=path_template,
                status_code=status_code,
            )
            observe_histogram(
                "elevenwriter_http_request_duration_seconds",
                duration,
                method=request.method,
                path=path_template,
                status_code=status_code,
            )
            response.headers[runtime_settings.request_id_header] = request_id
            log_level = logging.INFO if response.status_code < 500 else logging.WARNING
            log_event(
                logger,
                log_level,
                "request_completed",
                method=request.method,
                path=path_template,
                status_code=response.status_code,
                duration_ms=round(duration * 1000, 3),
                auth_enforced=auth_result.enforced,
                auth_subject=auth_result.subject,
                request_id=request_id,
            )
            return response
        except Exception as exc:
            duration = time.perf_counter() - started
            inc_counter(
                "elevenwriter_http_requests_total",
                method=request.method,
                path=path_template,
                status_code="500",
            )
            observe_histogram(
                "elevenwriter_http_request_duration_seconds",
                duration,
                method=request.method,
                path=path_template,
                status_code="500",
            )
            log_event(
                logger,
                logging.ERROR,
                "request_failed",
                method=request.method,
                path=path_template,
                status_code=500,
                duration_ms=round(duration * 1000, 3),
                auth_enforced=auth_result.enforced,
                auth_subject=auth_result.subject,
                error=str(exc),
                request_id=request_id,
            )
            raise
        finally:
            reset_request_id(token)

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
    application.include_router(imports_router, prefix=settings.api_prefix)
    application.include_router(observations_router, prefix=settings.api_prefix)
    application.include_router(operations_router, prefix=settings.api_prefix)
    application.include_router(scheduler_router, prefix=settings.api_prefix)
    application.include_router(storage_router, prefix=settings.api_prefix)
    application.include_router(sources_router, prefix=settings.api_prefix)
    application.include_router(source_trust_router, prefix=settings.api_prefix)
    return application
