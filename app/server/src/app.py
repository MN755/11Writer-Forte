import asyncio
import warnings
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic.warnings import UnsupportedFieldAttributeWarning

from src.config.settings import get_settings
from src.forte.api.connectors import root_router as forte_connector_root_router
from src.forte.api.connectors import router as forte_connector_router
from src.forte.api.discovery import root_router as forte_discovery_root_router
from src.forte.api.discovery import wave_router as forte_discovery_wave_router
from src.forte.api.domain_trust import router as forte_domain_trust_router
from src.forte.api.health import router as forte_health_router
from src.forte.api.policy_actions import source_router as forte_source_policy_action_router
from src.forte.api.policy_actions import wave_router as forte_wave_policy_action_router
from src.forte.api.records import router as forte_record_router
from src.forte.api.runs import connector_router as forte_connector_run_router
from src.forte.api.runs import wave_router as forte_wave_run_router
from src.forte.api.scheduler import router as forte_scheduler_router
from src.forte.api.signals import connector_router as forte_connector_signal_router
from src.forte.api.signals import root_router as forte_signal_root_router
from src.forte.api.signals import wave_router as forte_wave_signal_router
from src.forte.api.wave_trust_overrides import root_router as forte_wave_trust_override_root_router
from src.forte.api.wave_trust_overrides import wave_router as forte_wave_trust_override_wave_router
from src.forte.api.waves import router as forte_wave_router
from src.forte.api.deps import db as forte_db
from src.forte.core.settings import settings as forte_settings
from src.forte.db.init_db import init_db as init_forte_db
from src.intel.api import router as intel_router
from src.intel.db import db as intel_db
from src.intel.db import init_db as init_intel_db
from src.routes.aircraft import router as aircraft_router
from src.routes.analyst import router as analyst_router
from src.routes.anchorage_vaac import router as anchorage_vaac_router
from src.routes.aviation_weather import router as aviation_weather_router
from src.routes.base_earth_context import router as base_earth_context_router
from src.routes.cameras import router as cameras_router
from src.routes.catchments_context import router as catchments_context_router
from src.routes.cisa_cyber_advisories import router as cisa_cyber_advisories_router
from src.routes.cisa_kev import router as cisa_kev_router
from src.routes.cneos import router as cneos_router
from src.routes.data_ai_feeds import router as data_ai_feeds_router
from src.routes.environmental_context import router as environmental_context_router
from src.routes.events import router as events_router
from src.routes.faa_nas_status import router as faa_nas_status_router
from src.routes.fire_weather_context import router as fire_weather_context_router
from src.routes.feeds import router as feeds_router
from src.routes.geomagnetism import router as geomagnetism_router
from src.routes.gpsjam import router as gpsjam_router
from src.routes.health import router as health_router
from src.routes.internet_context import router as internet_context_router
from src.routes.institutional_context import router as institutional_context_router
from src.routes.marine import router as marine_router
from src.routes.ncei_space_weather_portal import router as ncei_space_weather_portal_router
from src.routes.nvd_cve import router as nvd_cve_router
from src.routes.opensky_states import router as opensky_states_router
from src.routes.ourairports_reference import router as ourairports_reference_router
from src.routes.first_epss import router as first_epss_router
from src.routes.reference import router as reference_router
from src.routes.risk_context import router as risk_context_router
from src.routes.satellite import router as satellite_router
from src.routes.seismic_context import router as seismic_context_router
from src.routes.status import router as status_router
from src.routes.source_discovery import router as source_discovery_router
from src.routes.swpc import router as swpc_router
from src.routes.tokyo_vaac import router as tokyo_vaac_router
from src.routes.washington_vaac import router as washington_vaac_router
from src.routes.wave_monitor import router as wave_monitor_router
from src.routes.water_quality_context import router as water_quality_context_router
from src.routes.weather_context import router as weather_context_router
from src.webcam.refresh import WebcamRefreshService, WebcamWorker
from src.services.runtime_scheduler_service import (
    RuntimeSchedulerCoordinator,
    configure_runtime_scheduler_state,
    should_start_source_discovery_scheduler,
    should_start_wave_monitor_scheduler,
)


warnings.filterwarnings("ignore", category=UnsupportedFieldAttributeWarning)


@asynccontextmanager
async def _lifespan(_: FastAPI):
    settings = get_settings()
    init_forte_db(forte_db)
    init_intel_db(intel_db)
    stop_event: asyncio.Event | None = None
    worker_task: asyncio.Task[None] | None = None
    source_discovery_stop_event: asyncio.Event | None = None
    source_discovery_task: asyncio.Task[None] | None = None
    wave_monitor_stop_event: asyncio.Event | None = None
    wave_monitor_task: asyncio.Task[None] | None = None
    coordinator = RuntimeSchedulerCoordinator(settings)
    if settings.webcam_worker_enabled and settings.webcam_worker_run_on_startup:
        stop_event = asyncio.Event()
        worker = WebcamWorker(
            WebcamRefreshService(settings),
            poll_seconds=settings.webcam_worker_poll_seconds,
        )
        worker_task = asyncio.create_task(worker.run_loop(stop_event=stop_event))
    if should_start_source_discovery_scheduler(settings):
        source_discovery_stop_event = asyncio.Event()
        source_discovery_task = asyncio.create_task(
            coordinator.source_discovery_loop(stop_event=source_discovery_stop_event)
        )
    if should_start_wave_monitor_scheduler(settings):
        wave_monitor_stop_event = asyncio.Event()
        wave_monitor_task = asyncio.create_task(
            coordinator.wave_monitor_loop(stop_event=wave_monitor_stop_event)
        )
    try:
        yield
    finally:
        if stop_event is not None:
            stop_event.set()
        if source_discovery_stop_event is not None:
            source_discovery_stop_event.set()
        if wave_monitor_stop_event is not None:
            wave_monitor_stop_event.set()
        if worker_task is not None:
            await worker_task
        if source_discovery_task is not None:
            await source_discovery_task
        if wave_monitor_task is not None:
            await wave_monitor_task


def create_application() -> FastAPI:
    settings = get_settings()
    configure_runtime_scheduler_state(settings)
    application = FastAPI(
        title="11Writer Forte Backend API",
        version="0.1.0",
        lifespan=_lifespan,
    )

    if settings.cors_origins:
        application.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    @application.middleware("http")
    async def _require_api_token(request: Request, call_next):
        if settings.app_api_token and request.url.path != "/health":
            expected = f"Bearer {settings.app_api_token}"
            if request.headers.get("authorization") != expected:
                return JSONResponse(status_code=401, content={"detail": "Missing or invalid API token."})
        return await call_next(request)

    application.include_router(health_router)
    application.include_router(base_earth_context_router)
    application.include_router(catchments_context_router)
    application.include_router(cisa_cyber_advisories_router)
    application.include_router(cisa_kev_router)
    application.include_router(data_ai_feeds_router)
    application.include_router(environmental_context_router)
    application.include_router(events_router)
    application.include_router(fire_weather_context_router)
    application.include_router(feeds_router)
    application.include_router(first_epss_router)
    application.include_router(geomagnetism_router)
    application.include_router(status_router)
    application.include_router(source_discovery_router)
    application.include_router(reference_router)
    application.include_router(risk_context_router)
    application.include_router(seismic_context_router)
    application.include_router(analyst_router)
    application.include_router(aircraft_router)
    application.include_router(gpsjam_router)
    application.include_router(opensky_states_router)
    application.include_router(ourairports_reference_router)
    application.include_router(aviation_weather_router)
    application.include_router(faa_nas_status_router)
    application.include_router(cneos_router)
    application.include_router(swpc_router)
    application.include_router(ncei_space_weather_portal_router)
    application.include_router(anchorage_vaac_router)
    application.include_router(tokyo_vaac_router)
    application.include_router(washington_vaac_router)
    application.include_router(water_quality_context_router)
    application.include_router(weather_context_router)
    application.include_router(internet_context_router)
    application.include_router(institutional_context_router)
    application.include_router(satellite_router)
    application.include_router(marine_router)
    application.include_router(nvd_cve_router)
    application.include_router(cameras_router)
    application.include_router(wave_monitor_router)
    application.include_router(intel_router)
    application.include_router(forte_health_router, prefix=forte_settings.api_prefix)
    application.include_router(forte_wave_router, prefix=forte_settings.api_prefix)
    application.include_router(forte_connector_router, prefix=forte_settings.api_prefix)
    application.include_router(forte_connector_root_router, prefix=forte_settings.api_prefix)
    application.include_router(forte_record_router, prefix=forte_settings.api_prefix)
    application.include_router(forte_wave_run_router, prefix=forte_settings.api_prefix)
    application.include_router(forte_connector_run_router, prefix=forte_settings.api_prefix)
    application.include_router(forte_scheduler_router, prefix=forte_settings.api_prefix)
    application.include_router(forte_wave_signal_router, prefix=forte_settings.api_prefix)
    application.include_router(forte_connector_signal_router, prefix=forte_settings.api_prefix)
    application.include_router(forte_signal_root_router, prefix=forte_settings.api_prefix)
    application.include_router(forte_discovery_wave_router, prefix=forte_settings.api_prefix)
    application.include_router(forte_discovery_root_router, prefix=forte_settings.api_prefix)
    application.include_router(forte_domain_trust_router, prefix=forte_settings.api_prefix)
    application.include_router(forte_wave_trust_override_wave_router, prefix=forte_settings.api_prefix)
    application.include_router(forte_wave_trust_override_root_router, prefix=forte_settings.api_prefix)
    application.include_router(forte_wave_policy_action_router, prefix=forte_settings.api_prefix)
    application.include_router(forte_source_policy_action_router, prefix=forte_settings.api_prefix)

    return application
