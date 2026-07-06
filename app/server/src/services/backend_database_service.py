from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import inspect, text
from sqlalchemy.engine import make_url
from sqlmodel import Session

from src.config.settings import Settings
from src.forte.api.deps import db as forte_db
from src.forte.db.init_db import init_db as init_forte_db
from src.intel.db import db as intel_db
from src.intel.db import get_database_status as get_intel_database_status
from src.intel.db import init_db as init_intel_db
from src.reference.db import get_engine as get_reference_engine
from src.source_discovery.db import init_db as init_source_discovery_db
from src.types.backend_database import BackendDatabaseComponentStatus, BackendDatabaseStatusResponse
from src.wave_monitor.db import init_db as init_wave_monitor_db


@dataclass(frozen=True)
class _DatabaseComponent:
    component_key: str
    database_url: str
    bootstrap_strategy: str
    required_tables: tuple[str, ...]
    shared_group: str
    shared_with: tuple[str, ...] = ()
    include_intel_spatial_status: bool = False


def bootstrap_backend_databases(
    settings: Settings,
    *,
    component_keys: set[str] | None = None,
) -> BackendDatabaseStatusResponse:
    selected = _selected_components(settings, component_keys)
    bootstrapped_components: list[str] = []

    if any(component.component_key == "primary" for component in selected):
        _bootstrap_primary(settings)
        bootstrapped_components.append("primary")
    if any(component.component_key == "source_discovery" for component in selected):
        init_source_discovery_db(settings.source_discovery_database_url)
        bootstrapped_components.append("source_discovery")
    if any(component.component_key == "wave_monitor" for component in selected):
        init_wave_monitor_db(settings.wave_monitor_database_url)
        bootstrapped_components.append("wave_monitor")

    alembic_urls = {
        component.database_url
        for component in selected
        if component.bootstrap_strategy == "alembic-head"
    }
    for database_url in sorted(alembic_urls):
        _upgrade_alembic_database(database_url)
    if alembic_urls:
        for component in selected:
            if component.bootstrap_strategy == "alembic-head":
                bootstrapped_components.append(component.component_key)

    return backend_database_status(settings, bootstrapped_components=bootstrapped_components, component_keys=component_keys)


def backend_database_status(
    settings: Settings,
    *,
    bootstrapped_components: list[str] | None = None,
    component_keys: set[str] | None = None,
) -> BackendDatabaseStatusResponse:
    components = _selected_components(settings, component_keys)
    statuses = [_inspect_component_status(component, settings) for component in components]
    distinct_database_count = len({item.database_identity for item in statuses})
    caveats = [
        "The backend currently mixes SQLModel bootstrap for intel/forte with explicit schema bootstrap for source discovery and wave monitor plus Alembic for reference/webcam/marine.",
        "Shared database identities indicate multiple subsystems are intentionally co-located on the same physical database URL.",
    ]
    return BackendDatabaseStatusResponse(
        generated_at=_utc_now_iso(),
        component_count=len(statuses),
        reachable_component_count=sum(1 for item in statuses if item.reachable),
        distinct_database_count=distinct_database_count,
        bootstrapped_components=bootstrapped_components or [],
        components=statuses,
        caveats=caveats,
    )


def _bootstrap_primary(settings: Settings) -> None:
    if getattr(intel_db, "_url", None) != settings.database_url:
        intel_db.reconfigure(settings.database_url)
    if getattr(forte_db, "_url", None) != settings.database_url:
        forte_db.reconfigure(settings.database_url)
    init_intel_db(intel_db)
    init_forte_db(forte_db)


def _upgrade_alembic_database(database_url: str) -> None:
    root_dir = Path(__file__).resolve().parents[2]
    config = Config(str(root_dir / "alembic.ini"))
    config.set_main_option("script_location", str(root_dir / "alembic"))
    config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(config, "head")


def _inspect_component_status(component: _DatabaseComponent, settings: Settings) -> BackendDatabaseComponentStatus:
    engine = get_reference_engine(component.database_url)
    inspector = inspect(engine)
    table_names: list[str] = []
    dialect = engine.dialect.name
    reachable = False
    migration_version: str | None = None
    caveats: list[str] = []
    spatial_backend: str | None = None
    postgis_available: bool | None = None

    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
            reachable = True
            table_names = sorted(inspector.get_table_names())
            if "alembic_version" in table_names:
                migration_version = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one_or_none()
            if component.include_intel_spatial_status:
                with Session(engine) as session:
                    intel_status = get_intel_database_status(session)
                spatial_backend = intel_status.spatial_backend
                postgis_available = intel_status.postgis_available
                caveats.extend(intel_status.caveats)
    except Exception as exc:  # noqa: BLE001
        caveats.append(f"Database connection failed: {exc.__class__.__name__}: {exc}")

    missing_tables = sorted(set(component.required_tables) - set(table_names))
    if missing_tables:
        caveats.append(f"Missing required tables: {', '.join(missing_tables)}.")

    return BackendDatabaseComponentStatus(
        component_key=component.component_key,
        database_url=_display_database_url(component.database_url),
        database_identity=_database_identity(component.database_url),
        shared_with=list(component.shared_with),
        dialect=dialect,
        reachable=reachable,
        bootstrap_strategy=component.bootstrap_strategy,
        table_count=len(table_names),
        required_tables=list(component.required_tables),
        missing_tables=missing_tables,
        migration_version=migration_version,
        spatial_backend=spatial_backend,
        postgis_available=postgis_available,
        caveats=caveats,
    )


def _selected_components(settings: Settings, component_keys: set[str] | None) -> list[_DatabaseComponent]:
    components = _component_specs(settings)
    if component_keys is None:
        return components
    return [component for component in components if component.component_key in component_keys]


def _component_specs(settings: Settings) -> list[_DatabaseComponent]:
    return [
        _DatabaseComponent(
            component_key="primary",
            database_url=settings.database_url,
            bootstrap_strategy="sqlmodel-create-all-plus-postgis",
            required_tables=("intel_sources", "intel_events", "wave"),
            shared_group="primary",
            shared_with=("intel", "forte"),
            include_intel_spatial_status=True,
        ),
        _DatabaseComponent(
            component_key="source_discovery",
            database_url=settings.source_discovery_database_url,
            bootstrap_strategy="metadata-create-all-plus-sqlite-backfill",
            required_tables=("source_memories", "runtime_scheduler_workers"),
            shared_group="source_discovery",
        ),
        _DatabaseComponent(
            component_key="wave_monitor",
            database_url=settings.wave_monitor_database_url,
            bootstrap_strategy="metadata-create-all",
            required_tables=("wave_monitors", "wave_connectors"),
            shared_group="wave_monitor",
        ),
        _DatabaseComponent(
            component_key="reference",
            database_url=settings.reference_database_url,
            bootstrap_strategy="alembic-head",
            required_tables=("reference_objects", "reference_spatial_index"),
            shared_group="reference_family",
            shared_with=_shared_reference_family(settings, "reference"),
        ),
        _DatabaseComponent(
            component_key="webcam",
            database_url=settings.camera_database_url,
            bootstrap_strategy="alembic-head",
            required_tables=("camera_sources", "camera_source_inventory"),
            shared_group="reference_family",
            shared_with=_shared_reference_family(settings, "webcam"),
        ),
        _DatabaseComponent(
            component_key="marine",
            database_url=settings.marine_database_url,
            bootstrap_strategy="alembic-head",
            required_tables=("marine_sources", "marine_vessel_latest"),
            shared_group="reference_family",
            shared_with=_shared_reference_family(settings, "marine"),
        ),
    ]


def _shared_reference_family(settings: Settings, current_key: str) -> tuple[str, ...]:
    mapping = {
        "reference": settings.reference_database_url,
        "webcam": settings.camera_database_url,
        "marine": settings.marine_database_url,
    }
    current_url = mapping[current_key]
    return tuple(key for key, url in mapping.items() if key != current_key and url == current_url)


def _database_identity(database_url: str) -> str:
    if database_url.startswith("sqlite:///"):
        raw_path = database_url.removeprefix("sqlite:///")
        return str(Path(raw_path).resolve())
    parsed = make_url(database_url)
    return parsed.render_as_string(hide_password=True)


def _display_database_url(database_url: str) -> str:
    return make_url(database_url).render_as_string(hide_password=True)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
