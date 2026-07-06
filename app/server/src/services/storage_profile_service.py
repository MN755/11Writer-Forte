from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import inspect, text

from src.config.settings import Settings
from src.reference.db import get_engine, init_db as init_reference_db
from src.reference.models import Base as ReferenceBase
from src.source_discovery.db import init_db as init_source_discovery_db
from src.source_discovery.models import SourceDiscoveryBase
from src.types.api import StorageComponentStatus, StorageStatusResponse
from src.wave_monitor.db import init_db as init_wave_monitor_db
from src.wave_monitor.models import WaveMonitorBase
from src.webcam.models import WebcamBase
from src.marine.models import MarineBase


StorageBackend = str


@dataclass(frozen=True)
class _StorageComponentDefinition:
    component: str
    database_url: str
    uses_primary_database: bool
    expected_tables: set[str]
    initializer: Callable[[str], None]


def build_storage_status(settings: Settings, *, bootstrap: bool = False) -> StorageStatusResponse:
    definitions = _component_definitions(settings)
    if bootstrap:
        _bootstrap_definitions(definitions, settings)

    components: list[StorageComponentStatus] = []
    for definition in definitions:
        backend = _classify_backend(
            definition.database_url,
            postgis_enabled=_uses_postgis(settings, definition.database_url),
        )
        components.append(_inspect_component(definition, backend=backend))

    primary_backend = None
    if settings.primary_database_url:
        primary_backend = _classify_backend(
            settings.primary_database_url,
            postgis_enabled=settings.primary_database_enable_postgis,
        )

    return StorageStatusResponse(
        runtime_mode=settings.app_runtime_mode,
        storage_mode=resolve_runtime_storage_mode(settings),
        primary_database_url=settings.primary_database_url,
        primary_database_backend=primary_backend,
        primary_database_postgis_enabled=settings.primary_database_enable_postgis,
        shared_storage=len({definition.database_url for definition in definitions}) == 1,
        distinct_database_count=len({definition.database_url for definition in definitions}),
        bootstrapped=bootstrap,
        components=components,
        caveats=_build_caveats(settings, components),
    )


def bootstrap_storage(settings: Settings) -> StorageStatusResponse:
    return build_storage_status(settings, bootstrap=True)


def resolve_runtime_storage_mode(settings: Settings) -> str:
    definitions = _component_definitions(settings)
    backends = {
        _classify_backend(
            definition.database_url,
            postgis_enabled=_uses_postgis(settings, definition.database_url),
        )
        for definition in definitions
    }
    if len(backends) == 1:
        backend = next(iter(backends))
        if backend == "sqlite":
            return "persistent-sqlite"
        if backend == "postgresql":
            return "persistent-postgres"
        if backend == "postgresql+postgis":
            return "persistent-postgis"
    if backends == {"sqlite"}:
        return "persistent-sqlite"
    return "hybrid-persistent"


def _component_definitions(settings: Settings) -> list[_StorageComponentDefinition]:
    return [
        _StorageComponentDefinition(
            component="reference",
            database_url=settings.reference_database_url,
            uses_primary_database=_uses_primary_database(settings, settings.reference_database_url),
            expected_tables=set(ReferenceBase.metadata.tables.keys()) | {"reference_spatial_index"},
            initializer=init_reference_db,
        ),
        _StorageComponentDefinition(
            component="webcam",
            database_url=settings.camera_database_url,
            uses_primary_database=_uses_primary_database(settings, settings.camera_database_url),
            expected_tables=set(WebcamBase.metadata.tables.keys()),
            initializer=lambda database_url: WebcamBase.metadata.create_all(get_engine(database_url)),
        ),
        _StorageComponentDefinition(
            component="marine",
            database_url=settings.marine_database_url,
            uses_primary_database=_uses_primary_database(settings, settings.marine_database_url),
            expected_tables=set(MarineBase.metadata.tables.keys()),
            initializer=lambda database_url: MarineBase.metadata.create_all(get_engine(database_url)),
        ),
        _StorageComponentDefinition(
            component="wave_monitor",
            database_url=settings.wave_monitor_database_url,
            uses_primary_database=_uses_primary_database(settings, settings.wave_monitor_database_url),
            expected_tables=set(WaveMonitorBase.metadata.tables.keys()),
            initializer=init_wave_monitor_db,
        ),
        _StorageComponentDefinition(
            component="source_discovery",
            database_url=settings.source_discovery_database_url,
            uses_primary_database=_uses_primary_database(settings, settings.source_discovery_database_url),
            expected_tables=set(SourceDiscoveryBase.metadata.tables.keys()),
            initializer=init_source_discovery_db,
        ),
    ]


def _bootstrap_definitions(definitions: list[_StorageComponentDefinition], settings: Settings) -> None:
    initialized_urls: set[str] = set()
    for definition in definitions:
        if definition.database_url in initialized_urls:
            continue
        if _uses_postgis(settings, definition.database_url):
            _ensure_postgis_extension(definition.database_url)
        initialized_urls.add(definition.database_url)
    for definition in definitions:
        definition.initializer(definition.database_url)


def _inspect_component(definition: _StorageComponentDefinition, *, backend: StorageBackend) -> StorageComponentStatus:
    sqlite_path = _sqlite_database_path(definition.database_url)
    if sqlite_path is not None and not sqlite_path.exists():
        return StorageComponentStatus(
            component=definition.component,
            database_url=definition.database_url,
            backend=backend,
            uses_primary_database=definition.uses_primary_database,
            reachable=True,
            initialized=False,
            table_count=0,
            error=None,
        )
    try:
        table_names = set(inspect(get_engine(definition.database_url)).get_table_names())
        return StorageComponentStatus(
            component=definition.component,
            database_url=definition.database_url,
            backend=backend,
            uses_primary_database=definition.uses_primary_database,
            reachable=True,
            initialized=definition.expected_tables.issubset(table_names),
            table_count=len(table_names),
            error=None,
        )
    except Exception as exc:  # noqa: BLE001
        return StorageComponentStatus(
            component=definition.component,
            database_url=definition.database_url,
            backend=backend,
            uses_primary_database=definition.uses_primary_database,
            reachable=False,
            initialized=False,
            table_count=0,
            error=str(exc)[:300],
        )


def _ensure_postgis_extension(database_url: str) -> None:
    engine = get_engine(database_url)
    with engine.begin() as connection:
        connection.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))


def _build_caveats(settings: Settings, components: list[StorageComponentStatus]) -> list[str]:
    caveats: list[str] = []
    if settings.primary_database_url:
        caveats.append("PRIMARY_DATABASE_URL fans shared storage defaults across backend modules unless a module-specific database URL overrides it.")
    else:
        caveats.append("Module-specific database URLs remain active; set PRIMARY_DATABASE_URL to collapse the backend onto one primary store.")
    if settings.primary_database_enable_postgis:
        caveats.append("PostGIS enablement only applies to PostgreSQL-compatible primary storage and requires the extension to be installed on the target database.")
    if any(not component.reachable for component in components):
        caveats.append("At least one configured database is unreachable or missing its driver; bootstrap and runtime calls will fail until that is fixed.")
    return caveats


def _uses_primary_database(settings: Settings, database_url: str) -> bool:
    return bool(settings.primary_database_url and settings.primary_database_url == database_url)


def _uses_postgis(settings: Settings, database_url: str) -> bool:
    return bool(
        settings.primary_database_enable_postgis
        and settings.primary_database_url
        and settings.primary_database_url == database_url
        and _is_postgresql_url(database_url)
    )


def _classify_backend(database_url: str, *, postgis_enabled: bool) -> StorageBackend:
    if database_url.startswith("sqlite"):
        return "sqlite"
    if _is_postgresql_url(database_url):
        return "postgresql+postgis" if postgis_enabled else "postgresql"
    return "unknown"


def _is_postgresql_url(database_url: str) -> bool:
    return database_url.startswith("postgresql") or database_url.startswith("postgres")


def _sqlite_database_path(database_url: str) -> Path | None:
    if not database_url.startswith("sqlite:///"):
        return None
    raw_path = database_url.removeprefix("sqlite:///")
    if raw_path == ":memory:":
        return None
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return Path.cwd() / path
