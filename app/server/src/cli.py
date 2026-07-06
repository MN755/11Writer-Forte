from __future__ import annotations

import argparse
import asyncio
import importlib
import json
from typing import Any

import uvicorn

from src.app import create_application
from src.config.settings import get_settings
from src.forte.api.deps import db as forte_db
from src.forte.db.init_db import init_db as init_forte_db
from src.forte.scheduler.worker import run_loop as run_forte_scheduler_loop
from src.intel.db import db as intel_db
from src.intel.db import init_db as init_intel_db
from src.intel.event_sync import EVENT_FEED_KEYS, EventFeedSyncService
from src.intel.models import EventFeedSyncRequest, IngestFileRequest
from src.intel.service import IntelService
from src.runtime_worker import _run as run_runtime_workers
from src.services.backend_database_service import backend_database_status, bootstrap_backend_databases


BANNER = r"""
   __ __ _       _       _ _              ______         __       
  /_ // /| |     | |     (_) |            / ____/___  ____/ /____  
   / // /_| | /| / /_  __ _| |_ ___  _____/ /_  / __ \/ __  / _ \ 
  /__  __/ |/ |/ /| |/_/ | | __/ _ \/ ___/ __/ / /_/ / /_/ /  __/ 
    /_/  |__/|__/  |___/|_|\__/\___/_/  /_/    \____/\__,_/\___/  
"""


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="11writer",
        description="Backend-only operator CLI for 11Writer Forte.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve = subparsers.add_parser("serve", help="Run the FastAPI backend.")
    serve.add_argument("--host", default=None, help="Bind host. Defaults to APP_BIND_HOST.")
    serve.add_argument("--port", type=int, default=None, help="Bind port. Defaults to APP_BIND_PORT.")
    serve.add_argument("--reload", action="store_true", help="Enable autoreload for local development.")
    serve.add_argument("--log-level", default="info", help="Uvicorn log level.")

    worker = subparsers.add_parser("worker", help="Run main runtime workers.")
    worker.add_argument("--worker", choices=["source_discovery", "wave_monitor", "intel_event_sync", "all"], default="all")
    worker.add_argument("--once", action="store_true", help="Run one bounded cycle and exit.")
    worker.add_argument("--loop", action="store_true", help="Run continuously until stopped.")

    forte_worker = subparsers.add_parser("forte-worker", help="Run folded source-ops scheduler loop.")
    forte_worker.add_argument("--interval-seconds", type=int, default=30)

    config = subparsers.add_parser("config", help="Print effective runtime configuration.")
    config.add_argument("--json", action="store_true", help="Emit JSON instead of plain text.")

    routes = subparsers.add_parser("routes", help="List mounted API routes.")
    routes.add_argument("--json", action="store_true", help="Emit JSON instead of plain text.")

    intel_overview = subparsers.add_parser("intel-overview", help="Print unified backend storage counts.")
    intel_overview.add_argument("--json", action="store_true", help="Emit JSON instead of plain text.")

    intel_db_status = subparsers.add_parser("intel-db", help="Print intel database and spatial backend status.")
    intel_db_status.add_argument("--json", action="store_true", help="Emit JSON instead of plain text.")

    db_status = subparsers.add_parser("db-status", help="Print backend database status across subsystems.")
    db_status.add_argument("--json", action="store_true", help="Emit JSON instead of plain text.")

    db_bootstrap = subparsers.add_parser("db-bootstrap", help="Bootstrap backend database schemas across subsystems.")
    db_bootstrap.add_argument("--json", action="store_true", help="Emit JSON instead of plain text.")

    ingest = subparsers.add_parser("ingest-file", help="Ingest a JSON, text, or SQLite file into the intel core.")
    ingest.add_argument("path", help="Path to the input file.")
    ingest.add_argument("--source-id", default=None, help="Existing or preferred source id.")
    ingest.add_argument("--source-name", default=None, help="Source display name if one needs to be created.")
    ingest.add_argument("--source-kind", default="historical_source", help="Source kind for newly created sources.")
    ingest.add_argument("--event-id", default=None, help="Attach ingested observations to an event.")
    ingest.add_argument("--entity-id", default=None, help="Attach ingested observations to an entity.")
    ingest.add_argument("--format", default="auto", help="Format hint: auto, json, jsonl, text, sqlite.")
    ingest.add_argument("--actor", default="cli", help="Actor name for chain-of-custody records.")

    evaluate_alerts = subparsers.add_parser("evaluate-alerts", help="Evaluate geofence rules and create alert records.")
    evaluate_alerts.add_argument("--actor", default="cli", help="Actor name for chain-of-custody records.")
    evaluate_alerts.add_argument("--json", action="store_true", help="Emit JSON instead of plain text.")

    sync_event_feeds = subparsers.add_parser("sync-event-feeds", help="Sync event feed surfaces into the intel core.")
    sync_event_feeds.add_argument(
        "--feeds",
        default="all",
        help=f"Comma-separated feed keys or 'all'. Supported: {', '.join(EVENT_FEED_KEYS)}",
    )
    sync_event_feeds.add_argument("--max-records-per-feed", type=int, default=100)
    sync_event_feeds.add_argument("--actor", default="cli", help="Actor name for chain-of-custody records.")
    sync_event_feeds.add_argument("--skip-geofence-alerts", action="store_true", help="Skip post-sync geofence alert evaluation.")
    sync_event_feeds.add_argument("--loop", action="store_true", help="Run continuously until stopped.")
    sync_event_feeds.add_argument("--interval-seconds", type=int, default=300, help="Loop interval in seconds.")
    sync_event_feeds.add_argument("--json", action="store_true", help="Emit JSON instead of plain text.")

    spatial_nearby = subparsers.add_parser("spatial-nearby", help="Query nearby intel records around a point.")
    spatial_nearby.add_argument("--latitude", type=float, required=True)
    spatial_nearby.add_argument("--longitude", type=float, required=True)
    spatial_nearby.add_argument("--radius-m", type=float, default=50000.0)
    spatial_nearby.add_argument("--limit", type=int, default=25)
    spatial_nearby.add_argument("--json", action="store_true", help="Emit JSON instead of plain text.")

    subparsers.add_parser("doctor", help="Run basic environment checks.")
    return parser


def _print_banner() -> None:
    print(BANNER)
    print("headless geospatial osint runtime")
    print()


def _redact(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str) and any(token in value.lower() for token in ("token", "key", "secret")):
        return "***redacted***"
    return value


def _config_payload() -> dict[str, Any]:
    settings = get_settings()
    return {
        "app_env": settings.app_env,
        "app_runtime_mode": settings.app_runtime_mode,
        "app_bind_host": settings.app_bind_host,
        "app_bind_port": settings.app_bind_port,
        "cors_origins": settings.cors_origins,
        "database_url": settings.database_url,
        "reference_database_url": settings.reference_database_url,
        "source_discovery_database_url": settings.source_discovery_database_url,
        "wave_monitor_database_url": settings.wave_monitor_database_url,
        "intel_event_sync_scheduler_enabled": settings.intel_event_sync_scheduler_enabled,
        "intel_event_sync_scheduler_poll_seconds": settings.intel_event_sync_scheduler_poll_seconds,
        "api_token_configured": bool(settings.app_api_token),
    }


def _print_config(as_json: bool) -> None:
    payload = _config_payload()
    if as_json:
        print(json.dumps(payload, indent=2))
        return
    for key, value in payload.items():
        print(f"{key}: {_redact(value)}")


def _print_routes(as_json: bool) -> None:
    app = create_application()
    payload = [
        {
            "path": route.path,
            "name": route.name,
            "methods": sorted(method for method in route.methods or [] if method not in {"HEAD", "OPTIONS"}),
        }
        for route in app.routes
        if getattr(route, "path", None)
    ]
    if as_json:
        print(json.dumps(payload, indent=2))
        return
    for item in payload:
        methods = ",".join(item["methods"])
        print(f"{methods:12} {item['path']} [{item['name']}]")


def _run_doctor() -> None:
    settings = get_settings()
    checks = {
        "sqlmodel": _module_version("sqlmodel"),
        "feedparser": _module_version("feedparser"),
        "psycopg": _module_version("psycopg"),
    }
    intel_status = "ok"
    intel_counts: dict[str, int] | None = None
    intel_database_status: dict[str, Any] | None = None
    backend_db_report: dict[str, Any] | None = None
    try:
        backend_db_report = bootstrap_backend_databases(settings).model_dump(mode="json")
        overview, runtime_db_status = _with_intel_service(lambda service: (service.overview(), service.database_status()))
        intel_counts = overview.counts
        intel_database_status = runtime_db_status.model_dump(mode="json")
    except Exception as exc:  # noqa: BLE001
        intel_status = f"unavailable ({exc.__class__.__name__}: {exc})"
    print("runtime_mode:", settings.app_runtime_mode)
    print("bind:", f"{settings.app_bind_host}:{settings.app_bind_port}")
    print("database_url:", settings.database_url)
    print("api_token_configured:", bool(settings.app_api_token))
    print("intel_status:", intel_status)
    if intel_counts is not None:
        print("intel_counts:", intel_counts)
    if intel_database_status is not None:
        print("spatial_backend:", intel_database_status["spatial_backend"])
        print("postgis_available:", intel_database_status["postgis_available"])
        if intel_database_status["postgis_version"]:
            print("postgis_version:", intel_database_status["postgis_version"])
    if backend_db_report is not None:
        print("database_components:", backend_db_report["component_count"])
        print("database_reachable_components:", backend_db_report["reachable_component_count"])
        print("database_distinct_identities:", backend_db_report["distinct_database_count"])
    for name, version in checks.items():
        print(f"{name}: {version}")


def _print_intel_overview(as_json: bool) -> None:
    init_intel_db(intel_db)
    overview = _with_intel_service(lambda service: service.overview())
    payload = overview.model_dump()
    if as_json:
        print(json.dumps(payload, indent=2))
        return
    for key, value in payload.items():
        print(f"{key}: {value}")


def _print_intel_db_status(as_json: bool) -> None:
    init_intel_db(intel_db)
    payload = _with_intel_service(lambda service: service.database_status().model_dump(mode="json"))
    if as_json:
        print(json.dumps(payload, indent=2))
        return
    for key, value in payload.items():
        print(f"{key}: {value}")


def _print_backend_db_status(*, as_json: bool, bootstrap: bool) -> None:
    settings = get_settings()
    report = (
        bootstrap_backend_databases(settings)
        if bootstrap
        else backend_database_status(settings)
    ).model_dump(mode="json")
    if as_json:
        print(json.dumps(report, indent=2))
        return
    print(f"component_count: {report['component_count']}")
    print(f"reachable_component_count: {report['reachable_component_count']}")
    print(f"distinct_database_count: {report['distinct_database_count']}")
    if report["bootstrapped_components"]:
        print(f"bootstrapped_components: {', '.join(report['bootstrapped_components'])}")
    for component in report["components"]:
        print(
            f"{component['component_key']}: reachable={component['reachable']} dialect={component['dialect']} "
            f"tables={component['table_count']} strategy={component['bootstrap_strategy']}"
        )
        if component["missing_tables"]:
            print(f"  missing_tables: {', '.join(component['missing_tables'])}")


def _ingest_file(args: argparse.Namespace) -> None:
    init_intel_db(intel_db)
    result = _with_intel_service(
        lambda service: (
            lambda payload: {
                "job": payload["job"].model_dump(mode="json"),
                "source": payload["source"].model_dump(mode="json"),
                "observations_created": payload["observations_created"],
                "observation_ids": payload["observation_ids"],
                "truncated": payload["truncated"],
            }
        )(
            service.ingest_file(
                IngestFileRequest(
                    path=args.path,
                    source_id=args.source_id,
                    source_name=args.source_name,
                    source_kind=args.source_kind,
                    event_id=args.event_id,
                    entity_id=args.entity_id,
                    format_hint=args.format,
                    actor=args.actor,
                )
            )
        )
    )
    print(json.dumps(result, indent=2))


def _evaluate_alerts(actor: str, as_json: bool) -> None:
    init_intel_db(intel_db)
    alerts = _with_intel_service(lambda service: service.evaluate_geofences(actor=actor))
    payload = [alert.model_dump(mode="json") for alert in alerts]
    if as_json:
        print(json.dumps(payload, indent=2))
        return
    print(f"created_alerts: {len(payload)}")
    for item in payload:
        print(f"{item['alert_id']}: {item['title']}")


async def _run_event_feed_sync_once(args: argparse.Namespace) -> dict[str, Any]:
    init_intel_db(intel_db)
    settings = get_settings()
    generator = intel_db.get_session()
    session = next(generator)
    try:
        service = EventFeedSyncService(settings, IntelService(session))
        feeds = [] if args.feeds == "all" else [feed.strip() for feed in args.feeds.split(",") if feed.strip()]
        result = await service.sync(
            EventFeedSyncRequest(
                feeds=feeds,
                max_records_per_feed=max(1, args.max_records_per_feed),
                actor=args.actor,
                evaluate_geofences=not args.skip_geofence_alerts,
            )
        )
        return result.model_dump(mode="json")
    finally:
        try:
            next(generator)
        except StopIteration:
            pass


def _print_event_feed_sync_result(payload: dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, indent=2))
        return
    print(f"started_at: {payload['started_at']}")
    print(f"completed_at: {payload['completed_at']}")
    print(f"feed_count: {payload['feed_count']}")
    print(f"synced_feed_count: {payload['synced_feed_count']}")
    print(f"created_alert_count: {payload['created_alert_count']}")
    for item in payload["results"]:
        print(
            f"{item['feed_key']}: status={item['status']} fetched={item['fetched_count']} "
            f"events+={item['events_created']} events~={item['events_updated']} "
            f"obs+={item['observations_created']} obs~={item['observations_updated']}"
        )
        if item["detail"]:
            print(f"  detail: {item['detail']}")


def _sync_event_feeds(args: argparse.Namespace) -> None:
    async def _loop() -> None:
        if not args.loop:
            payload = await _run_event_feed_sync_once(args)
            _print_event_feed_sync_result(payload, as_json=args.json)
            return
        while True:
            payload = await _run_event_feed_sync_once(args)
            _print_event_feed_sync_result(payload, as_json=args.json)
            await asyncio.sleep(max(5, args.interval_seconds))

    asyncio.run(_loop())


def _print_spatial_nearby(args: argparse.Namespace) -> None:
    init_intel_db(intel_db)
    payload = _with_intel_service(
        lambda service: service.spatial_nearby(
            latitude=args.latitude,
            longitude=args.longitude,
            radius_m=args.radius_m,
            limit=args.limit,
        ).model_dump(mode="json")
    )
    if args.json:
        print(json.dumps(payload, indent=2))
        return
    print(f"spatial_backend: {payload['spatial_backend']}")
    print(f"result_count: {len(payload['results'])}")
    for item in payload["results"]:
        print(
            f"{item['subject_kind']} {item['subject_id']} distance_m={item['distance_m']:.1f} "
            f"title={item['title']}"
        )


def _module_version(name: str) -> str:
    try:
        module = importlib.import_module(name)
    except Exception as exc:  # noqa: BLE001
        return f"missing ({exc.__class__.__name__})"
    return getattr(module, "__version__", "installed")


def _with_intel_service(callback):
    generator = intel_db.get_session()
    session = next(generator)
    try:
        return callback(IntelService(session))
    finally:
        try:
            next(generator)
        except StopIteration:
            pass


def main() -> None:
    args = _build_parser().parse_args()
    _print_banner()

    if args.command == "serve":
        settings = get_settings()
        uvicorn.run(
            "src.main:app",
            host=args.host or settings.app_bind_host,
            port=args.port or settings.app_bind_port,
            reload=args.reload,
            log_level=args.log_level,
        )
        return

    if args.command == "worker":
        asyncio.run(run_runtime_workers(args.worker, once=args.once, loop=args.loop))
        return

    if args.command == "forte-worker":
        init_forte_db(forte_db)
        run_forte_scheduler_loop(max(5, args.interval_seconds))
        return

    if args.command == "config":
        _print_config(as_json=args.json)
        return

    if args.command == "routes":
        _print_routes(as_json=args.json)
        return

    if args.command == "intel-overview":
        _print_intel_overview(as_json=args.json)
        return

    if args.command == "intel-db":
        _print_intel_db_status(as_json=args.json)
        return

    if args.command == "db-status":
        _print_backend_db_status(as_json=args.json, bootstrap=False)
        return

    if args.command == "db-bootstrap":
        _print_backend_db_status(as_json=args.json, bootstrap=True)
        return

    if args.command == "ingest-file":
        _ingest_file(args)
        return

    if args.command == "evaluate-alerts":
        _evaluate_alerts(actor=args.actor, as_json=args.json)
        return

    if args.command == "sync-event-feeds":
        _sync_event_feeds(args)
        return

    if args.command == "spatial-nearby":
        _print_spatial_nearby(args)
        return

    if args.command == "doctor":
        _run_doctor()
        return


if __name__ == "__main__":
    main()
