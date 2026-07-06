from __future__ import annotations

import argparse
import json
import platform
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import uvicorn

from src.config.settings import Settings, get_settings
from src.services.ops_audit_service import list_alert_records, list_provenance_events
from src.services.runtime_health_service import build_runtime_readiness_report
from src.reference.ingest import cli as reference_ingest_cli
from src import runtime_worker
from src.services.storage_profile_service import bootstrap_storage, build_storage_status
from src.webcam import worker as webcam_worker


ASCII_BANNER = r"""
 _ _ __        __    _ _            
/_\ /  \/\ /\ / / /\| | |_ ___ _ __ 
//_\\ /\  V  V / /  \ | __/ _ \ '__|
/  _  \ \_/\_/ / /\  \ | ||  __/ |   
\_/ \_/\_/   \/\/  \_/_|\__\___|_|   
11Writer Forte :: backend-only runtime
""".strip("\n")


def build_doctor_report(
    *,
    repo_root: Path | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    runtime_settings = settings or get_settings()
    root = repo_root or Path(__file__).resolve().parents[3]
    frontend_paths = {
        "app_client": root.joinpath("app", "client"),
        "sevenpo8_frontend": root.joinpath("7Po8", "apps", "frontend"),
        "code_oss_reference": root.joinpath("third_party", "code-oss-reference"),
    }
    frontend_status = {name: path.exists() for name, path in frontend_paths.items()}
    storage_status = build_storage_status(runtime_settings).model_dump(mode="json", by_alias=True)
    return {
        "runtime_mode": runtime_settings.app_runtime_mode,
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "python": platform.python_version(),
        },
        "api": {
            "cors_origins": runtime_settings.cors_origins,
            "reference_database_url": runtime_settings.reference_database_url,
            "wave_monitor_database_url": runtime_settings.wave_monitor_database_url,
            "source_discovery_database_url": runtime_settings.source_discovery_database_url,
        },
        "storage": storage_status,
        "workers": {
            "webcam_worker_enabled": runtime_settings.webcam_worker_enabled,
            "source_discovery_scheduler_enabled": runtime_settings.source_discovery_scheduler_enabled,
            "wave_monitor_scheduler_enabled": runtime_settings.wave_monitor_scheduler_enabled,
        },
        "frontend_runtime_removed": not any(frontend_status.values()),
        "frontend_runtime_paths": frontend_status,
        "repo_root": str(root),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="11writer",
        description="Operate the 11Writer Forte backend-only runtime.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve_parser = subparsers.add_parser("serve", help="Run the FastAPI backend API.")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8000)
    serve_parser.add_argument("--reload", action="store_true")
    serve_parser.set_defaults(handler=_handle_serve)

    worker_parser = subparsers.add_parser("worker", help="Run scheduled backend workers.")
    worker_parser.add_argument(
        "--worker",
        choices=runtime_worker.WORKER_CHOICES,
        default="all",
        help="Choose one worker or run every supported worker.",
    )
    worker_parser.add_argument("--once", action="store_true", help="Run one bounded cycle and exit.")
    worker_parser.add_argument("--loop", action="store_true", help="Run continuously until stopped.")
    worker_parser.set_defaults(handler=_handle_worker)

    webcam_parser = subparsers.add_parser(
        "webcam-worker",
        help="Run webcam refresh and validation tasks.",
    )
    webcam_parser.add_argument("--once", action="store_true", help="Run one refresh cycle and exit.")
    webcam_parser.add_argument("--loop", action="store_true", help="Run continuously until stopped.")
    webcam_parser.add_argument(
        "--validate-live",
        action="store_true",
        help="Run one bounded live validation cycle against configured webcam sources.",
    )
    webcam_parser.add_argument(
        "--source",
        action="append",
        default=[],
        help="Restrict live validation to one or more webcam source keys.",
    )
    webcam_parser.add_argument(
        "--include-blocked",
        action="store_true",
        help="Allow live validation to attempt sources currently marked blocked.",
    )
    webcam_parser.set_defaults(handler=_handle_webcam_worker)

    ingest_parser = subparsers.add_parser(
        "reference-ingest",
        help="Load canonical reference datasets into the backend reference store.",
    )
    ingest_parser.add_argument("dataset", choices=sorted(reference_ingest_cli.PARSERS.keys()))
    ingest_parser.add_argument("source_path")
    ingest_parser.add_argument("--database-url", default="sqlite:///./data/reference.db")
    ingest_parser.add_argument("--version", default="local")
    ingest_parser.add_argument("--coverage", default="global-core")
    ingest_parser.add_argument("--checksum", default=None)
    ingest_parser.add_argument("--source-mode", choices=["local", "remote"], default="local")
    ingest_parser.add_argument("--remote-url", default=None)
    ingest_parser.add_argument("--staging-root", default="./data/reference_staging")
    ingest_parser.set_defaults(handler=_handle_reference_ingest)

    doctor_parser = subparsers.add_parser(
        "doctor",
        help="Report backend-only runtime configuration and repo shape.",
    )
    doctor_parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    doctor_parser.set_defaults(handler=_handle_doctor)

    db_status_parser = subparsers.add_parser(
        "db-status",
        help="Report unified storage profile and component readiness.",
    )
    db_status_parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    db_status_parser.set_defaults(handler=_handle_db_status)

    db_bootstrap_parser = subparsers.add_parser(
        "db-bootstrap",
        help="Create backend storage objects for all configured components.",
    )
    db_bootstrap_parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    db_bootstrap_parser.set_defaults(handler=_handle_db_bootstrap)

    ready_parser = subparsers.add_parser(
        "ready",
        help="Check whether the backend is ready for headless operations.",
    )
    ready_parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    ready_parser.set_defaults(handler=_handle_ready)

    alerts_parser = subparsers.add_parser(
        "alerts",
        help="List persisted backend alert records.",
    )
    alerts_parser.add_argument("--limit", type=int, default=25)
    alerts_parser.add_argument("--subsystem", default=None)
    alerts_parser.add_argument("--status", default=None)
    alerts_parser.add_argument("--severity", default=None)
    alerts_parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    alerts_parser.set_defaults(handler=_handle_alerts)

    provenance_parser = subparsers.add_parser(
        "provenance",
        help="List backend provenance events and chain-of-custody entries.",
    )
    provenance_parser.add_argument("--limit", type=int, default=25)
    provenance_parser.add_argument("--subsystem", default=None)
    provenance_parser.add_argument("--subject-type", default=None)
    provenance_parser.add_argument("--subject-id", default=None)
    provenance_parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    provenance_parser.set_defaults(handler=_handle_provenance)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    result = args.handler(args)
    return 0 if result is None else int(result)


def _handle_serve(args: argparse.Namespace) -> None:
    uvicorn.run("src.main:app", host=args.host, port=args.port, reload=args.reload)


def _handle_worker(args: argparse.Namespace) -> None:
    forwarded = ["--worker", args.worker]
    if args.once:
        forwarded.append("--once")
    if args.loop:
        forwarded.append("--loop")
    runtime_worker.run_cli(forwarded)


def _handle_webcam_worker(args: argparse.Namespace) -> None:
    forwarded: list[str] = []
    if args.once:
        forwarded.append("--once")
    if args.loop:
        forwarded.append("--loop")
    if args.validate_live:
        forwarded.append("--validate-live")
    for source in args.source:
        forwarded.extend(["--source", source])
    if args.include_blocked:
        forwarded.append("--include-blocked")
    webcam_worker.run_cli(forwarded)


def _handle_reference_ingest(args: argparse.Namespace) -> None:
    forwarded = [
        args.dataset,
        args.source_path,
        "--database-url",
        args.database_url,
        "--version",
        args.version,
        "--coverage",
        args.coverage,
        "--source-mode",
        args.source_mode,
        "--staging-root",
        args.staging_root,
    ]
    if args.checksum is not None:
        forwarded.extend(["--checksum", args.checksum])
    if args.remote_url is not None:
        forwarded.extend(["--remote-url", args.remote_url])
    reference_ingest_cli.run_cli(forwarded)


def _handle_doctor(args: argparse.Namespace) -> None:
    report = build_doctor_report()
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return
    print(ASCII_BANNER)
    print()
    print(f"runtime mode   : {report['runtime_mode']}")
    print(f"python         : {report['platform']['python']}")
    print(f"platform       : {report['platform']['system']} {report['platform']['release']}")
    print(f"frontend purge : {report['frontend_runtime_removed']}")
    print(f"repo root      : {report['repo_root']}")
    print(f"storage mode   : {report['storage']['storageMode']}")
    print()
    print("databases")
    print(f"  primary      : {report['storage']['primaryDatabaseUrl'] or '(not set)'}")
    print(f"  reference    : {report['api']['reference_database_url']}")
    print(f"  wave-monitor : {report['api']['wave_monitor_database_url']}")
    print(f"  source-disc. : {report['api']['source_discovery_database_url']}")
    print()
    print("workers")
    print(f"  webcam       : {report['workers']['webcam_worker_enabled']}")
    print(f"  source-disc. : {report['workers']['source_discovery_scheduler_enabled']}")
    print(f"  wave-monitor : {report['workers']['wave_monitor_scheduler_enabled']}")
    print()
    print("frontend paths")
    for name, exists in report["frontend_runtime_paths"].items():
        print(f"  {name:<16} {exists}")


def _handle_db_status(args: argparse.Namespace) -> None:
    report = build_storage_status(get_settings()).model_dump(mode="json", by_alias=True)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return
    print(ASCII_BANNER)
    print()
    print(f"storage mode   : {report['storageMode']}")
    print(f"primary db     : {report['primaryDatabaseUrl'] or '(not set)'}")
    print(f"shared storage : {report['sharedStorage']}")
    print(f"db count       : {report['distinctDatabaseCount']}")
    print()
    print("components")
    for component in report["components"]:
        print(
            "  "
            f"{component['component']:<17}"
            f"{component['backend']:<20}"
            f"reachable={component['reachable']} "
            f"initialized={component['initialized']}"
        )
    if report["caveats"]:
        print()
        print("caveats")
        for caveat in report["caveats"]:
            print(f"  - {caveat}")


def _handle_db_bootstrap(args: argparse.Namespace) -> None:
    report = bootstrap_storage(get_settings()).model_dump(mode="json", by_alias=True)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return
    print(ASCII_BANNER)
    print()
    print("storage bootstrap complete")
    print(f"storage mode   : {report['storageMode']}")
    print(f"shared storage : {report['sharedStorage']}")
    print(f"db count       : {report['distinctDatabaseCount']}")
    print()
    print("components")
    for component in report["components"]:
        print(
            "  "
            f"{component['component']:<17}"
            f"{component['backend']:<20}"
            f"reachable={component['reachable']} "
            f"initialized={component['initialized']}"
        )


def _handle_ready(args: argparse.Namespace) -> int:
    report = build_runtime_readiness_report(get_settings()).model_dump(mode="json", by_alias=True)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if report["ready"] else 1
    print(ASCII_BANNER)
    print()
    print(f"status         : {report['status']}")
    print(f"ready          : {report['ready']}")
    print(f"runtime mode   : {report['runtimeMode']}")
    print(f"storage mode   : {report['storageMode']}")
    print()
    print("checks")
    for check in report["checks"]:
        state = "ready" if check["ready"] else "not-ready"
        print(f"  [{state}] {check['name']} :: {check['detail']}")
    if report["caveats"]:
        print()
        print("caveats")
        for caveat in report["caveats"]:
            print(f"  - {caveat}")
    return 0 if report["ready"] else 1


def _handle_alerts(args: argparse.Namespace) -> None:
    report = list_alert_records(
        get_settings(),
        limit=args.limit,
        subsystem=args.subsystem,
        status=args.status,
        severity=args.severity,
    ).model_dump(mode="json", by_alias=True)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return
    print(ASCII_BANNER)
    print()
    print(f"alert count    : {report['count']}")
    print()
    for alert in report["alerts"]:
        print(f"[{alert['severity']}/{alert['status']}] {alert['subsystem']} :: {alert['title']}")
        print(f"  subject      : {alert['subjectType']}:{alert['subjectId']}")
        print(f"  observed     : {alert['firstObservedAt']} -> {alert['lastObservedAt']} ({alert['occurrenceCount']}x)")
        print(f"  summary      : {alert['summary']}")


def _handle_provenance(args: argparse.Namespace) -> None:
    report = list_provenance_events(
        get_settings(),
        limit=args.limit,
        subsystem=args.subsystem,
        subject_type=args.subject_type,
        subject_id=args.subject_id,
    ).model_dump(mode="json", by_alias=True)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return
    print(ASCII_BANNER)
    print()
    print(f"event count    : {report['count']}")
    print()
    for event in report["events"]:
        print(f"[{event['status']}] {event['subsystem']} :: {event['eventKind']} :: {event['subjectType']}:{event['subjectId']}")
        print(f"  occurred     : {event['occurredAt']}")
        print(f"  operation    : {event['operation']}")
        print(f"  summary      : {event['summary']}")
