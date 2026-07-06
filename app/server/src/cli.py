from __future__ import annotations

import argparse
import json
import platform
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import uvicorn

from src.config.settings import Settings, get_settings
from src.services.geofence_service import GeofenceService
from src.services.local_dataset_import_service import LocalDatasetImportService
from src.services.ops_audit_service import list_alert_records, list_provenance_events
from src.services.runtime_health_service import build_runtime_readiness_report
from src.services.source_event_artifact_service import SourceEventArtifactService
from src.reference.ingest import cli as reference_ingest_cli
from src import runtime_worker
from src.services.storage_profile_service import bootstrap_storage, build_storage_status
from src.types.geofence import GeofenceCreateRequest, GeofenceEvaluationRequest
from src.types.local_import import LocalDatasetImportRequest
from src.types.source_discovery import SourceDiscoveryEventArtifactGenerationRequest
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

    event_report_parser = subparsers.add_parser(
        "event-report",
        help="Generate a persisted event artifact from a Source Discovery event cluster.",
    )
    event_report_parser.add_argument("event_id")
    event_report_parser.add_argument("--kind", choices=["cited_summary", "report"], default="report")
    event_report_parser.add_argument("--redaction-level", choices=["public", "restricted", "confidential"], default="public")
    event_report_parser.add_argument("--generated-by", default="11writer-cli")
    event_report_parser.add_argument("--title", default=None)
    event_report_parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    event_report_parser.set_defaults(handler=_handle_event_report)

    event_reports_parser = subparsers.add_parser(
        "event-reports",
        help="List persisted event artifacts for a Source Discovery event cluster.",
    )
    event_reports_parser.add_argument("event_id")
    event_reports_parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    event_reports_parser.set_defaults(handler=_handle_event_reports)

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

    geofences_parser = subparsers.add_parser(
        "geofences",
        help="List persisted geofences.",
    )
    geofences_parser.add_argument("--enabled-only", action="store_true")
    geofences_parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    geofences_parser.set_defaults(handler=_handle_geofences)

    geofence_show_parser = subparsers.add_parser(
        "geofence-show",
        help="Show one persisted geofence.",
    )
    geofence_show_parser.add_argument("geofence_id")
    geofence_show_parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    geofence_show_parser.set_defaults(handler=_handle_geofence_show)

    geofence_create_parser = subparsers.add_parser(
        "geofence-create",
        help="Create a persisted geofence.",
    )
    geofence_create_parser.add_argument("geofence_id")
    geofence_create_parser.add_argument("name")
    geofence_create_parser.add_argument("--shape-kind", choices=["bbox", "circle", "polygon"], required=True)
    geofence_create_parser.add_argument("--description", default=None)
    geofence_create_parser.add_argument("--redaction-level", choices=["public", "restricted", "confidential"], default="public")
    geofence_create_parser.add_argument("--created-by", default="11writer-cli")
    geofence_create_parser.add_argument("--disabled", action="store_true")
    geofence_create_parser.add_argument("--min-lat", type=float, default=None)
    geofence_create_parser.add_argument("--min-lon", type=float, default=None)
    geofence_create_parser.add_argument("--max-lat", type=float, default=None)
    geofence_create_parser.add_argument("--max-lon", type=float, default=None)
    geofence_create_parser.add_argument("--center-lat", type=float, default=None)
    geofence_create_parser.add_argument("--center-lon", type=float, default=None)
    geofence_create_parser.add_argument("--radius-m", type=float, default=None)
    geofence_create_parser.add_argument(
        "--point",
        action="append",
        default=[],
        help="Polygon vertex as lon,lat. Repeat for each point.",
    )
    geofence_create_parser.add_argument("--tag", action="append", default=[])
    geofence_create_parser.add_argument("--metadata-json", default="{}")
    geofence_create_parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    geofence_create_parser.set_defaults(handler=_handle_geofence_create)

    geofence_check_parser = subparsers.add_parser(
        "geofence-check",
        help="Evaluate one point against a persisted geofence.",
    )
    geofence_check_parser.add_argument("geofence_id")
    geofence_check_parser.add_argument("--lat", type=float, required=True)
    geofence_check_parser.add_argument("--lon", type=float, required=True)
    geofence_check_parser.add_argument("--observed-at", default=None)
    geofence_check_parser.add_argument("--subject-type", default="observation")
    geofence_check_parser.add_argument("--subject-id", default=None)
    geofence_check_parser.add_argument("--observation-label", default=None)
    geofence_check_parser.add_argument("--requested-by", default="11writer-cli")
    geofence_check_parser.add_argument("--reference-object-type", action="append", default=[])
    geofence_check_parser.add_argument("--reference-limit", type=int, default=10)
    geofence_check_parser.add_argument("--metadata-json", default="{}")
    geofence_check_parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    geofence_check_parser.set_defaults(handler=_handle_geofence_check)

    geofence_history_parser = subparsers.add_parser(
        "geofence-history",
        help="List persisted evaluations for one geofence.",
    )
    geofence_history_parser.add_argument("geofence_id")
    geofence_history_parser.add_argument("--limit", type=int, default=25)
    geofence_history_parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    geofence_history_parser.set_defaults(handler=_handle_geofence_history)

    import_local_parser = subparsers.add_parser(
        "import-local",
        help="Import a local JSON, TXT, or SQLite dataset into the backend evidence store.",
    )
    import_local_parser.add_argument("file_path")
    import_local_parser.add_argument("--source-id", default=None)
    import_local_parser.add_argument("--title", default=None)
    import_local_parser.add_argument("--format", choices=["auto", "json", "jsonl", "txt", "sqlite"], default="auto")
    import_local_parser.add_argument("--source-kind", choices=["historical_source", "data_feed_source", "data_source", "integrity_source"], default="historical_source")
    import_local_parser.add_argument("--source-class", choices=["static", "live", "article", "social_image", "official", "community", "dataset", "unknown"], default="dataset")
    import_local_parser.add_argument("--requested-by", default="11writer-cli")
    import_local_parser.add_argument("--encoding", default="utf-8")
    import_local_parser.add_argument("--table", action="append", default=[])
    import_local_parser.add_argument("--max-records", type=int, default=100)
    import_local_parser.add_argument("--tag", action="append", default=[])
    import_local_parser.add_argument("--metadata-json", default="{}")
    import_local_parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    import_local_parser.set_defaults(handler=_handle_import_local)

    import_runs_parser = subparsers.add_parser(
        "import-runs",
        help="List persisted local dataset import runs.",
    )
    import_runs_parser.add_argument("--limit", type=int, default=25)
    import_runs_parser.add_argument("--source-id", default=None)
    import_runs_parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    import_runs_parser.set_defaults(handler=_handle_import_runs)

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


def _handle_event_report(args: argparse.Namespace) -> None:
    service = SourceEventArtifactService(get_settings())
    report = service.generate_event_artifact(
        args.event_id,
        SourceDiscoveryEventArtifactGenerationRequest(
            artifact_kind=args.kind,
            redaction_level=args.redaction_level,
            generated_by=args.generated_by,
            title=args.title,
        ),
    ).model_dump(mode="json", by_alias=True)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return
    artifact = report["artifact"]
    print(ASCII_BANNER)
    print()
    print(f"artifact id    : {artifact['artifactId']}")
    print(f"event id       : {artifact['eventId']}")
    print(f"kind           : {artifact['artifactKind']}")
    print(f"redaction      : {artifact['redactionLevel']}")
    print(f"confidence     : {artifact['confidenceScore']:.2f} ({artifact['confidenceLabel']})")
    print(f"generated by   : {artifact['generatedBy']}")
    print()
    print(artifact["bodyText"])


def _handle_event_reports(args: argparse.Namespace) -> None:
    service = SourceEventArtifactService(get_settings())
    report = service.list_event_artifacts(args.event_id).model_dump(mode="json", by_alias=True)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return
    print(ASCII_BANNER)
    print()
    print(f"artifact count : {report['count']}")
    print()
    for artifact in report["artifacts"]:
        print(f"[{artifact['artifactKind']}/{artifact['redactionLevel']}] {artifact['title']}")
        print(f"  artifact id  : {artifact['artifactId']}")
        print(f"  confidence   : {artifact['confidenceScore']:.2f} ({artifact['confidenceLabel']})")
        print(f"  summary      : {artifact['summaryText']}")


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


def _handle_geofences(args: argparse.Namespace) -> None:
    report = GeofenceService(get_settings()).list_geofences(enabled_only=args.enabled_only).model_dump(mode="json", by_alias=True)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return
    print(ASCII_BANNER)
    print()
    print(f"geofence count : {report['count']}")
    print()
    for geofence in report["geofences"]:
        print(f"[{geofence['shapeKind']}/{geofence['redactionLevel']}] {geofence['geofenceId']} :: {geofence['name']}")
        print(f"  enabled      : {geofence['enabled']}")
        print(f"  bounds       : {_format_geofence_bounds(geofence)}")
        if geofence.get("description"):
            print(f"  description  : {geofence['description']}")


def _handle_geofence_show(args: argparse.Namespace) -> None:
    report = GeofenceService(get_settings()).get_geofence(args.geofence_id).model_dump(mode="json", by_alias=True)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return
    print(ASCII_BANNER)
    print()
    print(f"geofence id    : {report['geofenceId']}")
    print(f"name           : {report['name']}")
    print(f"shape          : {report['shapeKind']}")
    print(f"redaction      : {report['redactionLevel']}")
    print(f"enabled        : {report['enabled']}")
    print(f"bounds         : {_format_geofence_bounds(report)}")
    print(f"created by     : {report['createdBy']}")
    print(f"updated at     : {report['updatedAt']}")
    if report.get("description"):
        print(f"description    : {report['description']}")


def _handle_geofence_create(args: argparse.Namespace) -> None:
    payload = GeofenceService(get_settings()).create_geofence(
        GeofenceCreateRequest(
            geofence_id=args.geofence_id,
            name=args.name,
            shape_kind=args.shape_kind,
            redaction_level=args.redaction_level,
            created_by=args.created_by,
            enabled=not args.disabled,
            description=args.description,
            min_lat=args.min_lat,
            min_lon=args.min_lon,
            max_lat=args.max_lat,
            max_lon=args.max_lon,
            center_lat=args.center_lat,
            center_lon=args.center_lon,
            radius_m=args.radius_m,
            polygon_points=[_parse_lon_lat(value) for value in args.point],
            tags=args.tag,
            metadata=_parse_json_object(args.metadata_json, flag_name="--metadata-json"),
        )
    ).model_dump(mode="json", by_alias=True)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    print(ASCII_BANNER)
    print()
    print(f"created        : {payload['geofenceId']}")
    print(f"name           : {payload['name']}")
    print(f"shape          : {payload['shapeKind']}")
    print(f"bounds         : {_format_geofence_bounds(payload)}")


def _handle_geofence_check(args: argparse.Namespace) -> None:
    payload = GeofenceService(get_settings()).evaluate_point(
        args.geofence_id,
        GeofenceEvaluationRequest(
            lat=args.lat,
            lon=args.lon,
            observed_at=args.observed_at,
            subject_type=args.subject_type,
            subject_id=args.subject_id,
            observation_label=args.observation_label,
            requested_by=args.requested_by,
            reference_object_types=args.reference_object_type,
            reference_limit=args.reference_limit,
            metadata=_parse_json_object(args.metadata_json, flag_name="--metadata-json"),
        ),
    ).model_dump(mode="json", by_alias=True)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    evaluation = payload["evaluation"]
    print(ASCII_BANNER)
    print()
    print(f"geofence id    : {payload['geofence']['geofenceId']}")
    print(f"matched        : {evaluation['matched']}")
    print(f"method         : {evaluation['matchMethod']}")
    print(f"reference hits : {evaluation['referenceMatchCount']}")
    if evaluation.get("distanceToCenterM") is not None:
        print(f"distance m     : {evaluation['distanceToCenterM']:.1f}")
    if evaluation.get("alertId"):
        print(f"alert id       : {evaluation['alertId']}")
    if evaluation.get("provenanceEventId"):
        print(f"provenance id  : {evaluation['provenanceEventId']}")


def _handle_geofence_history(args: argparse.Namespace) -> None:
    report = GeofenceService(get_settings()).list_evaluations(args.geofence_id, limit=args.limit).model_dump(mode="json", by_alias=True)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return
    print(ASCII_BANNER)
    print()
    print(f"evaluation cnt : {report['count']}")
    print()
    for evaluation in report["evaluations"]:
        print(f"[{evaluation['matchMethod']}] {evaluation['evaluationId']}")
        print(f"  matched      : {evaluation['matched']}")
        print(f"  observed     : {evaluation['observedAt']} @ ({evaluation['observedLat']}, {evaluation['observedLon']})")
        if evaluation.get("alertId"):
            print(f"  alert id     : {evaluation['alertId']}")


def _handle_import_local(args: argparse.Namespace) -> None:
    payload = LocalDatasetImportService(get_settings()).import_dataset(
        LocalDatasetImportRequest(
            file_path=args.file_path,
            source_id=args.source_id,
            title=args.title,
            format=args.format,
            source_kind=args.source_kind,
            source_class=args.source_class,
            requested_by=args.requested_by,
            encoding=args.encoding,
            table_names=args.table,
            max_records=args.max_records,
            tags=args.tag,
            metadata=_parse_json_object(args.metadata_json, flag_name="--metadata-json"),
        )
    ).model_dump(mode="json", by_alias=True)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    run = payload["run"]
    memory = payload["memory"]
    print(ASCII_BANNER)
    print()
    print(f"import run     : {run['importRunId']}")
    print(f"source id      : {memory['sourceId']}")
    print(f"format         : {run['fileFormat']}")
    print(f"records        : {run['importedRecordCount']}")
    print(f"snapshots      : {run['snapshotCount']} ({run['duplicateSnapshotCount']} duplicate skips)")
    print(f"geospatial     : {run['geospatialRecordCount']}")
    if run.get("provenanceEventId"):
        print(f"provenance id  : {run['provenanceEventId']}")


def _handle_import_runs(args: argparse.Namespace) -> None:
    payload = LocalDatasetImportService(get_settings()).list_import_runs(limit=args.limit, source_id=args.source_id).model_dump(mode="json", by_alias=True)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    print(ASCII_BANNER)
    print()
    print(f"import count   : {payload['count']}")
    print()
    for run in payload["runs"]:
        print(f"[{run['status']}/{run['fileFormat']}] {run['importRunId']}")
        print(f"  source id    : {run['sourceId']}")
        print(f"  file path    : {run['filePath']}")
        print(f"  records      : {run['importedRecordCount']} -> snapshots={run['snapshotCount']} duplicates={run['duplicateSnapshotCount']}")


def _parse_lon_lat(value: str) -> list[float]:
    parts = [part.strip() for part in value.split(",", maxsplit=1)]
    if len(parts) != 2:
        raise ValueError("--point must use lon,lat format.")
    return [float(parts[0]), float(parts[1])]


def _parse_json_object(value: str, *, flag_name: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{flag_name} must be valid JSON.") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{flag_name} must decode to a JSON object.")
    return parsed


def _format_geofence_bounds(payload: dict[str, Any]) -> str:
    shape_kind = payload["shapeKind"]
    if shape_kind == "circle":
        return (
            f"center ({payload['centerLat']:.5f}, {payload['centerLon']:.5f}), "
            f"radius {payload['radiusM']:.1f} m"
        )
    return (
        f"lat {payload['minLat']:.5f}..{payload['maxLat']:.5f}, "
        f"lon {payload['minLon']:.5f}..{payload['maxLon']:.5f}"
    )
