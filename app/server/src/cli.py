from __future__ import annotations

import json
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse, urlunparse
from urllib.request import urlopen

import typer
from pydantic import TypeAdapter, ValidationError
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.config import get_settings
from src.db import get_session_factory, init_db
from src.models import (
    AlertORM,
    CameraInventoryORM,
    CameraSourceInventoryORM,
    CustodyLogORM,
    EntityObservationLinkORM,
    EntityORM,
    EventORM,
    LocalImportRunORM,
    ObservationORM,
    ScheduledTaskORM,
    ScheduledTaskRunORM,
    SituationProductORM,
    StorageObjectORM,
    SourceCheckpointORM,
    SourceDefinitionORM,
    SourceRunORM,
    SourceTrustProfileORM,
)
from src.schemas import (
    AlertInventorySummaryRead,
    AlertOpsExportSummaryRead,
    AlertOpsReportIndexRead,
    CameraSourceOpsExportSummaryRead,
    CameraSourceMaterializationResponse,
    CameraSourceOpsReportIndexRead,
    CameraSourceSummaryRead,
    CameraSourceVerificationResponse,
    ClickHouseArchiveResultRead,
    ClickHouseDiagnosticsRead,
    ClickHouseProvisionResultRead,
    ClickHouseR2ConfigRead,
    ClickHouseRehydrateResultRead,
    ClickHouseSyncResultRead,
    CameraOpsExportSummaryRead,
    CameraOpsReportIndexRead,
    CameraMaterializationResponse,
    DataLayerCreate,
    DatabaseDiagnosticsRead,
    EventExportBundleRead,
    EventFusionRequest,
    EventInventorySummaryRead,
    EventOpsExportSummaryRead,
    EventOpsReportIndexRead,
    EntityInventorySummaryRead,
    EntityOpsExportSummaryRead,
    EntityOpsReportIndexRead,
    EntityResolutionRequest,
    ObservationSearchResultRead,
    OperationsReportRead,
    PlatformRuntimeCycleRead,
    RuntimeReadinessRead,
    RuntimeRestoreResultRead,
    RuntimeSnapshotArtifactManifestRead,
    RuntimeSnapshotRead,
    SchedulerInventorySummaryRead,
    SchedulerOpsExportSummaryRead,
    SchedulerOpsReportIndexRead,
    ScheduledTaskCreate,
    ScheduledTaskUpdate,
    StorageActionResultRead,
    StorageLifecycleSweepResultRead,
    StorageManifestRead,
    WorkerStatusSummaryRead,
    StorageObjectCreate,
    StorageObjectPromoteRequest,
    StorageObjectRead,
    StorageObjectTransitionRequest,
    StorageReportRead,
    SourceDefinitionCreate,
    SourceDefinitionUpdate,
    SourceInventorySummaryRead,
    SourceOpsExportSummaryRead,
    SourceOpsReportIndexRead,
    WebSearchProviderRead,
    WatchCreate,
    WatchScheduleCreate,
    WatchUpdate,
)
from src.services.camera_source_service import (
    build_camera_source_inventory_ops_detail,
    build_camera_source_ops_export_summary,
    build_camera_source_ops_report_index,
    build_camera_source_inventory_summary,
    list_camera_sources,
    materialize_camera_source_inventory,
    verify_camera_source_inventory,
)
from src.services.alert_service import (
    build_alert_inventory_summary,
    build_alert_ops_export_summary,
    build_alert_ops_report_index,
    list_alert_records,
)
from src.services.clickhouse_service import (
    archive_clickhouse_observations_to_r2,
    build_clickhouse_diagnostics,
    build_clickhouse_r2_config_preview,
    default_clickhouse_r2_config_path,
    provision_clickhouse_backend,
    rehydrate_clickhouse_observations_from_r2,
    sync_runtime_to_clickhouse,
)
from src.services.camera_service import list_cameras
from src.services.camera_service import materialize_camera_inventory
from src.services.camera_service import build_camera_ops_export_summary
from src.services.camera_service import build_camera_ops_report_index
from src.services.camera_service import build_camera_inventory_ops_detail
from src.services.camera_service import build_camera_inventory_summary
from src.services.database_diagnostics_service import build_database_diagnostics
from src.services.entity_service import (
    build_entity_inventory_summary,
    build_entity_ops_export_summary,
    build_entity_ops_report_index,
    list_entity_records,
)
from src.services.entity_resolution_service import materialize_entities
from src.services.export_artifact_service import write_json_export_artifact, write_text_export_artifact
from src.services.event_export_service import build_event_export_bundle
from src.services.event_service import (
    build_event_inventory_summary,
    build_event_ops_export_summary,
    build_event_ops_report_index,
    list_event_records,
)
from src.services.event_fusion_service import materialize_fused_events
from src.services.import_service import import_local_path
from src.services.layer_service import create_data_layer, list_data_layers
from src.services.observation_service import (
    build_cross_verification_summaries,
    query_observations,
    search_observations,
)
from src.services.operations_report_service import (
    build_operations_report,
    export_operations_report_artifact,
)
from src.services.platform_runtime_service import run_platform_runtime_cycle
from src.services.platform_runtime_worker_service import run_platform_runtime_worker
from src.services.runtime_readiness_service import build_runtime_readiness
from src.services.redaction_service import enforce_export_redaction
from src.services.runtime_bundle_service import export_runtime_bundle, restore_runtime_bundle
from src.services.runtime_snapshot_service import (
    build_runtime_snapshot_section_counts,
    export_runtime_snapshot_artifacts,
    restore_runtime_snapshot,
)
from src.services.scheduler_runtime_service import run_scheduler_worker
from src.services.scheduler_service import (
    build_scheduler_inventory_summary,
    build_scheduler_ops_export_summary,
    build_scheduler_ops_report_index,
    create_scheduled_task,
    run_enabled_tasks,
    run_due_tasks,
    run_task,
    update_scheduled_task,
)
from src.services.source_runtime_service import run_source_runtime_worker
from src.services.storage_service import (
    archive_storage_object,
    build_storage_report,
    create_storage_object,
    get_storage_manifest_for_object,
    list_storage_objects,
    promote_storage_object,
    prune_storage_object,
    quarantine_storage_object,
    rehydrate_storage_object,
    request_storage_object_rehydration,
    sweep_expired_storage_objects,
    transition_storage_object,
    unquarantine_storage_object,
    hash_file,
    verify_storage_object,
)
from src.services.source_service import (
    build_source_inventory_summary,
    build_source_ops_detail,
    build_source_ops_export_summary,
    build_source_ops_report_index,
    create_source_definition,
    ingest_webhook_payload,
    list_source_checkpoints,
    list_source_dead_letters,
    list_source_definitions,
    list_supported_web_search_providers,
    list_source_runs,
    perform_source_maintenance,
    scan_source_health_alerts,
    replay_dead_letter_record,
    run_source_definition,
    run_source_runtime_cycle,
    upsert_and_run_source_definition,
    update_source_definition,
)
from src.services.trust_service import seed_default_integrity_sources
from src.services.watch_service import (
    attach_watch_schedule,
    create_watch,
    evaluate_watch,
    get_watch,
    list_watch_alerts,
    list_watch_evidence,
    list_watch_runs,
    list_watches,
    pause_watch,
    render_watch_alert_rss,
    resume_watch,
    update_watch,
)
from src.services.worker_status_service import build_worker_status_summary, evaluate_worker_health

app = typer.Typer(help="11Writer Forte backend operator CLI")

BANNER = r"""
  _ _ __        ___      _ _            
 / | |\ \      / / |    (_) |_ ___ _ __ 
 | | | \ \ /\ / /| |    | | __/ _ \ '__|
 | | |  \ V  V / | |___ | | ||  __/ |   
 |_|_|   \_/\_/  |_____||_|\__\___|_|   
                                        
   F O R T E   //   H E A D L E S S
"""


def print_banner() -> None:
    typer.echo(BANNER)


def parse_bbox(value: str | None) -> tuple[float | None, float | None, float | None, float | None]:
    if not value:
        return (None, None, None, None)
    parts = [part.strip() for part in value.split(",")]
    if len(parts) != 4:
        raise typer.BadParameter("bbox must be 'min_lon,min_lat,max_lon,max_lat'")
    min_lon, min_lat, max_lon, max_lat = (float(part) for part in parts)
    return (min_lon, min_lat, max_lon, max_lat)


def parse_observation_backend(value: str) -> str:
    normalized = value.strip().lower()
    if normalized not in {"runtime", "clickhouse", "r2_archive"}:
        raise typer.BadParameter("backend must be one of: runtime, clickhouse, r2_archive")
    return normalized


def resolve_report_since(hours: float | None) -> datetime | None:
    if hours is None:
        return None
    return datetime.now(timezone.utc) - timedelta(hours=hours)


def build_http_source_metadata(
    *,
    timeout_seconds: float,
    retry_attempts: int,
    retry_backoff_seconds: float,
    skip_unchanged: bool,
    header: list[str],
    basic_auth_username: str | None,
    basic_auth_password_env: str | None,
    max_payload_bytes: int | None = None,
    allow_private_networks: bool | None = None,
    min_request_interval_seconds: float | None = None,
) -> dict[str, object]:
    headers: dict[str, str] = {}
    for item in header:
        if ":" not in item:
            raise typer.BadParameter("header must be 'Name: Value'")
        key, value = item.split(":", 1)
        headers[key.strip()] = value.strip()
    metadata: dict[str, object] = {
        "request_timeout_seconds": timeout_seconds,
        "retry_attempts": retry_attempts,
        "retry_backoff_seconds": retry_backoff_seconds,
        "skip_unchanged": skip_unchanged,
        "headers": headers,
    }
    if bool(basic_auth_username) != bool(basic_auth_password_env):
        raise typer.BadParameter(
            "basic auth requires both --basic-auth-username and --basic-auth-password-env"
        )
    if basic_auth_username and basic_auth_password_env:
        metadata["basic_auth_username"] = basic_auth_username
        metadata["basic_auth_password_env"] = basic_auth_password_env
    if max_payload_bytes is not None:
        metadata["max_payload_bytes"] = max(1, int(max_payload_bytes))
    if allow_private_networks is not None:
        metadata["allow_private_networks"] = bool(allow_private_networks)
    if min_request_interval_seconds is not None:
        metadata["min_request_interval_seconds"] = max(0.0, float(min_request_interval_seconds))
    return metadata


def parse_json_object_option(value: str | None, option_name: str) -> dict[str, object] | None:
    if value is None:
        return None
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"{option_name} must be valid JSON.") from exc
    if not isinstance(parsed, dict):
        raise typer.BadParameter(f"{option_name} must decode to a JSON object.")
    return parsed


def parse_json_value_option(value: str | None, option_name: str) -> object | None:
    if value is None:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"{option_name} must be valid JSON.") from exc


def parse_mapping_option(values: list[str], option_name: str) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for item in values:
        if "=" not in item:
            raise typer.BadParameter(f"{option_name} entries must be KEY=VALUE.")
        key, value = item.split("=", 1)
        normalized_key = key.strip()
        normalized_value = value.strip()
        if not normalized_key or not normalized_value:
            raise typer.BadParameter(f"{option_name} entries must be KEY=VALUE.")
        mapping[normalized_key] = normalized_value
    return mapping


def derive_watch_slug(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.strip().lower()).strip("-")
    if not slug:
        raise typer.BadParameter(
            "Watch name must contain at least one letter or number, or provide --slug.",
            param_hint="--slug",
        )
    return slug


def resolve_scheduler_poll_seconds(value: float | None) -> float:
    if value is not None:
        return max(0.0, value)
    return max(0.0, get_settings().scheduler_poll_seconds)


def task_filters_include(task_types: list[str], expected_task_type: str) -> bool:
    return not task_types or expected_task_type in task_types


def derive_websocket_base_url(http_base_url: str) -> str:
    parsed = urlparse(http_base_url)
    if parsed.scheme not in {"http", "https"}:
        raise typer.BadParameter("base_url must use http:// or https://")
    websocket_scheme = "wss" if parsed.scheme == "https" else "ws"
    return urlunparse((websocket_scheme, parsed.netloc, parsed.path.rstrip("/"), "", "", ""))


def infer_content_type_for_payload_file(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".json", ".jsonl"}:
        return "application/json"
    if suffix in {".txt", ".log", ".md"}:
        return "text/plain; charset=utf-8"
    return "application/octet-stream"


def default_runtime_snapshot_manifest_path(snapshot_path: Path) -> Path:
    if snapshot_path.suffix:
        return snapshot_path.with_name(f"{snapshot_path.stem}.manifest.json")
    return snapshot_path.with_name(f"{snapshot_path.name}.manifest.json")


def load_json_file(path: Path, *, label: str) -> Any:
    resolved_path = path.expanduser().resolve()
    if not resolved_path.exists() or not resolved_path.is_file():
        raise typer.BadParameter(f"{label} '{resolved_path}' does not exist.")
    try:
        return json.loads(resolved_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"{label} '{resolved_path}' is not valid JSON.") from exc
    except OSError as exc:
        raise typer.BadParameter(f"{label} '{resolved_path}' could not be read: {exc}") from exc


def verify_runtime_snapshot_artifact(
    snapshot_path: Path,
    *,
    manifest_path: Path | None = None,
) -> tuple[RuntimeSnapshotRead, RuntimeSnapshotArtifactManifestRead, Path]:
    resolved_snapshot_path = snapshot_path.expanduser().resolve()
    snapshot_payload = load_json_file(resolved_snapshot_path, label="runtime snapshot")
    try:
        snapshot = TypeAdapter(RuntimeSnapshotRead).validate_python(snapshot_payload)
    except ValidationError as exc:
        raise typer.BadParameter(f"runtime snapshot '{resolved_snapshot_path}' failed schema validation: {exc}") from exc

    resolved_manifest_path = (
        manifest_path.expanduser().resolve()
        if manifest_path is not None
        else default_runtime_snapshot_manifest_path(resolved_snapshot_path)
    )
    manifest_payload = load_json_file(resolved_manifest_path, label="runtime snapshot manifest")
    try:
        manifest = TypeAdapter(RuntimeSnapshotArtifactManifestRead).validate_python(manifest_payload)
    except ValidationError as exc:
        raise typer.BadParameter(
            f"runtime snapshot manifest '{resolved_manifest_path}' failed schema validation: {exc}"
        ) from exc

    actual_sha256 = hash_file(resolved_snapshot_path)
    actual_byte_size = resolved_snapshot_path.stat().st_size
    expected_section_counts = build_runtime_snapshot_section_counts(snapshot)
    snapshot_row_counts = [row.model_dump(mode="json") for row in snapshot.row_counts]
    manifest_row_counts = [row.model_dump(mode="json") for row in manifest.row_counts]
    errors: list[str] = []

    if manifest.snapshot_file_name != resolved_snapshot_path.name:
        errors.append(
            f"manifest snapshot_file_name={manifest.snapshot_file_name!r} does not match file name {resolved_snapshot_path.name!r}."
        )
    if manifest.snapshot_sha256 != actual_sha256:
        errors.append("manifest snapshot_sha256 does not match the snapshot file content.")
    if manifest.snapshot_byte_size != actual_byte_size:
        errors.append("manifest snapshot_byte_size does not match the snapshot file size.")
    if manifest.exported_at != snapshot.exported_at:
        errors.append("manifest exported_at does not match the snapshot payload.")
    if manifest.app_name != snapshot.app_name or manifest.app_version != snapshot.app_version:
        errors.append("manifest app identity does not match the snapshot payload.")
    if manifest.database_backend != snapshot.database_backend or manifest.spatial_backend != snapshot.spatial_backend:
        errors.append("manifest backend metadata does not match the snapshot payload.")
    if manifest.database_revision != snapshot.database_revision:
        errors.append("manifest database_revision does not match the snapshot payload.")
    if manifest.database_head_revision != snapshot.database_head_revision:
        errors.append("manifest database_head_revision does not match the snapshot payload.")
    normalized_manifest_section_counts = dict(manifest.section_counts)
    for section_name, expected_count in expected_section_counts.items():
        if section_name not in normalized_manifest_section_counts and expected_count == 0:
            # Snapshots exported before the Watch Engine have no watch sections. The snapshot
            # schema supplies empty lists for them, so a missing manifest count is equivalent
            # to zero rather than a backup-format break.
            normalized_manifest_section_counts[section_name] = 0
    if normalized_manifest_section_counts != expected_section_counts:
        errors.append("manifest section_counts do not match the snapshot payload.")
    if manifest_row_counts != snapshot_row_counts:
        errors.append("manifest row_counts do not match the snapshot payload.")

    if errors:
        raise typer.BadParameter(
            f"runtime snapshot artifact verification failed for '{resolved_snapshot_path}': {' '.join(errors)}"
        )

    return snapshot, manifest, resolved_manifest_path


def build_local_live_source_definitions(base_url: str, layer_prefix: str) -> list[tuple[str, SourceDefinitionCreate]]:
    websocket_base_url = derive_websocket_base_url(base_url)
    normalized_base_url = base_url.rstrip("/")
    return [
        (
            "local-sse-harbor",
            SourceDefinitionCreate(
                name="local-sse-harbor",
                source_kind="sse_stream",
                layer_key=f"{layer_prefix}-harbor",
                target_uri=f"{normalized_base_url}/feeds/sse/harbor",
                notes="Local simulated SSE feed for backend-only validation.",
                metadata_json={
                    "request_timeout_seconds": 5,
                    "stream_idle_timeout_seconds": 0.25,
                    "stream_max_records": 25,
                    "skip_unchanged": False,
                },
            ),
        ),
        (
            "local-web-search-harbor",
            SourceDefinitionCreate(
                name="local-web-search-harbor",
                source_kind="web_search",
                layer_key=f"{layer_prefix}-web-search",
                target_uri=f"{normalized_base_url}/search/html",
                notes="Local simulated HTML search provider for backend-only discovery validation.",
                metadata_json={
                    "query": "harbor departure",
                    "request_timeout_seconds": 5,
                    "search_page_limit": 2,
                    "search_page_param": "page",
                    "search_page_start": 1,
                    "search_page_step": 1,
                    "search_result_limit": 10,
                    "page_fetch_limit": 5,
                    "fetch_result_pages": True,
                    "crawl_from_results": True,
                    "result_crawl_depth": 1,
                    "result_crawl_page_limit": 5,
                    "result_crawl_link_limit": 10,
                    "skip_provider_domain": False,
                    "result_redirect_query_param": "target",
                    "include_url_patterns": ["/sites/"],
                    "skip_unchanged": True,
                },
            ),
        ),
        (
            "local-web-discovery-campaign",
            SourceDefinitionCreate(
                name="local-web-discovery-campaign",
                source_kind="web_discovery",
                layer_key=f"{layer_prefix}-web-discovery",
                target_uri="discover://web",
                notes="Local simulated whole-web discovery campaign with search, sitemap, and crawl resume.",
                metadata_json={
                    "queries": ["harbor departure", "rail delay"],
                    "search_providers": ["generic_html"],
                    "search_provider_targets": {
                        "generic_html": f"{normalized_base_url}/search/html?page_size=1",
                    },
                    "search_page_param": "page",
                    "search_page_start": 1,
                    "search_page_step": 1,
                    "request_timeout_seconds": 5,
                    "fetch_result_pages": True,
                    "search_page_limit": 2,
                    "search_result_limit": 10,
                    "page_fetch_limit": 5,
                    "seed_urls": [f"{normalized_base_url}/crawl/root"],
                    "discover_sitemaps_from_seeds": True,
                    "sitemap_fetch_limit": 5,
                    "sitemap_url_limit": 20,
                    "crawl_depth": 2,
                    "crawl_page_limit": 10,
                    "crawl_link_limit": 10,
                    "same_domain_only": True,
                    "include_url_patterns": ["/sites/", "/crawl/root"],
                    "skip_unchanged": True,
                    "resume_frontier": True,
                },
            ),
        ),
        (
            "local-web-crawl-briefs",
            SourceDefinitionCreate(
                name="local-web-crawl-briefs",
                source_kind="web_crawl",
                layer_key=f"{layer_prefix}-web-crawl",
                target_uri=f"{normalized_base_url}/crawl/root",
                notes="Local simulated crawl graph for backend-only discovery validation.",
                metadata_json={
                    "request_timeout_seconds": 5,
                    "crawl_depth": 2,
                    "crawl_page_limit": 10,
                    "crawl_link_limit": 10,
                    "same_domain_only": True,
                    "include_url_patterns": ["/sites/", "/crawl/root"],
                    "skip_unchanged": True,
                },
            ),
        ),
        (
            "local-websocket-cameras",
            SourceDefinitionCreate(
                name="local-websocket-cameras",
                source_kind="websocket_stream",
                layer_key=f"{layer_prefix}-cameras",
                target_uri=f"{websocket_base_url}/feeds/ws/cameras",
                notes="Local simulated WebSocket feed for backend-only validation.",
                metadata_json={
                    "request_timeout_seconds": 5,
                    "stream_idle_timeout_seconds": 0.5,
                    "stream_max_records": 25,
                    "skip_unchanged": False,
                    "send_messages": [{"op": "subscribe", "topic": "cameras"}],
                },
            ),
        ),
    ]


def sync_local_live_sources(
    session: Session,
    *,
    base_url: str,
    layer_prefix: str,
    actor: str,
) -> dict[str, object]:
    desired_sources = build_local_live_source_definitions(base_url, layer_prefix)
    existing = {row.name: row for row in list_source_definitions(session)}
    created = 0
    updated = 0
    sources_by_name: dict[str, object] = {}
    messages: list[str] = []
    for name, definition in desired_sources:
        if name in existing:
            source = update_source_definition(
                session,
                existing[name].source_id,
                SourceDefinitionUpdate(
                    source_kind=definition.source_kind,
                    layer_key=definition.layer_key,
                    target_uri=definition.target_uri,
                    enabled=True,
                    integrity_source=definition.integrity_source,
                    notes=definition.notes,
                    metadata_json=definition.metadata_json,
                ),
                actor=actor,
            )
            updated += 1
            messages.append(f"updated source {source.source_id} -> {source.target_uri}")
        else:
            source = create_source_definition(session, definition)
            created += 1
            messages.append(f"created source {source.source_id} -> {source.target_uri}")
        sources_by_name[name] = source
    return {
        "created": created,
        "updated": updated,
        "sources_by_name": sources_by_name,
        "messages": messages,
    }


def upsert_scheduled_task_by_name(
    session: Session,
    payload: ScheduledTaskCreate,
    *,
    actor: str,
) -> tuple[ScheduledTaskORM, str]:
    existing = session.scalar(select(ScheduledTaskORM).where(ScheduledTaskORM.name == payload.name).limit(1))
    if existing is None:
        return create_scheduled_task(session, payload), "created"
    updated = update_scheduled_task(
        session,
        existing.task_id,
        ScheduledTaskUpdate(
            enabled=payload.enabled,
            interval_seconds=payload.interval_seconds,
            retry_attempts=payload.retry_attempts,
            retry_backoff_seconds=payload.retry_backoff_seconds,
            source_id=payload.source_id,
            target_path=payload.target_path,
            layer_key=payload.layer_key,
            geofence_id=payload.geofence_id,
            notes=payload.notes,
            payload_json=payload.payload_json,
        ),
        actor=actor,
    )
    return updated, "updated"


def perform_local_runtime_bootstrap(
    session: Session,
    *,
    base_url: str,
    layer_prefix: str,
    schedule_prefix: str,
    with_demo_sources: bool,
    with_default_schedules: bool,
    run_initial_cycle: bool,
    include_stream_runtime_in_initial_cycle: bool,
    include_enabled_schedules_in_initial_cycle: bool,
    initial_cycle_task_types: list[str] | None,
    initial_cycle_source_id: int | None,
    source_sync_interval_seconds: int,
    source_maintenance_interval_seconds: int,
    source_health_scan_interval_seconds: int,
    camera_refresh_interval_seconds: int,
    camera_source_verification_interval_seconds: int,
    entity_resolution_interval_seconds: int,
    event_fusion_interval_seconds: int,
    storage_lifecycle_interval_seconds: int,
    runtime_snapshot_interval_seconds: int,
    integrity_seed_interval_seconds: int,
    actor_prefix: str,
) -> dict[str, object]:
    seeded_domains = seed_default_integrity_sources(session)
    source_sync_result: dict[str, object] = {
        "created": 0,
        "updated": 0,
        "sources_by_name": {},
        "messages": [],
    }
    if with_demo_sources:
        source_sync_result = sync_local_live_sources(
            session,
            base_url=base_url,
            layer_prefix=layer_prefix,
            actor=actor_prefix,
        )

    task_messages: list[str] = []
    task_created = 0
    task_updated = 0
    if with_default_schedules:
        sources_by_name = dict(source_sync_result["sources_by_name"])
        syncable_sources = [
            source
            for source in sources_by_name.values()
            if getattr(source, "source_kind", None)
            not in {"sse_stream", "websocket_stream", "webhook_ingest"}
        ]
        desired_tasks = [
            ScheduledTaskCreate(
                name=f"{schedule_prefix}-integrity-seed",
                task_type="integrity_seed",
                interval_seconds=integrity_seed_interval_seconds,
                retry_attempts=2,
                retry_backoff_seconds=5.0,
                notes="Refresh default integrity/trust profiles for the local runtime.",
            ),
            ScheduledTaskCreate(
                name=f"{schedule_prefix}-source-maintenance",
                task_type="source_maintenance",
                interval_seconds=source_maintenance_interval_seconds,
                retry_attempts=2,
                retry_backoff_seconds=5.0,
                notes="Replay dead letters and rerun stale/failing sources for the local runtime.",
                payload_json={
                    "stale_after_hours": 24.0,
                    "source_limit": 100,
                    "dead_letter_limit": 250,
                    "replay_dead_letters": True,
                    "run_stale_sources": True,
                    "run_failing_sources": True,
                },
            ),
            ScheduledTaskCreate(
                name=f"{schedule_prefix}-source-health-scan",
                task_type="source_health_scan",
                interval_seconds=source_health_scan_interval_seconds,
                retry_attempts=2,
                retry_backoff_seconds=5.0,
                notes="Promote stale/failing source conditions into persistent alert records.",
                payload_json={
                    "stale_after_hours": 24.0,
                    "source_limit": 250,
                    "alert_on_stale": True,
                    "alert_on_failed": True,
                    "alert_on_dead_letters": True,
                },
            ),
            ScheduledTaskCreate(
                name=f"{schedule_prefix}-storage-lifecycle",
                task_type="storage_lifecycle",
                interval_seconds=storage_lifecycle_interval_seconds,
                retry_attempts=2,
                retry_backoff_seconds=5.0,
                notes="Sweep expired local storage objects and keep the artifact ledger tidy.",
                payload_json={"limit": 250},
            ),
            ScheduledTaskCreate(
                name=f"{schedule_prefix}-runtime-snapshot",
                task_type="runtime_snapshot_export",
                interval_seconds=runtime_snapshot_interval_seconds,
                retry_attempts=2,
                retry_backoff_seconds=5.0,
                notes="Export verified runtime snapshot artifacts for local backup/recovery coverage.",
                payload_json={"file_prefix": "runtime-snapshot"},
            ),
            ScheduledTaskCreate(
                name=f"{schedule_prefix}-camera-refresh",
                task_type="camera_inventory_refresh",
                interval_seconds=camera_refresh_interval_seconds,
                retry_attempts=2,
                retry_backoff_seconds=5.0,
                layer_key=f"{layer_prefix}-cameras",
                notes="Materialize camera inventory from locally ingested camera observations.",
                payload_json={"limit": 500},
            ),
            ScheduledTaskCreate(
                name=f"{schedule_prefix}-camera-source-verify",
                task_type="camera_source_verification",
                interval_seconds=camera_source_verification_interval_seconds,
                retry_attempts=2,
                retry_backoff_seconds=5.0,
                layer_key=f"{layer_prefix}-cameras",
                notes="Verify discovered camera endpoints inside the local runtime.",
                payload_json={"limit": 200, "timeout_seconds": 5.0},
            ),
            ScheduledTaskCreate(
                name=f"{schedule_prefix}-entity-resolution",
                task_type="entity_resolution_refresh",
                interval_seconds=entity_resolution_interval_seconds,
                retry_attempts=2,
                retry_backoff_seconds=5.0,
                notes="Continuously promote observations into reusable entities.",
                payload_json={"limit": 500, "min_observations": 2},
            ),
            ScheduledTaskCreate(
                name=f"{schedule_prefix}-event-fusion",
                task_type="event_fusion_refresh",
                interval_seconds=event_fusion_interval_seconds,
                retry_attempts=2,
                retry_backoff_seconds=5.0,
                notes="Continuously fuse corroborated observations into events and products.",
                payload_json={
                    "limit": 500,
                    "time_window_minutes": 120,
                    "distance_km": 10.0,
                    "min_independent_signals": 2,
                },
            ),
        ]
        desired_tasks.extend(
            ScheduledTaskCreate(
                name=f"{schedule_prefix}-source-sync-{getattr(source, 'name')}",
                task_type="source_sync",
                interval_seconds=source_sync_interval_seconds,
                retry_attempts=3,
                retry_backoff_seconds=5.0,
                source_id=int(getattr(source, "source_id")),
                notes=f"Pull-sync managed source {getattr(source, 'name')} for the local runtime.",
            )
            for source in syncable_sources
        )
        for payload in desired_tasks:
            task, action = upsert_scheduled_task_by_name(session, payload, actor=actor_prefix)
            if action == "created":
                task_created += 1
            else:
                task_updated += 1
            task_messages.append(f"{action} task {task.task_id} -> {task.name}")

    initial_cycle_result: dict[str, object] | None = None
    if run_initial_cycle:
        initial_cycle_result = run_platform_runtime_cycle(
            session,
            include_stream_runtime=include_stream_runtime_in_initial_cycle,
            include_enabled_schedules=include_enabled_schedules_in_initial_cycle,
            task_types=initial_cycle_task_types or None,
            source_id=initial_cycle_source_id,
            actor=f"{actor_prefix}_initial_cycle",
        )

    return {
        "seeded_domains": seeded_domains,
        "source_sync_result": source_sync_result,
        "task_messages": task_messages,
        "task_created": task_created,
        "task_updated": task_updated,
        "initial_cycle_result": initial_cycle_result,
    }


def print_local_runtime_bootstrap_result(
    *,
    result: dict[str, object],
    with_demo_sources: bool,
    with_default_schedules: bool,
) -> None:
    typer.echo(f"integrity domains seeded this run: {len(result['seeded_domains'])}")
    source_sync_result = result["source_sync_result"]
    if with_demo_sources:
        for message in source_sync_result["messages"]:
            typer.echo(str(message))
        typer.echo(
            f"local sources | created={source_sync_result['created']} updated={source_sync_result['updated']}"
        )
    else:
        typer.echo("local sources | skipped")
    if with_default_schedules:
        for message in result["task_messages"]:
            typer.echo(message)
        typer.echo(f"local schedules | created={result['task_created']} updated={result['task_updated']}")
    else:
        typer.echo("local schedules | skipped")
    initial_cycle_result = result["initial_cycle_result"]
    if initial_cycle_result is not None:
        serializable_cycle = (
            TypeAdapter(PlatformRuntimeCycleRead)
            .validate_python(initial_cycle_result)
            .model_dump(mode="json")
        )
        typer.echo(
            "initial_cycle "
            f"streams={serializable_cycle['source_runtime']['source_count']} "
            f"source_runs={len(serializable_cycle['source_runtime']['source_run_ids'])} "
            f"stream_seen={serializable_cycle['source_runtime']['records_seen']} "
            f"schedules={serializable_cycle['schedules']['runs_created']} "
            f"schedule_failures={serializable_cycle['schedules']['failed_count']}"
        )
        if serializable_cycle["task_type_filters"]:
            typer.echo(f"initial_cycle_task_type_filters={serializable_cycle['task_type_filters']}")


def readiness_check_lookup(readiness_payload: dict[str, object]) -> dict[str, dict[str, object]]:
    checks = readiness_payload.get("checks")
    if not isinstance(checks, list):
        return {}
    rows: dict[str, dict[str, object]] = {}
    for item in checks:
        if not isinstance(item, dict):
            continue
        key = item.get("key")
        if isinstance(key, str):
            rows[key] = item
    return rows


def fetch_remote_json(url: str, *, timeout_seconds: float) -> tuple[int, dict[str, object]]:
    try:
        with urlopen(url, timeout=timeout_seconds) as response:
            status_code = int(getattr(response, "status", response.getcode()))
            payload = json.loads(response.read().decode("utf-8"))
            if not isinstance(payload, dict):
                raise RuntimeError(f"Endpoint {url} returned a non-object JSON payload.")
            return status_code, payload
    except HTTPError as exc:
        raw_body = exc.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(raw_body) if raw_body else {}
        except json.JSONDecodeError:
            payload = {"message": raw_body or str(exc)}
        if not isinstance(payload, dict):
            payload = {"message": raw_body or str(exc)}
        payload.setdefault("message", str(exc))
        return int(exc.code), payload
    except URLError as exc:
        raise RuntimeError(f"Could not reach {url}: {exc.reason}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Endpoint {url} returned invalid JSON: {exc}") from exc


def build_local_runtime_stack_report(
    *,
    api_base_url: str,
    upstreams_base_url: str,
    timeout_seconds: float,
    require_ready: bool,
    require_worker: bool,
    expected_worker_type: str,
    check_local_upstreams: bool,
    check_clickhouse: bool,
    check_database_diagnostics: bool,
    expected_database_backend: str | None,
    expected_spatial_backend: str | None,
    require_postgis_ready: bool,
    require_database_status_ok: bool,
    require_schema_current: bool,
) -> dict[str, object]:
    api_health_status, api_health = fetch_remote_json(
        f"{api_base_url.rstrip('/')}/health",
        timeout_seconds=timeout_seconds,
    )
    ready_status, readiness = fetch_remote_json(
        f"{api_base_url.rstrip('/')}/ready",
        timeout_seconds=timeout_seconds,
    )
    worker_status, worker_summary = fetch_remote_json(
        f"{api_base_url.rstrip('/')}/api/operations/workers/summary",
        timeout_seconds=timeout_seconds,
    )

    database_status = None
    database_payload: dict[str, object] | None = None
    if check_database_diagnostics or expected_database_backend or expected_spatial_backend or require_postgis_ready or require_database_status_ok or require_schema_current:
        database_status, database_payload = fetch_remote_json(
            f"{api_base_url.rstrip('/')}/api/operations/database",
            timeout_seconds=timeout_seconds,
        )

    local_upstreams_status = None
    local_upstreams_health: dict[str, object] | None = None
    if check_local_upstreams:
        local_upstreams_status, local_upstreams_health = fetch_remote_json(
            f"{upstreams_base_url.rstrip('/')}/health",
            timeout_seconds=timeout_seconds,
        )

    clickhouse_status = None
    clickhouse_payload: dict[str, object] | None = None
    if check_clickhouse:
        clickhouse_status, clickhouse_payload = fetch_remote_json(
            f"{api_base_url.rstrip('/')}/api/operations/clickhouse",
            timeout_seconds=timeout_seconds,
        )

    matching_workers = [
        worker
        for worker in list(worker_summary.get("workers", []))
        if isinstance(worker, dict) and worker.get("worker_type") == expected_worker_type
    ]
    active_matching_workers = [worker for worker in matching_workers if worker.get("status") == "active"]
    database_connected = bool((database_payload or {}).get("database_connected"))
    database_backend = (
        str((database_payload or {}).get("database_backend"))
        if (database_payload or {}).get("database_backend") is not None
        else None
    )
    spatial_backend = (
        str((database_payload or {}).get("spatial_backend"))
        if (database_payload or {}).get("spatial_backend") is not None
        else None
    )
    postgis_expected = bool((database_payload or {}).get("postgis_expected"))
    postgis_installed = (database_payload or {}).get("postgis_extension_installed")
    migration = (database_payload or {}).get("migration")
    migration_ok = (
        isinstance(migration, dict)
        and bool(migration.get("version_table_present"))
        and bool(migration.get("schema_up_to_date"))
    )
    postgis_ready = (not postgis_expected) or bool(postgis_installed)
    database_ok = database_status == 200 and database_connected
    if require_database_status_ok:
        database_ok = database_ok and str((database_payload or {}).get("status")) == "ok"
    if expected_database_backend is not None:
        database_ok = database_ok and database_backend == expected_database_backend
    if expected_spatial_backend is not None:
        database_ok = database_ok and spatial_backend == expected_spatial_backend
    if require_postgis_ready:
        database_ok = database_ok and postgis_ready
    if require_schema_current:
        database_ok = database_ok and migration_ok

    checks = [
        {
            "key": "api_health",
            "ok": api_health_status == 200 and str(api_health.get("status")) == "ok",
            "summary": f"http={api_health_status} status={api_health.get('status')}",
        },
        {
            "key": "api_ready",
            "ok": bool(readiness.get("ready")) if require_ready else ready_status in {200, 503},
            "summary": f"http={ready_status} status={readiness.get('overall_status')} ready={readiness.get('ready')}",
        },
        {
            "key": "worker_summary",
            "ok": bool(active_matching_workers) if require_worker else worker_status == 200,
            "summary": (
                f"http={worker_status} matched={len(matching_workers)} "
                f"active={len(active_matching_workers)} stale={worker_summary.get('stale_count')}"
            ),
        },
    ]
    if database_status is not None:
        checks.append(
            {
                "key": "database_diagnostics",
                "ok": database_ok,
                "summary": (
                    f"http={database_status} status={(database_payload or {}).get('status')} "
                    f"backend={database_backend} spatial={spatial_backend} "
                    f"connected={database_connected} postgis_ready={postgis_ready} "
                    f"schema_current={migration_ok}"
                ),
            }
        )
    if check_local_upstreams:
        checks.append(
            {
                "key": "local_upstreams",
                "ok": local_upstreams_status == 200 and str((local_upstreams_health or {}).get("status")) == "ok",
                "summary": (
                    f"http={local_upstreams_status} "
                    f"status={(local_upstreams_health or {}).get('status')}"
                ),
            }
        )
    if check_clickhouse:
        checks.append(
            {
                "key": "clickhouse",
                "ok": clickhouse_status == 200 and bool((clickhouse_payload or {}).get("enabled")) and bool((clickhouse_payload or {}).get("reachable")),
                "summary": (
                    f"http={clickhouse_status} enabled={(clickhouse_payload or {}).get('enabled')} "
                    f"reachable={(clickhouse_payload or {}).get('reachable')}"
                ),
            }
        )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "ok": all(bool(check["ok"]) for check in checks),
        "checks": checks,
        "api_health": {"http_status": api_health_status, "payload": api_health},
        "readiness": {"http_status": ready_status, "payload": readiness},
        "worker_summary": {
            "http_status": worker_status,
            "payload": worker_summary,
            "matching_worker_count": len(matching_workers),
            "active_matching_worker_count": len(active_matching_workers),
            "expected_worker_type": expected_worker_type,
        },
        "database_diagnostics": {
            "http_status": database_status,
            "payload": database_payload,
            "expected_database_backend": expected_database_backend,
            "expected_spatial_backend": expected_spatial_backend,
            "require_postgis_ready": require_postgis_ready,
            "require_database_status_ok": require_database_status_ok,
            "require_schema_current": require_schema_current,
        }
        if database_status is not None
        else None,
        "local_upstreams": {
            "http_status": local_upstreams_status,
            "payload": local_upstreams_health,
        }
        if check_local_upstreams
        else None,
        "clickhouse": {
            "http_status": clickhouse_status,
            "payload": clickhouse_payload,
        }
        if check_clickhouse
        else None,
    }


def print_source_run_result(action: str, source: object, source_run: object) -> None:
    print_banner()
    typer.echo(
        " | ".join(
            [
                f"source={getattr(source, 'source_id')}",
                f"action={action}",
                f"kind={getattr(source, 'source_kind')}",
                f"target={getattr(source, 'target_uri')}",
                f"run={getattr(source_run, 'source_run_id')}",
                f"status={getattr(source_run, 'status')}",
                f"seen={getattr(source_run, 'records_seen')}",
                f"imported={getattr(source_run, 'records_imported')}",
                f"skipped={getattr(source_run, 'records_skipped')}",
                f"failed={getattr(source_run, 'records_failed')}",
            ]
        )
    )


def print_storage_manifest(manifest: dict[str, object]) -> None:
    serializable = TypeAdapter(StorageManifestRead).validate_python(manifest).model_dump(mode="json")
    typer.echo(
        "manifest "
        f"canonical_uri={serializable['canonical_uri']} "
        f"transfer_status={serializable['transfer_status']} "
        f"archive_eligible={serializable['archive_eligible']} "
        f"prune_eligible={serializable['prune_eligible']} "
        f"managed={serializable['storage_managed']}"
    )
    for replica in serializable["replicas"]:
        typer.echo(
            "  replica "
            f"role={replica['role']} backend={replica['backend']} status={replica['status']} "
            f"uri={replica['uri']}"
        )


def print_storage_action_result(result: dict[str, object]) -> None:
    serializable = TypeAdapter(StorageActionResultRead).validate_python(result).model_dump(mode="json")
    storage_object = serializable["storage_object"]
    print_banner()
    typer.echo(
        " | ".join(
            [
                f"action={serializable['action']}",
                f"verified={serializable['verified']}",
                f"storage_object={storage_object['storage_object_id']}",
                f"tier={storage_object['storage_tier']}",
                f"retention={storage_object['retention_class']}",
                f"status={storage_object['lifecycle_status']}",
            ]
        )
    )
    typer.echo(f"message={serializable['message']}")
    print_storage_manifest(serializable["manifest"])


@app.command("status")
def status() -> None:
    settings = get_settings()
    print_banner()
    typer.echo(f"env: {settings.app_env}")
    typer.echo(f"database: {settings.database_url_effective}")
    typer.echo(f"spatial backend: {settings.spatial_backend}")
    typer.echo(f"data dir: {settings.data_dir_effective}")
    typer.echo(f"storage archive backend: {settings.storage_archive_backend}")
    typer.echo(f"storage archive dir: {settings.storage_archive_dir_effective}")
    typer.echo(f"storage rehydrate dir: {settings.storage_rehydrate_dir_effective}")
    typer.echo(f"clickhouse enabled: {settings.clickhouse_enabled}")
    typer.echo(f"clickhouse local-only: {settings.clickhouse_enabled and not settings.clickhouse_r2_configured}")


@app.command("doctor")
def doctor(output_path: Path | None = None) -> None:
    init_db()
    session = get_session_factory()()
    try:
        report = build_database_diagnostics(session)
        serializable = TypeAdapter(DatabaseDiagnosticsRead).validate_python(report).model_dump(mode="json")
        print_banner()
        typer.echo(
            "status="
            f"{serializable['status']} backend={serializable['database_backend']} "
            f"connected={serializable['database_connected']} spatial={serializable['spatial_backend']} "
            f"warnings={serializable['warning_count']}"
        )
        typer.echo(
            "postgis_expected="
            f"{serializable['postgis_expected']} "
            f"postgis_installed={serializable['postgis_extension_installed']} "
            f"postgis_version={serializable['postgis_version']}"
        )
        typer.echo(f"scheduler_poll_seconds={serializable['scheduler_poll_seconds']}")
        typer.echo("table_counts:")
        for item in serializable["table_counts"]:
            typer.echo(f"  {item['table_name']}: {item['row_count']}")
        if serializable["warnings"]:
            typer.echo("warnings:")
            for warning in serializable["warnings"]:
                typer.echo(f"  - {warning}")
        if serializable["notes"]:
            typer.echo("notes:")
            for note in serializable["notes"]:
                typer.echo(f"  - {note}")
        if output_path is not None:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(serializable, indent=2), encoding="utf-8")
            typer.echo(f"wrote diagnostics to {output_path}")
    finally:
        session.close()


@app.command("show-runtime-readiness")
def show_runtime_readiness(output_path: Path | None = None) -> None:
    init_db()
    session = get_session_factory()()
    try:
        report = build_runtime_readiness(session)
        serializable = TypeAdapter(RuntimeReadinessRead).validate_python(report).model_dump(mode="json")
        print_banner()
        typer.echo(
            "status="
            f"{serializable['overall_status']} ready={serializable['ready']} "
            f"checks={serializable['check_count']} action_required={serializable['action_required_count']} "
            f"warnings={serializable['warning_count']}"
        )
        typer.echo("checks:")
        for item in serializable["checks"]:
            typer.echo(f"  - {item['key']}: {item['status']} | {item['summary']}")
        if serializable["operator_actions"]:
            typer.echo("operator_actions:")
            for action in serializable["operator_actions"]:
                typer.echo(f"  - {action}")
        if output_path is not None:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(serializable, indent=2), encoding="utf-8")
            typer.echo(f"wrote readiness report to {output_path}")
    finally:
        session.close()


@app.command("verify-local-runtime-stack")
def verify_local_runtime_stack(
    api_base_url: str = "http://127.0.0.1:8000",
    upstreams_base_url: str = "http://127.0.0.1:8010",
    timeout_seconds: float = typer.Option(default=5.0, min=0.1, max=60.0),
    wait_seconds: float = typer.Option(default=0.0, min=0.0, max=600.0),
    poll_seconds: float = typer.Option(default=2.0, min=0.1, max=60.0),
    require_ready: bool = True,
    require_worker: bool = True,
    expected_worker_type: str = "platform_runtime_worker",
    check_local_upstreams: bool = True,
    check_clickhouse: bool = False,
    check_database_diagnostics: bool = False,
    expected_database_backend: str | None = None,
    expected_spatial_backend: str | None = None,
    require_postgis_ready: bool = False,
    require_database_status_ok: bool = False,
    require_schema_current: bool = False,
    output_path: Path | None = None,
) -> None:
    deadline = time.monotonic() + wait_seconds
    attempts = 0
    last_error: RuntimeError | None = None
    report: dict[str, object] | None = None
    while True:
        attempts += 1
        try:
            report = build_local_runtime_stack_report(
                api_base_url=api_base_url,
                upstreams_base_url=upstreams_base_url,
                timeout_seconds=timeout_seconds,
                require_ready=require_ready,
                require_worker=require_worker,
                expected_worker_type=expected_worker_type,
                check_local_upstreams=check_local_upstreams,
                check_clickhouse=check_clickhouse,
                check_database_diagnostics=check_database_diagnostics,
                expected_database_backend=expected_database_backend,
                expected_spatial_backend=expected_spatial_backend,
                require_postgis_ready=require_postgis_ready,
                require_database_status_ok=require_database_status_ok,
                require_schema_current=require_schema_current,
            )
            last_error = None
            if bool(report["ok"]) or time.monotonic() >= deadline:
                break
        except RuntimeError as exc:
            last_error = exc
            if time.monotonic() >= deadline:
                break
        time.sleep(poll_seconds)

    if report is None and last_error is not None:
        print_banner()
        typer.echo(f"runtime_stack ok=False attempts={attempts} error={last_error}")
        raise typer.Exit(code=1) from last_error

    assert report is not None

    print_banner()
    api_health = report["api_health"]
    readiness = report["readiness"]
    worker_summary = report["worker_summary"]
    typer.echo(
        "api_health "
        f"http={api_health['http_status']} status={api_health['payload'].get('status')} "
        f"database_connected={api_health['payload'].get('database_connected')} "
        f"spatial={api_health['payload'].get('spatial_backend')}"
    )
    typer.echo(
        "api_ready "
        f"http={readiness['http_status']} status={readiness['payload'].get('overall_status')} "
        f"ready={readiness['payload'].get('ready')} "
        f"action_required={readiness['payload'].get('action_required_count')} "
        f"warnings={readiness['payload'].get('warning_count')}"
    )
    typer.echo(
        "workers "
        f"http={worker_summary['http_status']} "
        f"expected_type={worker_summary['expected_worker_type']} "
        f"matched={worker_summary['matching_worker_count']} "
        f"active={worker_summary['active_matching_worker_count']} "
        f"stale={worker_summary['payload'].get('stale_count')}"
    )
    if report["database_diagnostics"] is not None:
        database = report["database_diagnostics"]
        payload = database["payload"] or {}
        migration = payload.get("migration") if isinstance(payload, dict) else {}
        typer.echo(
            "database "
            f"http={database['http_status']} status={payload.get('status')} "
            f"backend={payload.get('database_backend')} spatial={payload.get('spatial_backend')} "
            f"connected={payload.get('database_connected')} postgis_ready={((not bool(payload.get('postgis_expected'))) or bool(payload.get('postgis_extension_installed')))} "
            f"schema_current={bool(isinstance(migration, dict) and migration.get('version_table_present') and migration.get('schema_up_to_date'))}"
        )
    if report["local_upstreams"] is not None:
        local_upstreams = report["local_upstreams"]
        typer.echo(
            "local_upstreams "
            f"http={local_upstreams['http_status']} status={local_upstreams['payload'].get('status')}"
        )
    if report["clickhouse"] is not None:
        clickhouse = report["clickhouse"]
        typer.echo(
            "clickhouse "
            f"http={clickhouse['http_status']} enabled={clickhouse['payload'].get('enabled')} "
            f"reachable={clickhouse['payload'].get('reachable')} status={clickhouse['payload'].get('status')}"
        )
    typer.echo(f"runtime_stack ok={report['ok']} attempts={attempts}")
    failed_checks = [check for check in report["checks"] if not bool(check["ok"])]
    if failed_checks:
        typer.echo("failed_checks:")
        for check in failed_checks:
            typer.echo(f"  - {check['key']}: {check['summary']}")
    operator_actions = list(readiness["payload"].get("operator_actions", []))
    if operator_actions:
        typer.echo("operator_actions:")
        for action in operator_actions:
            typer.echo(f"  - {action}")
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        typer.echo(f"wrote runtime stack report to {output_path}")
    if not bool(report["ok"]):
        raise typer.Exit(code=1)


@app.command("show-clickhouse-status")
def show_clickhouse_status() -> None:
    serializable = TypeAdapter(ClickHouseDiagnosticsRead).validate_python(
        build_clickhouse_diagnostics()
    ).model_dump(mode="json")
    print_banner()
    typer.echo(
        "status="
        f"{serializable['status']} enabled={serializable['enabled']} "
        f"reachable={serializable['reachable']} database={serializable['clickhouse_database']}"
    )
    typer.echo(
        f"url={serializable['clickhouse_url']} observations={serializable['observation_table']} storage={serializable['storage_object_table']}"
    )
    typer.echo(
        "storage_mode="
        f"{serializable['storage_mode']} storage_policy={serializable['storage_policy']} "
        f"r2_configured={serializable['r2_configured']}"
    )
    typer.echo(
        f"r2_archive_root={serializable['r2_archive_root']} r2_storage_root={serializable['r2_storage_root']}"
    )
    if serializable["warnings"]:
        typer.echo("warnings:")
        for warning in serializable["warnings"]:
            typer.echo(f"  - {warning}")
    if serializable["notes"]:
        typer.echo("notes:")
        for note in serializable["notes"]:
            typer.echo(f"  - {note}")


@app.command("provision-clickhouse")
def provision_clickhouse_command() -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = provision_clickhouse_backend(session, actor="cli_clickhouse")
        serializable = TypeAdapter(ClickHouseProvisionResultRead).validate_python(result).model_dump(mode="json")
        print_banner()
        typer.echo(
            f"provisioned_at={serializable['provisioned_at']} database={serializable['clickhouse_database']} observations={serializable['observation_table']} storage={serializable['storage_object_table']}"
        )
    except (RuntimeError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("sync-clickhouse")
def sync_clickhouse_command(
    layer: str | None = None,
    source_domain: str | None = None,
    limit: int = 1000,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = sync_runtime_to_clickhouse(
            session,
            layer_key=layer,
            source_domain=source_domain,
            limit=limit,
            actor="cli_clickhouse",
        )
        serializable = TypeAdapter(ClickHouseSyncResultRead).validate_python(result).model_dump(mode="json")
        print_banner()
        typer.echo(
            f"synced observations={serializable['observation_count']} storage_objects={serializable['storage_object_count']} database={serializable['clickhouse_database']}"
        )
    except (RuntimeError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("archive-clickhouse-observations")
def archive_clickhouse_observations_command(
    layer: str | None = None,
    source_domain: str | None = None,
    limit: int | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = archive_clickhouse_observations_to_r2(
            session,
            layer_key=layer,
            source_domain=source_domain,
            limit=limit,
            actor="cli_clickhouse",
        )
        serializable = TypeAdapter(ClickHouseArchiveResultRead).validate_python(result).model_dump(mode="json")
        print_banner()
        typer.echo(
            f"archived rows={serializable['exported_row_count']} database={serializable['clickhouse_database']} root={serializable['archive_root_url']}"
        )
    except (RuntimeError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("show-clickhouse-r2-config")
def show_clickhouse_r2_config() -> None:
    try:
        result = build_clickhouse_r2_config_preview()
        serializable = TypeAdapter(ClickHouseR2ConfigRead).validate_python(result).model_dump(mode="json")
        print_banner()
        typer.echo(
            f"storage_mode={serializable['storage_mode']} storage_policy={serializable['storage_policy']}"
        )
        typer.echo(f"archive_root: {serializable['archive_root_url']}")
        typer.echo(f"storage_root: {serializable['storage_root_url']}")
        typer.echo(f"docker_output_path: {serializable['docker_output_path']}")
        typer.echo("storage_xml:")
        typer.echo(serializable["storage_xml"])
        typer.echo("create_table_sql:")
        typer.echo(serializable["create_table_sql"])
        typer.echo("archive_example_sql:")
        typer.echo(serializable["archive_example_sql"])
        typer.echo("rehydrate_example_sql:")
        typer.echo(serializable["rehydrate_example_sql"])
        typer.echo("direct_query_example_sql:")
        typer.echo(serializable["direct_query_example_sql"])
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc


@app.command("write-clickhouse-r2-config")
def write_clickhouse_r2_config(
    output_path: Path | None = None,
) -> None:
    try:
        result = build_clickhouse_r2_config_preview()
        serializable = TypeAdapter(ClickHouseR2ConfigRead).validate_python(result).model_dump(mode="json")
        target_path = output_path or default_clickhouse_r2_config_path()
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(serializable["storage_xml"] + "\n", encoding="utf-8")
        print_banner()
        typer.echo(f"wrote {target_path}")
        typer.echo(
            f"storage_mode={serializable['storage_mode']} storage_policy={serializable['storage_policy']}"
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc


@app.command("rehydrate-clickhouse-observations")
def rehydrate_clickhouse_observations_command(archive_glob_url: str) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = rehydrate_clickhouse_observations_from_r2(
            session,
            archive_glob_url=archive_glob_url,
            actor="cli_clickhouse",
        )
        serializable = TypeAdapter(ClickHouseRehydrateResultRead).validate_python(result).model_dump(mode="json")
        print_banner()
        typer.echo(
            f"rehydrated rows={serializable['imported_row_count']} database={serializable['clickhouse_database']} source={serializable['archive_glob_url']}"
        )
    except (RuntimeError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("init-db")
def init_database() -> None:
    init_db()
    typer.echo("database initialized")


@app.command("seed-integrity")
def seed_integrity() -> None:
    init_db()
    session = get_session_factory()()
    try:
        created = seed_default_integrity_sources(session)
        typer.echo(f"seeded {len(created)} integrity domains")
    finally:
        session.close()


@app.command("trust-profiles")
def trust_profiles() -> None:
    init_db()
    session = get_session_factory()()
    try:
        profiles = list(
            session.scalars(select(SourceTrustProfileORM).order_by(SourceTrustProfileORM.domain.asc()))
        )
        print_banner()
        for profile in profiles:
            typer.echo(
                f"{profile.domain} | {profile.trust_level} | {profile.approval_policy} | integrity={profile.integrity_source}"
            )
    finally:
        session.close()


@app.command("import-local")
def import_local(source_path: Path, layer: str = "unassigned", notes: str = "") -> None:
    init_db()
    session = get_session_factory()()
    try:
        run = import_local_path(session, str(source_path), layer, notes)
        print_banner()
        typer.echo(
            f"import_run={run.import_run_id} format={run.source_format} imported={run.records_imported} skipped={run.records_skipped}"
        )
    finally:
        session.close()


@app.command("list-imports")
def list_imports() -> None:
    init_db()
    session = get_session_factory()()
    try:
        runs = list(
            session.scalars(select(LocalImportRunORM).order_by(LocalImportRunORM.created_at.desc()))
        )
        print_banner()
        for run in runs:
            typer.echo(
                f"{run.import_run_id} | {run.source_format} | {run.layer_key} | imported={run.records_imported} | skipped={run.records_skipped} | {run.source_path}"
            )
    finally:
        session.close()


@app.command("add-layer")
def add_layer(
    key: str,
    name: str,
    description: str = "",
    temporal_resolution: str = "unknown",
    data_latency: str = "unknown",
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        layer = create_data_layer(
            session,
            DataLayerCreate(
                key=key,
                name=name,
                description=description,
                temporal_resolution=temporal_resolution,
                data_latency=data_latency,
                metadata_json={},
            ),
            actor="cli",
        )
        print_banner()
        typer.echo(f"layer {layer.layer_id} created for key={layer.key}")
    finally:
        session.close()


@app.command("list-layers")
def list_layers_command() -> None:
    init_db()
    session = get_session_factory()()
    try:
        rows = list_data_layers(session)
        print_banner()
        for row in rows:
            typer.echo(
                f"{row.layer_id} | {row.key} | {row.name} | resolution={row.temporal_resolution} | latency={row.data_latency}"
            )
    finally:
        session.close()


@app.command("add-source-file")
def add_source_file(
    name: str,
    source_path: Path,
    layer: str,
    notes: str = "",
    integrity_source: bool = False,
    skip_unchanged: bool = True,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        source = create_source_definition(
            session,
            SourceDefinitionCreate(
                name=name,
                source_kind="local_file",
                layer_key=layer,
                target_uri=str(source_path),
                notes=notes,
                integrity_source=integrity_source,
                metadata_json={"skip_unchanged": skip_unchanged},
            ),
        )
        print_banner()
        typer.echo(f"source {source.source_id} created for {source.target_uri}")
    finally:
        session.close()


@app.command("add-source-http-json")
def add_source_http_json(
    name: str,
    target_uri: str,
    layer: str,
    notes: str = "",
    integrity_source: bool = False,
    timeout_seconds: float = 30.0,
    retry_attempts: int = 3,
    retry_backoff_seconds: float = 0.0,
    skip_unchanged: bool = True,
    header: list[str] = typer.Option(default_factory=list),
    basic_auth_username: str | None = None,
    basic_auth_password_env: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        source = create_source_definition(
            session,
            SourceDefinitionCreate(
                name=name,
                source_kind="http_json",
                layer_key=layer,
                target_uri=target_uri,
                notes=notes,
                integrity_source=integrity_source,
                metadata_json=build_http_source_metadata(
                    timeout_seconds=timeout_seconds,
                    retry_attempts=retry_attempts,
                    retry_backoff_seconds=retry_backoff_seconds,
                    skip_unchanged=skip_unchanged,
                    header=header,
                    basic_auth_username=basic_auth_username,
                    basic_auth_password_env=basic_auth_password_env,
                ),
            ),
        )
        print_banner()
        typer.echo(f"source {source.source_id} created for {source.target_uri}")
    finally:
        session.close()


@app.command("add-source-http-text")
def add_source_http_text(
    name: str,
    target_uri: str,
    layer: str,
    notes: str = "",
    integrity_source: bool = False,
    timeout_seconds: float = 30.0,
    retry_attempts: int = 3,
    retry_backoff_seconds: float = 0.0,
    skip_unchanged: bool = True,
    header: list[str] = typer.Option(default_factory=list),
    basic_auth_username: str | None = None,
    basic_auth_password_env: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        source = create_source_definition(
            session,
            SourceDefinitionCreate(
                name=name,
                source_kind="http_text",
                layer_key=layer,
                target_uri=target_uri,
                notes=notes,
                integrity_source=integrity_source,
                metadata_json=build_http_source_metadata(
                    timeout_seconds=timeout_seconds,
                    retry_attempts=retry_attempts,
                    retry_backoff_seconds=retry_backoff_seconds,
                    skip_unchanged=skip_unchanged,
                    header=header,
                    basic_auth_username=basic_auth_username,
                    basic_auth_password_env=basic_auth_password_env,
                ),
            ),
        )
        print_banner()
        typer.echo(f"source {source.source_id} created for {source.target_uri}")
    finally:
        session.close()


@app.command("add-source-http-xml")
def add_source_http_xml(
    name: str,
    target_uri: str,
    layer: str,
    notes: str = "",
    integrity_source: bool = False,
    timeout_seconds: float = 30.0,
    retry_attempts: int = 3,
    retry_backoff_seconds: float = 0.0,
    skip_unchanged: bool = True,
    header: list[str] = typer.Option(default_factory=list),
    basic_auth_username: str | None = None,
    basic_auth_password_env: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        source = create_source_definition(
            session,
            SourceDefinitionCreate(
                name=name,
                source_kind="http_xml",
                layer_key=layer,
                target_uri=target_uri,
                notes=notes,
                integrity_source=integrity_source,
                metadata_json=build_http_source_metadata(
                    timeout_seconds=timeout_seconds,
                    retry_attempts=retry_attempts,
                    retry_backoff_seconds=retry_backoff_seconds,
                    skip_unchanged=skip_unchanged,
                    header=header,
                    basic_auth_username=basic_auth_username,
                    basic_auth_password_env=basic_auth_password_env,
                ),
            ),
        )
        print_banner()
        typer.echo(f"source {source.source_id} created for {source.target_uri}")
    finally:
        session.close()


@app.command("add-source-web-search")
def add_source_web_search(
    name: str,
    target_uri: str,
    layer: str,
    query: list[str] = typer.Option(default_factory=list),
    notes: str = "",
    integrity_source: bool = False,
    timeout_seconds: float = 30.0,
    retry_attempts: int = 3,
    retry_backoff_seconds: float = 0.0,
    skip_unchanged: bool = True,
    fetch_result_pages: bool = True,
    search_provider: str | None = None,
    search_page_limit: int = 1,
    search_page_param: str | None = None,
    search_page_start: int | None = None,
    search_page_step: int | None = None,
    search_result_limit: int = 25,
    page_fetch_limit: int = 25,
    crawl_from_results: bool = False,
    result_crawl_depth: int = 1,
    result_crawl_page_limit: int = 25,
    result_crawl_link_limit: int = 50,
    result_crawl_same_domain_only: bool = True,
    result_redirect_query_param: str | None = None,
    allowed_domain: list[str] = typer.Option(default_factory=list),
    include_url_pattern: list[str] = typer.Option(default_factory=list),
    exclude_url_pattern: list[str] = typer.Option(default_factory=list),
    crawl_allowed_domain: list[str] = typer.Option(default_factory=list),
    crawl_include_url_pattern: list[str] = typer.Option(default_factory=list),
    crawl_exclude_url_pattern: list[str] = typer.Option(default_factory=list),
    max_payload_bytes: int | None = None,
    allow_private_networks: bool | None = None,
    min_request_interval_seconds: float | None = None,
    header: list[str] = typer.Option(default_factory=list),
    basic_auth_username: str | None = None,
    basic_auth_password_env: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        metadata = build_http_source_metadata(
            timeout_seconds=timeout_seconds,
            retry_attempts=retry_attempts,
            retry_backoff_seconds=retry_backoff_seconds,
            skip_unchanged=skip_unchanged,
            header=header,
            basic_auth_username=basic_auth_username,
            basic_auth_password_env=basic_auth_password_env,
            max_payload_bytes=max_payload_bytes,
            allow_private_networks=allow_private_networks,
            min_request_interval_seconds=min_request_interval_seconds,
        )
        metadata.update(
            {
                "queries": query,
                "fetch_result_pages": fetch_result_pages,
                "search_page_limit": search_page_limit,
                "search_result_limit": search_result_limit,
                "page_fetch_limit": page_fetch_limit,
                "crawl_from_results": crawl_from_results,
                "result_crawl_depth": result_crawl_depth,
                "result_crawl_page_limit": result_crawl_page_limit,
                "result_crawl_link_limit": result_crawl_link_limit,
                "result_crawl_same_domain_only": result_crawl_same_domain_only,
                "allowed_domains": allowed_domain,
                "include_url_patterns": include_url_pattern,
                "exclude_url_patterns": exclude_url_pattern,
            }
        )
        if search_provider:
            metadata["search_provider"] = search_provider
        if search_page_param:
            metadata["search_page_param"] = search_page_param
        if search_page_start is not None:
            metadata["search_page_start"] = search_page_start
        if search_page_step is not None:
            metadata["search_page_step"] = search_page_step
        if result_redirect_query_param:
            metadata["result_redirect_query_param"] = result_redirect_query_param
        if crawl_allowed_domain:
            metadata["crawl_allowed_domains"] = crawl_allowed_domain
        if crawl_include_url_pattern:
            metadata["crawl_include_url_patterns"] = crawl_include_url_pattern
        if crawl_exclude_url_pattern:
            metadata["crawl_exclude_url_patterns"] = crawl_exclude_url_pattern
        source = create_source_definition(
            session,
            SourceDefinitionCreate(
                name=name,
                source_kind="web_search",
                layer_key=layer,
                target_uri=target_uri,
                notes=notes,
                integrity_source=integrity_source,
                metadata_json=metadata,
            ),
        )
        print_banner()
        typer.echo(f"source {source.source_id} created for {source.target_uri}")
    finally:
        session.close()


@app.command("list-web-search-providers")
def list_web_search_providers_command() -> None:
    print_banner()
    providers = TypeAdapter(list[WebSearchProviderRead]).validate_python(list_supported_web_search_providers())
    for provider in providers:
        typer.echo(
            " | ".join(
                [
                    provider.provider_name,
                    f"target={provider.default_target_uri or 'operator-supplied'}",
                    f"query={provider.query_param}",
                    f"page_param={provider.search_page_param or 'none'}",
                    f"page_start={provider.search_page_start}",
                    f"page_step={provider.search_page_step}",
                    f"redirect={provider.redirect_query_param or 'none'}",
                ]
            )
        )


@app.command("add-source-web-search-engine")
def add_source_web_search_engine(
    name: str,
    layer: str,
    query: list[str] = typer.Option(default_factory=list),
    provider: str = "duckduckgo_html",
    target_uri: str | None = None,
    notes: str = "",
    integrity_source: bool = False,
    timeout_seconds: float = 30.0,
    retry_attempts: int = 3,
    retry_backoff_seconds: float = 0.0,
    skip_unchanged: bool = True,
    fetch_result_pages: bool = True,
    search_page_limit: int = 2,
    search_result_limit: int = 25,
    page_fetch_limit: int = 25,
    crawl_from_results: bool = True,
    result_crawl_depth: int = 1,
    result_crawl_page_limit: int = 25,
    result_crawl_link_limit: int = 50,
    result_crawl_same_domain_only: bool = True,
    result_redirect_query_param: str | None = None,
    allowed_domain: list[str] = typer.Option(default_factory=list),
    include_url_pattern: list[str] = typer.Option(default_factory=list),
    exclude_url_pattern: list[str] = typer.Option(default_factory=list),
    crawl_allowed_domain: list[str] = typer.Option(default_factory=list),
    crawl_include_url_pattern: list[str] = typer.Option(default_factory=list),
    crawl_exclude_url_pattern: list[str] = typer.Option(default_factory=list),
    respect_robots_txt: bool = True,
    robots_user_agent: list[str] = typer.Option(default_factory=list),
    max_payload_bytes: int | None = None,
    allow_private_networks: bool | None = None,
    min_request_interval_seconds: float | None = None,
    header: list[str] = typer.Option(default_factory=list),
    basic_auth_username: str | None = None,
    basic_auth_password_env: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        metadata = build_http_source_metadata(
            timeout_seconds=timeout_seconds,
            retry_attempts=retry_attempts,
            retry_backoff_seconds=retry_backoff_seconds,
            skip_unchanged=skip_unchanged,
            header=header,
            basic_auth_username=basic_auth_username,
            basic_auth_password_env=basic_auth_password_env,
            max_payload_bytes=max_payload_bytes,
            allow_private_networks=allow_private_networks,
            min_request_interval_seconds=min_request_interval_seconds,
        )
        metadata.update(
            {
                "queries": query,
                "search_provider": provider,
                "fetch_result_pages": fetch_result_pages,
                "search_page_limit": search_page_limit,
                "search_result_limit": search_result_limit,
                "page_fetch_limit": page_fetch_limit,
                "crawl_from_results": crawl_from_results,
                "result_crawl_depth": result_crawl_depth,
                "result_crawl_page_limit": result_crawl_page_limit,
                "result_crawl_link_limit": result_crawl_link_limit,
                "result_crawl_same_domain_only": result_crawl_same_domain_only,
                "allowed_domains": allowed_domain,
                "include_url_patterns": include_url_pattern,
                "exclude_url_patterns": exclude_url_pattern,
                "respect_robots_txt": respect_robots_txt,
                "robots_user_agents": robots_user_agent,
            }
        )
        if result_redirect_query_param:
            metadata["result_redirect_query_param"] = result_redirect_query_param
        if crawl_allowed_domain:
            metadata["crawl_allowed_domains"] = crawl_allowed_domain
        if crawl_include_url_pattern:
            metadata["crawl_include_url_patterns"] = crawl_include_url_pattern
        if crawl_exclude_url_pattern:
            metadata["crawl_exclude_url_patterns"] = crawl_exclude_url_pattern
        resolved_target_uri = target_uri or f"search://web/{provider}"
        source = create_source_definition(
            session,
            SourceDefinitionCreate(
                name=name,
                source_kind="web_search",
                layer_key=layer,
                target_uri=resolved_target_uri,
                notes=notes,
                integrity_source=integrity_source,
                metadata_json=metadata,
            ),
        )
        print_banner()
        typer.echo(f"source {source.source_id} created for {source.target_uri}")
    finally:
        session.close()


@app.command("add-source-web-crawl")
def add_source_web_crawl(
    name: str,
    target_uri: str,
    layer: str,
    notes: str = "",
    integrity_source: bool = False,
    timeout_seconds: float = 30.0,
    retry_attempts: int = 3,
    retry_backoff_seconds: float = 0.0,
    skip_unchanged: bool = True,
    crawl_depth: int = 1,
    crawl_page_limit: int = 25,
    crawl_link_limit: int = 50,
    same_domain_only: bool = True,
    allowed_domain: list[str] = typer.Option(default_factory=list),
    include_url_pattern: list[str] = typer.Option(default_factory=list),
    exclude_url_pattern: list[str] = typer.Option(default_factory=list),
    respect_robots_txt: bool = True,
    robots_user_agent: list[str] = typer.Option(default_factory=list),
    max_payload_bytes: int | None = None,
    allow_private_networks: bool | None = None,
    min_request_interval_seconds: float | None = None,
    header: list[str] = typer.Option(default_factory=list),
    basic_auth_username: str | None = None,
    basic_auth_password_env: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        metadata = build_http_source_metadata(
            timeout_seconds=timeout_seconds,
            retry_attempts=retry_attempts,
            retry_backoff_seconds=retry_backoff_seconds,
            skip_unchanged=skip_unchanged,
            header=header,
            basic_auth_username=basic_auth_username,
            basic_auth_password_env=basic_auth_password_env,
            max_payload_bytes=max_payload_bytes,
            allow_private_networks=allow_private_networks,
            min_request_interval_seconds=min_request_interval_seconds,
        )
        metadata.update(
            {
                "crawl_depth": crawl_depth,
                "crawl_page_limit": crawl_page_limit,
                "crawl_link_limit": crawl_link_limit,
                "same_domain_only": same_domain_only,
                "allowed_domains": allowed_domain,
                "include_url_patterns": include_url_pattern,
                "exclude_url_patterns": exclude_url_pattern,
                "respect_robots_txt": respect_robots_txt,
                "robots_user_agents": robots_user_agent,
            }
        )
        source = create_source_definition(
            session,
            SourceDefinitionCreate(
                name=name,
                source_kind="web_crawl",
                layer_key=layer,
                target_uri=target_uri,
                notes=notes,
                integrity_source=integrity_source,
                metadata_json=metadata,
            ),
        )
        print_banner()
        typer.echo(f"source {source.source_id} created for {source.target_uri}")
    finally:
        session.close()


@app.command("add-source-web-discovery")
def add_source_web_discovery(
    name: str,
    layer: str,
    target_uri: str = "discover://web",
    query: list[str] = typer.Option(default_factory=list),
    provider: list[str] = typer.Option(default_factory=list),
    provider_target: list[str] = typer.Option(default_factory=list),
    seed_url: list[str] = typer.Option(default_factory=list),
    sitemap_url: list[str] = typer.Option(default_factory=list),
    notes: str = "",
    integrity_source: bool = False,
    timeout_seconds: float = 30.0,
    retry_attempts: int = 3,
    retry_backoff_seconds: float = 0.0,
    skip_unchanged: bool = True,
    fetch_result_pages: bool = True,
    search_page_limit: int = 2,
    search_result_limit: int = 25,
    page_fetch_limit: int = 10,
    crawl_depth: int = 1,
    crawl_page_limit: int = 50,
    crawl_link_limit: int = 50,
    same_domain_only: bool = False,
    discover_sitemaps_from_seeds: bool = True,
    sitemap_fetch_limit: int = 10,
    sitemap_url_limit: int = 250,
    resume_frontier: bool = True,
    allowed_domain: list[str] = typer.Option(default_factory=list),
    include_url_pattern: list[str] = typer.Option(default_factory=list),
    exclude_url_pattern: list[str] = typer.Option(default_factory=list),
    respect_robots_txt: bool = True,
    robots_user_agent: list[str] = typer.Option(default_factory=list),
    max_payload_bytes: int | None = None,
    allow_private_networks: bool | None = None,
    min_request_interval_seconds: float | None = None,
    header: list[str] = typer.Option(default_factory=list),
    basic_auth_username: str | None = None,
    basic_auth_password_env: str | None = None,
) -> None:
    if not query and not seed_url and not sitemap_url and not target_uri.startswith("http"):
        raise typer.BadParameter(
            "web discovery needs at least one query, seed_url, sitemap_url, or an http(s) target_uri."
        )
    init_db()
    session = get_session_factory()()
    try:
        metadata = build_http_source_metadata(
            timeout_seconds=timeout_seconds,
            retry_attempts=retry_attempts,
            retry_backoff_seconds=retry_backoff_seconds,
            skip_unchanged=skip_unchanged,
            header=header,
            basic_auth_username=basic_auth_username,
            basic_auth_password_env=basic_auth_password_env,
            max_payload_bytes=max_payload_bytes,
            allow_private_networks=allow_private_networks,
            min_request_interval_seconds=min_request_interval_seconds,
        )
        metadata.update(
            {
                "queries": query,
                "search_providers": provider,
                "fetch_result_pages": fetch_result_pages,
                "search_page_limit": search_page_limit,
                "search_result_limit": search_result_limit,
                "page_fetch_limit": page_fetch_limit,
                "seed_urls": seed_url,
                "sitemap_urls": sitemap_url,
                "crawl_depth": crawl_depth,
                "crawl_page_limit": crawl_page_limit,
                "crawl_link_limit": crawl_link_limit,
                "same_domain_only": same_domain_only,
                "discover_sitemaps_from_seeds": discover_sitemaps_from_seeds,
                "sitemap_fetch_limit": sitemap_fetch_limit,
                "sitemap_url_limit": sitemap_url_limit,
                "resume_frontier": resume_frontier,
                "allowed_domains": allowed_domain,
                "include_url_patterns": include_url_pattern,
                "exclude_url_patterns": exclude_url_pattern,
                "respect_robots_txt": respect_robots_txt,
                "robots_user_agents": robots_user_agent,
            }
        )
        provider_targets = parse_mapping_option(provider_target, "--provider-target")
        if provider_targets:
            metadata["search_provider_targets"] = provider_targets
        source = create_source_definition(
            session,
            SourceDefinitionCreate(
                name=name,
                source_kind="web_discovery",
                layer_key=layer,
                target_uri=target_uri,
                notes=notes,
                integrity_source=integrity_source,
                metadata_json=metadata,
            ),
        )
        print_banner()
        typer.echo(f"source {source.source_id} created for {source.target_uri}")
    finally:
        session.close()


@app.command("run-web-search-now")
def run_web_search_now(
    name: str,
    layer: str,
    query: list[str] = typer.Option(default_factory=list),
    provider: str = "duckduckgo_html",
    target_uri: str | None = None,
    notes: str = "",
    integrity_source: bool = False,
    timeout_seconds: float = 30.0,
    retry_attempts: int = 3,
    retry_backoff_seconds: float = 0.0,
    skip_unchanged: bool = True,
    fetch_result_pages: bool = True,
    search_page_limit: int = 2,
    search_result_limit: int = 25,
    page_fetch_limit: int = 25,
    crawl_from_results: bool = True,
    result_crawl_depth: int = 1,
    result_crawl_page_limit: int = 25,
    result_crawl_link_limit: int = 50,
    result_crawl_same_domain_only: bool = True,
    result_redirect_query_param: str | None = None,
    allowed_domain: list[str] = typer.Option(default_factory=list),
    include_url_pattern: list[str] = typer.Option(default_factory=list),
    exclude_url_pattern: list[str] = typer.Option(default_factory=list),
    crawl_allowed_domain: list[str] = typer.Option(default_factory=list),
    crawl_include_url_pattern: list[str] = typer.Option(default_factory=list),
    crawl_exclude_url_pattern: list[str] = typer.Option(default_factory=list),
    respect_robots_txt: bool = True,
    robots_user_agent: list[str] = typer.Option(default_factory=list),
    max_payload_bytes: int | None = None,
    allow_private_networks: bool | None = None,
    min_request_interval_seconds: float | None = None,
    header: list[str] = typer.Option(default_factory=list),
    basic_auth_username: str | None = None,
    basic_auth_password_env: str | None = None,
    upsert_existing: bool = True,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        metadata = build_http_source_metadata(
            timeout_seconds=timeout_seconds,
            retry_attempts=retry_attempts,
            retry_backoff_seconds=retry_backoff_seconds,
            skip_unchanged=skip_unchanged,
            header=header,
            basic_auth_username=basic_auth_username,
            basic_auth_password_env=basic_auth_password_env,
            max_payload_bytes=max_payload_bytes,
            allow_private_networks=allow_private_networks,
            min_request_interval_seconds=min_request_interval_seconds,
        )
        metadata.update(
            {
                "queries": query,
                "search_provider": provider,
                "fetch_result_pages": fetch_result_pages,
                "search_page_limit": search_page_limit,
                "search_result_limit": search_result_limit,
                "page_fetch_limit": page_fetch_limit,
                "crawl_from_results": crawl_from_results,
                "result_crawl_depth": result_crawl_depth,
                "result_crawl_page_limit": result_crawl_page_limit,
                "result_crawl_link_limit": result_crawl_link_limit,
                "result_crawl_same_domain_only": result_crawl_same_domain_only,
                "allowed_domains": allowed_domain,
                "include_url_patterns": include_url_pattern,
                "exclude_url_patterns": exclude_url_pattern,
                "respect_robots_txt": respect_robots_txt,
                "robots_user_agents": robots_user_agent,
            }
        )
        if result_redirect_query_param:
            metadata["result_redirect_query_param"] = result_redirect_query_param
        if crawl_allowed_domain:
            metadata["crawl_allowed_domains"] = crawl_allowed_domain
        if crawl_include_url_pattern:
            metadata["crawl_include_url_patterns"] = crawl_include_url_pattern
        if crawl_exclude_url_pattern:
            metadata["crawl_exclude_url_patterns"] = crawl_exclude_url_pattern
        result = upsert_and_run_source_definition(
            session,
            SourceDefinitionCreate(
                name=name,
                source_kind="web_search",
                layer_key=layer,
                target_uri=target_uri or f"search://web/{provider}",
                notes=notes,
                integrity_source=integrity_source,
                metadata_json=metadata,
            ),
            actor="cli_run_web_search",
            allow_update=upsert_existing,
        )
        print_source_run_result(result["action"], result["source"], result["source_run"])
    finally:
        session.close()


@app.command("run-web-crawl-now")
def run_web_crawl_now(
    name: str,
    target_uri: str,
    layer: str,
    notes: str = "",
    integrity_source: bool = False,
    timeout_seconds: float = 30.0,
    retry_attempts: int = 3,
    retry_backoff_seconds: float = 0.0,
    skip_unchanged: bool = True,
    crawl_depth: int = 1,
    crawl_page_limit: int = 25,
    crawl_link_limit: int = 50,
    same_domain_only: bool = True,
    allowed_domain: list[str] = typer.Option(default_factory=list),
    include_url_pattern: list[str] = typer.Option(default_factory=list),
    exclude_url_pattern: list[str] = typer.Option(default_factory=list),
    respect_robots_txt: bool = True,
    robots_user_agent: list[str] = typer.Option(default_factory=list),
    max_payload_bytes: int | None = None,
    allow_private_networks: bool | None = None,
    min_request_interval_seconds: float | None = None,
    header: list[str] = typer.Option(default_factory=list),
    basic_auth_username: str | None = None,
    basic_auth_password_env: str | None = None,
    upsert_existing: bool = True,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        metadata = build_http_source_metadata(
            timeout_seconds=timeout_seconds,
            retry_attempts=retry_attempts,
            retry_backoff_seconds=retry_backoff_seconds,
            skip_unchanged=skip_unchanged,
            header=header,
            basic_auth_username=basic_auth_username,
            basic_auth_password_env=basic_auth_password_env,
            max_payload_bytes=max_payload_bytes,
            allow_private_networks=allow_private_networks,
            min_request_interval_seconds=min_request_interval_seconds,
        )
        metadata.update(
            {
                "crawl_depth": crawl_depth,
                "crawl_page_limit": crawl_page_limit,
                "crawl_link_limit": crawl_link_limit,
                "same_domain_only": same_domain_only,
                "allowed_domains": allowed_domain,
                "include_url_patterns": include_url_pattern,
                "exclude_url_patterns": exclude_url_pattern,
                "respect_robots_txt": respect_robots_txt,
                "robots_user_agents": robots_user_agent,
            }
        )
        result = upsert_and_run_source_definition(
            session,
            SourceDefinitionCreate(
                name=name,
                source_kind="web_crawl",
                layer_key=layer,
                target_uri=target_uri,
                notes=notes,
                integrity_source=integrity_source,
                metadata_json=metadata,
            ),
            actor="cli_run_web_crawl",
            allow_update=upsert_existing,
        )
        print_source_run_result(result["action"], result["source"], result["source_run"])
    finally:
        session.close()


@app.command("run-web-discovery-now")
def run_web_discovery_now(
    name: str,
    layer: str,
    target_uri: str = "discover://web",
    query: list[str] = typer.Option(default_factory=list),
    provider: list[str] = typer.Option(default_factory=list),
    provider_target: list[str] = typer.Option(default_factory=list),
    seed_url: list[str] = typer.Option(default_factory=list),
    sitemap_url: list[str] = typer.Option(default_factory=list),
    notes: str = "",
    integrity_source: bool = False,
    timeout_seconds: float = 30.0,
    retry_attempts: int = 3,
    retry_backoff_seconds: float = 0.0,
    skip_unchanged: bool = True,
    fetch_result_pages: bool = True,
    search_page_limit: int = 2,
    search_result_limit: int = 25,
    page_fetch_limit: int = 10,
    crawl_depth: int = 1,
    crawl_page_limit: int = 50,
    crawl_link_limit: int = 50,
    same_domain_only: bool = False,
    discover_sitemaps_from_seeds: bool = True,
    sitemap_fetch_limit: int = 10,
    sitemap_url_limit: int = 250,
    resume_frontier: bool = True,
    allowed_domain: list[str] = typer.Option(default_factory=list),
    include_url_pattern: list[str] = typer.Option(default_factory=list),
    exclude_url_pattern: list[str] = typer.Option(default_factory=list),
    respect_robots_txt: bool = True,
    robots_user_agent: list[str] = typer.Option(default_factory=list),
    max_payload_bytes: int | None = None,
    allow_private_networks: bool | None = None,
    min_request_interval_seconds: float | None = None,
    header: list[str] = typer.Option(default_factory=list),
    basic_auth_username: str | None = None,
    basic_auth_password_env: str | None = None,
    upsert_existing: bool = True,
) -> None:
    if not query and not seed_url and not sitemap_url and not target_uri.startswith("http"):
        raise typer.BadParameter(
            "web discovery needs at least one query, seed_url, sitemap_url, or an http(s) target_uri."
        )
    init_db()
    session = get_session_factory()()
    try:
        metadata = build_http_source_metadata(
            timeout_seconds=timeout_seconds,
            retry_attempts=retry_attempts,
            retry_backoff_seconds=retry_backoff_seconds,
            skip_unchanged=skip_unchanged,
            header=header,
            basic_auth_username=basic_auth_username,
            basic_auth_password_env=basic_auth_password_env,
            max_payload_bytes=max_payload_bytes,
            allow_private_networks=allow_private_networks,
            min_request_interval_seconds=min_request_interval_seconds,
        )
        metadata.update(
            {
                "queries": query,
                "search_providers": provider,
                "fetch_result_pages": fetch_result_pages,
                "search_page_limit": search_page_limit,
                "search_result_limit": search_result_limit,
                "page_fetch_limit": page_fetch_limit,
                "seed_urls": seed_url,
                "sitemap_urls": sitemap_url,
                "crawl_depth": crawl_depth,
                "crawl_page_limit": crawl_page_limit,
                "crawl_link_limit": crawl_link_limit,
                "same_domain_only": same_domain_only,
                "discover_sitemaps_from_seeds": discover_sitemaps_from_seeds,
                "sitemap_fetch_limit": sitemap_fetch_limit,
                "sitemap_url_limit": sitemap_url_limit,
                "resume_frontier": resume_frontier,
                "allowed_domains": allowed_domain,
                "include_url_patterns": include_url_pattern,
                "exclude_url_patterns": exclude_url_pattern,
                "respect_robots_txt": respect_robots_txt,
                "robots_user_agents": robots_user_agent,
            }
        )
        provider_targets = parse_mapping_option(provider_target, "--provider-target")
        if provider_targets:
            metadata["search_provider_targets"] = provider_targets
        result = upsert_and_run_source_definition(
            session,
            SourceDefinitionCreate(
                name=name,
                source_kind="web_discovery",
                layer_key=layer,
                target_uri=target_uri,
                notes=notes,
                integrity_source=integrity_source,
                metadata_json=metadata,
            ),
            actor="cli_run_web_discovery",
            allow_update=upsert_existing,
        )
        print_source_run_result(result["action"], result["source"], result["source_run"])
    finally:
        session.close()


@app.command("add-source-sse")
def add_source_sse(
    name: str,
    target_uri: str,
    layer: str,
    notes: str = "",
    integrity_source: bool = False,
    timeout_seconds: float = 30.0,
    stream_idle_timeout_seconds: float = 1.0,
    stream_max_records: int = 100,
    skip_unchanged: bool = False,
    header: list[str] = typer.Option(default_factory=list),
    basic_auth_username: str | None = None,
    basic_auth_password_env: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        metadata = build_http_source_metadata(
            timeout_seconds=timeout_seconds,
            retry_attempts=1,
            retry_backoff_seconds=0.0,
            skip_unchanged=skip_unchanged,
            header=header,
            basic_auth_username=basic_auth_username,
            basic_auth_password_env=basic_auth_password_env,
        )
        metadata["stream_idle_timeout_seconds"] = stream_idle_timeout_seconds
        metadata["stream_max_records"] = stream_max_records
        source = create_source_definition(
            session,
            SourceDefinitionCreate(
                name=name,
                source_kind="sse_stream",
                layer_key=layer,
                target_uri=target_uri,
                notes=notes,
                integrity_source=integrity_source,
                metadata_json=metadata,
            ),
        )
        print_banner()
        typer.echo(f"source {source.source_id} created for {source.target_uri}")
    finally:
        session.close()


@app.command("add-source-websocket")
def add_source_websocket(
    name: str,
    target_uri: str,
    layer: str,
    notes: str = "",
    integrity_source: bool = False,
    timeout_seconds: float = 30.0,
    stream_idle_timeout_seconds: float = 1.0,
    stream_max_records: int = 100,
    skip_unchanged: bool = False,
    header: list[str] = typer.Option(default_factory=list),
    metadata_json: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        metadata = build_http_source_metadata(
            timeout_seconds=timeout_seconds,
            retry_attempts=1,
            retry_backoff_seconds=0.0,
            skip_unchanged=skip_unchanged,
            header=header,
            basic_auth_username=None,
            basic_auth_password_env=None,
        )
        metadata["stream_idle_timeout_seconds"] = stream_idle_timeout_seconds
        metadata["stream_max_records"] = stream_max_records
        extra_metadata = parse_json_object_option(metadata_json, "--metadata-json")
        if extra_metadata is not None:
            metadata.update(extra_metadata)
        source = create_source_definition(
            session,
            SourceDefinitionCreate(
                name=name,
                source_kind="websocket_stream",
                layer_key=layer,
                target_uri=target_uri,
                notes=notes,
                integrity_source=integrity_source,
                metadata_json=metadata,
            ),
        )
        print_banner()
        typer.echo(f"source {source.source_id} created for {source.target_uri}")
    finally:
        session.close()


@app.command("add-source-webhook")
def add_source_webhook(
    name: str,
    layer: str,
    target_uri: str = "webhook://local",
    notes: str = "",
    integrity_source: bool = False,
    skip_unchanged: bool = False,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        source = create_source_definition(
            session,
            SourceDefinitionCreate(
                name=name,
                source_kind="webhook_ingest",
                layer_key=layer,
                target_uri=target_uri,
                notes=notes,
                integrity_source=integrity_source,
                metadata_json={"skip_unchanged": skip_unchanged},
            ),
        )
        print_banner()
        typer.echo(f"source {source.source_id} created for {source.target_uri}")
        typer.echo(f"webhook endpoint: /api/sources/{source.source_id}/webhook")
    finally:
        session.close()


@app.command("seed-local-live-sources")
def seed_local_live_sources(
    base_url: str = "http://127.0.0.1:8010",
    layer_prefix: str = "local-live",
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = sync_local_live_sources(
            session,
            base_url=base_url,
            layer_prefix=layer_prefix,
            actor="cli",
        )
        print_banner()
        for message in result["messages"]:
            typer.echo(str(message))
        typer.echo(
            f"seeded local live sources | created={result['created']} updated={result['updated']}"
        )
    finally:
        session.close()


@app.command("bootstrap-local-runtime")
def bootstrap_local_runtime(
    base_url: str = "http://127.0.0.1:8010",
    layer_prefix: str = "local-live",
    schedule_prefix: str = "local-runtime",
    with_demo_sources: bool = True,
    with_default_schedules: bool = True,
    run_initial_cycle: bool = False,
    include_stream_runtime_in_initial_cycle: bool = True,
    include_enabled_schedules_in_initial_cycle: bool = True,
    initial_cycle_task_type: list[str] = typer.Option(default_factory=list),
    initial_cycle_source_id: int | None = None,
    source_sync_interval_seconds: int = 300,
    source_maintenance_interval_seconds: int = 600,
    source_health_scan_interval_seconds: int = 600,
    camera_refresh_interval_seconds: int = 900,
    camera_source_verification_interval_seconds: int = 900,
    entity_resolution_interval_seconds: int = 900,
    event_fusion_interval_seconds: int = 900,
    storage_lifecycle_interval_seconds: int = 3600,
    runtime_snapshot_interval_seconds: int = 21600,
    integrity_seed_interval_seconds: int = 43200,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = perform_local_runtime_bootstrap(
            session,
            base_url=base_url,
            layer_prefix=layer_prefix,
            schedule_prefix=schedule_prefix,
            with_demo_sources=with_demo_sources,
            with_default_schedules=with_default_schedules,
            run_initial_cycle=run_initial_cycle,
            include_stream_runtime_in_initial_cycle=include_stream_runtime_in_initial_cycle,
            include_enabled_schedules_in_initial_cycle=include_enabled_schedules_in_initial_cycle,
            initial_cycle_task_types=initial_cycle_task_type,
            initial_cycle_source_id=initial_cycle_source_id,
            source_sync_interval_seconds=source_sync_interval_seconds,
            source_maintenance_interval_seconds=source_maintenance_interval_seconds,
            source_health_scan_interval_seconds=source_health_scan_interval_seconds,
            camera_refresh_interval_seconds=camera_refresh_interval_seconds,
            camera_source_verification_interval_seconds=camera_source_verification_interval_seconds,
            entity_resolution_interval_seconds=entity_resolution_interval_seconds,
            event_fusion_interval_seconds=event_fusion_interval_seconds,
            storage_lifecycle_interval_seconds=storage_lifecycle_interval_seconds,
            runtime_snapshot_interval_seconds=runtime_snapshot_interval_seconds,
            integrity_seed_interval_seconds=integrity_seed_interval_seconds,
            actor_prefix="cli_bootstrap",
        )
        print_banner()
        print_local_runtime_bootstrap_result(
            result=result,
            with_demo_sources=with_demo_sources,
            with_default_schedules=with_default_schedules,
        )
        typer.echo("next: run `elevenwriter platform-runtime-worker --poll-seconds 5`")
    finally:
        session.close()


@app.command("smoke-test-local-runtime")
def smoke_test_local_runtime(
    base_url: str = "http://127.0.0.1:8010",
    layer_prefix: str = "local-live",
    schedule_prefix: str = "local-runtime",
    with_demo_sources: bool = True,
    with_default_schedules: bool = True,
    run_initial_cycle: bool = True,
    include_stream_runtime_in_initial_cycle: bool = True,
    include_enabled_schedules_in_initial_cycle: bool = True,
    initial_cycle_task_type: list[str] = typer.Option(default_factory=list),
    initial_cycle_source_id: int | None = None,
    run_worker_once: bool = True,
    worker_include_stream_runtime: bool = True,
    worker_include_enabled_schedules: bool = True,
    worker_task_type: list[str] = typer.Option(default_factory=list),
    worker_source_id: int | None = None,
    source_sync_interval_seconds: int = 300,
    source_maintenance_interval_seconds: int = 600,
    source_health_scan_interval_seconds: int = 600,
    camera_refresh_interval_seconds: int = 900,
    camera_source_verification_interval_seconds: int = 900,
    entity_resolution_interval_seconds: int = 900,
    event_fusion_interval_seconds: int = 900,
    storage_lifecycle_interval_seconds: int = 3600,
    runtime_snapshot_interval_seconds: int = 21600,
    integrity_seed_interval_seconds: int = 43200,
) -> None:
    init_db()
    bootstrap_session = get_session_factory()()
    try:
        bootstrap_result = perform_local_runtime_bootstrap(
            bootstrap_session,
            base_url=base_url,
            layer_prefix=layer_prefix,
            schedule_prefix=schedule_prefix,
            with_demo_sources=with_demo_sources,
            with_default_schedules=with_default_schedules,
            run_initial_cycle=run_initial_cycle,
            include_stream_runtime_in_initial_cycle=include_stream_runtime_in_initial_cycle,
            include_enabled_schedules_in_initial_cycle=include_enabled_schedules_in_initial_cycle,
            initial_cycle_task_types=initial_cycle_task_type,
            initial_cycle_source_id=initial_cycle_source_id,
            source_sync_interval_seconds=source_sync_interval_seconds,
            source_maintenance_interval_seconds=source_maintenance_interval_seconds,
            source_health_scan_interval_seconds=source_health_scan_interval_seconds,
            camera_refresh_interval_seconds=camera_refresh_interval_seconds,
            camera_source_verification_interval_seconds=camera_source_verification_interval_seconds,
            entity_resolution_interval_seconds=entity_resolution_interval_seconds,
            event_fusion_interval_seconds=event_fusion_interval_seconds,
            storage_lifecycle_interval_seconds=storage_lifecycle_interval_seconds,
            runtime_snapshot_interval_seconds=runtime_snapshot_interval_seconds,
            integrity_seed_interval_seconds=integrity_seed_interval_seconds,
            actor_prefix="cli_smoke_test",
        )
    finally:
        bootstrap_session.close()

    worker_result = None
    if run_worker_once:
        worker_result = run_platform_runtime_worker(
            get_session_factory(),
            poll_seconds=0.0,
            actor="cli_local_runtime_smoke_test",
            once=True,
            max_iterations=1,
            include_stream_runtime=worker_include_stream_runtime,
            include_enabled_schedules=worker_include_enabled_schedules,
            task_types=worker_task_type or None,
            source_id=worker_source_id,
        )

    validate_demo_camera_runtime = with_demo_sources and (
        (
            run_initial_cycle
            and include_stream_runtime_in_initial_cycle
            and include_enabled_schedules_in_initial_cycle
        )
        or (
            run_worker_once
            and worker_include_stream_runtime
            and worker_include_enabled_schedules
        )
    )
    validate_demo_web_runtime = with_demo_sources and with_default_schedules and (
        (
            run_initial_cycle
            and include_enabled_schedules_in_initial_cycle
            and task_filters_include(initial_cycle_task_type, "source_sync")
        )
        or (
            run_worker_once
            and worker_include_enabled_schedules
            and task_filters_include(worker_task_type, "source_sync")
        )
    )

    validation_session = get_session_factory()()
    try:
        diagnostics = build_database_diagnostics(validation_session)
        readiness = build_runtime_readiness(validation_session)
        readiness_checks = readiness_check_lookup(readiness)
        camera_inventory_count = int(
            validation_session.scalar(select(func.count()).select_from(CameraInventoryORM)) or 0
        )
        camera_source_inventory_count = int(
            validation_session.scalar(select(func.count()).select_from(CameraSourceInventoryORM)) or 0
        )
        reachable_camera_source_count = int(
            validation_session.scalar(
                select(func.count())
                .select_from(CameraSourceInventoryORM)
                .where(CameraSourceInventoryORM.verification_state == "reachable")
            )
            or 0
        )
        entity_count = int(validation_session.scalar(select(func.count()).select_from(EntityORM)) or 0)
        event_count = int(validation_session.scalar(select(func.count()).select_from(EventORM)) or 0)
        situation_product_count = int(
            validation_session.scalar(select(func.count()).select_from(SituationProductORM)) or 0
        )
        runtime_snapshot_count = int(
            validation_session.scalar(
                select(func.count())
                .select_from(StorageObjectORM)
                .where(StorageObjectORM.object_kind == "runtime_snapshot_export")
            )
            or 0
        )
        runtime_snapshot_manifest_count = int(
            validation_session.scalar(
                select(func.count())
                .select_from(StorageObjectORM)
                .where(StorageObjectORM.object_kind == "runtime_snapshot_manifest_export")
            )
            or 0
        )
        web_layer_prefixes = {
            "web_search": f"{layer_prefix}-web-search",
            "web_discovery": f"{layer_prefix}-web-discovery",
            "web_crawl": f"{layer_prefix}-web-crawl",
        }
        web_observation_counts = {
            kind: int(
                validation_session.scalar(
                    select(func.count())
                    .select_from(ObservationORM)
                    .where(ObservationORM.layer_key == scoped_layer_key)
                )
                or 0
            )
            for kind, scoped_layer_key in web_layer_prefixes.items()
        }
        web_sources = list(
            validation_session.scalars(
                select(SourceDefinitionORM)
                .where(
                    SourceDefinitionORM.layer_key.in_(list(web_layer_prefixes.values())),
                    SourceDefinitionORM.source_kind.in_(["web_search", "web_discovery", "web_crawl"]),
                )
                .order_by(SourceDefinitionORM.source_id.asc())
            )
        )
        web_source_ids = [source.source_id for source in web_sources]
        web_source_run_count = (
            int(
                validation_session.scalar(
                    select(func.count())
                    .select_from(SourceRunORM)
                    .where(SourceRunORM.source_id.in_(web_source_ids))
                )
                or 0
            )
            if web_source_ids
            else 0
        )
        web_checkpoint_count = (
            int(
                validation_session.scalar(
                    select(func.count())
                    .select_from(SourceCheckpointORM)
                    .where(SourceCheckpointORM.source_id.in_(web_source_ids))
                )
                or 0
            )
            if web_source_ids
            else 0
        )
        diagnostics_json = TypeAdapter(DatabaseDiagnosticsRead).validate_python(diagnostics).model_dump(mode="json")
        readiness_json = TypeAdapter(RuntimeReadinessRead).validate_python(readiness).model_dump(mode="json")
        print_banner()
        print_local_runtime_bootstrap_result(
            result=bootstrap_result,
            with_demo_sources=with_demo_sources,
            with_default_schedules=with_default_schedules,
        )
        if worker_result is not None:
            typer.echo(
                "worker_once "
                f"iterations={worker_result.iterations} schedules={worker_result.runs_created} "
                f"source_runs={len(worker_result.source_run_ids)} seen={worker_result.records_seen} "
                f"imported={worker_result.records_imported} failed={worker_result.records_failed}"
            )
        typer.echo(
            "database "
            f"status={diagnostics_json['status']} connected={diagnostics_json['database_connected']} "
            f"backend={diagnostics_json['database_backend']} warnings={diagnostics_json['warning_count']}"
        )
        typer.echo(
            "readiness "
            f"status={readiness_json['overall_status']} ready={readiness_json['ready']} "
            f"checks={readiness_json['check_count']} action_required={readiness_json['action_required_count']} "
            f"warnings={readiness_json['warning_count']}"
        )
        typer.echo(
            "camera_runtime "
            f"inventory={camera_inventory_count} source_candidates={camera_source_inventory_count} "
            f"reachable_sources={reachable_camera_source_count}"
        )
        typer.echo(
            "web_runtime "
            f"web_search_obs={web_observation_counts['web_search']} "
            f"web_discovery_obs={web_observation_counts['web_discovery']} "
            f"web_crawl_obs={web_observation_counts['web_crawl']} "
            f"source_runs={web_source_run_count} checkpoints={web_checkpoint_count}"
        )
        typer.echo(
            "analytics_runtime "
            f"events={event_count} entities={entity_count} products={situation_product_count}"
        )
        typer.echo(
            "backup_runtime "
            f"snapshots={runtime_snapshot_count} manifests={runtime_snapshot_manifest_count}"
        )
        snapshot_check = readiness_checks.get("runtime_snapshot_coverage")
        if snapshot_check is not None:
            typer.echo(
                "backup_coverage "
                f"status={snapshot_check['status']} summary={snapshot_check['summary']}"
            )
        if readiness_json["operator_actions"]:
            typer.echo("operator_actions:")
            for action in readiness_json["operator_actions"]:
                typer.echo(f"  - {action}")
    finally:
        validation_session.close()

    if validate_demo_camera_runtime and (
        camera_inventory_count < 1
        or camera_source_inventory_count < 1
        or reachable_camera_source_count < 1
    ):
        raise typer.Exit(code=1)
    if validate_demo_web_runtime and (
        web_observation_counts["web_search"] < 1
        or web_observation_counts["web_discovery"] < 1
        or web_observation_counts["web_crawl"] < 1
        or web_source_run_count < 3
        or web_checkpoint_count < 3
    ):
        raise typer.Exit(code=1)

    if not diagnostics_json["database_connected"] or not readiness_json["ready"]:
        raise typer.Exit(code=1)


@app.command("list-sources")
def list_sources() -> None:
    init_db()
    session = get_session_factory()()
    try:
        rows = list_source_definitions(session)
        print_banner()
        for row in rows:
            typer.echo(
                f"{row.source_id} | {row.source_kind} | {row.layer_key} | enabled={row.enabled} | {row.name}"
            )
    finally:
        session.close()


@app.command("run-source")
def run_source(source_id: int) -> None:
    init_db()
    session = get_session_factory()()
    try:
        run = run_source_definition(session, source_id, actor="cli")
        print_banner()
        typer.echo(
            f"source_run={run.source_run_id} import_run={run.import_run_id} records={run.records_imported}"
        )
    finally:
        session.close()


@app.command("run-source-runtime")
def run_source_runtime_command(source_id: int) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = run_source_runtime_cycle(session, source_id=source_id, actor="cli_source_runtime")
        print_banner()
        typer.echo(
            f"runtime source_count={result['source_count']} runs={result['source_run_ids']} "
            f"seen={result['records_seen']} imported={result['records_imported']} failed={result['records_failed']}"
        )
    finally:
        session.close()


@app.command("update-source")
def update_source_command(
    source_id: int,
    name: str | None = None,
    layer: str | None = None,
    target_uri: str | None = None,
    enabled: bool | None = typer.Option(default=None),
    integrity_source: bool | None = typer.Option(default=None),
    notes: str | None = None,
    metadata_json: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        payload: dict[str, object] = {}
        if name is not None:
            payload["name"] = name
        if layer is not None:
            payload["layer_key"] = layer
        if target_uri is not None:
            payload["target_uri"] = target_uri
        if enabled is not None:
            payload["enabled"] = enabled
        if integrity_source is not None:
            payload["integrity_source"] = integrity_source
        if notes is not None:
            payload["notes"] = notes
        parsed_metadata = parse_json_object_option(metadata_json, "--metadata-json")
        if parsed_metadata is not None:
            payload["metadata_json"] = parsed_metadata
        source = update_source_definition(
            session,
            source_id,
            SourceDefinitionUpdate(**payload),
            actor="cli",
        )
        print_banner()
        typer.echo(
            f"source {source.source_id} | enabled={source.enabled} | layer={source.layer_key} | target={source.target_uri}"
        )
    finally:
        session.close()


@app.command("list-source-runs")
def list_source_runs_command() -> None:
    init_db()
    session = get_session_factory()()
    try:
        rows = list_source_runs(session)
        print_banner()
        for row in rows:
            typer.echo(
                f"{row.source_run_id} | source={row.source_id} | status={row.status} | records={row.records_imported}"
            )
    finally:
        session.close()


@app.command("list-source-checkpoints")
def list_source_checkpoints_command(source_id: int | None = typer.Option(default=None)) -> None:
    init_db()
    session = get_session_factory()()
    try:
        rows = list_source_checkpoints(session)
        if source_id is not None:
            rows = [row for row in rows if row.source_id == source_id]
        print_banner()
        for row in rows:
            typer.echo(
                f"{row.source_checkpoint_id} | source={row.source_id} | {row.adapter_kind} | {row.fetch_mode} | "
                f"status={row.status} | last_event_id={row.last_event_id} | last_offset={row.last_offset} | "
                f"success={row.last_success_at} | failure={row.last_failure_at} | failures={row.failure_count}"
            )
    finally:
        session.close()


@app.command("list-source-dead-letters")
def list_source_dead_letters_command(
    source_id: int | None = None,
    status: str | None = None,
    limit: int = 200,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        rows = list_source_dead_letters(session, source_id=source_id, status=status, limit=limit)
        print_banner()
        for row in rows:
            typer.echo(
                f"{row.source_dead_letter_id} | source={row.source_id} | run={row.source_run_id} | "
                f"{row.source_kind} | stage={row.stage} | status={row.status} | "
                f"replays={row.replay_count} | created={row.created_at} | reason={row.failure_reason}"
            )
    finally:
        session.close()


@app.command("replay-source-dead-letter")
def replay_source_dead_letter_command(source_dead_letter_id: int) -> None:
    init_db()
    session = get_session_factory()()
    try:
        record = replay_dead_letter_record(session, source_dead_letter_id, actor="cli_source_replay")
        print_banner()
        typer.echo(
            f"dead_letter={record.source_dead_letter_id} source={record.source_id} status={record.status} "
            f"replay_count={record.replay_count} last_replayed_at={record.last_replayed_at}"
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("push-source-webhook")
def push_source_webhook_command(
    source_id: int,
    payload_json: str | None = None,
    payload_text: str | None = None,
    payload_file: Path | None = None,
    content_type: str | None = None,
) -> None:
    provided_inputs = [payload_json is not None, payload_text is not None, payload_file is not None]
    if sum(1 for item in provided_inputs if item) != 1:
        raise typer.BadParameter("Provide exactly one of --payload-json, --payload-text, or --payload-file.")

    if payload_json is not None:
        parsed_json = parse_json_value_option(payload_json, "--payload-json")
        payload_bytes = json.dumps(parsed_json, sort_keys=True).encode("utf-8")
        resolved_content_type = content_type or "application/json"
    elif payload_text is not None:
        payload_bytes = payload_text.encode("utf-8")
        resolved_content_type = content_type or "text/plain; charset=utf-8"
    else:
        assert payload_file is not None
        payload_bytes = payload_file.read_bytes()
        resolved_content_type = content_type or infer_content_type_for_payload_file(payload_file)

    init_db()
    session = get_session_factory()()
    try:
        run = ingest_webhook_payload(
            session,
            source_id,
            payload=payload_bytes,
            content_type=resolved_content_type,
            actor="cli_source_webhook",
        )
        print_banner()
        typer.echo(
            f"source_run={run.source_run_id} source={run.source_id} status={run.status} "
            f"import_run={run.import_run_id} records={run.records_imported} content_type={resolved_content_type}"
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("show-source-ops")
def show_source_ops_command(source_id: int) -> None:
    init_db()
    session = get_session_factory()()
    try:
        detail = build_source_ops_detail(session, source_id)
        source = detail["source"]
        report_status = detail["report_status"]
        checkpoint = detail["checkpoint"]
        print_banner()
        typer.echo(
            f"source={source.source_id} name={source.name} kind={source.source_kind} enabled={source.enabled} layer={source.layer_key}"
        )
        typer.echo(f"target_uri={source.target_uri}")
        if report_status is not None:
            typer.echo(
                f"health runtime_state={report_status.runtime_state} stale={report_status.is_stale} failing={report_status.is_failing} "
                f"pending_dead_letters={report_status.pending_dead_letter_count} replayed_dead_letters={report_status.replayed_dead_letter_count}"
            )
        if checkpoint is not None:
            typer.echo(
                f"checkpoint status={checkpoint.status} last_event_id={checkpoint.last_event_id} "
                f"last_offset={checkpoint.last_offset} last_success_at={checkpoint.last_success_at} "
                f"last_failure_at={checkpoint.last_failure_at} failure_count={checkpoint.failure_count}"
            )
        typer.echo("recent_runs:")
        for row in detail["recent_runs"][:10]:
            typer.echo(
                f"  {row.source_run_id} | {row.status} | import_run={row.import_run_id} | records={row.records_imported} | started={row.started_at}"
            )
        typer.echo("dead_letters:")
        for row in detail["dead_letters"][:10]:
            typer.echo(
                f"  {row.source_dead_letter_id} | stage={row.stage} | status={row.status} | replays={row.replay_count} | created={row.created_at}"
            )
        typer.echo("storage_objects:")
        for row in detail["storage_objects"][:10]:
            typer.echo(
                f"  {row.storage_object_id} | {row.object_kind} | tier={row.storage_tier} | retention={row.retention_class} | {row.object_uri}"
            )
        typer.echo("custody_logs:")
        for row in detail["custody_logs"][:10]:
            typer.echo(f"  {row.custody_log_id} | {row.object_type} | {row.action} | {row.actor}")
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("show-source-summary")
def show_source_summary_command(stale_after_hours: float = 24.0) -> None:
    init_db()
    session = get_session_factory()()
    try:
        summary = build_source_inventory_summary(session, stale_after_hours=stale_after_hours)
        serializable = TypeAdapter(SourceInventorySummaryRead).validate_python(summary).model_dump(mode="json")
        print_banner()
        typer.echo(
            "totals="
            f"{serializable['total_count']} enabled={serializable['enabled_count']} disabled={serializable['disabled_count']} "
            f"stale={serializable['stale_count']} failing={serializable['failing_count']} "
            f"scheduled={serializable['scheduled_count']} unscheduled={serializable['unscheduled_count']}"
        )
        typer.echo(f"stale_before={serializable['stale_before']}")
        web_collection = serializable["web_collection_summary"]
        typer.echo(
            "web_collection "
            f"sources={web_collection['total_source_count']} enabled={web_collection['enabled_source_count']} "
            f"problems={web_collection['problem_source_count']} robots_blocked={web_collection['total_robots_blocked_count']} "
            f"fetch_errors={web_collection['total_search_page_fetch_error_count'] + web_collection['total_crawl_fetch_error_count']}"
        )
        for group_name in ("source_kind_counts", "layer_counts", "latest_status_counts"):
            typer.echo(f"{group_name}:")
            for item in serializable[group_name]:
                typer.echo(
                    f"  {item['key']} | total={item['total_count']} | enabled={item['enabled_count']} | disabled={item['disabled_count']} | stale={item['stale_count']} | failing={item['failing_count']}"
                )
    finally:
        session.close()


@app.command("show-source-report-index")
def show_source_report_index_command(
    stale_after_hours: float = 24.0,
    limit: int = 25,
    stale_source_limit: int = 25,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        report = build_source_ops_report_index(
            session,
            stale_after_hours=stale_after_hours,
            limit=limit,
            stale_source_limit=stale_source_limit,
        )
        serializable = TypeAdapter(SourceOpsReportIndexRead).validate_python(report).model_dump(mode="json")
        print_banner()
        typer.echo(
            f"sync_tasks={serializable['sync_task_count']} sync_runs={serializable['sync_run_count']} sync_failures={serializable['sync_failure_count']}"
        )
        typer.echo(
            f"latest_run_at={serializable['latest_run_at']} stale_after_hours={serializable['stale_after_hours']}"
        )
        inventory = serializable["inventory_summary"]
        typer.echo(
            f"inventory total={inventory['total_count']} stale={inventory['stale_count']} failing={inventory['failing_count']} unscheduled={inventory['unscheduled_count']}"
        )
        web_collection = serializable["web_collection_summary"]
        typer.echo(
            "web_collection "
            f"sources={web_collection['total_source_count']} problems={web_collection['problem_source_count']} "
            f"search_pages={web_collection['total_search_page_count']} crawl_pages={web_collection['total_crawl_page_count']} "
            f"robots_blocked={web_collection['total_robots_blocked_count']} fetch_errors={web_collection['total_search_page_fetch_error_count'] + web_collection['total_crawl_fetch_error_count']}"
        )
        typer.echo("recent_runs:")
        for row in serializable["recent_runs"]:
            typer.echo(
                f"  {row['source_run_id']} | source={row['source_id']} | {row['status']} | records={row['records_imported']} | started={row['started_at']}"
            )
        typer.echo("stale_sources:")
        for row in serializable["stale_sources"]:
            typer.echo(
                f"  {row['source']['source_id']} | {row['source']['name']} | stale={row['is_stale']} | next_run_at={row['next_run_at']} | latest_success_at={row['latest_success_at']}"
            )
        typer.echo("failing_sources:")
        for row in serializable["failing_sources"]:
            typer.echo(
                f"  {row['source']['source_id']} | {row['source']['name']} | latest_run={row['latest_run']['status'] if row['latest_run'] else 'none'}"
            )
        typer.echo("unscheduled_sources:")
        for row in serializable["unscheduled_sources"]:
            typer.echo(
                f"  {row['source']['source_id']} | {row['source']['name']} | enabled={row['source']['enabled']}"
            )
        typer.echo("web_collection_issue_sources:")
        for row in serializable["web_collection_issue_sources"]:
            stats = row["web_collection_stats"]
            typer.echo(
                f"  {row['source']['source_id']} | {row['source']['name']} | kind={row['web_collection_kind']} | "
                f"robots_blocked={stats.get('robots_blocked_count', 0)} | "
                f"private_blocked={stats.get('fetch_private_network_blocked_count', 0)} | "
                f"fetch_errors={stats.get('search_page_fetch_error_count', 0) + stats.get('crawl_fetch_error_count', 0)}"
            )
    finally:
        session.close()


@app.command("export-source-summary")
def export_source_summary_command(
    output_path: Path,
    stale_after_hours: float = 24.0,
    source_limit: int = 500,
    report_limit: int = 25,
    stale_source_limit: int = 25,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        report = build_source_ops_export_summary(
            session,
            stale_after_hours=stale_after_hours,
            source_limit=source_limit,
            report_limit=report_limit,
            stale_source_limit=stale_source_limit,
        )
        serializable = TypeAdapter(SourceOpsExportSummaryRead).validate_python(report).model_dump(mode="json")
        write_json_export_artifact(
            session,
            payload=serializable,
            object_kind="source_summary_export",
            owner_type="source_export",
            owner_id="scoped",
            output_path=output_path,
            source_uri="/api/sources/export/summary",
            observed_at=report["generated_at"],
            metadata_json=serializable["filters_json"],
            actor="cli_export",
        )
        print_banner()
        typer.echo(f"exported source summary to {output_path}")
    finally:
        session.close()


@app.command("run-source-maintenance")
def run_source_maintenance_command(
    stale_after_hours: float = 24.0,
    source_limit: int = 25,
    dead_letter_limit: int = 100,
    replay_dead_letters: bool = True,
    run_stale_sources: bool = True,
    run_failing_sources: bool = True,
    source_kind: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = perform_source_maintenance(
            session,
            stale_after_hours=stale_after_hours,
            source_limit=source_limit,
            dead_letter_limit=dead_letter_limit,
            replay_dead_letters=replay_dead_letters,
            run_stale_sources=run_stale_sources,
            run_failing_sources=run_failing_sources,
            source_kind=source_kind,
            actor="cli_source_maintenance",
        )
        print_banner()
        typer.echo(
            f"selected_sources={result['selected_source_count']} replayed_dead_letters={result['replayed_dead_letter_count']} "
            f"rerun_sources={result['rerun_source_count']} rerun_failures={result['rerun_failure_count']}"
        )
    finally:
        session.close()


@app.command("run-source-health-scan")
def run_source_health_scan_command(
    stale_after_hours: float = 24.0,
    source_limit: int = 100,
    source_kind: str | None = None,
    alert_on_stale: bool = True,
    alert_on_failed: bool = True,
    alert_on_dead_letters: bool = True,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = scan_source_health_alerts(
            session,
            stale_after_hours=stale_after_hours,
            source_limit=source_limit,
            source_kind=source_kind,
            alert_on_stale=alert_on_stale,
            alert_on_failed=alert_on_failed,
            alert_on_dead_letters=alert_on_dead_letters,
            actor="cli_source_health_scan",
        )
        print_banner()
        typer.echo(
            f"scanned_sources={len(result['scanned_source_ids'])} created_alerts={result['created_alert_count']} "
            f"active_alerts={result['active_alert_count']} closed_alerts={result['closed_alert_count']}"
        )
    finally:
        session.close()


@app.command("materialize-cameras")
def materialize_cameras_command(
    layer: str | None = None,
    source_domain: str | None = None,
    limit: int = 500,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = materialize_camera_inventory(
            session,
            layer_key=layer,
            source_domain=source_domain,
            limit=limit,
            actor="cli_camera_registry",
        )
        source_result = materialize_camera_source_inventory(
            session,
            layer_key=layer,
            source_domain=source_domain,
            limit=limit,
            actor="cli_camera_source_registry",
        )
        result["source_created_count"] = int(source_result["created_count"])
        result["source_updated_count"] = int(source_result["updated_count"])
        result["source_scanned_endpoint_count"] = int(source_result["scanned_endpoint_count"])
        serializable = TypeAdapter(CameraMaterializationResponse).validate_python(result).model_dump(mode="json")
        print_banner()
        typer.echo(
            f"scanned={serializable['scanned_count']} created={serializable['created_count']} updated={serializable['updated_count']}"
        )
        typer.echo(
            f"source_candidates created={serializable['source_created_count']} updated={serializable['source_updated_count']} scanned_endpoints={serializable['source_scanned_endpoint_count']}"
        )
        for camera in serializable["cameras"]:
            typer.echo(
                f"{camera['camera_inventory_id']} | {camera['name']} | {camera['status']} | active={camera['active']} | layer={camera['layer_key']}"
            )
    finally:
        session.close()


@app.command("materialize-camera-sources")
def materialize_camera_sources_command(
    layer: str | None = None,
    source_domain: str | None = None,
    active: bool | None = typer.Option(default=None),
    limit: int = 500,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = materialize_camera_source_inventory(
            session,
            layer_key=layer,
            source_domain=source_domain,
            active=active,
            limit=limit,
            actor="cli_camera_source_registry",
        )
        serializable = TypeAdapter(CameraSourceMaterializationResponse).validate_python(result).model_dump(mode="json")
        print_banner()
        typer.echo(
            f"scanned_cameras={serializable['scanned_camera_count']} scanned_endpoints={serializable['scanned_endpoint_count']} created={serializable['created_count']} updated={serializable['updated_count']}"
        )
        for source in serializable["sources"]:
            typer.echo(
                f"{source['camera_source_inventory_id']} | {source['endpoint_kind']} | {source['status']} | score={source['graduation_score']} | {source['endpoint_url']}"
            )
    finally:
        session.close()


@app.command("verify-camera-sources")
def verify_camera_sources_command(
    camera_source_inventory_id: int | None = None,
    layer: str | None = None,
    source_domain: str | None = None,
    endpoint_kind: str | None = None,
    status: str | None = None,
    verification_state: str | None = None,
    active: bool | None = typer.Option(default=None),
    limit: int = 200,
    timeout_seconds: float = 5.0,
    max_payload_bytes: int | None = None,
    allow_private_networks: bool = typer.Option(
        True,
        "--allow-private-networks/--block-private-networks",
    ),
    min_request_interval_seconds: float | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = verify_camera_source_inventory(
            session,
            camera_source_inventory_id=camera_source_inventory_id,
            layer_key=layer,
            source_domain=source_domain,
            endpoint_kind=endpoint_kind,
            status=status,
            verification_state=verification_state,
            active=active,
            limit=limit,
            timeout_seconds=timeout_seconds,
            max_payload_bytes=max_payload_bytes,
            allow_private_networks=allow_private_networks,
            min_request_interval_seconds=min_request_interval_seconds,
            actor="cli_camera_source_verifier",
        )
        serializable = TypeAdapter(CameraSourceVerificationResponse).validate_python(result).model_dump(mode="json")
        print_banner()
        typer.echo(
            f"verified={serializable['verified_count']} reachable={serializable['reachable_count']} failed={serializable['failed_count']}"
        )
        for source in serializable["sources"]:
            typer.echo(
                f"{source['camera_source_inventory_id']} | {source['endpoint_kind']} | {source['status']} | verify={source['verification_state']} | score={source['graduation_score']} | {source['endpoint_url']}"
            )
    finally:
        session.close()


@app.command("list-cameras")
def list_cameras_command(
    layer: str | None = None,
    source_domain: str | None = None,
    status: str | None = None,
    active: bool | None = typer.Option(default=None),
    bbox: str | None = None,
    limit: int = 200,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        min_lon, min_lat, max_lon, max_lat = parse_bbox(bbox)
        rows = list_cameras(
            session,
            layer_key=layer,
            source_domain=source_domain,
            status=status,
            active=active,
            min_lon=min_lon,
            min_lat=min_lat,
            max_lon=max_lon,
            max_lat=max_lat,
            limit=limit,
        )
        print_banner()
        for row in rows:
            typer.echo(
                f"{row.camera_inventory_id} | {row.name} | {row.status} | active={row.active} | provider={row.provider} | road={row.road_name}"
            )
    finally:
        session.close()


@app.command("list-camera-sources")
def list_camera_sources_command(
    layer: str | None = None,
    source_domain: str | None = None,
    endpoint_kind: str | None = None,
    status: str | None = None,
    verification_state: str | None = None,
    active: bool | None = typer.Option(default=None),
    limit: int = 200,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        rows = list_camera_sources(
            session,
            layer_key=layer,
            source_domain=source_domain,
            endpoint_kind=endpoint_kind,
            status=status,
            verification_state=verification_state,
            active=active,
            limit=limit,
        )
        print_banner()
        for row in rows:
            typer.echo(
                f"{row.camera_source_inventory_id} | {row.endpoint_kind} | {row.status} | verify={row.verification_state} | score={row.graduation_score:.3f} | {row.endpoint_url}"
            )
    finally:
        session.close()


@app.command("show-camera-summary")
def show_camera_summary_command(
    layer: str | None = None,
    source_domain: str | None = None,
    status: str | None = None,
    active: bool | None = typer.Option(default=None),
    bbox: str | None = None,
    stale_after_hours: float = 24.0,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        min_lon, min_lat, max_lon, max_lat = parse_bbox(bbox)
        summary = build_camera_inventory_summary(
            session,
            layer_key=layer,
            source_domain=source_domain,
            status=status,
            active=active,
            min_lon=min_lon,
            min_lat=min_lat,
            max_lon=max_lon,
            max_lat=max_lat,
            stale_after_hours=stale_after_hours,
        )
        print_banner()
        typer.echo(
            "totals="
            f"{summary['total_count']} active={summary['active_count']} inactive={summary['inactive_count']} "
            f"stale={summary['stale_count']} stale_before={summary['stale_before'].isoformat()}"
        )
        for group_name in ("layer_counts", "source_domain_counts", "provider_counts", "status_counts"):
            typer.echo(f"{group_name}:")
            for item in summary[group_name]:
                typer.echo(
                    f"  {item['key']} | total={item['total_count']} | active={item['active_count']} | inactive={item['inactive_count']} | stale={item['stale_count']}"
                )
    finally:
        session.close()


@app.command("show-camera-source-summary")
def show_camera_source_summary_command(
    layer: str | None = None,
    source_domain: str | None = None,
    endpoint_kind: str | None = None,
    status: str | None = None,
    verification_state: str | None = None,
    active: bool | None = typer.Option(default=None),
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        summary = build_camera_source_inventory_summary(
            session,
            layer_key=layer,
            source_domain=source_domain,
            endpoint_kind=endpoint_kind,
            status=status,
            verification_state=verification_state,
            active=active,
        )
        serializable = TypeAdapter(CameraSourceSummaryRead).validate_python(summary).model_dump(mode="json")
        print_banner()
        typer.echo(
            "totals="
            f"{serializable['total_count']} active={serializable['active_count']} ready={serializable['ready_count']} "
            f"review={serializable['review_count']} candidate={serializable['candidate_count']} graduated={serializable['graduated_count']}"
        )
        for group_name in ("source_domain_counts", "endpoint_kind_counts", "status_counts", "verification_state_counts"):
            typer.echo(f"{group_name}:")
            for item in serializable[group_name]:
                typer.echo(
                    f"  {item['key']} | total={item['total_count']} | active={item['active_count']} | ready={item['ready_count']} | review={item['review_count']}"
                )
    finally:
        session.close()


@app.command("show-camera-ops")
def show_camera_ops_command(camera_inventory_id: int) -> None:
    init_db()
    session = get_session_factory()()
    try:
        detail = build_camera_inventory_ops_detail(session, camera_inventory_id)
        camera = detail["camera"]
        print_banner()
        typer.echo(
            f"camera={camera.camera_inventory_id} key={camera.camera_key} name={camera.name} status={camera.status} active={camera.active}"
        )
        typer.echo(
            f"layer={camera.layer_key} source_domain={camera.source_domain} provider={camera.provider} last_observed_at={camera.last_observed_at}"
        )
        latest_observation = detail["latest_observation"]
        if latest_observation is not None:
            typer.echo(
                f"latest_observation={latest_observation.observation_id} import_run={latest_observation.import_run_id} source_domain={latest_observation.source_domain}"
            )
        latest_import_run = detail["latest_import_run"]
        if latest_import_run is not None:
            typer.echo(
                f"latest_import_run={latest_import_run.import_run_id} status={latest_import_run.status} imported={latest_import_run.records_imported}"
            )
        typer.echo("refresh_tasks:")
        for task in detail["refresh_tasks"]:
            typer.echo(
                f"  {task.task_id} | enabled={task.enabled} | every={task.interval_seconds}s | next={task.next_run_at} | payload={json.dumps(task.payload_json, sort_keys=True)}"
            )
        typer.echo("custody_logs:")
        for log in detail["custody_logs"]:
            typer.echo(f"  {log.custody_log_id} | {log.action} | {log.actor} | {log.created_at}")
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("show-camera-source-ops")
def show_camera_source_ops_command(camera_source_inventory_id: int) -> None:
    init_db()
    session = get_session_factory()()
    try:
        detail = build_camera_source_inventory_ops_detail(session, camera_source_inventory_id)
        source = detail["source"]
        print_banner()
        typer.echo(
            f"camera_source={source.camera_source_inventory_id} kind={source.endpoint_kind} status={source.status} verify={source.verification_state} score={source.graduation_score:.3f}"
        )
        typer.echo(
            f"layer={source.layer_key} source_domain={source.source_domain} endpoint={source.endpoint_url}"
        )
        camera = detail["camera"]
        if camera is not None:
            typer.echo(
                f"camera={camera.camera_inventory_id} key={camera.camera_key} name={camera.name} active={camera.active}"
            )
        latest_observation = detail["latest_observation"]
        if latest_observation is not None:
            typer.echo(
                f"latest_observation={latest_observation.observation_id} import_run={latest_observation.import_run_id} source_domain={latest_observation.source_domain}"
            )
        typer.echo("custody_logs:")
        for log in detail["custody_logs"]:
            typer.echo(f"  {log.custody_log_id} | {log.action} | {log.actor} | {log.created_at}")
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("show-camera-source-report-index")
def show_camera_source_report_index_command(
    layer: str | None = None,
    source_domain: str | None = None,
    endpoint_kind: str | None = None,
    status: str | None = None,
    verification_state: str | None = None,
    active: bool | None = typer.Option(default=None),
    stale_after_hours: float = 24.0,
    limit: int = 25,
    stale_source_limit: int = 25,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        report = build_camera_source_ops_report_index(
            session,
            layer_key=layer,
            source_domain=source_domain,
            endpoint_kind=endpoint_kind,
            status=status,
            verification_state=verification_state,
            active=active,
            stale_after_hours=stale_after_hours,
            limit=limit,
            stale_source_limit=stale_source_limit,
        )
        serializable = TypeAdapter(CameraSourceOpsReportIndexRead).validate_python(report).model_dump(mode="json")
        print_banner()
        typer.echo(
            f"refresh_tasks={serializable['refresh_task_count']} refresh_runs={serializable['refresh_run_count']} failures={serializable['refresh_failure_count']}"
        )
        typer.echo(
            f"verification_tasks={serializable['verification_task_count']} verification_runs={serializable['verification_run_count']} failures={serializable['verification_failure_count']}"
        )
        typer.echo(
            f"latest_materialization_at={serializable['latest_materialization_at']} stale_after_hours={serializable['stale_after_hours']}"
        )
        inventory = serializable["inventory_summary"]
        typer.echo(
            "inventory "
            f"total={inventory['total_count']} active={inventory['active_count']} ready={inventory['ready_count']} "
            f"review={inventory['review_count']} candidate={inventory['candidate_count']} graduated={inventory['graduated_count']}"
        )
        typer.echo("recent_refresh_runs:")
        for row in serializable["recent_refresh_runs"]:
            typer.echo(
                f"  {row['task_run_id']} | task={row['task_name']} | status={row['status']} | records={row['records_affected']} | started={row['started_at']}"
            )
        typer.echo("recent_verification_runs:")
        for row in serializable["recent_verification_runs"]:
            typer.echo(
                f"  {row['task_run_id']} | task={row['task_name']} | status={row['status']} | records={row['records_affected']} | started={row['started_at']}"
            )
        typer.echo("recent_verifications:")
        for row in serializable["recent_verifications"]:
            typer.echo(
                f"  {row['created_at']} | reachable={row['details_json'].get('reachable_count')} | failed={row['details_json'].get('failed_count')} | endpoint_kind={row['details_json'].get('endpoint_kind') or 'all'}"
            )
        typer.echo("stale_sources:")
        for row in serializable["stale_sources"]:
            typer.echo(
                f"  {row['camera_source_inventory_id']} | {row['endpoint_kind']} | {row['status']} | source={row['source_domain']} | last_observed_at={row['last_observed_at']}"
            )
    finally:
        session.close()


@app.command("show-camera-report-index")
def show_camera_report_index_command(
    layer: str | None = None,
    source_domain: str | None = None,
    status: str | None = None,
    active: bool | None = typer.Option(default=None),
    bbox: str | None = None,
    stale_after_hours: float = 24.0,
    limit: int = 25,
    stale_camera_limit: int = 25,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        min_lon, min_lat, max_lon, max_lat = parse_bbox(bbox)
        report = build_camera_ops_report_index(
            session,
            layer_key=layer,
            source_domain=source_domain,
            status=status,
            active=active,
            min_lon=min_lon,
            min_lat=min_lat,
            max_lon=max_lon,
            max_lat=max_lat,
            stale_after_hours=stale_after_hours,
            limit=limit,
            stale_camera_limit=stale_camera_limit,
        )
        serializable = TypeAdapter(CameraOpsReportIndexRead).validate_python(report).model_dump(mode="json")
        print_banner()
        typer.echo(
            f"refresh_tasks={serializable['refresh_task_count']} refresh_runs={serializable['refresh_run_count']} failures={serializable['refresh_failure_count']}"
        )
        typer.echo(
            f"latest_materialization_at={serializable['latest_materialization_at']} stale_after_hours={serializable['stale_after_hours']}"
        )
        inventory = serializable["inventory_summary"]
        typer.echo(
            f"inventory total={inventory['total_count']} active={inventory['active_count']} inactive={inventory['inactive_count']} stale={inventory['stale_count']}"
        )
        typer.echo("recent_refresh_runs:")
        for row in serializable["recent_refresh_runs"]:
            typer.echo(
                f"  {row['task_run_id']} | task={row['task_name']} | status={row['status']} | records={row['records_affected']} | started={row['started_at']}"
            )
        typer.echo("stale_cameras:")
        for row in serializable["stale_cameras"]:
            typer.echo(
                f"  {row['camera_inventory_id']} | {row['name']} | source={row['source_domain']} | last_observed_at={row['last_observed_at']}"
            )
    finally:
        session.close()


@app.command("export-camera-source-summary")
def export_camera_source_summary_command(
    output_path: Path,
    layer: str | None = None,
    source_domain: str | None = None,
    endpoint_kind: str | None = None,
    status: str | None = None,
    verification_state: str | None = None,
    active: bool | None = typer.Option(default=None),
    stale_after_hours: float = 24.0,
    source_limit: int = 500,
    report_limit: int = 25,
    stale_source_limit: int = 25,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        report = build_camera_source_ops_export_summary(
            session,
            layer_key=layer,
            source_domain=source_domain,
            endpoint_kind=endpoint_kind,
            status=status,
            verification_state=verification_state,
            active=active,
            stale_after_hours=stale_after_hours,
            source_limit=source_limit,
            report_limit=report_limit,
            stale_source_limit=stale_source_limit,
        )
        serializable = TypeAdapter(CameraSourceOpsExportSummaryRead).validate_python(report).model_dump(mode="json")
        write_json_export_artifact(
            session,
            payload=serializable,
            object_kind="camera_source_summary_export",
            owner_type="camera_source_export",
            owner_id=layer or source_domain or "scoped",
            output_path=output_path,
            source_uri="/api/camera-sources/export/summary",
            observed_at=report["generated_at"],
            metadata_json=serializable["filters_json"],
            actor="cli_export",
        )
        print_banner()
        typer.echo(f"exported camera source summary to {output_path}")
    finally:
        session.close()


@app.command("export-camera-summary")
def export_camera_summary_command(
    output_path: Path,
    layer: str | None = None,
    source_domain: str | None = None,
    status: str | None = None,
    active: bool | None = typer.Option(default=None),
    bbox: str | None = None,
    stale_after_hours: float = 24.0,
    camera_limit: int = 500,
    report_limit: int = 25,
    stale_camera_limit: int = 25,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        min_lon, min_lat, max_lon, max_lat = parse_bbox(bbox)
        report = build_camera_ops_export_summary(
            session,
            layer_key=layer,
            source_domain=source_domain,
            status=status,
            active=active,
            min_lon=min_lon,
            min_lat=min_lat,
            max_lon=max_lon,
            max_lat=max_lat,
            stale_after_hours=stale_after_hours,
            camera_limit=camera_limit,
            report_limit=report_limit,
            stale_camera_limit=stale_camera_limit,
        )
        serializable = TypeAdapter(CameraOpsExportSummaryRead).validate_python(report).model_dump(mode="json")
        write_json_export_artifact(
            session,
            payload=serializable,
            object_kind="camera_summary_export",
            owner_type="camera_export",
            owner_id=layer or source_domain or "scoped",
            output_path=output_path,
            source_uri="/api/cameras/export/summary",
            observed_at=report["generated_at"],
            metadata_json=serializable["filters_json"],
            actor="cli_export",
        )
        print_banner()
        typer.echo(f"exported camera summary to {output_path}")
    finally:
        session.close()


@app.command("fuse-events")
def fuse_events_command(
    bbox: str | None = None,
    layer: str | None = None,
    limit: int = 500,
    time_window_minutes: int = 60,
    distance_km: float = 25.0,
    redaction_level: str = "public",
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        min_lon, min_lat, max_lon, max_lat = parse_bbox(bbox)
        results = materialize_fused_events(
            session,
            EventFusionRequest(
                layer_key=layer,
                min_lon=min_lon,
                min_lat=min_lat,
                max_lon=max_lon,
                max_lat=max_lat,
                limit=limit,
                time_window_minutes=time_window_minutes,
                distance_km=distance_km,
                redaction_level=redaction_level,
            ),
        )
        print_banner()
        for result in results:
            typer.echo(
                f"{result.event.event_id} | {result.event.slug} | new={result.created_new} | observations={result.observation_count} | products={result.product_count} | score={result.verification_score:.2f}"
            )
    finally:
        session.close()


@app.command("list-events")
def list_events(
    status: str | None = None,
    redaction_level: str | None = None,
    limit: int = 200,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        rows = list_event_records(
            session,
            status=status,
            redaction_level=redaction_level,
            limit=limit,
        )
        print_banner()
        for row in rows:
            typer.echo(f"{row.event_id} | {row.slug} | {row.title} | {row.status} | {row.redaction_level}")
    finally:
        session.close()


@app.command("show-event-summary")
def show_event_summary_command(
    status: str | None = None,
    redaction_level: str | None = None,
    stale_after_hours: float = 24.0,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        summary = build_event_inventory_summary(
            session,
            status=status,
            redaction_level=redaction_level,
            stale_after_hours=stale_after_hours,
        )
        serializable = TypeAdapter(EventInventorySummaryRead).validate_python(summary).model_dump(mode="json")
        print_banner()
        typer.echo(
            "totals="
            f"{serializable['total_count']} open={serializable['open_count']} closed={serializable['closed_count']} "
            f"stale_open={serializable['stale_open_count']}"
        )
        typer.echo(
            f"alert_scoped={serializable['alert_scoped_count']} product_covered={serializable['product_covered_count']}"
        )
        for group_name in ("status_counts", "redaction_level_counts", "confidence_band_counts"):
            typer.echo(f"{group_name}:")
            for item in serializable[group_name]:
                typer.echo(
                    f"  {item['key']} | total={item['total_count']} | open={item['open_count']} | closed={item['closed_count']} | stale_open={item['stale_open_count']}"
                )
    finally:
        session.close()


@app.command("show-event-report-index")
def show_event_report_index_command(
    status: str | None = None,
    redaction_level: str | None = None,
    stale_after_hours: float = 24.0,
    limit: int = 25,
    stale_event_limit: int = 25,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        report = build_event_ops_report_index(
            session,
            status=status,
            redaction_level=redaction_level,
            stale_after_hours=stale_after_hours,
            limit=limit,
            stale_event_limit=stale_event_limit,
        )
        serializable = TypeAdapter(EventOpsReportIndexRead).validate_python(report).model_dump(mode="json")
        print_banner()
        typer.echo(
            f"event_fusion_tasks={serializable['event_fusion_task_count']} event_fusion_runs={serializable['event_fusion_run_count']} failures={serializable['event_fusion_failure_count']}"
        )
        typer.echo(
            f"latest_event_at={serializable['latest_event_at']} stale_after_hours={serializable['stale_after_hours']}"
        )
        inventory = serializable["inventory_summary"]
        typer.echo(
            f"inventory total={inventory['total_count']} open={inventory['open_count']} closed={inventory['closed_count']} stale_open={inventory['stale_open_count']}"
        )
        typer.echo("recent_events:")
        for row in serializable["recent_events"]:
            typer.echo(
                f"  {row['event_id']} | {row['slug']} | {row['status']} | {row['redaction_level']} | {row['title']}"
            )
        typer.echo("stale_open_events:")
        for row in serializable["stale_open_events"]:
            typer.echo(
                f"  {row['event_id']} | {row['slug']} | occurred_at={row['occurred_at']} | updated_at={row['updated_at']}"
            )
    finally:
        session.close()


@app.command("export-event-summary")
def export_event_summary_command(
    output_path: Path,
    status: str | None = None,
    redaction_level: str | None = None,
    stale_after_hours: float = 24.0,
    event_limit: int = 500,
    report_limit: int = 25,
    stale_event_limit: int = 25,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        report = build_event_ops_export_summary(
            session,
            status=status,
            redaction_level=redaction_level,
            stale_after_hours=stale_after_hours,
            event_limit=event_limit,
            report_limit=report_limit,
            stale_event_limit=stale_event_limit,
        )
        serializable = TypeAdapter(EventOpsExportSummaryRead).validate_python(report).model_dump(mode="json")
        write_json_export_artifact(
            session,
            payload=serializable,
            object_kind="event_summary_export",
            owner_type="event_export",
            owner_id=str(status or redaction_level or "scoped"),
            output_path=output_path,
            source_uri="/api/events/export/summary",
            observed_at=report["generated_at"],
            metadata_json=serializable["filters_json"],
            actor="cli_export",
        )
        print_banner()
        typer.echo(f"exported event summary to {output_path}")
    finally:
        session.close()


@app.command("resolve-entities")
def resolve_entities_command(
    bbox: str | None = None,
    layer: str | None = None,
    source_domain: str | None = None,
    trust_level: str | None = None,
    limit: int = 500,
    min_observations: int = 2,
    entity_type: str | None = None,
    redaction_level: str = "public",
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        min_lon, min_lat, max_lon, max_lat = parse_bbox(bbox)
        results = materialize_entities(
            session,
            EntityResolutionRequest(
                layer_key=layer,
                source_domain=source_domain,
                trust_level=trust_level,
                min_lon=min_lon,
                min_lat=min_lat,
                max_lon=max_lon,
                max_lat=max_lat,
                limit=limit,
                min_observations=min_observations,
                entity_type=entity_type,
                redaction_level=redaction_level,
            ),
            actor="cli",
        )
        print_banner()
        for result in results:
            typer.echo(
                f"{result.entity.entity_id} | {result.entity.entity_type} | {result.entity.canonical_name} | new={result.created_new} | observations={result.observation_count} | signals={result.signal_count} | score={result.confidence_score:.2f}"
            )
    finally:
        session.close()


@app.command("list-entities")
def list_entities(
    entity_type: str | None = None,
    redaction_level: str | None = None,
    min_confidence_score: float | None = None,
    limit: int = 200,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        rows = list_entity_records(
            session,
            entity_type=entity_type,
            redaction_level=redaction_level,
            min_confidence_score=min_confidence_score,
            limit=limit,
        )
        print_banner()
        for row in rows:
            typer.echo(
                f"{row.entity_id} | {row.entity_type} | {row.canonical_name} | score={row.confidence_score:.2f} | {row.redaction_level}"
            )
    finally:
        session.close()


@app.command("show-entity-summary")
def show_entity_summary_command(
    entity_type: str | None = None,
    redaction_level: str | None = None,
    min_confidence_score: float | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        summary = build_entity_inventory_summary(
            session,
            entity_type=entity_type,
            redaction_level=redaction_level,
            min_confidence_score=min_confidence_score,
        )
        serializable = TypeAdapter(EntityInventorySummaryRead).validate_python(summary).model_dump(mode="json")
        print_banner()
        typer.echo(
            "totals="
            f"{serializable['total_count']} high={serializable['high_confidence_count']} medium={serializable['medium_confidence_count']} "
            f"low={serializable['low_confidence_count']} signal_conflicts={serializable['signal_conflict_count']}"
        )
        for group_name in ("entity_type_counts", "redaction_level_counts", "confidence_band_counts", "evidence_strength_counts"):
            typer.echo(f"{group_name}:")
            for item in serializable[group_name]:
                typer.echo(
                    f"  {item['key']} | total={item['total_count']} | high_confidence={item['high_confidence_count']} | signal_conflicts={item['signal_conflict_count']}"
                )
    finally:
        session.close()


@app.command("show-entity-report-index")
def show_entity_report_index_command(
    entity_type: str | None = None,
    redaction_level: str | None = None,
    min_confidence_score: float | None = None,
    limit: int = 25,
    conflict_limit: int = 25,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        report = build_entity_ops_report_index(
            session,
            entity_type=entity_type,
            redaction_level=redaction_level,
            min_confidence_score=min_confidence_score,
            limit=limit,
            conflict_limit=conflict_limit,
        )
        serializable = TypeAdapter(EntityOpsReportIndexRead).validate_python(report).model_dump(mode="json")
        print_banner()
        typer.echo(
            f"entity_resolution_tasks={serializable['entity_resolution_task_count']} entity_resolution_runs={serializable['entity_resolution_run_count']} failures={serializable['entity_resolution_failure_count']}"
        )
        typer.echo(f"latest_entity_at={serializable['latest_entity_at']}")
        inventory = serializable["inventory_summary"]
        typer.echo(
            f"inventory total={inventory['total_count']} high={inventory['high_confidence_count']} medium={inventory['medium_confidence_count']} low={inventory['low_confidence_count']} signal_conflicts={inventory['signal_conflict_count']}"
        )
        typer.echo("recent_entities:")
        for row in serializable["recent_entities"]:
            typer.echo(
                f"  {row['entity_id']} | {row['entity_type']} | {row['canonical_name']} | score={row['confidence_score']:.2f}"
            )
        typer.echo("conflicting_entities:")
        for row in serializable["conflicting_entities"]:
            typer.echo(
                f"  {row['entity_id']} | {row['entity_type']} | {row['canonical_name']} | score={row['confidence_score']:.2f}"
            )
    finally:
        session.close()


@app.command("export-entity-summary")
def export_entity_summary_command(
    output_path: Path,
    entity_type: str | None = None,
    redaction_level: str | None = None,
    min_confidence_score: float | None = None,
    entity_limit: int = 500,
    report_limit: int = 25,
    conflict_limit: int = 25,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        report = build_entity_ops_export_summary(
            session,
            entity_type=entity_type,
            redaction_level=redaction_level,
            min_confidence_score=min_confidence_score,
            entity_limit=entity_limit,
            report_limit=report_limit,
            conflict_limit=conflict_limit,
        )
        serializable = TypeAdapter(EntityOpsExportSummaryRead).validate_python(report).model_dump(mode="json")
        write_json_export_artifact(
            session,
            payload=serializable,
            object_kind="entity_summary_export",
            owner_type="entity_export",
            owner_id=str(entity_type or redaction_level or "scoped"),
            output_path=output_path,
            source_uri="/api/entities/export/summary",
            observed_at=report["generated_at"],
            metadata_json=serializable["filters_json"],
            actor="cli_export",
        )
        print_banner()
        typer.echo(f"exported entity summary to {output_path}")
    finally:
        session.close()


@app.command("show-entity-observations")
def show_entity_observations(entity_id: int) -> None:
    init_db()
    session = get_session_factory()()
    try:
        rows = list(
            session.scalars(
                select(EntityObservationLinkORM)
                .where(EntityObservationLinkORM.entity_id == entity_id)
                .order_by(EntityObservationLinkORM.entity_observation_link_id.asc())
            )
        )
        print_banner()
        for row in rows:
            typer.echo(
                f"{row.entity_observation_link_id} | entity={row.entity_id} | observation={row.observation_id} | {row.match_basis} | score={row.confidence_contribution:.2f}"
            )
    finally:
        session.close()


@app.command("show-event-products")
def show_event_products(event_id: int) -> None:
    init_db()
    session = get_session_factory()()
    try:
        rows = list(
            session.scalars(
                select(SituationProductORM).where(SituationProductORM.event_id == event_id)
            )
        )
        print_banner()
        for row in rows:
            typer.echo(f"{row.product_id} | {row.product_type} | {row.redaction_level} | {row.title}")
    finally:
        session.close()


@app.command("export-event-product")
def export_event_product(
    event_id: int,
    product_type: str,
    output_path: Path,
    max_redaction_level: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        product = session.scalar(
            select(SituationProductORM).where(
                SituationProductORM.event_id == event_id,
                SituationProductORM.product_type == product_type,
            )
        )
        if product is None:
            raise typer.BadParameter(
                f"No product '{product_type}' exists for event {event_id}."
            )
        try:
            enforce_export_redaction(product, max_redaction_level)
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        write_text_export_artifact(
            session,
            body_text=product.body_text,
            object_kind="situation_product_export",
            owner_type="situation_product",
            owner_id=str(product.product_id),
            output_path=output_path,
            source_uri=f"/api/events/{event_id}/products",
            observed_at=product.updated_at,
            metadata_json={
                "event_id": event_id,
                "product_type": product.product_type,
                "redaction_level": product.redaction_level,
                "requested_redaction_level": max_redaction_level,
            },
            actor="cli_export",
        )
        print_banner()
        typer.echo(f"exported {product.product_type} to {output_path}")
    finally:
        session.close()


@app.command("export-event-bundle")
def export_event_bundle(
    event_id: int,
    output_path: Path,
    max_redaction_level: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        try:
            bundle = build_event_export_bundle(session, event_id, max_redaction_level=max_redaction_level)
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        serializable = TypeAdapter(EventExportBundleRead).validate_python(bundle).model_dump(mode="json")
        write_json_export_artifact(
            session,
            payload=serializable,
            object_kind="event_bundle_export",
            owner_type="event",
            owner_id=str(event_id),
            output_path=output_path,
            source_uri=f"/api/events/{event_id}/export",
            observed_at=bundle["exported_at"],
            metadata_json={
                "requested_redaction_level": max_redaction_level,
                "observation_count": len(serializable["observations"]),
                "entity_count": len(serializable["entities"]),
                "product_count": len(serializable["products"]),
            },
            actor="cli_export",
        )
        print_banner()
        typer.echo(f"exported event bundle to {output_path}")
    finally:
        session.close()


@app.command("query-observations")
def query_observations_command(
    bbox: str | None = None,
    layer: str | None = None,
    source_domain: str | None = None,
    trust_level: str | None = None,
    limit: int = 50,
    backend: str = "runtime",
    archive_glob_url: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        query_backend = parse_observation_backend(backend)
        min_lon, min_lat, max_lon, max_lat = parse_bbox(bbox)
        rows = query_observations(
            session,
            layer_key=layer,
            source_domain=source_domain,
            trust_level=trust_level,
            min_lon=min_lon,
            min_lat=min_lat,
            max_lon=max_lon,
            max_lat=max_lat,
            limit=limit,
            backend=query_backend,
            archive_glob_url=archive_glob_url,
        )
        print_banner()
        for row in rows:
            typer.echo(
                f"{row.observation_id} | {row.layer_key} | {row.source_domain} | {row.confidence_score:.2f} | {row.created_at.isoformat()}"
            )
    finally:
        session.close()


@app.command("search-observations")
def search_observations_command(
    query: str,
    bbox: str | None = None,
    layer: str | None = None,
    source_domain: str | None = None,
    trust_level: str | None = None,
    limit: int = 20,
    candidate_limit: int = 500,
    backend: str = "runtime",
    archive_glob_url: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        query_backend = parse_observation_backend(backend)
        min_lon, min_lat, max_lon, max_lat = parse_bbox(bbox)
        rows = search_observations(
            session,
            query_text=query,
            layer_key=layer,
            source_domain=source_domain,
            trust_level=trust_level,
            min_lon=min_lon,
            min_lat=min_lat,
            max_lon=max_lon,
            max_lat=max_lat,
            limit=limit,
            candidate_limit=candidate_limit,
            backend=query_backend,
            archive_glob_url=archive_glob_url,
        )
        serializable = TypeAdapter(list[ObservationSearchResultRead]).dump_python(rows, mode="json")
        print_banner()
        for row in serializable:
            title = row["title"] or row["observation"]["content_text"] or "(untitled)"
            url = row["source_url"] or row["observation"]["source_domain"] or "unknown"
            typer.echo(
                f"{row['observation']['observation_id']} | score={row['search_score']:.2f} | {title} | {url}"
            )
            if row["snippet"]:
                typer.echo(f"  {row['snippet']}")
    finally:
        session.close()


@app.command("cross-verify")
def cross_verify_command(
    bbox: str | None = None,
    layer: str | None = None,
    source_domain: str | None = None,
    trust_level: str | None = None,
    limit: int = 200,
    time_window_minutes: int = 60,
    distance_km: float = 25.0,
    backend: str = "runtime",
    archive_glob_url: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        query_backend = parse_observation_backend(backend)
        min_lon, min_lat, max_lon, max_lat = parse_bbox(bbox)
        rows = query_observations(
            session,
            layer_key=layer,
            source_domain=source_domain,
            trust_level=trust_level,
            min_lon=min_lon,
            min_lat=min_lat,
            max_lon=max_lon,
            max_lat=max_lat,
            limit=limit,
            backend=query_backend,
            archive_glob_url=archive_glob_url,
        )
        summaries = build_cross_verification_summaries(
            session,
            rows,
            time_window_minutes=time_window_minutes,
            distance_km=distance_km,
        )
        print_banner()
        for summary in summaries:
            typer.echo(
                f"{summary['cluster_id']} | observations={summary['observation_count']} | domains={summary['source_domain_count']} | layers={summary['layer_count']} | trusted={summary['trusted_observation_count']} | integrity={summary['integrity_source_count']} | ground_truth={summary['ground_truth_count']} | span_min={summary['time_span_minutes']} | score={summary['verification_score']:.2f}"
            )
    finally:
        session.close()


@app.command("add-watch")
def add_watch_command(
    name: str,
    watch_type: str,
    objective: str,
    slug: str | None = None,
    description: str = "",
    state: str = "enabled",
    source_id: int | None = None,
    camera_inventory_id: int | None = None,
    camera_source_inventory_id: int | None = None,
    layer: str | None = None,
    event_id: int | None = None,
    geofence_id: int | None = None,
    scheduled_task_id: int | None = None,
    interval_seconds: int | None = None,
    severity: str = "info",
    rule_json: str | None = None,
    notification_policy_json: str | None = None,
    metadata_json: str | None = None,
    provenance_json: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        rule_payload = parse_json_object_option(rule_json, "--rule-json") or {
            "mode": watch_type,
        }
        notification_payload = parse_json_object_option(
            notification_policy_json,
            "--notification-policy-json",
        ) or {
            "api_enabled": True,
            "rss_enabled": True,
            "analysis_on_change": False,
        }
        watch = create_watch(
            session,
            WatchCreate(
                name=name,
                slug=slug or derive_watch_slug(name),
                objective=objective,
                description=description,
                watch_type=watch_type,
                state=state,
                rule_json=rule_payload,
                source_id=source_id,
                camera_inventory_id=camera_inventory_id,
                camera_source_inventory_id=camera_source_inventory_id,
                layer_key=layer,
                event_id=event_id,
                geofence_id=geofence_id,
                scheduled_task_id=scheduled_task_id,
                interval_seconds=interval_seconds,
                severity=severity,
                notification_policy_json=notification_payload,
                metadata_json=parse_json_object_option(metadata_json, "--metadata-json") or {},
                provenance_json=(
                    parse_json_object_option(provenance_json, "--provenance-json") or {}
                ),
            ),
            actor="cli",
        )
        print_banner()
        typer.echo(
            f"watch={watch.watch_id} | slug={watch.slug} | type={watch.watch_type} "
            f"| state={watch.state} | severity={watch.severity}"
        )
    finally:
        session.close()


@app.command("list-watches")
def list_watches_command(
    state: str | None = None,
    watch_type: str | None = None,
    limit: int = 200,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        rows = list_watches(session, state=state, watch_type=watch_type, limit=limit)
        print_banner()
        for watch in rows:
            typer.echo(
                f"{watch.watch_id} | {watch.slug} | {watch.watch_type} | state={watch.state} "
                f"| severity={watch.severity} | next={watch.next_run_at} | {watch.name}"
            )
    finally:
        session.close()


@app.command("show-watch")
def show_watch_command(watch_id: int) -> None:
    init_db()
    session = get_session_factory()()
    try:
        watch = get_watch(session, watch_id)
        print_banner()
        typer.echo(
            f"watch={watch.watch_id} | name={watch.name} | slug={watch.slug} "
            f"| type={watch.watch_type} | state={watch.state} | severity={watch.severity}"
        )
        typer.echo(f"objective={watch.objective}")
        typer.echo(f"description={watch.description}")
        typer.echo(
            "references="
            f"source:{watch.source_id},camera:{watch.camera_inventory_id},"
            f"camera_source:{watch.camera_source_inventory_id},layer:{watch.layer_key},"
            f"event:{watch.event_id},geofence:{watch.geofence_id},schedule:{watch.scheduled_task_id}"
        )
        typer.echo(f"rule_json={json.dumps(watch.rule_json, sort_keys=True)}")
        typer.echo(
            "notification_policy_json="
            f"{json.dumps(watch.notification_policy_json, sort_keys=True)}"
        )
        typer.echo(
            f"last_evaluated={watch.last_evaluated_at} | last_changed={watch.last_changed_at} "
            f"| next={watch.next_run_at}"
        )
    finally:
        session.close()


@app.command("update-watch")
def update_watch_command(
    watch_id: int,
    name: str | None = None,
    slug: str | None = None,
    objective: str | None = None,
    description: str | None = None,
    watch_type: str | None = None,
    state: str | None = None,
    source_id: int | None = None,
    camera_inventory_id: int | None = None,
    camera_source_inventory_id: int | None = None,
    layer: str | None = None,
    event_id: int | None = None,
    geofence_id: int | None = None,
    scheduled_task_id: int | None = None,
    interval_seconds: int | None = None,
    severity: str | None = None,
    rule_json: str | None = None,
    notification_policy_json: str | None = None,
    metadata_json: str | None = None,
    provenance_json: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        payload: dict[str, object] = {}
        optional_values: dict[str, object | None] = {
            "name": name,
            "slug": slug,
            "objective": objective,
            "description": description,
            "watch_type": watch_type,
            "state": state,
            "source_id": source_id,
            "camera_inventory_id": camera_inventory_id,
            "camera_source_inventory_id": camera_source_inventory_id,
            "layer_key": layer,
            "event_id": event_id,
            "geofence_id": geofence_id,
            "scheduled_task_id": scheduled_task_id,
            "interval_seconds": interval_seconds,
            "severity": severity,
        }
        payload.update({key: value for key, value in optional_values.items() if value is not None})
        json_options = (
            ("rule_json", rule_json, "--rule-json"),
            (
                "notification_policy_json",
                notification_policy_json,
                "--notification-policy-json",
            ),
            ("metadata_json", metadata_json, "--metadata-json"),
            ("provenance_json", provenance_json, "--provenance-json"),
        )
        for key, raw_value, option_name in json_options:
            parsed_value = parse_json_object_option(raw_value, option_name)
            if parsed_value is not None:
                payload[key] = parsed_value
        watch = update_watch(
            session,
            watch_id,
            WatchUpdate(**payload),
            actor="cli",
        )
        print_banner()
        typer.echo(
            f"watch={watch.watch_id} | slug={watch.slug} | type={watch.watch_type} "
            f"| state={watch.state} | severity={watch.severity}"
        )
    finally:
        session.close()


@app.command("pause-watch")
def pause_watch_command(watch_id: int) -> None:
    init_db()
    session = get_session_factory()()
    try:
        watch = pause_watch(session, watch_id, actor="cli")
        print_banner()
        typer.echo(f"watch={watch.watch_id} | state={watch.state}")
    finally:
        session.close()


@app.command("resume-watch")
def resume_watch_command(watch_id: int) -> None:
    init_db()
    session = get_session_factory()()
    try:
        watch = resume_watch(session, watch_id, actor="cli")
        print_banner()
        typer.echo(f"watch={watch.watch_id} | state={watch.state} | next={watch.next_run_at}")
    finally:
        session.close()


@app.command("run-watch")
def run_watch_command(watch_id: int, force: bool = False) -> None:
    init_db()
    session = get_session_factory()()
    try:
        watch_run = evaluate_watch(session, watch_id, actor="cli", force=force)
        print_banner()
        typer.echo(
            f"watch_run={watch_run.watch_run_id} | watch={watch_run.watch_id} "
            f"| status={watch_run.status} | outcome={watch_run.outcome} "
            f"| changed={watch_run.change_detected} | alert={watch_run.alert_id} "
            f"| storage={watch_run.storage_object_id}"
        )
    finally:
        session.close()


@app.command("add-watch-schedule")
def add_watch_schedule_command(
    watch_id: int,
    name: str,
    interval_seconds: int,
    enabled: bool = True,
    retry_attempts: int = 1,
    retry_backoff_seconds: float = 0.0,
    notes: str = "",
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        task = attach_watch_schedule(
            session,
            watch_id,
            WatchScheduleCreate(
                name=name,
                interval_seconds=interval_seconds,
                enabled=enabled,
                retry_attempts=retry_attempts,
                retry_backoff_seconds=retry_backoff_seconds,
                notes=notes,
            ),
            actor="cli",
        )
        print_banner()
        typer.echo(
            f"watch={watch_id} | scheduled_task={task.task_id} | every={task.interval_seconds}s "
            f"| enabled={task.enabled} | next={task.next_run_at}"
        )
    finally:
        session.close()


@app.command("list-watch-runs")
def list_watch_runs_command(
    watch_id: int | None = None,
    status: str | None = None,
    outcome: str | None = None,
    limit: int = 200,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        rows = list_watch_runs(
            session,
            watch_id=watch_id,
            status=status,
            outcome=outcome,
            limit=limit,
        )
        print_banner()
        for row in rows:
            typer.echo(
                f"{row.watch_run_id} | watch={row.watch_id} | {row.status} | {row.outcome} "
                f"| changed={row.change_detected} | alert={row.alert_id} "
                f"| storage={row.storage_object_id} | started={row.started_at}"
            )
    finally:
        session.close()


@app.command("list-watch-alerts")
def list_watch_alerts_command(
    watch_id: int | None = None,
    status: str | None = None,
    limit: int = 200,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        rows = list_watch_alerts(session, watch_id=watch_id, status=status, limit=limit)
        print_banner()
        for row in rows:
            typer.echo(f"{row.alert_id} | {row.severity} | {row.status} | {row.message}")
    finally:
        session.close()


@app.command("show-watch-evidence")
def show_watch_evidence_command(watch_id: int, limit: int = 200) -> None:
    init_db()
    session = get_session_factory()()
    try:
        rows = list_watch_evidence(session, watch_id, limit=limit)
        print_banner()
        for row in rows:
            typer.echo(
                f"{row.storage_object_id} | {row.object_kind} | hash={row.content_hash} "
                f"| media={row.media_type} | bytes={row.byte_size} | uri={row.object_uri}"
            )
    finally:
        session.close()


@app.command("show-watch-feed")
def show_watch_feed_command(
    base_url: str = "http://127.0.0.1:8000",
    preview: bool = False,
    watch_id: int | None = None,
    status: str = "open",
    limit: int = 50,
) -> None:
    normalized_base_url = base_url.rstrip("/")
    query: dict[str, object] = {"status": status}
    if watch_id is not None:
        query["watch_id"] = watch_id
    if limit != 50:
        query["limit"] = limit
    feed_url = f"{normalized_base_url}/api/watches/feed.rss?{urlencode(query)}"
    if not preview:
        print_banner()
        typer.echo(feed_url)
        return

    init_db()
    session = get_session_factory()()
    try:
        content = render_watch_alert_rss(
            session,
            base_url=normalized_base_url,
            watch_id=watch_id,
            status=status,
            limit=limit,
        )
        print_banner()
        typer.echo(content.decode("utf-8") if isinstance(content, bytes) else content)
    finally:
        session.close()


@app.command("list-alerts")
def list_alerts(
    status: str | None = None,
    geofence_id: int | None = None,
    event_id: int | None = None,
    severity: str | None = None,
    limit: int = 50,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        alerts = list_alert_records(
            session,
            status=status,
            geofence_id=geofence_id,
            event_id=event_id,
            severity=severity,
            limit=limit,
        )
        print_banner()
        for alert in alerts:
            typer.echo(
                f"{alert.alert_id} | geofence={alert.geofence_id} | event={alert.event_id} | {alert.severity} | {alert.status} | {alert.message}"
            )
    finally:
        session.close()


@app.command("show-alert-summary")
def show_alert_summary_command(
    status: str | None = None,
    geofence_id: int | None = None,
    event_id: int | None = None,
    severity: str | None = None,
    stale_after_hours: float = 24.0,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        summary = build_alert_inventory_summary(
            session,
            status=status,
            geofence_id=geofence_id,
            event_id=event_id,
            severity=severity,
            stale_after_hours=stale_after_hours,
        )
        serializable = TypeAdapter(AlertInventorySummaryRead).validate_python(summary).model_dump(mode="json")
        print_banner()
        typer.echo(
            "totals="
            f"{serializable['total_count']} open={serializable['open_count']} acknowledged={serializable['acknowledged_count']} "
            f"closed={serializable['closed_count']} stale_open={serializable['stale_open_count']}"
        )
        typer.echo(
            f"scoped geofence={serializable['geofence_scoped_count']} event={serializable['event_scoped_count']} unscoped={serializable['unscoped_count']}"
        )
        for group_name in ("severity_counts", "status_counts", "geofence_counts"):
            typer.echo(f"{group_name}:")
            for item in serializable[group_name]:
                typer.echo(
                    f"  {item['key']} | total={item['total_count']} | open={item['open_count']} | acknowledged={item['acknowledged_count']} | closed={item['closed_count']} | stale_open={item['stale_open_count']}"
                )
    finally:
        session.close()


@app.command("show-alert-report-index")
def show_alert_report_index_command(
    status: str | None = None,
    geofence_id: int | None = None,
    event_id: int | None = None,
    severity: str | None = None,
    stale_after_hours: float = 24.0,
    limit: int = 25,
    stale_alert_limit: int = 25,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        report = build_alert_ops_report_index(
            session,
            status=status,
            geofence_id=geofence_id,
            event_id=event_id,
            severity=severity,
            stale_after_hours=stale_after_hours,
            limit=limit,
            stale_alert_limit=stale_alert_limit,
        )
        serializable = TypeAdapter(AlertOpsReportIndexRead).validate_python(report).model_dump(mode="json")
        print_banner()
        typer.echo(
            f"geofence_scan_tasks={serializable['geofence_scan_task_count']} geofence_scan_runs={serializable['geofence_scan_run_count']} failures={serializable['geofence_scan_failure_count']}"
        )
        typer.echo(
            f"latest_alert_at={serializable['latest_alert_at']} stale_after_hours={serializable['stale_after_hours']}"
        )
        inventory = serializable["inventory_summary"]
        typer.echo(
            f"inventory total={inventory['total_count']} open={inventory['open_count']} acknowledged={inventory['acknowledged_count']} closed={inventory['closed_count']} stale_open={inventory['stale_open_count']}"
        )
        typer.echo("recent_alerts:")
        for row in serializable["recent_alerts"]:
            typer.echo(
                f"  {row['alert_id']} | geofence={row['geofence_id']} | event={row['event_id']} | {row['severity']} | {row['status']} | {row['message']}"
            )
        typer.echo("stale_open_alerts:")
        for row in serializable["stale_open_alerts"]:
            typer.echo(
                f"  {row['alert_id']} | geofence={row['geofence_id']} | event={row['event_id']} | {row['severity']} | created={row['created_at']}"
            )
    finally:
        session.close()


@app.command("export-alert-summary")
def export_alert_summary_command(
    output_path: Path,
    status: str | None = None,
    geofence_id: int | None = None,
    event_id: int | None = None,
    severity: str | None = None,
    stale_after_hours: float = 24.0,
    alert_limit: int = 500,
    report_limit: int = 25,
    stale_alert_limit: int = 25,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        report = build_alert_ops_export_summary(
            session,
            status=status,
            geofence_id=geofence_id,
            event_id=event_id,
            severity=severity,
            stale_after_hours=stale_after_hours,
            alert_limit=alert_limit,
            report_limit=report_limit,
            stale_alert_limit=stale_alert_limit,
        )
        serializable = TypeAdapter(AlertOpsExportSummaryRead).validate_python(report).model_dump(mode="json")
        write_json_export_artifact(
            session,
            payload=serializable,
            object_kind="alert_summary_export",
            owner_type="alert_export",
            owner_id=str(geofence_id or event_id or "scoped"),
            output_path=output_path,
            source_uri="/api/alerts/export/summary",
            observed_at=report["generated_at"],
            metadata_json=serializable["filters_json"],
            actor="cli_export",
        )
        print_banner()
        typer.echo(f"exported alert summary to {output_path}")
    finally:
        session.close()


@app.command("update-alert")
def update_alert_command(
    alert_id: int,
    status: str,
    disposition_note: str = "",
    severity: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        alert = session.get(AlertORM, alert_id)
        if alert is None:
            raise typer.BadParameter(f"Alert {alert_id} does not exist.")
        previous_status = alert.status
        alert.status = status
        if severity is not None:
            alert.severity = severity
        alert.disposition_note = disposition_note
        session.add(
            CustodyLogORM(
                object_type="alert",
                object_id=str(alert.alert_id),
                action="alert_updated",
                actor="cli",
                details_json={
                    "previous_status": previous_status,
                    "status": alert.status,
                    "severity": alert.severity,
                    "disposition_note": alert.disposition_note,
                },
            )
        )
        session.commit()
        session.refresh(alert)
        print_banner()
        typer.echo(
            f"alert={alert.alert_id} status={alert.status} severity={alert.severity} note={alert.disposition_note}"
        )
    finally:
        session.close()


@app.command("list-custody")
def list_custody(limit: int = 20) -> None:
    init_db()
    session = get_session_factory()()
    try:
        statement = select(CustodyLogORM).order_by(CustodyLogORM.created_at.desc()).limit(limit)
        rows = list(session.scalars(statement))
        print_banner()
        for row in rows:
            typer.echo(f"{row.custody_log_id} | {row.object_type} | {row.action} | {row.actor}")
    finally:
        session.close()


@app.command("list-storage-objects")
def list_storage_objects_command(
    owner_type: str | None = None,
    owner_id: str | None = None,
    object_kind: str | None = None,
    lifecycle_status: str | None = None,
    retention_class: str | None = None,
    limit: int = 50,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        rows = list_storage_objects(
            session,
            owner_type=owner_type,
            owner_id=owner_id,
            object_kind=object_kind,
            lifecycle_status=lifecycle_status,
            retention_class=retention_class,
            limit=limit,
        )
        print_banner()
        for row in rows:
            typer.echo(
                f"{row.storage_object_id} | {row.object_kind} | owner={row.owner_type}:{row.owner_id} | tier={row.storage_tier} | retention={row.retention_class} | status={row.lifecycle_status} | {row.object_uri}"
            )
    finally:
        session.close()


@app.command("show-storage-report")
def show_storage_report_command(limit: int = 25) -> None:
    init_db()
    session = get_session_factory()()
    try:
        report = build_storage_report(session, limit=limit)
        serializable = TypeAdapter(StorageReportRead).validate_python(report).model_dump(mode="json")
        print_banner()
        typer.echo(
            "storage "
            f"total={serializable['total_count']} active={serializable['active_count']} "
            f"expired={serializable['expired_count']} promoted={serializable['promoted_count']} "
            f"archived={serializable['archived_count']}"
        )
        typer.echo(
            f"next_expiration_at={serializable['next_expiration_at']} oldest_expired_at={serializable['oldest_expired_at']}"
        )
        if serializable["expiring_objects"]:
            typer.echo("expiring_objects:")
            for row in serializable["expiring_objects"]:
                typer.echo(
                    f"  {row['storage_object_id']} | {row['object_kind']} | status={row['lifecycle_status']} | expires_at={row['expires_at']}"
                )
    finally:
        session.close()


@app.command("show-storage-manifest")
def show_storage_manifest_command(storage_object_id: int) -> None:
    init_db()
    session = get_session_factory()()
    try:
        manifest = get_storage_manifest_for_object(session, storage_object_id)
        print_banner()
        typer.echo(f"storage_object={storage_object_id}")
        print_storage_manifest(manifest)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("add-storage-object")
def add_storage_object_command(
    object_key: str,
    object_kind: str,
    owner_type: str,
    owner_id: str,
    object_uri: str,
    storage_tier: str = "hot",
    retention_class: str = "operational",
    lifecycle_status: str = "active",
    source_uri: str | None = None,
    content_hash: str | None = None,
    media_type: str | None = None,
    byte_size: int | None = None,
    metadata_json: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        metadata = parse_json_object_option(metadata_json, "--metadata-json") or {}
        record = create_storage_object(
            session,
            StorageObjectCreate(
                object_key=object_key,
                object_kind=object_kind,
                owner_type=owner_type,
                owner_id=owner_id,
                object_uri=object_uri,
                storage_tier=storage_tier,
                retention_class=retention_class,
                lifecycle_status=lifecycle_status,
                source_uri=source_uri,
                content_hash=content_hash,
                media_type=media_type,
                byte_size=byte_size,
                metadata_json=metadata,
            ),
            actor="cli_storage",
        )
        serializable = TypeAdapter(StorageObjectRead).validate_python(record).model_dump(mode="json")
        print_banner()
        typer.echo(
            f"storage_object={serializable['storage_object_id']} tier={serializable['storage_tier']} retention={serializable['retention_class']} status={serializable['lifecycle_status']}"
        )
    finally:
        session.close()


@app.command("run-storage-lifecycle")
def run_storage_lifecycle_command(
    retention_class: str | None = None,
    limit: int = 100,
    dry_run: bool = False,
    operation: list[str] = typer.Option(default_factory=list),
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = sweep_expired_storage_objects(
            session,
            retention_class=retention_class,
            limit=limit,
            dry_run=dry_run,
            actor="cli_storage",
            operations=operation or None,
        )
        serializable = TypeAdapter(StorageLifecycleSweepResultRead).validate_python(result).model_dump(mode="json")
        print_banner()
        typer.echo(
            f"swept_at={serializable['swept_at']} dry_run={serializable['dry_run']} "
            f"candidates={serializable['expired_candidate_count']} transitioned={serializable['transitioned_count']} "
            f"processed={serializable['processed_count']} failed={serializable['failed_count']}"
        )
        typer.echo(f"operations={serializable['filters_json']['operations']}")
        for row in serializable["candidates"]:
            typer.echo(
                f"  {row['storage_object_id']} | {row['object_kind']} | retention={row['retention_class']} | status={row['lifecycle_status']} | expires_at={row['expires_at']}"
            )
    finally:
        session.close()


@app.command("promote-storage-object")
def promote_storage_object_command(
    storage_object_id: int,
    promoted_by_type: str,
    promoted_by_id: str,
    storage_tier: str = "warm",
    retention_class: str | None = None,
    metadata_json: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        metadata = parse_json_object_option(metadata_json, "--metadata-json") or {}
        record = promote_storage_object(
            session,
            storage_object_id,
            StorageObjectPromoteRequest(
                storage_tier=storage_tier,
                retention_class=retention_class,
                promoted_by_type=promoted_by_type,
                promoted_by_id=promoted_by_id,
                metadata_json=metadata,
            ),
            actor="cli_storage",
        )
        print_banner()
        typer.echo(
            f"storage_object={record.storage_object_id} tier={record.storage_tier} retention={record.retention_class} status={record.lifecycle_status}"
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("transition-storage-object")
def transition_storage_object_command(
    storage_object_id: int,
    lifecycle_status: str,
    storage_tier: str | None = None,
    metadata_json: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        metadata = parse_json_object_option(metadata_json, "--metadata-json") or {}
        record = transition_storage_object(
            session,
            storage_object_id,
            StorageObjectTransitionRequest(
                lifecycle_status=lifecycle_status,
                storage_tier=storage_tier,
                metadata_json=metadata,
            ),
            actor="cli_storage",
        )
        print_banner()
        typer.echo(
            f"storage_object={record.storage_object_id} tier={record.storage_tier} retention={record.retention_class} status={record.lifecycle_status}"
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("archive-storage-object")
def archive_storage_object_command(storage_object_id: int, prune_local: bool = False) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = archive_storage_object(
            session,
            storage_object_id,
            actor="cli_storage",
            prune_local=prune_local,
        )
        print_storage_action_result(result)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("verify-storage-object")
def verify_storage_object_command(storage_object_id: int) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = verify_storage_object(session, storage_object_id, actor="cli_storage")
        print_storage_action_result(result)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("request-storage-rehydrate")
def request_storage_rehydrate_command(storage_object_id: int) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = request_storage_object_rehydration(session, storage_object_id, actor="cli_storage")
        print_storage_action_result(result)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("rehydrate-storage-object")
def rehydrate_storage_object_command(
    storage_object_id: int,
    target_path: Path | None = None,
    replace_existing: bool = False,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = rehydrate_storage_object(
            session,
            storage_object_id,
            target_path=target_path,
            replace_existing=replace_existing,
            actor="cli_storage",
        )
        print_storage_action_result(result)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("prune-storage-object")
def prune_storage_object_command(storage_object_id: int) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = prune_storage_object(session, storage_object_id, actor="cli_storage")
        print_storage_action_result(result)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("quarantine-storage-object")
def quarantine_storage_object_command(storage_object_id: int, reason: str) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = quarantine_storage_object(
            session,
            storage_object_id,
            reason=reason,
            actor="cli_storage",
        )
        print_storage_action_result(result)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("unquarantine-storage-object")
def unquarantine_storage_object_command(storage_object_id: int, note: str | None = None) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = unquarantine_storage_object(
            session,
            storage_object_id,
            note=note,
            actor="cli_storage",
        )
        print_storage_action_result(result)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("show-operations-report")
def show_operations_report(hours: float | None = 24.0, limit: int = 10) -> None:
    init_db()
    session = get_session_factory()()
    try:
        since = resolve_report_since(hours)
        report = TypeAdapter(OperationsReportRead).validate_python(
            build_operations_report(session, since=since, limit=limit)
        ).model_dump(mode="json")
        summary = report["summary"]
        print_banner()
        typer.echo(f"generated_at: {report['generated_at']}")
        typer.echo(f"scope_since: {report['scope_since']}")
        typer.echo(
            f"imports={summary['import_run_count']} imported={summary['imported_record_count']} skipped={summary['skipped_record_count']}"
        )
        typer.echo(
            f"source_runs={summary['source_run_count']} source_failures={summary['source_run_failure_count']}"
        )
        typer.echo(
            f"task_runs={summary['scheduled_task_run_count']} task_failures={summary['scheduled_task_run_failure_count']}"
        )
        typer.echo(
            f"alerts={summary['alert_count']} open={summary['open_alert_count']} acknowledged={summary['acknowledged_alert_count']} closed={summary['closed_alert_count']}"
        )
        typer.echo(
            f"events={summary['event_count']} entities={summary['entity_count']} observations={summary['observation_count']}"
        )
        alert_summary = report["alert_inventory_summary"]
        typer.echo(
            f"alert_inventory={alert_summary['total_count']} open={alert_summary['open_count']} acknowledged={alert_summary['acknowledged_count']} closed={alert_summary['closed_count']} stale_open={alert_summary['stale_open_count']}"
        )
        alert_report = report["alert_report_index"]
        typer.echo(
            f"alert_geofence_scan_tasks={alert_report['geofence_scan_task_count']} geofence_scan_runs={alert_report['geofence_scan_run_count']} geofence_scan_failures={alert_report['geofence_scan_failure_count']}"
        )
        storage_report = report["storage_report"]
        typer.echo(
            "storage="
            f"{storage_report['total_count']} active={storage_report['active_count']} "
            f"expired={storage_report['expired_count']} archived={storage_report['archived_count']}"
        )
        worker_summary = report["worker_status_summary"]
        typer.echo(
            f"workers={worker_summary['total_count']} active={worker_summary['active_count']} stale={worker_summary['stale_count']}"
        )
        clickhouse_diagnostics = report["clickhouse_diagnostics"]
        typer.echo(
            "clickhouse="
            f"{clickhouse_diagnostics['status']} enabled={clickhouse_diagnostics['enabled']} "
            f"reachable={clickhouse_diagnostics['reachable']} mode={clickhouse_diagnostics['storage_mode']}"
        )
        readiness = report["runtime_readiness"]
        typer.echo(
            "readiness="
            f"{readiness['overall_status']} ready={readiness['ready']} "
            f"action_required={readiness['action_required_count']} warnings={readiness['warning_count']}"
        )
        readiness_checks = readiness_check_lookup(readiness)
        snapshot_check = readiness_checks.get("runtime_snapshot_coverage")
        if snapshot_check is not None:
            typer.echo(
                "backup_coverage="
                f"{snapshot_check['status']} | {snapshot_check['summary']}"
            )
        scheduler_summary = report["scheduler_inventory_summary"]
        typer.echo(
            "scheduler="
            f"{scheduler_summary['total_count']} due={scheduler_summary['due_count']} "
            f"overdue={scheduler_summary['overdue_count']} failing={scheduler_summary['failing_count']}"
        )
        scheduler_report = report["scheduler_report_index"]
        typer.echo(
            f"scheduler_runs={scheduler_report['task_run_count']} scheduler_failures={scheduler_report['task_run_failure_count']} "
            f"maintenance_runs={scheduler_report['maintenance_run_count']} maintenance_failures={scheduler_report['maintenance_failure_count']}"
        )
        source_summary = report["source_inventory_summary"]
        typer.echo(
            f"sources={source_summary['total_count']} scheduled={source_summary['scheduled_count']} unscheduled={source_summary['unscheduled_count']} stale={source_summary['stale_count']} failing={source_summary['failing_count']}"
        )
        source_report = report["source_report_index"]
        typer.echo(
            f"source_sync_tasks={source_report['sync_task_count']} source_sync_runs={source_report['sync_run_count']} source_sync_failures={source_report['sync_failure_count']}"
        )
        event_summary = report["event_inventory_summary"]
        typer.echo(
            "event_inventory="
            f"{event_summary['total_count']} open={event_summary['open_count']} closed={event_summary['closed_count']} "
            f"stale_open={event_summary['stale_open_count']} product_covered={event_summary['product_covered_count']} "
            f"alert_scoped={event_summary['alert_scoped_count']}"
        )
        event_report = report["event_report_index"]
        typer.echo(
            f"event_fusion_tasks={event_report['event_fusion_task_count']} event_fusion_runs={event_report['event_fusion_run_count']} event_fusion_failures={event_report['event_fusion_failure_count']}"
        )
        entity_summary = report["entity_inventory_summary"]
        typer.echo(
            "entity_inventory="
            f"{entity_summary['total_count']} high={entity_summary['high_confidence_count']} "
            f"medium={entity_summary['medium_confidence_count']} low={entity_summary['low_confidence_count']} "
            f"signal_conflicts={entity_summary['signal_conflict_count']}"
        )
        entity_report = report["entity_report_index"]
        typer.echo(
            f"entity_resolution_tasks={entity_report['entity_resolution_task_count']} entity_resolution_runs={entity_report['entity_resolution_run_count']} entity_resolution_failures={entity_report['entity_resolution_failure_count']}"
        )
        camera_summary = report["camera_inventory_summary"]
        typer.echo(
            f"cameras={camera_summary['total_count']} active={camera_summary['active_count']} inactive={camera_summary['inactive_count']} stale={camera_summary['stale_count']}"
        )
        camera_report = report["camera_report_index"]
        typer.echo(
            f"camera_refresh_tasks={camera_report['refresh_task_count']} refresh_runs={camera_report['refresh_run_count']} refresh_failures={camera_report['refresh_failure_count']}"
        )
        camera_source_summary = report["camera_source_inventory_summary"]
        typer.echo(
            f"camera_sources={camera_source_summary['total_count']} active={camera_source_summary['active_count']} ready={camera_source_summary['ready_count']} review={camera_source_summary['review_count']} candidate={camera_source_summary['candidate_count']} graduated={camera_source_summary['graduated_count']}"
        )
        camera_source_report = report["camera_source_report_index"]
        typer.echo(
            f"camera_source_refresh_tasks={camera_source_report['refresh_task_count']} refresh_runs={camera_source_report['refresh_run_count']} refresh_failures={camera_source_report['refresh_failure_count']}"
        )
    finally:
        session.close()


@app.command("show-worker-summary")
def show_worker_summary(stale_after_seconds: float | None = None) -> None:
    init_db()
    session = get_session_factory()()
    try:
        report = build_worker_status_summary(session, stale_after_seconds=stale_after_seconds)
        serializable = TypeAdapter(WorkerStatusSummaryRead).validate_python(report).model_dump(mode="json")
        print_banner()
        typer.echo(
            f"workers={serializable['total_count']} active={serializable['active_count']} stale={serializable['stale_count']} stale_before={serializable['stale_before']}"
        )
        typer.echo("worker_type_counts:")
        for item in serializable["worker_type_counts"]:
            typer.echo(
                f"  {item['key']} | total={item['total_count']} | active={item['active_count']} | stale={item['stale_count']}"
            )
        typer.echo("workers:")
        for item in serializable["workers"]:
            typer.echo(
                f"  {item['worker_key']} | {item['status']} | seen={item['last_seen_at']} | started={item['last_started_at']} | stopped={item['last_stopped_at']}"
            )
    finally:
        session.close()


@app.command("check-worker-health")
def check_worker_health_command(
    worker_type: str | None = None,
    actor: str | None = None,
    worker_key: str | None = None,
    stale_after_seconds: float | None = None,
    require_active: bool = False,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        report = evaluate_worker_health(
            session,
            worker_type=worker_type,
            actor=actor,
            worker_key=worker_key,
            stale_after_seconds=stale_after_seconds,
            require_active=require_active,
        )
        print_banner()
        typer.echo(
            "worker_health "
            f"healthy={report['healthy']} matching={report['matching_count']} "
            f"healthy_matching={report['healthy_count']} stale_after_seconds={report['stale_after_seconds']}"
        )
        if not report["healthy"]:
            raise typer.Exit(code=1)
    finally:
        session.close()


@app.command("export-operations-report")
def export_operations_report(output_path: Path, hours: float | None = 24.0, limit: int = 25) -> None:
    init_db()
    session = get_session_factory()()
    try:
        export_operations_report_artifact(
            session,
            output_path=output_path,
            hours=hours,
            limit=limit,
            actor="cli_export",
        )
        print_banner()
        typer.echo(f"exported operations report to {output_path}")
    finally:
        session.close()


@app.command("export-runtime-snapshot")
def export_runtime_snapshot_command(
    output_path: Path,
    manifest_path: Path | None = typer.Option(
        None,
        "--manifest-path",
        help="Optional sidecar manifest path. Defaults to '<snapshot>.manifest.json'.",
    ),
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        export_result = export_runtime_snapshot_artifacts(
            session,
            output_path=output_path,
            manifest_path=manifest_path,
            actor="cli_export",
        )
        print_banner()
        typer.echo(f"exported runtime snapshot to {export_result['output_path']}")
        typer.echo(f"exported runtime snapshot manifest to {export_result['manifest_path']}")
    finally:
        session.close()


@app.command("verify-runtime-snapshot")
def verify_runtime_snapshot_command(
    input_path: Path,
    manifest_path: Path | None = typer.Option(
        None,
        "--manifest-path",
        help="Optional manifest path. Defaults to '<snapshot>.manifest.json'.",
    ),
) -> None:
    snapshot, manifest, resolved_manifest_path = verify_runtime_snapshot_artifact(
        input_path,
        manifest_path=manifest_path,
    )
    print_banner()
    typer.echo(
        "verified runtime snapshot artifact "
        f"| snapshot={input_path.expanduser().resolve()} "
        f"| manifest={resolved_manifest_path} "
        f"| exported_at={snapshot.exported_at.isoformat()} "
        f"| total_records={sum(item.row_count for item in manifest.row_counts)}"
    )
    typer.echo(
        "snapshot integrity "
        f"sha256={manifest.snapshot_sha256} "
        f"byte_size={manifest.snapshot_byte_size} "
        f"sections={len(manifest.section_counts)}"
    )


@app.command("restore-runtime-snapshot")
def restore_runtime_snapshot_command(
    input_path: Path,
    replace_existing: bool = False,
    manifest_path: Path | None = typer.Option(
        None,
        "--manifest-path",
        help="Optional manifest path. Defaults to '<snapshot>.manifest.json'.",
    ),
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        snapshot, manifest, resolved_manifest_path = verify_runtime_snapshot_artifact(
            input_path,
            manifest_path=manifest_path,
        )
        try:
            result = restore_runtime_snapshot(
                session,
                snapshot.model_dump(mode="python"),
                replace_existing=replace_existing,
            )
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        serializable = TypeAdapter(RuntimeRestoreResultRead).validate_python(result).model_dump(mode="json")
        print_banner()
        typer.echo(
            "restored runtime snapshot "
            f"| replaced_existing={serializable['replaced_existing']} "
            f"| total_records={serializable['total_records']} "
            f"| manifest={resolved_manifest_path} "
            f"| verified_sha256={manifest.snapshot_sha256}"
        )
        for item in serializable["row_counts"]:
            typer.echo(f"{item['table_name']}: {item['row_count']}")
    finally:
        session.close()


@app.command("export-runtime-bundle")
def export_runtime_bundle_command(output_path: Path) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = export_runtime_bundle(
            session,
            output_path=output_path,
            actor="cli_export",
        )
        manifest = result["manifest"]
        evidence = manifest.get("evidence", []) if isinstance(manifest, dict) else []
        print_banner()
        typer.echo(
            "exported runtime bundle "
            f"| path={result['output_path']} | sha256={result['bundle_sha256']} "
            f"| evidence_count={len(evidence)}"
        )
    finally:
        session.close()


@app.command("restore-runtime-bundle")
def restore_runtime_bundle_command(
    input_path: Path,
    replace_existing: bool = False,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        try:
            result = restore_runtime_bundle(
                session,
                bundle_path=input_path,
                replace_existing=replace_existing,
                actor="cli_restore",
            )
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        restore_result = result["restore_result"]
        print_banner()
        typer.echo(
            "restored runtime bundle "
            f"| path={result['bundle_path']} | sha256={result['bundle_sha256']} "
            f"| evidence_count={result['restored_evidence_count']} "
            f"| replaced_existing={restore_result['replaced_existing']} "
            f"| total_records={restore_result['total_records']}"
        )
        for item in restore_result["row_counts"]:
            typer.echo(f"{item['table_name']}: {item['row_count']}")
    finally:
        session.close()


@app.command("add-local-import-schedule")
def add_local_import_schedule(
    name: str,
    source_path: Path,
    interval_seconds: int,
    layer: str = "unassigned",
    notes: str = "",
    retry_attempts: int = 1,
    retry_backoff_seconds: float = 0.0,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        task = create_scheduled_task(
            session,
            ScheduledTaskCreate(
                name=name,
                task_type="local_import",
                interval_seconds=interval_seconds,
                retry_attempts=retry_attempts,
                retry_backoff_seconds=retry_backoff_seconds,
                target_path=str(source_path),
                layer_key=layer,
                notes=notes,
            ),
        )
        print_banner()
        typer.echo(f"scheduled task {task.task_id} created for local import")
    finally:
        session.close()


@app.command("add-geofence-scan-schedule")
def add_geofence_scan_schedule(
    name: str,
    interval_seconds: int,
    geofence_id: int | None = None,
    notes: str = "",
    retry_attempts: int = 1,
    retry_backoff_seconds: float = 0.0,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        task = create_scheduled_task(
            session,
            ScheduledTaskCreate(
                name=name,
                task_type="geofence_scan",
                interval_seconds=interval_seconds,
                retry_attempts=retry_attempts,
                retry_backoff_seconds=retry_backoff_seconds,
                geofence_id=geofence_id,
                notes=notes,
            ),
        )
        print_banner()
        typer.echo(f"scheduled task {task.task_id} created for geofence scan")
    finally:
        session.close()


@app.command("add-observation-watch-schedule")
def add_observation_watch_schedule(
    name: str,
    query: str,
    interval_seconds: int,
    layer: str | None = None,
    source_domain: str | None = None,
    trust_level: str | None = None,
    severity: str = "info",
    limit: int = 25,
    candidate_limit: int = 500,
    lookback_hours: float = 24.0,
    notes: str = "",
    retry_attempts: int = 1,
    retry_backoff_seconds: float = 0.0,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        payload_json: dict[str, object] = {
            "query": query,
            "severity": severity,
            "limit": limit,
            "candidate_limit": candidate_limit,
            "lookback_hours": lookback_hours,
        }
        if source_domain:
            payload_json["source_domain"] = source_domain
        if trust_level:
            payload_json["trust_level"] = trust_level
        task = create_scheduled_task(
            session,
            ScheduledTaskCreate(
                name=name,
                task_type="observation_watch_scan",
                interval_seconds=interval_seconds,
                retry_attempts=retry_attempts,
                retry_backoff_seconds=retry_backoff_seconds,
                layer_key=layer,
                notes=notes,
                payload_json=payload_json,
            ),
        )
        print_banner()
        typer.echo(f"scheduled task {task.task_id} created for observation watch scan")
    finally:
        session.close()


@app.command("add-source-sync-schedule")
def add_source_sync_schedule(
    name: str,
    source_id: int,
    interval_seconds: int,
    notes: str = "",
    retry_attempts: int = 1,
    retry_backoff_seconds: float = 0.0,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        task = create_scheduled_task(
            session,
            ScheduledTaskCreate(
                name=name,
                task_type="source_sync",
                interval_seconds=interval_seconds,
                retry_attempts=retry_attempts,
                retry_backoff_seconds=retry_backoff_seconds,
                source_id=source_id,
                notes=notes,
            ),
        )
        print_banner()
        typer.echo(f"scheduled task {task.task_id} created for source sync")
    finally:
        session.close()


@app.command("add-source-maintenance-schedule")
def add_source_maintenance_schedule(
    name: str,
    interval_seconds: int,
    stale_after_hours: float = 24.0,
    source_limit: int = 25,
    dead_letter_limit: int = 100,
    replay_dead_letters: bool = True,
    run_stale_sources: bool = True,
    run_failing_sources: bool = True,
    source_kind: str | None = None,
    notes: str = "",
    retry_attempts: int = 1,
    retry_backoff_seconds: float = 0.0,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        payload_json: dict[str, object] = {
            "stale_after_hours": stale_after_hours,
            "source_limit": source_limit,
            "dead_letter_limit": dead_letter_limit,
            "replay_dead_letters": replay_dead_letters,
            "run_stale_sources": run_stale_sources,
            "run_failing_sources": run_failing_sources,
        }
        if source_kind:
            payload_json["source_kind"] = source_kind
        task = create_scheduled_task(
            session,
            ScheduledTaskCreate(
                name=name,
                task_type="source_maintenance",
                interval_seconds=interval_seconds,
                retry_attempts=retry_attempts,
                retry_backoff_seconds=retry_backoff_seconds,
                notes=notes,
                payload_json=payload_json,
            ),
        )
        print_banner()
        typer.echo(f"scheduled task {task.task_id} created for source maintenance")
    finally:
        session.close()


@app.command("add-source-health-scan-schedule")
def add_source_health_scan_schedule(
    name: str,
    interval_seconds: int,
    stale_after_hours: float = 24.0,
    source_limit: int = 100,
    source_kind: str | None = None,
    alert_on_stale: bool = True,
    alert_on_failed: bool = True,
    alert_on_dead_letters: bool = True,
    notes: str = "",
    retry_attempts: int = 1,
    retry_backoff_seconds: float = 0.0,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        payload_json: dict[str, object] = {
            "stale_after_hours": stale_after_hours,
            "source_limit": source_limit,
            "alert_on_stale": alert_on_stale,
            "alert_on_failed": alert_on_failed,
            "alert_on_dead_letters": alert_on_dead_letters,
        }
        if source_kind:
            payload_json["source_kind"] = source_kind
        task = create_scheduled_task(
            session,
            ScheduledTaskCreate(
                name=name,
                task_type="source_health_scan",
                interval_seconds=interval_seconds,
                retry_attempts=retry_attempts,
                retry_backoff_seconds=retry_backoff_seconds,
                notes=notes,
                payload_json=payload_json,
            ),
        )
        print_banner()
        typer.echo(f"scheduled task {task.task_id} created for source health scan")
    finally:
        session.close()


@app.command("add-storage-lifecycle-schedule")
def add_storage_lifecycle_schedule(
    name: str,
    interval_seconds: int,
    retention_class: str | None = None,
    limit: int = 100,
    notes: str = "",
    retry_attempts: int = 1,
    retry_backoff_seconds: float = 0.0,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        payload_json: dict[str, object] = {"limit": limit}
        if retention_class:
            payload_json["retention_class"] = retention_class
        task = create_scheduled_task(
            session,
            ScheduledTaskCreate(
                name=name,
                task_type="storage_lifecycle",
                interval_seconds=interval_seconds,
                retry_attempts=retry_attempts,
                retry_backoff_seconds=retry_backoff_seconds,
                notes=notes,
                payload_json=payload_json,
            ),
        )
        print_banner()
        typer.echo(f"scheduled task {task.task_id} created for storage lifecycle sweep")
    finally:
        session.close()


@app.command("add-runtime-snapshot-schedule")
def add_runtime_snapshot_schedule(
    name: str,
    interval_seconds: int,
    output_dir: Path | None = None,
    file_prefix: str = "runtime-snapshot",
    notes: str = "",
    retry_attempts: int = 1,
    retry_backoff_seconds: float = 0.0,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        payload_json: dict[str, object] = {"file_prefix": file_prefix}
        if output_dir is not None:
            payload_json["output_dir"] = str(output_dir.expanduser().resolve())
        task = create_scheduled_task(
            session,
            ScheduledTaskCreate(
                name=name,
                task_type="runtime_snapshot_export",
                interval_seconds=interval_seconds,
                retry_attempts=retry_attempts,
                retry_backoff_seconds=retry_backoff_seconds,
                notes=notes,
                payload_json=payload_json,
            ),
        )
        print_banner()
        typer.echo(f"scheduled task {task.task_id} created for runtime snapshot export")
    finally:
        session.close()


@app.command("add-clickhouse-sync-schedule")
def add_clickhouse_sync_schedule(
    name: str,
    interval_seconds: int,
    layer: str | None = None,
    source_domain: str | None = None,
    limit: int = 1000,
    notes: str = "",
    retry_attempts: int = 1,
    retry_backoff_seconds: float = 0.0,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        payload_json: dict[str, object] = {"limit": limit}
        if source_domain:
            payload_json["source_domain"] = source_domain
        task = create_scheduled_task(
            session,
            ScheduledTaskCreate(
                name=name,
                task_type="clickhouse_sync",
                interval_seconds=interval_seconds,
                retry_attempts=retry_attempts,
                retry_backoff_seconds=retry_backoff_seconds,
                layer_key=layer,
                notes=notes,
                payload_json=payload_json,
            ),
        )
        print_banner()
        typer.echo(f"scheduled task {task.task_id} created for ClickHouse sync")
    finally:
        session.close()


@app.command("add-clickhouse-archive-schedule")
def add_clickhouse_archive_schedule(
    name: str,
    interval_seconds: int,
    layer: str | None = None,
    source_domain: str | None = None,
    limit: int | None = None,
    notes: str = "",
    retry_attempts: int = 1,
    retry_backoff_seconds: float = 0.0,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        payload_json: dict[str, object] = {}
        if source_domain:
            payload_json["source_domain"] = source_domain
        if limit is not None:
            payload_json["limit"] = limit
        task = create_scheduled_task(
            session,
            ScheduledTaskCreate(
                name=name,
                task_type="clickhouse_archive",
                interval_seconds=interval_seconds,
                retry_attempts=retry_attempts,
                retry_backoff_seconds=retry_backoff_seconds,
                layer_key=layer,
                notes=notes,
                payload_json=payload_json,
            ),
        )
        print_banner()
        typer.echo(f"scheduled task {task.task_id} created for ClickHouse archive")
    finally:
        session.close()


@app.command("add-camera-refresh-schedule")
def add_camera_refresh_schedule(
    name: str,
    interval_seconds: int,
    layer: str | None = None,
    source_domain: str | None = None,
    limit: int = 500,
    notes: str = "",
    retry_attempts: int = 1,
    retry_backoff_seconds: float = 0.0,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        payload_json: dict[str, object] = {"limit": limit}
        if source_domain:
            payload_json["source_domain"] = source_domain
        task = create_scheduled_task(
            session,
            ScheduledTaskCreate(
                name=name,
                task_type="camera_inventory_refresh",
                interval_seconds=interval_seconds,
                retry_attempts=retry_attempts,
                retry_backoff_seconds=retry_backoff_seconds,
                layer_key=layer,
                notes=notes,
                payload_json=payload_json,
            ),
        )
        print_banner()
        typer.echo(f"scheduled task {task.task_id} created for camera inventory refresh")
    finally:
        session.close()


@app.command("add-camera-source-verification-schedule")
def add_camera_source_verification_schedule(
    name: str,
    interval_seconds: int,
    layer: str | None = None,
    source_domain: str | None = None,
    endpoint_kind: str | None = None,
    limit: int = 200,
    timeout_seconds: float = 5.0,
    max_payload_bytes: int | None = None,
    allow_private_networks: bool = typer.Option(
        True,
        "--allow-private-networks/--block-private-networks",
    ),
    min_request_interval_seconds: float | None = None,
    notes: str = "",
    retry_attempts: int = 1,
    retry_backoff_seconds: float = 0.0,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        payload_json: dict[str, object] = {
            "limit": limit,
            "timeout_seconds": timeout_seconds,
            "allow_private_networks": allow_private_networks,
        }
        if source_domain:
            payload_json["source_domain"] = source_domain
        if endpoint_kind:
            payload_json["endpoint_kind"] = endpoint_kind
        if max_payload_bytes is not None:
            payload_json["max_payload_bytes"] = max_payload_bytes
        if min_request_interval_seconds is not None:
            payload_json["min_request_interval_seconds"] = min_request_interval_seconds
        task = create_scheduled_task(
            session,
            ScheduledTaskCreate(
                name=name,
                task_type="camera_source_verification",
                interval_seconds=interval_seconds,
                retry_attempts=retry_attempts,
                retry_backoff_seconds=retry_backoff_seconds,
                layer_key=layer,
                notes=notes,
                payload_json=payload_json,
            ),
        )
        print_banner()
        typer.echo(f"scheduled task {task.task_id} created for camera source verification")
    finally:
        session.close()


@app.command("add-entity-resolution-schedule")
def add_entity_resolution_schedule(
    name: str,
    interval_seconds: int,
    bbox: str | None = None,
    layer: str | None = None,
    source_domain: str | None = None,
    trust_level: str | None = None,
    limit: int = 500,
    min_observations: int = 2,
    entity_type: str | None = None,
    redaction_level: str = "public",
    notes: str = "",
    retry_attempts: int = 1,
    retry_backoff_seconds: float = 0.0,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        min_lon, min_lat, max_lon, max_lat = parse_bbox(bbox)
        payload_json: dict[str, object] = {
            "limit": limit,
            "min_observations": min_observations,
            "redaction_level": redaction_level,
        }
        optional_values = {
            "source_domain": source_domain,
            "trust_level": trust_level,
            "entity_type": entity_type,
            "min_lon": min_lon,
            "min_lat": min_lat,
            "max_lon": max_lon,
            "max_lat": max_lat,
        }
        for key, value in optional_values.items():
            if value is not None:
                payload_json[key] = value
        task = create_scheduled_task(
            session,
            ScheduledTaskCreate(
                name=name,
                task_type="entity_resolution_refresh",
                interval_seconds=interval_seconds,
                retry_attempts=retry_attempts,
                retry_backoff_seconds=retry_backoff_seconds,
                layer_key=layer,
                notes=notes,
                payload_json=payload_json,
            ),
        )
        print_banner()
        typer.echo(f"scheduled task {task.task_id} created for entity resolution refresh")
    finally:
        session.close()


@app.command("add-event-fusion-schedule")
def add_event_fusion_schedule(
    name: str,
    interval_seconds: int,
    bbox: str | None = None,
    layer: str | None = None,
    source_domain: str | None = None,
    trust_level: str | None = None,
    limit: int = 500,
    time_window_minutes: int = 60,
    distance_km: float = 25.0,
    min_independent_signals: int = 2,
    redaction_level: str = "public",
    notes: str = "",
    retry_attempts: int = 1,
    retry_backoff_seconds: float = 0.0,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        min_lon, min_lat, max_lon, max_lat = parse_bbox(bbox)
        payload_json: dict[str, object] = {
            "limit": limit,
            "time_window_minutes": time_window_minutes,
            "distance_km": distance_km,
            "min_independent_signals": min_independent_signals,
            "redaction_level": redaction_level,
        }
        optional_values = {
            "source_domain": source_domain,
            "trust_level": trust_level,
            "min_lon": min_lon,
            "min_lat": min_lat,
            "max_lon": max_lon,
            "max_lat": max_lat,
        }
        for key, value in optional_values.items():
            if value is not None:
                payload_json[key] = value
        task = create_scheduled_task(
            session,
            ScheduledTaskCreate(
                name=name,
                task_type="event_fusion_refresh",
                interval_seconds=interval_seconds,
                retry_attempts=retry_attempts,
                retry_backoff_seconds=retry_backoff_seconds,
                layer_key=layer,
                notes=notes,
                payload_json=payload_json,
            ),
        )
        print_banner()
        typer.echo(f"scheduled task {task.task_id} created for event fusion refresh")
    finally:
        session.close()


@app.command("show-scheduler-summary")
def show_scheduler_summary_command() -> None:
    init_db()
    session = get_session_factory()()
    try:
        summary = build_scheduler_inventory_summary(session)
        serializable = TypeAdapter(SchedulerInventorySummaryRead).validate_python(summary).model_dump(mode="json")
        print_banner()
        typer.echo(
            f"{serializable['total_count']} enabled={serializable['enabled_count']} disabled={serializable['disabled_count']} "
            f"due={serializable['due_count']} overdue={serializable['overdue_count']} failing={serializable['failing_count']} "
            f"maintenance={serializable['maintenance_task_count']}"
        )
        for group_name in ("task_type_counts", "latest_status_counts"):
            typer.echo(f"{group_name}:")
            for item in serializable[group_name]:
                typer.echo(
                    f"  {item['key']}: total={item['total_count']} enabled={item['enabled_count']} "
                    f"disabled={item['disabled_count']} due={item['due_count']} failing={item['failing_count']}"
                )
    finally:
        session.close()


@app.command("show-scheduler-report-index")
def show_scheduler_report_index_command(limit: int = 25, overdue_task_limit: int = 25) -> None:
    init_db()
    session = get_session_factory()()
    try:
        report = build_scheduler_ops_report_index(
            session,
            limit=limit,
            overdue_task_limit=overdue_task_limit,
        )
        serializable = TypeAdapter(SchedulerOpsReportIndexRead).validate_python(report).model_dump(mode="json")
        print_banner()
        typer.echo(
            f"task_runs={serializable['task_run_count']} failures={serializable['task_run_failure_count']} "
            f"maintenance_runs={serializable['maintenance_run_count']} maintenance_failures={serializable['maintenance_failure_count']}"
        )
        inventory = serializable["inventory_summary"]
        typer.echo(
            f"tasks={inventory['total_count']} due={inventory['due_count']} overdue={inventory['overdue_count']} "
            f"failing={inventory['failing_count']}"
        )
        typer.echo("task_type_run_counts:")
        for item in serializable["task_type_run_counts"]:
            typer.echo(
                f"  {item['key']}: total={item['total_count']} completed={item['completed_count']} failures={item['failure_count']}"
            )
        typer.echo("overdue_tasks:")
        for item in serializable["overdue_tasks"]:
            typer.echo(
                f"  {item['task']['task_id']} | {item['task']['task_type']} | next={item['task']['next_run_at']} | failing={item['is_failing']}"
            )
        typer.echo("failing_tasks:")
        for item in serializable["failing_tasks"]:
            latest_status = item["latest_run"]["status"] if item["latest_run"] else "never_run"
            typer.echo(
                f"  {item['task']['task_id']} | {item['task']['task_type']} | latest={latest_status} | next={item['task']['next_run_at']}"
            )
        typer.echo("maintenance_tasks:")
        for item in serializable["maintenance_tasks"]:
            typer.echo(
                f"  {item['task']['task_id']} | {item['task']['task_type']} | enabled={item['task']['enabled']} | next={item['task']['next_run_at']}"
            )
    finally:
        session.close()


@app.command("export-scheduler-summary")
def export_scheduler_summary_command(
    output_path: Path,
    task_limit: int = 500,
    report_limit: int = 25,
    overdue_task_limit: int = 25,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        report = build_scheduler_ops_export_summary(
            session,
            task_limit=task_limit,
            report_limit=report_limit,
            overdue_task_limit=overdue_task_limit,
        )
        serializable = TypeAdapter(SchedulerOpsExportSummaryRead).validate_python(report).model_dump(mode="json")
        write_json_export_artifact(
            session,
            payload=serializable,
            object_kind="scheduler_summary_export",
            owner_type="scheduler_export",
            owner_id="scoped",
            output_path=output_path,
            source_uri="/api/scheduler/export/summary",
            observed_at=report["generated_at"],
            metadata_json=serializable["filters_json"],
            actor="cli_export",
        )
        print_banner()
        typer.echo(f"exported scheduler summary to {output_path}")
    finally:
        session.close()


@app.command("list-schedules")
def list_schedules() -> None:
    init_db()
    session = get_session_factory()()
    try:
        rows = list(session.scalars(select(ScheduledTaskORM).order_by(ScheduledTaskORM.task_id.asc())))
        print_banner()
        for row in rows:
            parts = [
                f"{row.task_id}",
                row.task_type,
                f"every={row.interval_seconds}s",
                f"retry={row.retry_attempts}",
                f"backoff={row.retry_backoff_seconds}s",
                f"enabled={row.enabled}",
                f"next={row.next_run_at}",
            ]
            if row.layer_key:
                parts.append(f"layer={row.layer_key}")
            if row.source_id is not None:
                parts.append(f"source={row.source_id}")
            if row.geofence_id is not None:
                parts.append(f"geofence={row.geofence_id}")
            if row.target_path:
                parts.append(f"path={row.target_path}")
            if row.payload_json:
                parts.append(f"payload={json.dumps(row.payload_json, sort_keys=True)}")
            typer.echo(" | ".join(parts))
    finally:
        session.close()


@app.command("list-schedule-runs")
def list_schedule_runs(task_id: int | None = None) -> None:
    init_db()
    session = get_session_factory()()
    try:
        statement = select(ScheduledTaskRunORM).order_by(ScheduledTaskRunORM.task_run_id.desc())
        if task_id is not None:
            statement = statement.where(ScheduledTaskRunORM.task_id == task_id)
        rows = list(session.scalars(statement))
        print_banner()
        for row in rows:
            typer.echo(
                f"{row.task_run_id} | task={row.task_id} | {row.status} | records={row.records_affected} | started={row.started_at} | finished={row.finished_at}"
            )
    finally:
        session.close()


@app.command("run-due-schedules")
def run_due_schedules() -> None:
    init_db()
    session = get_session_factory()()
    try:
        runs = run_due_tasks(session, actor="cli")
        print_banner()
        typer.echo(f"runs_created={len(runs)}")
    finally:
        session.close()


@app.command("run-enabled-schedules")
def run_enabled_schedules(task_type: list[str] = typer.Option(default_factory=list)) -> None:
    init_db()
    session = get_session_factory()()
    try:
        runs = run_enabled_tasks(session, actor="cli", task_types=task_type or None)
        print_banner()
        typer.echo(f"runs_created={len(runs)} task_types={sorted({run.task.task_type for run in runs if run.task is not None})}")
    finally:
        session.close()


@app.command("scheduler-worker")
def scheduler_worker_command(
    poll_seconds: float | None = None,
    once: bool = False,
    max_iterations: int | None = None,
) -> None:
    init_db()
    resolved_poll_seconds = resolve_scheduler_poll_seconds(poll_seconds)
    print_banner()
    typer.echo(
        f"scheduler worker starting | poll_seconds={resolved_poll_seconds} | once={once} | max_iterations={max_iterations}"
    )

    def on_iteration(iteration: int, runs: list[ScheduledTaskRunORM]) -> None:
        typer.echo(
            f"iteration={iteration} runs_created={len(runs)} statuses={[run.status for run in runs]}"
        )

    result = run_scheduler_worker(
        get_session_factory(),
        poll_seconds=resolved_poll_seconds,
        actor="cli_scheduler_worker",
        once=once,
        max_iterations=max_iterations,
        on_iteration=on_iteration,
    )
    typer.echo(
        f"scheduler worker stopped | iterations={result.iterations} runs_created={result.runs_created}"
    )


@app.command("source-runtime-worker")
def source_runtime_worker_command(
    poll_seconds: float | None = None,
    once: bool = False,
    max_iterations: int | None = None,
    source_id: int | None = None,
) -> None:
    init_db()
    resolved_poll_seconds = resolve_scheduler_poll_seconds(poll_seconds)
    print_banner()
    typer.echo(
        "source runtime worker starting | "
        f"poll_seconds={resolved_poll_seconds} | once={once} | max_iterations={max_iterations} | source_id={source_id}"
    )

    def on_iteration(iteration: int, cycle: dict[str, object]) -> None:
        typer.echo(
            "iteration="
            f"{iteration} source_count={cycle.get('source_count', 0)} "
            f"runs={cycle.get('source_run_ids', [])} "
            f"seen={cycle.get('records_seen', 0)} imported={cycle.get('records_imported', 0)} "
            f"failed={cycle.get('records_failed', 0)}"
        )

    result = run_source_runtime_worker(
        get_session_factory(),
        poll_seconds=resolved_poll_seconds,
        actor="cli_source_runtime_worker",
        once=once,
        max_iterations=max_iterations,
        source_id=source_id,
        on_iteration=on_iteration,
    )
    typer.echo(
        "source runtime worker stopped | "
        f"iterations={result.iterations} source_runs={len(result.source_run_ids)} "
        f"seen={result.records_seen} imported={result.records_imported} failed={result.records_failed}"
    )


@app.command("platform-runtime-worker")
def platform_runtime_worker_command(
    poll_seconds: float | None = None,
    once: bool = False,
    max_iterations: int | None = None,
    source_id: int | None = None,
    include_stream_runtime: bool = True,
    include_enabled_schedules: bool = True,
    task_type: list[str] = typer.Option(default_factory=list),
) -> None:
    init_db()
    resolved_poll_seconds = resolve_scheduler_poll_seconds(poll_seconds)
    print_banner()
    typer.echo(
        "platform runtime worker starting | "
        f"poll_seconds={resolved_poll_seconds} | once={once} | max_iterations={max_iterations} "
        f"| source_id={source_id} | include_stream_runtime={include_stream_runtime} "
        f"| include_enabled_schedules={include_enabled_schedules}"
    )

    def on_iteration(iteration: int, cycle: dict[str, object]) -> None:
        typer.echo(
            "iteration="
            f"{iteration} source_runs={cycle['source_runtime']['source_run_ids']} "
            f"schedule_runs={cycle['schedules']['task_run_ids']} "
            f"seen={cycle['source_runtime']['records_seen']} imported={cycle['source_runtime']['records_imported']} "
            f"failed={cycle['source_runtime']['records_failed']}"
        )

    result = run_platform_runtime_worker(
        get_session_factory(),
        poll_seconds=resolved_poll_seconds,
        actor="cli_platform_runtime_worker",
        once=once,
        max_iterations=max_iterations,
        source_id=source_id,
        include_stream_runtime=include_stream_runtime,
        include_enabled_schedules=include_enabled_schedules,
        task_types=task_type or None,
        on_iteration=on_iteration,
    )
    typer.echo(
        "platform runtime worker stopped | "
        f"iterations={result.iterations} schedule_runs={result.runs_created} "
        f"source_runs={len(result.source_run_ids)} seen={result.records_seen} "
        f"imported={result.records_imported} failed={result.records_failed}"
    )


@app.command("run-platform-cycle")
def run_platform_cycle_command(
    include_stream_runtime: bool = True,
    include_enabled_schedules: bool = True,
    task_type: list[str] = typer.Option(default_factory=list),
    source_id: int | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = run_platform_runtime_cycle(
            session,
            include_stream_runtime=include_stream_runtime,
            include_enabled_schedules=include_enabled_schedules,
            task_types=task_type or None,
            source_id=source_id,
            actor="cli_platform_runtime_cycle",
        )
        serializable = TypeAdapter(PlatformRuntimeCycleRead).validate_python(result).model_dump(mode="json")
        print_banner()
        typer.echo(
            "runtime_cycle "
            f"streams={serializable['source_runtime']['source_count']} "
            f"source_runs={len(serializable['source_runtime']['source_run_ids'])} "
            f"stream_seen={serializable['source_runtime']['records_seen']} "
            f"schedules={serializable['schedules']['runs_created']} "
            f"schedule_failures={serializable['schedules']['failed_count']}"
        )
        if serializable["task_type_filters"]:
            typer.echo(f"task_type_filters={serializable['task_type_filters']}")
    finally:
        session.close()


@app.command("run-schedule")
def run_schedule(task_id: int) -> None:
    init_db()
    session = get_session_factory()()
    try:
        task_run = run_task(session, task_id, actor="cli")
        print_banner()
        typer.echo(
            f"task_run={task_run.task_run_id} status={task_run.status} records={task_run.records_affected}"
        )
    finally:
        session.close()


@app.command("update-schedule")
def update_schedule(
    task_id: int,
    name: str | None = None,
    enabled: bool | None = typer.Option(default=None),
    interval_seconds: int | None = None,
    retry_attempts: int | None = None,
    retry_backoff_seconds: float | None = None,
    source_id: int | None = None,
    target_path: Path | None = None,
    layer: str | None = None,
    geofence_id: int | None = None,
    notes: str | None = None,
    payload_json: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        payload: dict[str, object] = {}
        if name is not None:
            payload["name"] = name
        if enabled is not None:
            payload["enabled"] = enabled
        if interval_seconds is not None:
            payload["interval_seconds"] = interval_seconds
        if retry_attempts is not None:
            payload["retry_attempts"] = retry_attempts
        if retry_backoff_seconds is not None:
            payload["retry_backoff_seconds"] = retry_backoff_seconds
        if source_id is not None:
            payload["source_id"] = source_id
        if target_path is not None:
            payload["target_path"] = str(target_path)
        if layer is not None:
            payload["layer_key"] = layer
        if geofence_id is not None:
            payload["geofence_id"] = geofence_id
        if notes is not None:
            payload["notes"] = notes
        parsed_payload = parse_json_object_option(payload_json, "--payload-json")
        if parsed_payload is not None:
            payload["payload_json"] = parsed_payload
        task = update_scheduled_task(
            session,
            task_id,
            ScheduledTaskUpdate(**payload),
            actor="cli",
        )
        print_banner()
        typer.echo(
            f"task {task.task_id} | enabled={task.enabled} | every={task.interval_seconds}s | next={task.next_run_at}"
        )
    finally:
        session.close()


if __name__ == "__main__":
    app()
