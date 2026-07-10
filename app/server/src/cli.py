from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import typer
from pydantic import TypeAdapter
from sqlalchemy import select

from src.config import get_settings
from src.db import get_session_factory, init_db
from src.models import (
    AlertORM,
    CustodyLogORM,
    EntityObservationLinkORM,
    EntityORM,
    LocalImportRunORM,
    ScheduledTaskORM,
    ScheduledTaskRunORM,
    SituationProductORM,
    SourceTrustProfileORM,
)
from src.schemas import (
    AlertCreate,
    AlertUpdate,
    CandidateHealthCheckRead,
    CandidateHealthScanRequest,
    CandidateHealthScanResultRead,
    CandidatePromotionRequest,
    CandidatePromotionResultRead,
    CandidateScoreExplanationRead,
    CandidateSuppressionRead,
    CandidateSuppressionRequest,
    CameraSourceOpsExportSummaryRead,
    CameraSourceMaterializationResponse,
    CameraSourceOpsReportIndexRead,
    CameraSourceSummaryRead,
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
    DiscoveryCampaignCreate,
    DiscoveryCampaignRead,
    DiscoveryCampaignUpdate,
    DiscoveryDomainPolicyCreate,
    DiscoveryDomainPolicyRead,
    DiscoveryDomainPolicyUpdate,
    DiscoveryExportSummaryRead,
    DiscoveryInventoryDiffRead,
    DiscoveryLineageSummaryRead,
    DiscoveryOpsSummaryRead,
    DiscoveryRevisitRequest,
    DiscoveryRevisitResultRead,
    DiscoveryRunRead,
    DiscoveryRunRequest,
    DiscoveryRunResultRead,
    EventExportBundleRead,
    EventCreate,
    EventFusionRequest,
    EventRead,
    GeofenceCreate,
    GeofenceRead,
    EntityResolutionRequest,
    OperationsReportRead,
    RuntimeRestoreResultRead,
    RuntimeSnapshotRead,
    SchedulerInventorySummaryRead,
    SchedulerOpsExportSummaryRead,
    SchedulerOpsReportIndexRead,
    ScheduledTaskCreate,
    ScheduledTaskUpdate,
    StorageLifecycleSweepResultRead,
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
    SourceCandidateDetailRead,
    SourceCandidateRead,
    SourceTrustProfileCreate,
    SourceTrustProfileUpdate,
)
from src.services.camera_source_service import (
    build_camera_source_inventory_ops_detail,
    build_camera_source_ops_export_summary,
    build_camera_source_ops_report_index,
    build_camera_source_inventory_summary,
    list_camera_sources,
    materialize_camera_source_inventory,
)
from src.services.codex_agent_service import (
    CodexAgentError,
    build_codex_exec_command,
    build_codex_mcp_add_command,
    register_codex_mcp,
    run_codex_research,
)
from src.services.alert_service import (
    create_alert as create_alert_record,
    list_alerts as list_alert_records,
    update_alert as update_alert_record,
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
from src.services.custody_service import list_custody_logs
from src.services.database_diagnostics_service import build_database_diagnostics
from src.services.discovery_service import (
    build_candidate_lineage,
    build_discovery_campaign_detail,
    build_discovery_export_summary,
    build_discovery_ops_summary,
    build_source_candidate_detail,
    check_candidate_health,
    create_discovery_campaign,
    diff_discovery_inventories,
    explain_candidate_score,
    list_discovery_campaigns,
    list_domain_policies,
    list_discovery_runs,
    list_source_candidates,
    promote_source_candidate,
    revisit_discovery,
    run_discovery_campaign,
    scan_candidate_health,
    suppress_source_candidate,
    update_discovery_campaign,
    update_domain_policy,
    upsert_domain_policy,
)
from src.services.entity_resolution_service import materialize_entities
from src.services.export_artifact_service import write_json_export_artifact, write_text_export_artifact
from src.services.event_export_service import build_event_export_bundle
from src.services.event_fusion_service import materialize_fused_events
from src.services.event_service import create_event as create_event_record
from src.services.event_service import list_events as list_event_records
from src.services.geofence_service import create_geofence, list_geofences
from src.services.import_service import import_local_path
from src.services.layer_service import create_data_layer, list_data_layers
from src.services.observation_service import build_cross_verification_summaries, query_observations
from src.services.operations_report_service import build_operations_report
from src.services.redaction_service import enforce_export_redaction
from src.services.runtime_bundle_service import export_runtime_bundle, restore_runtime_bundle
from src.services.runtime_snapshot_service import build_runtime_snapshot
from src.services.runtime_snapshot_service import restore_runtime_snapshot
from src.services.scheduler_runtime_service import run_scheduler_worker
from src.services.scheduler_service import (
    build_scheduler_inventory_summary,
    build_scheduler_ops_export_summary,
    build_scheduler_ops_report_index,
    create_scheduled_task,
    run_due_tasks,
    run_task,
    update_scheduled_task,
)
from src.services.storage_service import (
    build_storage_report,
    create_storage_object,
    list_storage_objects,
    promote_storage_object,
    sweep_expired_storage_objects,
    transition_storage_object,
)
from src.services.source_service import (
    build_source_inventory_summary,
    build_source_ops_detail,
    build_source_ops_export_summary,
    build_source_ops_report_index,
    create_source_definition,
    list_source_definitions,
    list_source_runs,
    run_source_definition,
    update_source_definition,
)
from src.services.trust_service import (
    create_source_trust_profile,
    seed_default_integrity_sources,
    update_source_trust_profile,
)

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


def parse_optional_bool_option(value: str | None, option_name: str) -> bool | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes", "on"}:
        return True
    if normalized in {"false", "0", "no", "off"}:
        return False
    raise typer.BadParameter(f"{option_name} must be a boolean value.")


def echo_model_json(schema_cls: type, value: object) -> None:
    payload = schema_cls.model_validate(value).model_dump(mode="json")
    typer.echo(json.dumps(payload, indent=2, sort_keys=True))


def resolve_scheduler_poll_seconds(value: float | None) -> float:
    if value is not None:
        return max(0.0, value)
    return max(0.0, get_settings().scheduler_poll_seconds)


@app.command("status")
def status() -> None:
    settings = get_settings()
    print_banner()
    typer.echo(f"env: {settings.app_env}")
    typer.echo(f"database: {settings.database_url}")
    typer.echo(f"spatial backend: {settings.spatial_backend}")
    typer.echo(f"data dir: {settings.data_dir}")


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
        created = seed_default_integrity_sources(session, actor="cli_trust")
        typer.echo(f"seeded {len(created)} integrity domains")
    finally:
        session.close()


@app.command("show-codex-agent-command")
def show_codex_agent_command(
    objective: str,
    model: str | None = None,
    reasoning_effort: str | None = None,
) -> None:
    try:
        typer.echo(json.dumps(build_codex_exec_command(
            objective,
            model=model,
            reasoning_effort=reasoning_effort,
        )))
    except CodexAgentError as exc:
        raise typer.BadParameter(str(exc)) from exc


@app.command("show-codex-mcp-command")
def show_codex_mcp_command() -> None:
    typer.echo(json.dumps(build_codex_mcp_add_command()))


@app.command("configure-codex-agent")
def configure_codex_agent() -> None:
    """Register Forte's read-only MCP evidence server with the local Codex CLI."""
    try:
        result = register_codex_mcp()
    except CodexAgentError as exc:
        typer.echo(f"Codex MCP registration failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    print_banner()
    typer.echo(result)


@app.command("research")
def research(
    objective: str,
    model: str | None = None,
    reasoning_effort: str | None = None,
) -> None:
    """Run a bounded Codex investigation against local Forte evidence."""
    init_db()
    session = get_session_factory()()
    try:
        result = run_codex_research(
            session,
            objective,
            model=model,
            reasoning_effort=reasoning_effort,
        )
    except CodexAgentError as exc:
        typer.echo(f"research failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    finally:
        session.close()
    print_banner()
    typer.echo(
        f"model={result.model} effort={result.reasoning_effort} storage_object={result.storage_object_id}"
    )
    typer.echo(f"report={result.report_path}")
    typer.echo(result.output_text)


@app.command("add-trust-profile")
def add_trust_profile(
    domain: str,
    trust_level: str = "neutral",
    approval_policy: str = "manual_review",
    integrity_source: bool = False,
    notes: str = "",
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        profile = create_source_trust_profile(
            session,
            SourceTrustProfileCreate(
                domain=domain,
                trust_level=trust_level,  # type: ignore[arg-type]
                approval_policy=approval_policy,  # type: ignore[arg-type]
                integrity_source=integrity_source,
                notes=notes,
            ),
            actor="cli_trust",
        )
        print_banner()
        typer.echo(
            f"{profile.trust_profile_id} | {profile.domain} | {profile.trust_level} | "
            f"{profile.approval_policy} | integrity={profile.integrity_source}"
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
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


@app.command("update-trust-profile")
def update_trust_profile(
    trust_profile_id: int,
    domain: str | None = None,
    trust_level: str | None = None,
    approval_policy: str | None = None,
    integrity_source: bool | None = None,
    notes: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        profile = update_source_trust_profile(
            session,
            trust_profile_id,
            SourceTrustProfileUpdate(
                domain=domain,
                trust_level=trust_level,  # type: ignore[arg-type]
                approval_policy=approval_policy,  # type: ignore[arg-type]
                integrity_source=integrity_source,
                notes=notes,
            ),
            actor="cli_trust",
        )
        print_banner()
        typer.echo(
            f"{profile.trust_profile_id} | {profile.domain} | {profile.trust_level} | "
            f"{profile.approval_policy} | integrity={profile.integrity_source}"
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
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


@app.command("add-geofence")
def add_geofence(
    name: str,
    geometry_json: str,
    description: str = "",
    rule_expression: str = "",
    enabled: bool = True,
) -> None:
    geometry_geojson = parse_json_object_option(geometry_json, "geometry_json")
    if geometry_geojson is None:
        raise typer.BadParameter("geometry_json must decode to a JSON object.")
    init_db()
    session = get_session_factory()()
    try:
        geofence = create_geofence(
            session,
            GeofenceCreate(
                name=name,
                description=description,
                geometry_geojson=geometry_geojson,
                rule_expression=rule_expression,
                enabled=enabled,
            ),
            actor="cli",
        )
        print_banner()
        typer.echo(f"geofence {geofence.geofence_id} created for {geofence.name}")
    finally:
        session.close()


@app.command("list-geofences")
def list_geofences_command() -> None:
    init_db()
    session = get_session_factory()()
    try:
        rows = list_geofences(session)
        print_banner()
        for row in rows:
            typer.echo(
                f"{row.geofence_id} | enabled={row.enabled} | {row.name} | {row.rule_expression}"
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


@app.command("add-source-http-jsonl")
def add_source_http_jsonl(
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
                source_kind="http_jsonl",
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


@app.command("add-source-http-csv")
def add_source_http_csv(
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
                source_kind="http_csv",
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


@app.command("add-source-rss")
def add_source_rss(
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
                source_kind="rss",
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


@app.command("add-source-arcgis-feature-json")
def add_source_arcgis_feature_json(
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
                source_kind="arcgis_feature_json",
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


@app.command("add-source-ckan-package-search")
def add_source_ckan_package_search(
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
                source_kind="ckan_package_search",
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
                source_kind="web_search",
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
                source_kind="web_crawl",
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


@app.command("add-source-web-discovery")
def add_source_web_discovery(
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
                source_kind="web_discovery",
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


@app.command("show-source-ops")
def show_source_ops_command(source_id: int) -> None:
    init_db()
    session = get_session_factory()()
    try:
        detail = build_source_ops_detail(session, source_id)
        source = detail["source"]
        print_banner()
        typer.echo(
            f"source={source.source_id} name={source.name} kind={source.source_kind} enabled={source.enabled} layer={source.layer_key}"
        )
        typer.echo(f"target_uri={source.target_uri}")
        typer.echo("recent_runs:")
        for row in detail["recent_runs"][:10]:
            typer.echo(
                f"  {row.source_run_id} | {row.status} | import_run={row.import_run_id} | records={row.records_imported} | started={row.started_at}"
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
        for group_name in ("source_domain_counts", "endpoint_kind_counts", "status_counts"):
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


@app.command("create-event")
def create_event_command(
    slug: str,
    title: str,
    summary: str = "",
    occurred_at: str | None = None,
    status: str = "open",
    redaction_level: str = "public",
    metadata_json: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        parsed_metadata = parse_json_object_option(metadata_json, "--metadata-json") or {}
        occurred_at_value = (
            TypeAdapter(datetime).validate_python(occurred_at)
            if occurred_at is not None
            else None
        )
        event = create_event_record(
            session,
            EventCreate(
                slug=slug,
                title=title,
                summary=summary,
                occurred_at=occurred_at_value,
                status=status,
                redaction_level=redaction_level,
                metadata_json=parsed_metadata,
            ),
            actor="cli",
        )
        print_banner()
        typer.echo(f"event {event.event_id} created for slug={event.slug}")
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
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
def list_events() -> None:
    init_db()
    session = get_session_factory()()
    try:
        rows = list_event_records(session)
        print_banner()
        for row in rows:
            typer.echo(f"{row.event_id} | {row.slug} | {row.title} | {row.redaction_level}")
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
def list_entities() -> None:
    init_db()
    session = get_session_factory()()
    try:
        rows = list(
            session.scalars(
                select(EntityORM).order_by(EntityORM.confidence_score.desc(), EntityORM.created_at.desc())
            )
        )
        print_banner()
        for row in rows:
            typer.echo(
                f"{row.entity_id} | {row.entity_type} | {row.canonical_name} | score={row.confidence_score:.2f} | {row.redaction_level}"
            )
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


@app.command("create-alert")
def create_alert_command(
    message: str,
    severity: str = "info",
    status: str = "open",
    geofence_id: int | None = None,
    event_id: int | None = None,
    dedupe_key: str | None = None,
    trigger_basis_json: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        parsed_trigger_basis = (
            parse_json_object_option(trigger_basis_json, "--trigger-basis-json") or {}
        )
        alert = create_alert_record(
            session,
            AlertCreate(
                event_id=event_id,
                geofence_id=geofence_id,
                severity=severity,
                status=status,
                dedupe_key=dedupe_key,
                message=message,
                trigger_basis_json=parsed_trigger_basis,
            ),
            actor="cli",
        )
        print_banner()
        typer.echo(
            f"alert={alert.alert_id} geofence={alert.geofence_id} event={alert.event_id} severity={alert.severity} status={alert.status}"
        )
    finally:
        session.close()


@app.command("list-alerts")
def list_alerts(
    status: str | None = None,
    geofence_id: int | None = None,
    event_id: int | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        alerts = list_alert_records(
            session,
            status=status,
            geofence_id=geofence_id,
            event_id=event_id,
        )
        print_banner()
        for alert in alerts:
            typer.echo(
                f"{alert.alert_id} | geofence={alert.geofence_id} | {alert.severity} | {alert.status} | {alert.message}"
            )
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
        alert = update_alert_record(
            session,
            alert_id,
            AlertUpdate(
                status=status,
                severity=severity,
                disposition_note=disposition_note,
            ),
            actor="cli",
        )
        print_banner()
        typer.echo(
            f"alert={alert.alert_id} status={alert.status} severity={alert.severity} note={alert.disposition_note}"
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()

@app.command("list-custody")
def list_custody(
    limit: int = 20,
    object_type: str | None = None,
    object_id: str | None = None,
    action: str | None = None,
    actor: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        rows = list_custody_logs(
            session,
            object_type=object_type,
            object_id=object_id,
            action=action,
            actor=actor,
            since=since,
            until=until,
            limit=limit,
        )
        print_banner()
        for row in rows:
            typer.echo(
                f"{row.custody_log_id} | {row.object_type} | {row.object_id} | "
                f"{row.action} | {row.actor} | {row.created_at}"
            )
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
        )
        serializable = TypeAdapter(StorageLifecycleSweepResultRead).validate_python(result).model_dump(mode="json")
        print_banner()
        typer.echo(
            f"swept_at={serializable['swept_at']} dry_run={serializable['dry_run']} candidates={serializable['expired_candidate_count']} transitioned={serializable['transitioned_count']}"
        )
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


@app.command("show-operations-report")
def show_operations_report(hours: float | None = 24.0, limit: int = 10) -> None:
    init_db()
    session = get_session_factory()()
    try:
        since = resolve_report_since(hours)
        report = build_operations_report(session, since=since, limit=limit)
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
        storage_report = report["storage_report"]
        typer.echo(
            "storage="
            f"{storage_report['total_count']} active={storage_report['active_count']} "
            f"expired={storage_report['expired_count']} archived={storage_report['archived_count']}"
        )
        clickhouse_diagnostics = report["clickhouse_diagnostics"]
        typer.echo(
            "clickhouse="
            f"{clickhouse_diagnostics['status']} enabled={clickhouse_diagnostics['enabled']} "
            f"reachable={clickhouse_diagnostics['reachable']} mode={clickhouse_diagnostics['storage_mode']}"
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
        discovery_summary = report.get("discovery_ops_summary")
        if discovery_summary is not None:
            discovery_health = discovery_summary["health_summary"]
            discovery_inventory = discovery_summary["inventory_summary"]
            typer.echo(
                "discovery="
                f"{discovery_health['status']} campaigns={discovery_health['campaign_count']} "
                f"running={discovery_health['running_run_count']} "
                f"candidates={discovery_inventory['total_count']} "
                f"promoted={discovery_inventory['promoted_count']} "
                f"failing={discovery_inventory['failing_count']} "
                f"stale={discovery_inventory['stale_count']}"
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


@app.command("export-operations-report")
def export_operations_report(output_path: Path, hours: float | None = 24.0, limit: int = 25) -> None:
    init_db()
    session = get_session_factory()()
    try:
        since = resolve_report_since(hours)
        report = build_operations_report(session, since=since, limit=limit)
        serializable = TypeAdapter(OperationsReportRead).validate_python(report).model_dump(mode="json")
        write_json_export_artifact(
            session,
            payload=serializable,
            object_kind="operations_report_export",
            owner_type="operations_report",
            owner_id="scoped",
            output_path=output_path,
            source_uri="/api/operations/report",
            observed_at=report["generated_at"],
            metadata_json={
                "scope_since": serializable["scope_since"],
                "scope_until": serializable["scope_until"],
                "limit": limit,
                "hours": hours,
            },
            actor="cli_export",
        )
        print_banner()
        typer.echo(f"exported operations report to {output_path}")
    finally:
        session.close()


@app.command("export-runtime-snapshot")
def export_runtime_snapshot_command(output_path: Path) -> None:
    init_db()
    session = get_session_factory()()
    try:
        snapshot = build_runtime_snapshot(session)
        serializable = TypeAdapter(RuntimeSnapshotRead).validate_python(snapshot).model_dump(mode="json")
        write_json_export_artifact(
            session,
            payload=serializable,
            object_kind="runtime_snapshot_export",
            owner_type="runtime_snapshot",
            owner_id=serializable["exported_at"],
            output_path=output_path,
            source_uri="/api/operations/runtime/export",
            observed_at=snapshot["exported_at"],
            metadata_json={
                "database_backend": serializable["database_backend"],
                "spatial_backend": serializable["spatial_backend"],
            },
            actor="cli_export",
        )
        print_banner()
        typer.echo(f"exported runtime snapshot to {output_path}")
    finally:
        session.close()


@app.command("export-runtime-bundle")
def export_runtime_bundle_command(output_path: Path) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = export_runtime_bundle(session, output_path, actor="cli_export")
        print_banner()
        typer.echo(
            "exported runtime bundle "
            f"| files={result['data_file_count']} "
            f"| sha256={result['bundle_sha256']} "
            f"| path={result['output_path']}"
        )
    finally:
        session.close()


@app.command("restore-runtime-snapshot")
def restore_runtime_snapshot_command(
    input_path: Path,
    replace_existing: bool = False,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        payload = json.loads(input_path.read_text(encoding="utf-8"))
        try:
            result = restore_runtime_snapshot(
                session,
                payload,
                replace_existing=replace_existing,
            )
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        serializable = TypeAdapter(RuntimeRestoreResultRead).validate_python(result).model_dump(mode="json")
        print_banner()
        typer.echo(
            "restored runtime snapshot "
            f"| replaced_existing={serializable['replaced_existing']} "
            f"| total_records={serializable['total_records']}"
        )
        for item in serializable["row_counts"]:
            typer.echo(f"{item['table_name']}: {item['row_count']}")
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
                input_path,
                replace_existing=replace_existing,
            )
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        print_banner()
        typer.echo(
            "restored runtime bundle "
            f"| replaced_existing={result['replaced_existing']} "
            f"| total_records={result['total_records']} "
            f"| restored_files={result['restored_file_count']}"
        )
        for item in result["row_counts"]:
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


@app.command("add-integrity-seed-schedule")
def add_integrity_seed_schedule(
    name: str,
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
                task_type="integrity_seed",
                interval_seconds=interval_seconds,
                retry_attempts=retry_attempts,
                retry_backoff_seconds=retry_backoff_seconds,
                notes=notes,
            ),
        )
        print_banner()
        typer.echo(f"scheduled task {task.task_id} created for integrity seeding")
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


@app.command("create-discovery-campaign")
def create_discovery_campaign_command(
    name: str,
    description: str = "",
    mode: str = "query_seeded",
    status: str = "draft",
    layer: str | None = None,
    enabled: bool = True,
    discovery_mode: list[str] = typer.Option(default_factory=list),
    query: list[str] = typer.Option(default_factory=list),
    seed: list[str] = typer.Option(default_factory=list),
    search_template: list[str] = typer.Option(default_factory=list),
    format_target: list[str] = typer.Option(default_factory=list),
    locale: list[str] = typer.Option(default_factory=list),
    language: list[str] = typer.Option(default_factory=list),
    allow_domain: list[str] = typer.Option(default_factory=list),
    deny_domain: list[str] = typer.Option(default_factory=list),
    policy_json: str | None = None,
    geo_json: str | None = None,
    entities_json: str | None = None,
    historical_backfill: bool = False,
    recency_days: int | None = None,
    max_depth: int = 2,
    max_pages: int = 100,
    max_candidates: int = 1000,
) -> None:
    parsed_policy = parse_json_object_option(policy_json, "--policy-json") or {}
    parsed_geo = parse_json_object_option(geo_json, "--geo-json") or {}
    parsed_entities = parse_json_value_option(entities_json, "--entities-json")
    if parsed_entities is None:
        entity_seeds: list[dict[str, object]] = []
    elif isinstance(parsed_entities, dict):
        entity_seeds = [parsed_entities]
    elif isinstance(parsed_entities, list) and all(
        isinstance(item, dict) for item in parsed_entities
    ):
        entity_seeds = parsed_entities
    else:
        raise typer.BadParameter("--entities-json must be an object or an array of objects.")

    request_json: dict[str, object] = {}
    if query:
        request_json["queries"] = query
    if search_template:
        request_json["search_templates"] = search_template

    payload = DiscoveryCampaignCreate(
        name=name,
        description=description,
        mode=mode,
        status=status,
        enabled=enabled,
        layer_key=layer,
        query_text="\n".join(query),
        modes_json=discovery_mode or [mode],
        query_strings_json=query,
        search_templates_json=search_template,
        format_targets_json=format_target,
        seed_urls_json=seed,
        locale_variants_json=locale,
        language_variants_json=language,
        domain_allowlist_json=allow_domain,
        domain_denylist_json=deny_domain,
        target_geography_json=parsed_geo,
        entity_seeds_json=entity_seeds,
        historical_backfill=historical_backfill,
        recency_days=recency_days,
        max_depth=max_depth,
        max_pages=max_pages,
        max_candidates=max_candidates,
        request_json=request_json,
        crawl_policy_json=parsed_policy,
    )

    init_db()
    session = get_session_factory()()
    try:
        campaign = create_discovery_campaign(session, payload, actor="cli_discovery")
        serializable = DiscoveryCampaignRead.model_validate(campaign).model_dump(mode="json")
        print_banner()
        typer.echo(
            f"campaign={serializable['campaign_id']} status={serializable['status']} "
            f"mode={serializable['mode']} enabled={serializable['enabled']} name={serializable['name']}"
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("update-discovery-campaign")
def update_discovery_campaign_command(
    campaign_id: int,
    name: str | None = None,
    description: str | None = None,
    mode: str | None = None,
    status: str | None = None,
    enabled: str | None = None,
    layer: str | None = None,
    query_text: str | None = None,
    modes_json: str | None = None,
    query_strings_json: str | None = None,
    search_templates_json: str | None = None,
    format_targets_json: str | None = None,
    seed_urls_json: str | None = None,
    locale_variants_json: str | None = None,
    language_variants_json: str | None = None,
    domain_allowlist_json: str | None = None,
    domain_denylist_json: str | None = None,
    target_geography_json: str | None = None,
    entity_seeds_json: str | None = None,
    historical_backfill: str | None = None,
    recency_days: int | None = None,
    max_depth: int | None = None,
    max_pages: int | None = None,
    max_candidates: int | None = None,
    request_json: str | None = None,
    crawl_policy_json: str | None = None,
    scoring_weights_json: str | None = None,
    schedule_json: str | None = None,
    metadata_json: str | None = None,
) -> None:
    payload_data: dict[str, object] = {}
    if name is not None:
        payload_data["name"] = name
    if description is not None:
        payload_data["description"] = description
    if mode is not None:
        payload_data["mode"] = mode
    if status is not None:
        payload_data["status"] = status
    parsed_enabled = parse_optional_bool_option(enabled, "--enabled")
    if parsed_enabled is not None:
        payload_data["enabled"] = parsed_enabled
    if layer is not None:
        payload_data["layer_key"] = layer
    if query_text is not None:
        payload_data["query_text"] = query_text
    if modes_json is not None:
        payload_data["modes_json"] = parse_json_value_option(
            modes_json, "--modes-json"
        )
    if query_strings_json is not None:
        payload_data["query_strings_json"] = parse_json_value_option(
            query_strings_json, "--query-strings-json"
        )
    if search_templates_json is not None:
        payload_data["search_templates_json"] = parse_json_value_option(
            search_templates_json, "--search-templates-json"
        )
    if format_targets_json is not None:
        payload_data["format_targets_json"] = parse_json_value_option(
            format_targets_json, "--format-targets-json"
        )
    if seed_urls_json is not None:
        payload_data["seed_urls_json"] = parse_json_value_option(
            seed_urls_json, "--seed-urls-json"
        )
    if locale_variants_json is not None:
        payload_data["locale_variants_json"] = parse_json_value_option(
            locale_variants_json, "--locale-variants-json"
        )
    if language_variants_json is not None:
        payload_data["language_variants_json"] = parse_json_value_option(
            language_variants_json, "--language-variants-json"
        )
    if domain_allowlist_json is not None:
        payload_data["domain_allowlist_json"] = parse_json_value_option(
            domain_allowlist_json, "--domain-allowlist-json"
        )
    if domain_denylist_json is not None:
        payload_data["domain_denylist_json"] = parse_json_value_option(
            domain_denylist_json, "--domain-denylist-json"
        )
    parsed_target_geography = parse_json_object_option(
        target_geography_json, "--target-geography-json"
    )
    if parsed_target_geography is not None:
        payload_data["target_geography_json"] = parsed_target_geography
    if entity_seeds_json is not None:
        payload_data["entity_seeds_json"] = parse_json_value_option(
            entity_seeds_json, "--entity-seeds-json"
        )
    parsed_historical_backfill = parse_optional_bool_option(
        historical_backfill, "--historical-backfill"
    )
    if parsed_historical_backfill is not None:
        payload_data["historical_backfill"] = parsed_historical_backfill
    if recency_days is not None:
        payload_data["recency_days"] = recency_days
    if max_depth is not None:
        payload_data["max_depth"] = max_depth
    if max_pages is not None:
        payload_data["max_pages"] = max_pages
    if max_candidates is not None:
        payload_data["max_candidates"] = max_candidates
    parsed_request = parse_json_object_option(request_json, "--request-json")
    if parsed_request is not None:
        payload_data["request_json"] = parsed_request
    parsed_crawl_policy = parse_json_object_option(
        crawl_policy_json, "--crawl-policy-json"
    )
    if parsed_crawl_policy is not None:
        payload_data["crawl_policy_json"] = parsed_crawl_policy
    parsed_scoring_weights = parse_json_object_option(
        scoring_weights_json, "--scoring-weights-json"
    )
    if parsed_scoring_weights is not None:
        payload_data["scoring_weights_json"] = parsed_scoring_weights
    parsed_schedule = parse_json_object_option(schedule_json, "--schedule-json")
    if parsed_schedule is not None:
        payload_data["schedule_json"] = parsed_schedule
    parsed_metadata = parse_json_object_option(metadata_json, "--metadata-json")
    if parsed_metadata is not None:
        payload_data["metadata_json"] = parsed_metadata

    payload = DiscoveryCampaignUpdate(**payload_data)
    init_db()
    session = get_session_factory()()
    try:
        campaign = update_discovery_campaign(
            session,
            campaign_id,
            payload,
            actor="cli_discovery",
        )
        print_banner()
        echo_model_json(DiscoveryCampaignRead, campaign)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("run-discovery")
def run_discovery_command(
    campaign_id: int,
    resume: bool = True,
    resume_run_id: int | None = None,
    max_pages: int | None = None,
    max_candidates: int | None = None,
    max_seconds: float | None = None,
    dry_run: bool = False,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = run_discovery_campaign(
            session,
            campaign_id,
            DiscoveryRunRequest(
                campaign_id=campaign_id,
                actor="cli_discovery",
                resume=resume,
                resume_run_id=resume_run_id,
                max_pages=max_pages,
                max_candidates=max_candidates,
                max_seconds=max_seconds,
                dry_run=dry_run,
            ),
            actor="cli_discovery",
        )
        print_banner()
        echo_model_json(DiscoveryRunResultRead, result)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("list-discovery-campaigns")
def list_discovery_campaigns_command(
    status: str | None = None,
    enabled: bool | None = typer.Option(default=None),
    limit: int = 200,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        rows = list_discovery_campaigns(
            session,
            status=status,
            enabled=enabled,
            limit=limit,
        )
        adapter = TypeAdapter(list[DiscoveryCampaignRead])
        serializable = adapter.dump_python(adapter.validate_python(rows), mode="json")
        print_banner()
        for row in serializable:
            typer.echo(
                f"{row['campaign_id']} | {row['status']} | {row['mode']} | "
                f"enabled={row['enabled']} | {row['name']}"
            )
    finally:
        session.close()


@app.command("show-discovery-campaign")
def show_discovery_campaign_command(campaign_id: int) -> None:
    init_db()
    session = get_session_factory()()
    try:
        detail = build_discovery_campaign_detail(session, campaign_id)
        campaign_value = detail.get("campaign", detail) if isinstance(detail, dict) else detail
        print_banner()
        echo_model_json(DiscoveryCampaignRead, campaign_value)
        if isinstance(detail, dict):
            for key in ("run_count", "candidate_count", "frontier_count", "promotion_count"):
                if key in detail:
                    typer.echo(f"{key}={detail[key]}")
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("list-discovery-runs")
def list_discovery_runs_command(
    campaign_id: int | None = None,
    status: str | None = None,
    limit: int = 200,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        rows = list_discovery_runs(
            session,
            campaign_id=campaign_id,
            status=status,
            limit=limit,
        )
        adapter = TypeAdapter(list[DiscoveryRunRead])
        serializable = adapter.dump_python(adapter.validate_python(rows), mode="json")
        print_banner()
        for row in serializable:
            run_id = row.get("discovery_run_id", row.get("run_id"))
            typer.echo(
                f"{run_id} | campaign={row['campaign_id']} | {row['status']} | "
                f"started={row.get('started_at')} | finished={row.get('finished_at')}"
            )
    finally:
        session.close()


@app.command("list-discovery-candidates")
def list_discovery_candidates_command(
    campaign_id: int | None = None,
    run_id: int | None = None,
    status: str | None = None,
    outcome: str | None = None,
    candidate_type: str | None = None,
    domain: str | None = None,
    min_score: float | None = None,
    limit: int = 200,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        rows = list_source_candidates(
            session,
            campaign_id=campaign_id,
            run_id=run_id,
            status=status,
            score_bucket=outcome,
            candidate_type=candidate_type,
            domain=domain,
            min_score=min_score,
            limit=limit,
        )
        adapter = TypeAdapter(list[SourceCandidateRead])
        serializable = adapter.dump_python(adapter.validate_python(rows), mode="json")
        print_banner()
        for row in serializable:
            typer.echo(
                f"{row['candidate_id']} | score={row['score']} | {row['score_bucket']} | "
                f"{row['status']} | {row['candidate_type']} | {row['canonical_url']}"
            )
    finally:
        session.close()


@app.command("show-discovery-candidate")
def show_discovery_candidate_command(candidate_id: int) -> None:
    init_db()
    session = get_session_factory()()
    try:
        detail = build_source_candidate_detail(session, candidate_id)
        print_banner()
        echo_model_json(SourceCandidateDetailRead, detail)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("explain-discovery-candidate")
def explain_discovery_candidate_command(candidate_id: int) -> None:
    init_db()
    session = get_session_factory()()
    try:
        explanation = explain_candidate_score(session, candidate_id)
        print_banner()
        echo_model_json(CandidateScoreExplanationRead, explanation)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("show-discovery-lineage")
def show_discovery_lineage_command(candidate_id: int) -> None:
    init_db()
    session = get_session_factory()()
    try:
        lineage = build_candidate_lineage(session, candidate_id)
        print_banner()
        echo_model_json(DiscoveryLineageSummaryRead, lineage)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("promote-discovery-candidate")
def promote_discovery_candidate_command(
    candidate_id: int,
    reason: str,
    source_kind: str | None = None,
    source_name: str | None = None,
    layer: str | None = None,
    enabled: bool = True,
    integrity_source: bool = False,
    create_schedule: bool = True,
    schedule_interval_seconds: int | None = None,
    metadata_json: str | None = None,
) -> None:
    parsed_metadata = parse_json_object_option(metadata_json, "--metadata-json") or {}
    init_db()
    session = get_session_factory()()
    try:
        payload = CandidatePromotionRequest(
            candidate_id=candidate_id,
            source_kind=source_kind,
            recommended_source_kind=source_kind,
            source_name=source_name,
            layer_key=layer,
            enabled=enabled,
            integrity_source=integrity_source,
            create_schedule=create_schedule,
            schedule_interval_seconds=schedule_interval_seconds,
            reason=reason,
            actor="cli_discovery",
            metadata_json=parsed_metadata,
        )
        decision = promote_source_candidate(
            session,
            candidate_id,
            payload,
            actor="cli_discovery",
        )
        print_banner()
        echo_model_json(CandidatePromotionResultRead, decision)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("suppress-discovery-candidate")
def suppress_discovery_candidate_command(
    candidate_id: int,
    reason: str,
    reason_code: str = "operator_suppressed",
    expires_at: datetime | None = None,
    metadata_json: str | None = None,
) -> None:
    parsed_metadata = parse_json_object_option(metadata_json, "--metadata-json") or {}
    init_db()
    session = get_session_factory()()
    try:
        suppression = suppress_source_candidate(
            session,
            candidate_id,
            CandidateSuppressionRequest(
                candidate_id=candidate_id,
                scope="candidate",
                reason_code=reason_code,
                reason=reason,
                actor="cli_discovery",
                expires_at=expires_at,
                metadata_json=parsed_metadata,
            ),
            actor="cli_discovery",
        )
        print_banner()
        echo_model_json(CandidateSuppressionRead, suppression)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("revisit-discovery")
def revisit_discovery_command(
    candidate_id: int | None = None,
    domain: str | None = None,
    campaign_id: int | None = None,
    force: bool = False,
    include_suppressed: bool = False,
    priority: float = 0.0,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = revisit_discovery(
            session,
            DiscoveryRevisitRequest(
                candidate_id=candidate_id,
                normalized_domain=domain,
                campaign_id=campaign_id,
                force=force,
                include_suppressed=include_suppressed,
                priority=priority,
                actor="cli_discovery",
            ),
            actor="cli_discovery",
        )
        print_banner()
        echo_model_json(DiscoveryRevisitResultRead, result)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("check-discovery-candidate-health")
def check_discovery_candidate_health_command(candidate_id: int) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = check_candidate_health(session, candidate_id, actor="cli_discovery")
        print_banner()
        echo_model_json(CandidateHealthCheckRead, result)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("scan-discovery-health")
def scan_discovery_health_command(
    candidate_id: int | None = None,
    domain: str | None = None,
    campaign_id: int | None = None,
    limit: int = 100,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = scan_candidate_health(
            session,
            CandidateHealthScanRequest(
                candidate_id=candidate_id,
                normalized_domain=domain,
                campaign_id=campaign_id,
                limit=limit,
                actor="cli_discovery",
            ),
            actor="cli_discovery",
        )
        print_banner()
        echo_model_json(CandidateHealthScanResultRead, result)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("list-failing-discovery-candidates")
def list_failing_discovery_candidates_command(
    stale_after_hours: float = 24.0,
    limit: int = 100,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        summary = build_discovery_ops_summary(
            session,
            stale_after_hours=stale_after_hours,
            limit=limit,
        )
        serializable = DiscoveryOpsSummaryRead.model_validate(summary).model_dump(mode="json")
        print_banner()
        emitted = False
        for key in ("failing_candidates", "stale_candidates", "quarantined_candidates"):
            rows = serializable.get(key, [])
            if not rows:
                continue
            emitted = True
            typer.echo(f"{key}:")
            for row in rows:
                typer.echo(
                    f"  {row.get('candidate_id')} | {row.get('status')} | "
                    f"score={row.get('score')} | {row.get('canonical_url')}"
                )
        if not emitted:
            typer.echo("no failing, stale, or quarantined candidates")
    finally:
        session.close()


@app.command("show-discovery-ops")
def show_discovery_ops_command(
    stale_after_hours: float = 24.0,
    limit: int = 25,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        summary = build_discovery_ops_summary(
            session,
            stale_after_hours=stale_after_hours,
            limit=limit,
        )
        print_banner()
        echo_model_json(DiscoveryOpsSummaryRead, summary)
    finally:
        session.close()


@app.command("export-discovery-summary")
def export_discovery_summary_command(
    output_path: Path,
    stale_after_hours: float = 24.0,
    candidate_limit: int = 500,
    report_limit: int = 25,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        report = build_discovery_export_summary(
            session,
            stale_after_hours=stale_after_hours,
            candidate_limit=candidate_limit,
            report_limit=report_limit,
        )
        serializable = DiscoveryExportSummaryRead.model_validate(report).model_dump(mode="json")
        write_json_export_artifact(
            session,
            payload=serializable,
            object_kind="discovery_summary_export",
            owner_type="discovery_export",
            owner_id="scoped",
            output_path=output_path,
            source_uri="/api/discovery/export/summary",
            observed_at=report["generated_at"],
            metadata_json=serializable.get("filters_json", {}),
            actor="cli_export",
        )
        print_banner()
        typer.echo(f"exported discovery summary to {output_path}")
    finally:
        session.close()


@app.command("diff-discovery-inventories")
def diff_discovery_inventories_command(
    from_run_id: int,
    to_run_id: int,
    campaign_id: int | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = diff_discovery_inventories(
            session,
            from_run_id=from_run_id,
            to_run_id=to_run_id,
            campaign_id=campaign_id,
        )
        print_banner()
        echo_model_json(DiscoveryInventoryDiffRead, result)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("list-discovery-domain-policies")
def list_discovery_domain_policies_command(
    domain: str | None = None,
    limit: int = 200,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        rows = list_domain_policies(session, domain=domain, limit=limit)
        adapter = TypeAdapter(list[DiscoveryDomainPolicyRead])
        serializable = adapter.dump_python(adapter.validate_python(rows), mode="json")
        print_banner()
        for row in serializable:
            typer.echo(
                f"{row['domain_policy_id']} | {row['normalized_domain']} | "
                f"{row['policy']} | robots={row['robots_mode']} | enabled={row['enabled']} | "
                f"subdomains={row['allow_subdomains']} | concurrency={row['max_concurrency']}"
            )
    finally:
        session.close()


@app.command("upsert-discovery-domain-policy")
def upsert_discovery_domain_policy_command(
    normalized_domain: str,
    policy: str = "allow",
    robots_mode: str = "respect",
    enabled: bool = typer.Option(True, "--enabled/--disabled"),
    allow_subdomains: bool = typer.Option(True, "--allow-subdomains/--no-allow-subdomains"),
    crawl_delay_seconds: float = 1.0,
    max_concurrency: int = 1,
    max_depth: int = 2,
    max_pages_per_run: int = 100,
    max_response_bytes: int = 5_000_000,
    request_timeout_seconds: float = 20.0,
    retry_attempts: int = 2,
    retry_backoff_seconds: float = 1.0,
    allowed_path_patterns_json: str | None = None,
    denied_path_patterns_json: str | None = None,
    allowed_content_types_json: str | None = None,
    notes: str = "",
    metadata_json: str | None = None,
) -> None:
    allowed_path_patterns = parse_json_value_option(
        allowed_path_patterns_json, "--allowed-path-patterns-json"
    )
    denied_path_patterns = parse_json_value_option(
        denied_path_patterns_json, "--denied-path-patterns-json"
    )
    allowed_content_types = parse_json_value_option(
        allowed_content_types_json, "--allowed-content-types-json"
    )
    metadata = parse_json_object_option(metadata_json, "--metadata-json") or {}
    payload = DiscoveryDomainPolicyCreate(
        normalized_domain=normalized_domain,
        policy=policy,
        robots_mode=robots_mode,
        enabled=enabled,
        allow_subdomains=allow_subdomains,
        crawl_delay_seconds=crawl_delay_seconds,
        max_concurrency=max_concurrency,
        max_depth=max_depth,
        max_pages_per_run=max_pages_per_run,
        max_response_bytes=max_response_bytes,
        request_timeout_seconds=request_timeout_seconds,
        retry_attempts=retry_attempts,
        retry_backoff_seconds=retry_backoff_seconds,
        allowed_path_patterns_json=(
            allowed_path_patterns if isinstance(allowed_path_patterns, list) else []
        ),
        denied_path_patterns_json=(
            denied_path_patterns if isinstance(denied_path_patterns, list) else []
        ),
        allowed_content_types_json=(
            allowed_content_types if isinstance(allowed_content_types, list) else []
        ),
        notes=notes,
        metadata_json=metadata,
    )
    init_db()
    session = get_session_factory()()
    try:
        record = upsert_domain_policy(session, payload, actor="cli_discovery")
        print_banner()
        echo_model_json(DiscoveryDomainPolicyRead, record)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("update-discovery-domain-policy")
def update_discovery_domain_policy_command(
    normalized_domain: str,
    policy: str | None = None,
    robots_mode: str | None = None,
    enabled: str | None = None,
    allow_subdomains: str | None = None,
    crawl_delay_seconds: float | None = None,
    max_concurrency: int | None = None,
    max_depth: int | None = None,
    max_pages_per_run: int | None = None,
    max_response_bytes: int | None = None,
    request_timeout_seconds: float | None = None,
    retry_attempts: int | None = None,
    retry_backoff_seconds: float | None = None,
    allowed_path_patterns_json: str | None = None,
    denied_path_patterns_json: str | None = None,
    allowed_content_types_json: str | None = None,
    notes: str | None = None,
    metadata_json: str | None = None,
) -> None:
    allowed_path_patterns = parse_json_value_option(
        allowed_path_patterns_json, "--allowed-path-patterns-json"
    )
    denied_path_patterns = parse_json_value_option(
        denied_path_patterns_json, "--denied-path-patterns-json"
    )
    allowed_content_types = parse_json_value_option(
        allowed_content_types_json, "--allowed-content-types-json"
    )
    metadata = parse_json_object_option(metadata_json, "--metadata-json")
    payload = DiscoveryDomainPolicyUpdate(
        policy=policy,
        robots_mode=robots_mode,
        enabled=parse_optional_bool_option(enabled, "--enabled"),
        allow_subdomains=parse_optional_bool_option(
            allow_subdomains, "--allow-subdomains"
        ),
        crawl_delay_seconds=crawl_delay_seconds,
        max_concurrency=max_concurrency,
        max_depth=max_depth,
        max_pages_per_run=max_pages_per_run,
        max_response_bytes=max_response_bytes,
        request_timeout_seconds=request_timeout_seconds,
        retry_attempts=retry_attempts,
        retry_backoff_seconds=retry_backoff_seconds,
        allowed_path_patterns_json=(
            allowed_path_patterns if isinstance(allowed_path_patterns, list) else None
        ),
        denied_path_patterns_json=(
            denied_path_patterns if isinstance(denied_path_patterns, list) else None
        ),
        allowed_content_types_json=(
            allowed_content_types if isinstance(allowed_content_types, list) else None
        ),
        notes=notes,
        metadata_json=metadata,
    )
    init_db()
    session = get_session_factory()()
    try:
        record = update_domain_policy(
            session,
            normalized_domain,
            payload,
            actor="cli_discovery",
        )
        print_banner()
        echo_model_json(DiscoveryDomainPolicyRead, record)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("add-discovery-schedule")
def add_discovery_schedule_command(
    name: str,
    campaign_id: int,
    interval_seconds: int,
    resume: bool = True,
    max_pages: int | None = None,
    notes: str = "",
    retry_attempts: int = 1,
    retry_backoff_seconds: float = 0.0,
) -> None:
    payload_json: dict[str, object] = {
        "campaign_id": campaign_id,
        "resume": resume,
    }
    if max_pages is not None:
        payload_json["max_pages"] = max_pages
    init_db()
    session = get_session_factory()()
    try:
        task = create_scheduled_task(
            session,
            ScheduledTaskCreate(
                name=name,
                task_type="discovery_campaign",
                interval_seconds=interval_seconds,
                retry_attempts=retry_attempts,
                retry_backoff_seconds=retry_backoff_seconds,
                notes=notes,
                payload_json=payload_json,
            ),
        )
        print_banner()
        typer.echo(
            f"scheduled task {task.task_id} created for discovery campaign {campaign_id}"
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("add-discovery-health-scan-schedule")
def add_discovery_health_scan_schedule_command(
    name: str,
    interval_seconds: int,
    candidate_id: int | None = None,
    normalized_domain: str | None = None,
    campaign_id: int | None = None,
    limit: int = 100,
    notes: str = "",
    retry_attempts: int = 1,
    retry_backoff_seconds: float = 0.0,
) -> None:
    payload_json: dict[str, object] = {"limit": limit}
    if candidate_id is not None:
        payload_json["candidate_id"] = candidate_id
    if normalized_domain is not None:
        payload_json["normalized_domain"] = normalized_domain
    if campaign_id is not None:
        payload_json["campaign_id"] = campaign_id
    init_db()
    session = get_session_factory()()
    try:
        task = create_scheduled_task(
            session,
            ScheduledTaskCreate(
                name=name,
                task_type="discovery_health_scan",
                interval_seconds=interval_seconds,
                retry_attempts=retry_attempts,
                retry_backoff_seconds=retry_backoff_seconds,
                notes=notes,
                payload_json=payload_json,
            ),
        )
        print_banner()
        typer.echo(f"scheduled task {task.task_id} created for discovery health scans")
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("add-discovery-revisit-schedule")
def add_discovery_revisit_schedule_command(
    name: str,
    interval_seconds: int,
    candidate_id: int | None = None,
    normalized_domain: str | None = None,
    campaign_id: int | None = None,
    force: bool = False,
    include_suppressed: bool = False,
    priority: float = 0.0,
    notes: str = "",
    retry_attempts: int = 1,
    retry_backoff_seconds: float = 0.0,
) -> None:
    payload_json: dict[str, object] = {
        "force": force,
        "include_suppressed": include_suppressed,
        "priority": priority,
    }
    if candidate_id is not None:
        payload_json["candidate_id"] = candidate_id
    if normalized_domain is not None:
        payload_json["normalized_domain"] = normalized_domain
    if campaign_id is not None:
        payload_json["campaign_id"] = campaign_id
    init_db()
    session = get_session_factory()()
    try:
        task = create_scheduled_task(
            session,
            ScheduledTaskCreate(
                name=name,
                task_type="discovery_revisit",
                interval_seconds=interval_seconds,
                retry_attempts=retry_attempts,
                retry_backoff_seconds=retry_backoff_seconds,
                notes=notes,
                payload_json=payload_json,
            ),
        )
        print_banner()
        typer.echo(f"scheduled task {task.task_id} created for discovery revisits")
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


if __name__ == "__main__":
    app()

