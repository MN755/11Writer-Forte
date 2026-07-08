from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import typer
from pydantic import TypeAdapter
from sqlalchemy import select

from src.config import get_settings
from src.db import get_engine, get_session_factory, init_db
from src.migrations import upgrade_database
from src.models import (
    AlertORM,
    CustodyLogORM,
    EntityObservationLinkORM,
    EntityORM,
    EventORM,
    LocalImportRunORM,
    ScheduledTaskORM,
    ScheduledTaskRunORM,
    SituationProductORM,
    SourceTrustProfileORM,
)
from src.schemas import (
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
    EventExportBundleRead,
    EventFusionRequest,
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
    StorageManifestRead,
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
)
from src.services.camera_source_service import (
    build_camera_source_inventory_ops_detail,
    build_camera_source_ops_export_summary,
    build_camera_source_ops_report_index,
    build_camera_source_inventory_summary,
    list_camera_sources,
    materialize_camera_source_inventory,
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
from src.services.entity_resolution_service import materialize_entities
from src.services.export_artifact_service import write_json_export_artifact, write_text_export_artifact
from src.services.event_export_service import build_event_export_bundle
from src.services.event_fusion_service import materialize_fused_events
from src.services.import_service import import_local_path
from src.services.layer_service import create_data_layer, list_data_layers
from src.services.observation_service import build_cross_verification_summaries, query_observations
from src.services.operations_report_service import build_operations_report
from src.services.redaction_service import enforce_export_redaction
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
    verify_storage_object,
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
from src.services.trust_service import seed_default_integrity_sources

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


def resolve_scheduler_poll_seconds(value: float | None) -> float:
    if value is not None:
        return max(0.0, value)
    return max(0.0, get_settings().scheduler_poll_seconds)


def print_database_diagnostics(serializable: dict[str, object]) -> None:
    typer.echo(
        "status="
        f"{serializable['status']} backend={serializable['database_backend']} "
        f"connected={serializable['database_connected']} spatial={serializable['spatial_backend']} "
        f"warnings={serializable['warning_count']}"
    )
    migration = serializable["migration"]
    typer.echo(
        "migration="
        f"current={migration['current_revision']} "
        f"head={migration['head_revision']} "
        f"version_table_present={migration['version_table_present']} "
        f"schema_up_to_date={migration['schema_up_to_date']}"
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


def write_database_diagnostics(output_path: Path | None, serializable: dict[str, object]) -> None:
    if output_path is None:
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(serializable, indent=2), encoding="utf-8")
    typer.echo(f"wrote diagnostics to {output_path}")


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
    session = get_session_factory()()
    try:
        report = build_database_diagnostics(session)
        serializable = TypeAdapter(DatabaseDiagnosticsRead).validate_python(report).model_dump(mode="json")
        print_banner()
        print_database_diagnostics(serializable)
        write_database_diagnostics(output_path, serializable)
    finally:
        session.close()


@app.command("verify-db")
def verify_db(
    output_path: Path | None = None,
    fail_on_warnings: bool = True,
) -> None:
    session = get_session_factory()()
    try:
        report = build_database_diagnostics(session)
        serializable = TypeAdapter(DatabaseDiagnosticsRead).validate_python(report).model_dump(mode="json")
        print_banner()
        print_database_diagnostics(serializable)
        write_database_diagnostics(output_path, serializable)
    finally:
        session.close()
    if fail_on_warnings and int(serializable["warning_count"]) > 0:
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
    status = init_db(auto_upgrade=True)
    typer.echo(
        f"database initialized at revision {status.current_revision} "
        f"(head={status.head_revision})"
    )


@app.command("migrate-db")
def migrate_database(revision: str = "head") -> None:
    status = upgrade_database(get_engine(), revision=revision)
    typer.echo(
        f"database migrated to revision {status.current_revision} "
        f"(head={status.head_revision})"
    )


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
        rows = list(session.scalars(select(EventORM).order_by(EventORM.created_at.desc())))
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


@app.command("list-alerts")
def list_alerts() -> None:
    init_db()
    session = get_session_factory()()
    try:
        alerts = list(session.scalars(select(AlertORM).order_by(AlertORM.created_at.desc())))
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
            f"archived={serializable['archived_count']} quarantined={serializable['quarantined_count']}"
        )
        typer.echo(
            f"archive_pending={serializable['archive_pending_count']} verification_failures={serializable['verification_failure_count']} "
            f"rehydration_pending={serializable['rehydration_pending_count']}"
        )
        typer.echo(
            f"next_expiration_at={serializable['next_expiration_at']} oldest_expired_at={serializable['oldest_expired_at']}"
        )
        if serializable["problem_objects"]:
            typer.echo("problem_objects:")
            for row in serializable["problem_objects"]:
                typer.echo(
                    f"  {row['storage_object_id']} | {row['object_kind']} | status={row['lifecycle_status']} | uri={row['object_uri']}"
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
        serializable = TypeAdapter(StorageManifestRead).validate_python(manifest).model_dump(mode="json")
        print_banner()
        typer.echo(
            f"canonical_uri={serializable['canonical_uri']} transfer_status={serializable['transfer_status']} "
            f"managed={serializable['managed']} archive_eligible={serializable['archive_eligible']} "
            f"prune_eligible={serializable['prune_eligible']}"
        )
        typer.echo(
            f"archived_at={serializable['archived_at']} rehydrated_at={serializable['rehydrated_at']} "
            f"last_verified_at={serializable['last_verified_at']} failure_reason={serializable['failure_reason']}"
        )
        typer.echo("replicas:")
        for replica in serializable["replicas"]:
            typer.echo(
                f"  {replica['role']} | backend={replica['backend']} | status={replica['status']} | "
                f"size={replica['byte_size']} | {replica['uri']}"
            )
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
    operation: list[str] | None = typer.Option(None, "--operation"),
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
            operations=operation,
        )
        serializable = TypeAdapter(StorageLifecycleSweepResultRead).validate_python(result).model_dump(mode="json")
        print_banner()
        typer.echo(
            f"swept_at={serializable['swept_at']} dry_run={serializable['dry_run']} "
            f"processed={serializable['processed_count']} failed={serializable['failed_count']} "
            f"expired_candidates={serializable['expired_candidate_count']} transitioned={serializable['transitioned_count']}"
        )
        typer.echo(f"operations={','.join(serializable['filters_json'].get('operations', []))}")
        for operation_result in serializable["operation_results"]:
            typer.echo(
                f"  {operation_result['operation']} | candidates={operation_result['candidate_count']} "
                f"processed={operation_result['processed_count']} failed={operation_result['failed_count']}"
            )
        if serializable["candidates"]:
            typer.echo("candidates:")
            for row in serializable["candidates"]:
                typer.echo(
                    f"  {row['storage_object_id']} | {row['object_kind']} | retention={row['retention_class']} | "
                    f"status={row['lifecycle_status']} | expires_at={row['expires_at']}"
                )
    finally:
        session.close()


@app.command("archive-storage-object")
def archive_storage_object_command(
    storage_object_id: int,
    prune_local: bool = False,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = archive_storage_object(
            session,
            storage_object_id,
            prune_local=prune_local,
            actor="cli_storage",
        )
        print_banner()
        typer.echo(
            f"action={result['action']} verified={result['verified']} "
            f"storage_object={result['storage_object']['storage_object_id']} "
            f"canonical_uri={result['manifest']['canonical_uri']}"
        )
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
        print_banner()
        typer.echo(
            f"action={result['action']} verified={result['verified']} "
            f"storage_object={result['storage_object']['storage_object_id']} "
            f"last_verified_at={result['manifest']['last_verified_at']}"
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("request-storage-rehydration")
def request_storage_rehydration_command(storage_object_id: int) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = request_storage_object_rehydration(session, storage_object_id, actor="cli_storage")
        print_banner()
        typer.echo(
            f"action={result['action']} status={result['manifest']['transfer_status']} "
            f"storage_object={result['storage_object']['storage_object_id']}"
        )
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
        print_banner()
        typer.echo(
            f"action={result['action']} verified={result['verified']} "
            f"storage_object={result['storage_object']['storage_object_id']} "
            f"rehydrated_at={result['manifest']['rehydrated_at']}"
        )
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
        print_banner()
        typer.echo(
            f"action={result['action']} verified={result['verified']} "
            f"storage_object={result['storage_object']['storage_object_id']} "
            f"canonical_uri={result['manifest']['canonical_uri']}"
        )
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
        print_banner()
        typer.echo(
            f"action={result['action']} status={result['storage_object']['lifecycle_status']} "
            f"reason={result['manifest']['failure_reason']}"
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        session.close()


@app.command("unquarantine-storage-object")
def unquarantine_storage_object_command(
    storage_object_id: int,
    note: str | None = None,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        result = unquarantine_storage_object(
            session,
            storage_object_id,
            note=note,
            actor="cli_storage",
        )
        print_banner()
        typer.echo(
            f"action={result['action']} status={result['storage_object']['lifecycle_status']} "
            f"transfer_status={result['manifest']['transfer_status']}"
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
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
            f"expired={storage_report['expired_count']} archived={storage_report['archived_count']} "
            f"quarantined={storage_report['quarantined_count']} archive_pending={storage_report['archive_pending_count']} "
            f"verify_failures={storage_report['verification_failure_count']} rehydrate_pending={storage_report['rehydration_pending_count']}"
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
                "database_revision": serializable["database_revision"],
                "database_head_revision": serializable["database_head_revision"],
            },
            actor="cli_export",
        )
        print_banner()
        typer.echo(f"exported runtime snapshot to {output_path}")
    finally:
        session.close()


@app.command("backup-runtime")
def backup_runtime_command(output_path: Path) -> None:
    export_runtime_snapshot_command(output_path)


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


@app.command("restore-runtime")
def restore_runtime_command(
    input_path: Path,
    replace_existing: bool = False,
) -> None:
    restore_runtime_snapshot_command(input_path=input_path, replace_existing=replace_existing)


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


@app.command("add-storage-lifecycle-schedule")
def add_storage_lifecycle_schedule(
    name: str,
    interval_seconds: int,
    retention_class: str | None = None,
    limit: int = 100,
    operation: list[str] | None = typer.Option(None, "--operation"),
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
        if operation:
            payload_json["operations"] = operation
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


if __name__ == "__main__":
    app()
