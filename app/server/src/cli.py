from __future__ import annotations

import json
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
    EventORM,
    LocalImportRunORM,
    ScheduledTaskORM,
    SituationProductORM,
    SourceTrustProfileORM,
)
from src.schemas import (
    EventExportBundleRead,
    EventFusionRequest,
    EntityResolutionRequest,
    ScheduledTaskCreate,
    SourceDefinitionCreate,
)
from src.services.entity_resolution_service import materialize_entities
from src.services.event_export_service import build_event_export_bundle
from src.services.event_fusion_service import materialize_fused_events
from src.services.import_service import import_local_path
from src.services.observation_service import build_cross_verification_summaries, query_observations
from src.services.scheduler_service import create_scheduled_task, run_due_tasks, run_task
from src.services.source_service import (
    create_source_definition,
    list_source_definitions,
    list_source_runs,
    run_source_definition,
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


def build_http_source_metadata(
    *,
    timeout_seconds: float,
    retry_attempts: int,
    retry_backoff_seconds: float,
    skip_unchanged: bool,
    header: list[str],
) -> dict[str, object]:
    headers: dict[str, str] = {}
    for item in header:
        if ":" not in item:
            raise typer.BadParameter("header must be 'Name: Value'")
        key, value = item.split(":", 1)
        headers[key.strip()] = value.strip()
    return {
        "request_timeout_seconds": timeout_seconds,
        "retry_attempts": retry_attempts,
        "retry_backoff_seconds": retry_backoff_seconds,
        "skip_unchanged": skip_unchanged,
        "headers": headers,
    }


@app.command("status")
def status() -> None:
    settings = get_settings()
    print_banner()
    typer.echo(f"env: {settings.app_env}")
    typer.echo(f"database: {settings.database_url}")
    typer.echo(f"spatial backend: {settings.spatial_backend}")
    typer.echo(f"data dir: {settings.data_dir}")


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
def export_event_product(event_id: int, product_type: str, output_path: Path) -> None:
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
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(product.body_text, encoding="utf-8")
        print_banner()
        typer.echo(f"exported {product.product_type} to {output_path}")
    finally:
        session.close()


@app.command("export-event-bundle")
def export_event_bundle(event_id: int, output_path: Path) -> None:
    init_db()
    session = get_session_factory()()
    try:
        bundle = build_event_export_bundle(session, event_id)
        serializable = TypeAdapter(EventExportBundleRead).validate_python(bundle).model_dump(mode="json")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(serializable, indent=2), encoding="utf-8")
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
) -> None:
    init_db()
    session = get_session_factory()()
    try:
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
    limit: int = 200,
    time_window_minutes: int = 60,
    distance_km: float = 25.0,
) -> None:
    init_db()
    session = get_session_factory()()
    try:
        min_lon, min_lat, max_lon, max_lat = parse_bbox(bbox)
        rows = query_observations(
            session,
            layer_key=layer,
            min_lon=min_lon,
            min_lat=min_lat,
            max_lon=max_lon,
            max_lat=max_lat,
            limit=limit,
        )
        summaries = build_cross_verification_summaries(
            rows,
            time_window_minutes=time_window_minutes,
            distance_km=distance_km,
        )
        print_banner()
        for summary in summaries:
            typer.echo(
                f"{summary['cluster_id']} | observations={summary['observation_count']} | domains={summary['source_domain_count']} | layers={summary['layer_count']} | score={summary['verification_score']:.2f}"
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


@app.command("add-local-import-schedule")
def add_local_import_schedule(
    name: str,
    source_path: Path,
    interval_seconds: int,
    layer: str = "unassigned",
    notes: str = "",
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
                source_id=source_id,
                notes=notes,
            ),
        )
        print_banner()
        typer.echo(f"scheduled task {task.task_id} created for source sync")
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
            typer.echo(
                f"{row.task_id} | {row.task_type} | every={row.interval_seconds}s | enabled={row.enabled} | next={row.next_run_at}"
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


if __name__ == "__main__":
    app()
