from __future__ import annotations

from pathlib import Path

import typer
from sqlalchemy import select

from src.config import get_settings
from src.db import get_session_factory, init_db
from src.models import (
    AlertORM,
    CustodyLogORM,
    LocalImportRunORM,
    ScheduledTaskORM,
    SourceTrustProfileORM,
)
from src.schemas import ScheduledTaskCreate
from src.services.import_service import import_local_path
from src.services.observation_service import build_cross_verification_summaries, query_observations
from src.services.scheduler_service import create_scheduled_task, run_due_tasks, run_task
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


@app.command("status")
def status() -> None:
    settings = get_settings()
    print_banner()
    typer.echo(f"env: {settings.app_env}")
    typer.echo(f"database: {settings.database_url}")
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
            f"import_run={run.import_run_id} format={run.source_format} records={run.records_imported}"
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
                f"{run.import_run_id} | {run.source_format} | {run.layer_key} | {run.records_imported} | {run.source_path}"
            )
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
