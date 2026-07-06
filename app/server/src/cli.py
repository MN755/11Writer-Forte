from __future__ import annotations

from pathlib import Path

import typer
from sqlalchemy import select

from src.config import get_settings
from src.db import get_session_factory, init_db
from src.models import LocalImportRunORM, SourceTrustProfileORM
from src.services.import_service import import_local_path
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


if __name__ == "__main__":
    app()

