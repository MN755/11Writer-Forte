from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.config import get_settings
from src.models import CustodyLogORM, SourceDefinitionORM, SourceRunORM
from src.schemas import SourceDefinitionCreate
from src.services.import_service import import_local_path


def source_now() -> datetime:
    return datetime.now(timezone.utc)


def create_source_definition(session: Session, payload: SourceDefinitionCreate) -> SourceDefinitionORM:
    record = SourceDefinitionORM(**payload.model_dump())
    session.add(record)
    session.add(
        CustodyLogORM(
            object_type="source_definition",
            object_id=payload.name,
            action="source_created",
            actor="system",
            details_json=payload.model_dump(),
        )
    )
    session.commit()
    session.refresh(record)
    return record


def list_source_definitions(session: Session) -> list[SourceDefinitionORM]:
    statement = select(SourceDefinitionORM).order_by(SourceDefinitionORM.name.asc())
    return list(session.scalars(statement))


def list_source_runs(session: Session) -> list[SourceRunORM]:
    statement = select(SourceRunORM).order_by(SourceRunORM.source_run_id.desc())
    return list(session.scalars(statement))


def run_source_definition(session: Session, source_id: int, actor: str = "source_runner") -> SourceRunORM:
    source = session.get(SourceDefinitionORM, source_id)
    if source is None:
        raise ValueError(f"Source {source_id} does not exist.")
    if not source.enabled:
        raise ValueError(f"Source {source_id} is disabled.")

    run = SourceRunORM(source_id=source.source_id, status="running")
    session.add(run)
    session.flush()

    try:
        import_path = materialize_source_payload(source)
        import_run = import_local_path(
            session,
            import_path,
            source.layer_key,
            source.notes,
            actor=actor,
        )
        finished_at = source_now()
        run.status = "completed"
        run.import_run_id = import_run.import_run_id
        run.records_imported = import_run.records_imported
        run.finished_at = finished_at
        run.output_json = {
            "import_run_id": import_run.import_run_id,
            "source_kind": source.source_kind,
            "target_uri": source.target_uri,
        }
        session.add(
            CustodyLogORM(
                object_type="source_definition",
                object_id=str(source.source_id),
                action="source_run_completed",
                actor=actor,
                details_json={
                    "source_run_id": run.source_run_id,
                    "records_imported": run.records_imported,
                    "import_run_id": import_run.import_run_id,
                },
            )
        )
        session.commit()
    except Exception as exc:
        run.status = "failed"
        run.error_text = str(exc)
        run.finished_at = source_now()
        session.add(
            CustodyLogORM(
                object_type="source_definition",
                object_id=str(source.source_id),
                action="source_run_failed",
                actor=actor,
                details_json={
                    "source_run_id": run.source_run_id,
                    "error_text": str(exc),
                },
            )
        )
        session.commit()
        raise

    session.refresh(run)
    return run


def materialize_source_payload(source: SourceDefinitionORM) -> str:
    if source.source_kind == "local_file":
        return str(Path(source.target_uri).expanduser().resolve())

    if source.source_kind in {"http_json", "http_text"}:
        parsed = urlparse(source.target_uri)
        suffix = ".json" if source.source_kind == "http_json" else ".txt"
        destination = build_cached_path(source.source_id, suffix)
        with urlopen(source.target_uri, timeout=30) as response:
            payload = response.read()
        destination.write_bytes(payload)
        return str(destination)

    raise ValueError(f"Unsupported source kind: {source.source_kind}")


def build_cached_path(source_id: int, suffix: str) -> Path:
    settings = get_settings()
    cache_dir = settings.data_dir / "source_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / f"source-{source_id}{suffix}"
