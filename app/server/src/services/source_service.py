from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.config import get_settings
from src.models import CustodyLogORM, SourceDefinitionORM, SourceRunORM
from src.schemas import SourceDefinitionCreate
from src.services.import_service import import_local_path


@dataclass(frozen=True)
class SourceFetchConfig:
    timeout_seconds: float
    retry_attempts: int
    retry_backoff_seconds: float
    headers: dict[str, str]


@dataclass(frozen=True)
class MaterializedSourcePayload:
    path: str
    metadata: dict[str, Any]


def source_now() -> datetime:
    return datetime.now(timezone.utc)


def create_source_definition(session: Session, payload: SourceDefinitionCreate) -> SourceDefinitionORM:
    record = SourceDefinitionORM(**payload.model_dump())
    session.add(record)
    session.flush()
    session.add(
        CustodyLogORM(
            object_type="source_definition",
            object_id=str(record.source_id),
            action="source_created",
            actor="system",
            details_json={
                **payload.model_dump(),
                "source_id": record.source_id,
            },
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
    session.add(
        CustodyLogORM(
            object_type="source_run",
            object_id=str(run.source_run_id),
            action="source_run_started",
            actor=actor,
            details_json={
                "source_id": source.source_id,
                "source_kind": source.source_kind,
                "target_uri": source.target_uri,
            },
        )
    )

    try:
        materialized = materialize_source_payload(source)
        session.add(
            CustodyLogORM(
                object_type="source_run",
                object_id=str(run.source_run_id),
                action="source_payload_materialized",
                actor=actor,
                details_json=materialized.metadata,
            )
        )
        if should_skip_unchanged_source(session, source, materialized.metadata):
            finished_at = source_now()
            run.status = "skipped"
            run.records_imported = 0
            run.finished_at = finished_at
            run.output_json = {
                "source_kind": source.source_kind,
                "target_uri": source.target_uri,
                "skip_reason": "payload_unchanged",
                **materialized.metadata,
            }
            session.add(
                CustodyLogORM(
                    object_type="source_run",
                    object_id=str(run.source_run_id),
                    action="source_run_skipped",
                    actor=actor,
                    details_json={
                        "source_id": source.source_id,
                        "skip_reason": "payload_unchanged",
                        "payload_sha256": materialized.metadata.get("payload_sha256"),
                    },
                )
            )
            session.add(
                CustodyLogORM(
                    object_type="source_definition",
                    object_id=str(source.source_id),
                    action="source_run_skipped",
                    actor=actor,
                    details_json={
                        "source_run_id": run.source_run_id,
                        "skip_reason": "payload_unchanged",
                        "payload_sha256": materialized.metadata.get("payload_sha256"),
                    },
                )
            )
            session.commit()
            session.refresh(run)
            return run
        import_run = import_local_path(
            session,
            materialized.path,
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
            **materialized.metadata,
        }
        session.add(
            CustodyLogORM(
                object_type="source_run",
                object_id=str(run.source_run_id),
                action="source_run_completed",
                actor=actor,
                details_json={
                    "source_id": source.source_id,
                    "records_imported": run.records_imported,
                    "import_run_id": import_run.import_run_id,
                },
            )
        )
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
                object_type="source_run",
                object_id=str(run.source_run_id),
                action="source_run_failed",
                actor=actor,
                details_json={
                    "source_id": source.source_id,
                    "error_text": str(exc),
                },
            )
        )
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


def materialize_source_payload(source: SourceDefinitionORM) -> MaterializedSourcePayload:
    if source.source_kind == "local_file":
        resolved = Path(source.target_uri).expanduser().resolve()
        payload = resolved.read_bytes()
        path = str(resolved)
        return MaterializedSourcePayload(
            path=path,
            metadata={
                "materialization_kind": "local_file",
                "resolved_path": path,
                "byte_count": len(payload),
                "payload_sha256": hashlib.sha256(payload).hexdigest(),
            },
        )

    if source.source_kind in {"http_json", "http_text"}:
        parsed = urlparse(source.target_uri)
        suffix = ".json" if source.source_kind == "http_json" else ".txt"
        destination = build_cached_path(source.source_id, suffix)
        fetch_config = parse_fetch_config(source)
        payload, fetch_metadata = fetch_http_source(source, fetch_config)
        destination.write_bytes(payload)
        return MaterializedSourcePayload(
            path=str(destination),
            metadata={
                "materialization_kind": "http_fetch",
                "cached_path": str(destination),
                **fetch_metadata,
            },
        )

    raise ValueError(f"Unsupported source kind: {source.source_kind}")


def build_cached_path(source_id: int, suffix: str) -> Path:
    settings = get_settings()
    cache_dir = settings.data_dir / "source_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / f"source-{source_id}{suffix}"


def parse_fetch_config(source: SourceDefinitionORM) -> SourceFetchConfig:
    metadata = source.metadata_json if isinstance(source.metadata_json, dict) else {}
    timeout_seconds = float(metadata.get("request_timeout_seconds", 30.0))
    retry_attempts = max(1, int(metadata.get("retry_attempts", 3)))
    retry_backoff_seconds = max(0.0, float(metadata.get("retry_backoff_seconds", 0.0)))
    user_headers = metadata.get("headers", {})
    headers = {
        "User-Agent": str(metadata.get("user_agent", "11Writer-Forte/0.1 (+headless-source-fetch)")),
        "Accept": "application/json" if source.source_kind == "http_json" else "text/plain, */*",
    }
    if isinstance(user_headers, dict):
        headers.update({str(key): str(value) for key, value in user_headers.items()})
    return SourceFetchConfig(
        timeout_seconds=timeout_seconds,
        retry_attempts=retry_attempts,
        retry_backoff_seconds=retry_backoff_seconds,
        headers=headers,
    )


def should_skip_unchanged_source(
    session: Session,
    source: SourceDefinitionORM,
    metadata: dict[str, Any],
) -> bool:
    if not source_skip_unchanged_enabled(source):
        return False
    payload_sha256 = metadata.get("payload_sha256")
    if not isinstance(payload_sha256, str) or not payload_sha256:
        return False
    previous_run = session.scalar(
        select(SourceRunORM)
        .where(
            SourceRunORM.source_id == source.source_id,
            SourceRunORM.status.in_(("completed", "skipped")),
        )
        .order_by(SourceRunORM.source_run_id.desc())
        .limit(1)
    )
    if previous_run is None:
        return False
    previous_hash = previous_run.output_json.get("payload_sha256")
    return previous_hash == payload_sha256


def source_skip_unchanged_enabled(source: SourceDefinitionORM) -> bool:
    metadata = source.metadata_json if isinstance(source.metadata_json, dict) else {}
    return bool(metadata.get("skip_unchanged", True))


def fetch_http_source(
    source: SourceDefinitionORM,
    fetch_config: SourceFetchConfig,
) -> tuple[bytes, dict[str, Any]]:
    last_error: Exception | None = None
    for attempt in range(1, fetch_config.retry_attempts + 1):
        request = Request(source.target_uri, headers=fetch_config.headers)
        try:
            with urlopen(request, timeout=fetch_config.timeout_seconds) as response:
                payload = response.read()
                content_type = response.headers.get("Content-Type")
                status_code = getattr(response, "status", None) or getattr(response, "code", None) or 200
                return payload, {
                    "attempt_count": attempt,
                    "http_status": int(status_code),
                    "content_type": content_type,
                    "byte_count": len(payload),
                    "payload_sha256": hashlib.sha256(payload).hexdigest(),
                    "request_timeout_seconds": fetch_config.timeout_seconds,
                    "retry_attempts": fetch_config.retry_attempts,
                    "headers": fetch_config.headers,
                    "host": urlparse(source.target_uri).netloc,
                }
        except HTTPError as exc:
            last_error = exc
            if not should_retry_http_error(exc.code) or attempt >= fetch_config.retry_attempts:
                break
            apply_retry_backoff(fetch_config, attempt)
        except URLError as exc:
            last_error = exc
            if attempt >= fetch_config.retry_attempts:
                break
            apply_retry_backoff(fetch_config, attempt)

    assert last_error is not None
    raise RuntimeError(
        f"HTTP source fetch failed after {fetch_config.retry_attempts} attempts: {last_error}"
    ) from last_error


def should_retry_http_error(status_code: int) -> bool:
    return status_code in {408, 425, 429, 500, 502, 503, 504}


def apply_retry_backoff(fetch_config: SourceFetchConfig, attempt: int) -> None:
    if fetch_config.retry_backoff_seconds <= 0:
        return
    time.sleep(fetch_config.retry_backoff_seconds * attempt)
