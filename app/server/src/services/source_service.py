from __future__ import annotations

import base64
import hashlib
import json
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from xml.etree import ElementTree

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from src.config import get_settings
from src.models import (
    CustodyLogORM,
    ScheduledTaskORM,
    ScheduledTaskRunORM,
    SourceDefinitionORM,
    SourceRunORM,
    StorageObjectORM,
)
from src.schemas import SourceDefinitionCreate, SourceDefinitionUpdate
from src.services.import_service import import_local_path
from src.services.layer_service import ensure_data_layer
from src.services.storage_service import register_source_run_storage_object


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
    ensure_unique_source_name(session, payload.name)
    ensure_data_layer(session, payload.layer_key, actor="source_registry")
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


def update_source_definition(
    session: Session,
    source_id: int,
    payload: SourceDefinitionUpdate,
    actor: str = "system",
) -> SourceDefinitionORM:
    record = session.get(SourceDefinitionORM, source_id)
    if record is None:
        raise ValueError(f"Source {source_id} does not exist.")

    changes = payload.model_dump(exclude_unset=True)
    if not changes:
        return record

    if "name" in changes and changes["name"] != record.name:
        ensure_unique_source_name(session, str(changes["name"]), source_id=source_id)
    if "layer_key" in changes and changes["layer_key"]:
        ensure_data_layer(session, str(changes["layer_key"]), actor=actor)

    change_details = apply_changes(record, changes)
    session.add(
        CustodyLogORM(
            object_type="source_definition",
            object_id=str(record.source_id),
            action="source_updated",
            actor=actor,
            details_json={
                "source_id": record.source_id,
                "changes": change_details,
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


def build_source_ops_detail(session: Session, source_id: int) -> dict[str, object]:
    source = session.get(SourceDefinitionORM, source_id)
    if source is None:
        raise ValueError(f"Source {source_id} does not exist.")

    recent_runs = list(
        session.scalars(
            select(SourceRunORM)
            .where(SourceRunORM.source_id == source.source_id)
            .order_by(SourceRunORM.source_run_id.desc())
            .limit(25)
        )
    )
    run_ids = [str(run.source_run_id) for run in recent_runs]
    storage_objects = (
        list(
            session.scalars(
                select(StorageObjectORM)
                .where(
                    StorageObjectORM.owner_type == "source_run",
                    StorageObjectORM.owner_id.in_(run_ids),
                )
                .order_by(
                    StorageObjectORM.observed_at.desc().nullslast(),
                    StorageObjectORM.storage_object_id.desc(),
                )
            )
        )
        if run_ids
        else []
    )
    storage_object_ids = [str(row.storage_object_id) for row in storage_objects]
    custody_filters = [(CustodyLogORM.object_type == "source_definition") & (CustodyLogORM.object_id == str(source.source_id))]
    if run_ids:
        custody_filters.append((CustodyLogORM.object_type == "source_run") & CustodyLogORM.object_id.in_(run_ids))
    if storage_object_ids:
        custody_filters.append(
            (CustodyLogORM.object_type == "storage_object") & CustodyLogORM.object_id.in_(storage_object_ids)
        )
    custody_logs = list(
        session.scalars(
            select(CustodyLogORM)
            .where(or_(*custody_filters))
            .order_by(CustodyLogORM.created_at.desc())
            .limit(100)
        )
    )
    return {
        "source": source,
        "recent_runs": recent_runs,
        "storage_objects": storage_objects,
        "custody_logs": custody_logs,
    }


def build_source_inventory_summary(
    session: Session,
    *,
    stale_after_hours: float = 24.0,
) -> dict[str, object]:
    generated_at = source_now()
    stale_before = generated_at - timedelta(hours=max(0.0, stale_after_hours))
    statuses = collect_source_ops_statuses(session, stale_before=stale_before)
    return {
        "generated_at": generated_at,
        "stale_before": stale_before,
        "total_count": len(statuses),
        "enabled_count": sum(1 for status in statuses if status["source"].enabled),
        "disabled_count": sum(1 for status in statuses if not status["source"].enabled),
        "stale_count": sum(1 for status in statuses if status["is_stale"]),
        "failing_count": sum(1 for status in statuses if status["is_failing"]),
        "scheduled_count": sum(1 for status in statuses if status["has_schedule"]),
        "unscheduled_count": sum(1 for status in statuses if not status["has_schedule"]),
        "source_kind_counts": build_source_summary_buckets(
            statuses,
            lambda status: status["source"].source_kind,
        ),
        "layer_counts": build_source_summary_buckets(
            statuses,
            lambda status: status["source"].layer_key,
        ),
        "latest_status_counts": build_source_summary_buckets(
            statuses,
            lambda status: status["latest_run"].status if status["latest_run"] is not None else "never_run",
        ),
    }


def build_source_ops_report_index(
    session: Session,
    *,
    stale_after_hours: float = 24.0,
    limit: int = 25,
    stale_source_limit: int = 25,
) -> dict[str, object]:
    generated_at = source_now()
    stale_before = generated_at - timedelta(hours=max(0.0, stale_after_hours))
    statuses = collect_source_ops_statuses(session, stale_before=stale_before)
    summary = build_source_inventory_summary(session, stale_after_hours=stale_after_hours)
    sync_tasks = list(
        session.scalars(
            select(ScheduledTaskORM)
            .where(ScheduledTaskORM.task_type == "source_sync")
            .order_by(ScheduledTaskORM.task_id.asc())
        )
    )
    sync_task_ids = [task.task_id for task in sync_tasks]
    sync_runs = (
        list(
            session.scalars(
                select(SourceRunORM)
                .order_by(SourceRunORM.source_run_id.desc())
                .limit(limit)
            )
        )
        if statuses
        else []
    )
    latest_run_at = first_timestamp(
        normalize_timestamp(status["latest_run"].started_at)
        for status in statuses
        if status["latest_run"] is not None
    )
    return {
        "generated_at": generated_at,
        "stale_after_hours": stale_after_hours,
        "latest_run_at": latest_run_at,
        "inventory_summary": summary,
        "sync_task_count": len(sync_tasks),
        "sync_run_count": count_source_sync_task_runs(session, sync_task_ids),
        "sync_failure_count": count_source_sync_task_runs(session, sync_task_ids, failed_only=True),
        "sync_tasks": sync_tasks,
        "recent_runs": sync_runs,
        "stale_sources": [status for status in statuses if status["is_stale"]][:stale_source_limit],
        "failing_sources": [status for status in statuses if status["is_failing"]][:stale_source_limit],
        "unscheduled_sources": [status for status in statuses if not status["has_schedule"]][:stale_source_limit],
    }


def build_source_ops_export_summary(
    session: Session,
    *,
    stale_after_hours: float = 24.0,
    source_limit: int = 500,
    report_limit: int = 25,
    stale_source_limit: int = 25,
) -> dict[str, object]:
    generated_at = source_now()
    sources = list_source_definitions(session)[:source_limit]
    return {
        "generated_at": generated_at,
        "filters_json": {
            "stale_after_hours": stale_after_hours,
            "source_limit": source_limit,
            "report_limit": report_limit,
            "stale_source_limit": stale_source_limit,
        },
        "report_index": build_source_ops_report_index(
            session,
            stale_after_hours=stale_after_hours,
            limit=report_limit,
            stale_source_limit=stale_source_limit,
        ),
        "sources": sources,
    }


def collect_source_ops_statuses(
    session: Session,
    *,
    stale_before: datetime,
) -> list[dict[str, Any]]:
    sources = list_source_definitions(session)
    latest_runs_by_source = build_latest_runs_by_source(session)
    latest_success_at_by_source = build_latest_success_times_by_source(session)
    sync_tasks_by_source = build_sync_tasks_by_source(session)
    storage_stats_by_source = build_storage_stats_by_source(session)

    statuses = [
        build_source_ops_status(
            source,
            latest_run=latest_runs_by_source.get(source.source_id),
            latest_success_at=latest_success_at_by_source.get(source.source_id),
            sync_tasks=sync_tasks_by_source.get(source.source_id, []),
            storage_stats=storage_stats_by_source.get(source.source_id, {"count": 0, "latest_observed_at": None}),
            stale_before=stale_before,
        )
        for source in sources
    ]
    return sorted(
        statuses,
        key=lambda status: (
            not status["is_failing"],
            not status["is_stale"],
            status["source"].name.lower(),
        ),
    )


def build_latest_runs_by_source(session: Session) -> dict[int, SourceRunORM]:
    runs = list(
        session.scalars(
            select(SourceRunORM)
            .order_by(SourceRunORM.source_id.asc(), SourceRunORM.source_run_id.desc())
        )
    )
    latest: dict[int, SourceRunORM] = {}
    for run in runs:
        latest.setdefault(run.source_id, run)
    return latest


def build_latest_success_times_by_source(session: Session) -> dict[int, datetime]:
    runs = list(
        session.scalars(
            select(SourceRunORM)
            .where(SourceRunORM.status == "completed")
            .order_by(SourceRunORM.source_id.asc(), SourceRunORM.source_run_id.desc())
        )
    )
    latest: dict[int, datetime] = {}
    for run in runs:
        finished_at = normalize_timestamp(run.finished_at or run.started_at)
        latest.setdefault(run.source_id, finished_at)
    return latest


def build_sync_tasks_by_source(session: Session) -> dict[int, list[ScheduledTaskORM]]:
    tasks = list(
        session.scalars(
            select(ScheduledTaskORM)
            .where(
                ScheduledTaskORM.task_type == "source_sync",
                ScheduledTaskORM.source_id.is_not(None),
            )
            .order_by(ScheduledTaskORM.task_id.asc())
        )
    )
    grouped: dict[int, list[ScheduledTaskORM]] = defaultdict(list)
    for task in tasks:
        if task.source_id is None:
            continue
        grouped[task.source_id].append(task)
    return grouped


def build_storage_stats_by_source(session: Session) -> dict[int, dict[str, Any]]:
    runs = list(session.scalars(select(SourceRunORM)))
    source_id_by_run_id = {run.source_run_id: run.source_id for run in runs}
    storage_rows = list(
        session.scalars(
            select(StorageObjectORM)
            .where(StorageObjectORM.owner_type == "source_run")
            .order_by(
                StorageObjectORM.observed_at.desc().nullslast(),
                StorageObjectORM.storage_object_id.desc(),
            )
        )
    )
    stats: dict[int, dict[str, Any]] = {}
    for row in storage_rows:
        try:
            run_id = int(row.owner_id)
        except ValueError:
            continue
        source_id = source_id_by_run_id.get(run_id)
        if source_id is None:
            continue
        bucket = stats.setdefault(source_id, {"count": 0, "latest_observed_at": None})
        bucket["count"] += 1
        observed_at = normalize_timestamp(row.observed_at) if row.observed_at is not None else None
        if observed_at is not None and bucket["latest_observed_at"] is None:
            bucket["latest_observed_at"] = observed_at
    return stats


def build_source_ops_status(
    source: SourceDefinitionORM,
    *,
    latest_run: SourceRunORM | None,
    latest_success_at: datetime | None,
    sync_tasks: list[ScheduledTaskORM],
    storage_stats: dict[str, Any],
    stale_before: datetime,
) -> dict[str, Any]:
    enabled_tasks = [task for task in sync_tasks if task.enabled]
    next_run_at = first_timestamp(
        normalize_timestamp(task.next_run_at)
        for task in enabled_tasks
        if task.next_run_at is not None
    )
    is_stale = source.enabled and (latest_success_at is None or latest_success_at < stale_before)
    is_failing = latest_run is not None and latest_run.status == "failed"
    return {
        "source": source,
        "latest_run": latest_run,
        "has_schedule": bool(enabled_tasks),
        "next_run_at": next_run_at,
        "latest_success_at": latest_success_at,
        "is_stale": is_stale,
        "is_failing": is_failing,
        "storage_object_count": int(storage_stats.get("count", 0)),
        "last_storage_observed_at": storage_stats.get("latest_observed_at"),
    }


def count_source_sync_task_runs(
    session: Session,
    sync_task_ids: list[int],
    *,
    failed_only: bool = False,
) -> int:
    if not sync_task_ids:
        return 0
    statement = select(ScheduledTaskRunORM).where(ScheduledTaskRunORM.task_id.in_(sync_task_ids))
    if failed_only:
        statement = statement.where(ScheduledTaskRunORM.status == "failed")
    return len(list(session.scalars(statement)))


def build_source_summary_buckets(
    statuses: list[dict[str, Any]],
    key_fn,
) -> list[dict[str, object]]:
    buckets: dict[str, dict[str, object]] = {}
    for status in statuses:
        key = str(key_fn(status) or "unknown")
        bucket = buckets.setdefault(
            key,
            {
                "key": key,
                "total_count": 0,
                "enabled_count": 0,
                "disabled_count": 0,
                "stale_count": 0,
                "failing_count": 0,
            },
        )
        bucket["total_count"] += 1
        if status["source"].enabled:
            bucket["enabled_count"] += 1
        else:
            bucket["disabled_count"] += 1
        if status["is_stale"]:
            bucket["stale_count"] += 1
        if status["is_failing"]:
            bucket["failing_count"] += 1
    return sorted(
        buckets.values(),
        key=lambda item: (-int(item["total_count"]), str(item["key"]).lower()),
    )


def normalize_timestamp(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def first_timestamp(values: Any) -> datetime | None:
    for value in values:
        if value is not None:
            return value
    return None


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
        register_source_run_storage_object(
            session,
            source,
            run,
            materialized.path,
            materialized.metadata,
            run_status="materialized",
            actor=actor,
        )
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
            register_source_run_storage_object(
                session,
                source,
                run,
                materialized.path,
                materialized.metadata,
                run_status="skipped",
                actor=actor,
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
        register_source_run_storage_object(
            session,
            source,
            run,
            materialized.path,
            materialized.metadata,
            import_run_id=import_run.import_run_id,
            run_status="completed",
            actor=actor,
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
        if "materialized" in locals():
            register_source_run_storage_object(
                session,
                source,
                run,
                materialized.path,
                materialized.metadata,
                run_status="failed",
                actor=actor,
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

    if source.source_kind in {"http_json", "http_text", "http_xml"}:
        suffix = ".json" if source.source_kind in {"http_json", "http_xml"} else ".txt"
        destination = build_cached_path(source.source_id, suffix)
        fetch_config = parse_fetch_config(source)
        payload, fetch_metadata = fetch_http_source(source, fetch_config)
        if source.source_kind == "http_xml":
            records = parse_http_xml_payload(payload, source.target_uri)
            destination.write_text(json.dumps(records), encoding="utf-8")
            fetch_metadata = {
                **fetch_metadata,
                "cached_record_count": len(records),
                "materialized_content_type": "application/json",
                "original_content_type": fetch_metadata.get("content_type"),
            }
        else:
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
        "Accept": (
            "application/json"
            if source.source_kind == "http_json"
            else "application/xml, text/xml, */*"
            if source.source_kind == "http_xml"
            else "text/plain, */*"
        ),
    }
    if isinstance(user_headers, dict):
        headers.update({str(key): str(value) for key, value in user_headers.items()})
    apply_basic_auth_headers(metadata, headers)
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


def apply_basic_auth_headers(metadata: dict[str, Any], headers: dict[str, str]) -> None:
    username = metadata.get("basic_auth_username")
    password_env = metadata.get("basic_auth_password_env")
    if username is None and password_env is None:
        return
    if not isinstance(username, str) or not username:
        raise RuntimeError("HTTP source basic auth username is missing.")
    if not isinstance(password_env, str) or not password_env:
        raise RuntimeError("HTTP source basic auth password env var is missing.")
    password = os.getenv(password_env)
    if password is None:
        raise RuntimeError(f"HTTP source basic auth password env var '{password_env}' is not set.")
    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    headers.setdefault("Authorization", f"Basic {token}")


def parse_http_xml_payload(payload: bytes, source_uri: str) -> list[dict[str, Any]]:
    root = ElementTree.fromstring(payload)
    root_tag = strip_xml_namespace(root.tag)
    records: list[dict[str, Any]] = []
    for child in root:
        if not isinstance(child.tag, str):
            continue
        child_payload = xml_element_to_data(child)
        if not isinstance(child_payload, dict):
            child_payload = {"value": child_payload}
        record_type = strip_xml_namespace(child.tag)
        headline = extract_xml_headline(child_payload)
        route_designator = first_nested_value(child_payload, "route-designator")
        event_id = first_nested_value(child_payload, "event-id")
        status = first_nested_value(child_payload, "status")
        observed_at = first_feu_timestamp(child_payload)
        latitude = normalize_coordinate(first_nested_value(child_payload, "latitude"))
        longitude = normalize_coordinate(first_nested_value(child_payload, "longitude"))

        record: dict[str, Any] = {
            "source_url": source_uri,
            "feed_type": root_tag,
            "record_type": record_type,
            "title": headline or event_id or record_type,
            "text": " | ".join(
                part
                for part in (
                    event_id,
                    headline,
                    route_designator,
                    f"status={status}" if status else None,
                )
                if part
            ),
            "event_id": event_id,
            "status": status,
            "route_designator": route_designator,
            "observed_at": observed_at,
            "raw_xml": child_payload,
        }
        if latitude is not None and longitude is not None:
            record["latitude"] = latitude
            record["longitude"] = longitude
        records.append(record)
    return records


def strip_xml_namespace(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    if ":" in tag:
        return tag.rsplit(":", 1)[-1]
    return tag


def xml_element_to_data(element: ElementTree.Element) -> Any:
    children = list(element)
    text = (element.text or "").strip()
    if not children and not element.attrib:
        return text

    node: dict[str, Any] = {}
    for key, value in element.attrib.items():
        node[f"@{strip_xml_namespace(key)}"] = value

    grouped: dict[str, list[Any]] = defaultdict(list)
    for child in children:
        grouped[strip_xml_namespace(child.tag)].append(xml_element_to_data(child))

    for key, values in grouped.items():
        node[key] = values[0] if len(values) == 1 else values

    if text:
        node["text"] = text
    return node


def first_nested_value(payload: Any, key: str) -> str | None:
    if isinstance(payload, dict):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, (int, float)):
            return str(value)
        for child_value in payload.values():
            nested = first_nested_value(child_value, key)
            if nested:
                return nested
        return None
    if isinstance(payload, list):
        for item in payload:
            nested = first_nested_value(item, key)
            if nested:
                return nested
    return None


def collect_scalar_strings(payload: Any, limit: int = 6) -> list[str]:
    values: list[str] = []
    if isinstance(payload, dict):
        for item in payload.values():
            if len(values) >= limit:
                break
            values.extend(collect_scalar_strings(item, limit=limit - len(values)))
    elif isinstance(payload, list):
        for item in payload:
            if len(values) >= limit:
                break
            values.extend(collect_scalar_strings(item, limit=limit - len(values)))
    elif isinstance(payload, str):
        stripped = payload.strip()
        if stripped:
            values.append(stripped)
    elif isinstance(payload, (int, float)):
        values.append(str(payload))
    return values[:limit]


def extract_xml_headline(payload: dict[str, Any]) -> str | None:
    headline_payload = payload.get("headline")
    if headline_payload is None:
        return None
    parts = [part for part in collect_scalar_strings(headline_payload, limit=4) if not part.isdigit()]
    if not parts:
        return None
    return " | ".join(parts)


def first_feu_timestamp(payload: dict[str, Any]) -> str | None:
    candidates = [
        payload.get("message-header"),
        payload.get("times"),
        payload.get("detail"),
    ]
    for candidate in candidates:
        timestamp = find_timestamp_in_payload(candidate)
        if timestamp:
            return timestamp
    return None


def find_timestamp_in_payload(payload: Any) -> str | None:
    if isinstance(payload, dict):
        date_value = payload.get("date")
        time_value = payload.get("time")
        offset_value = payload.get("utc-offset")
        if isinstance(date_value, str) and isinstance(time_value, str):
            return format_feu_timestamp(date_value, time_value, offset_value if isinstance(offset_value, str) else None)
        for value in payload.values():
            timestamp = find_timestamp_in_payload(value)
            if timestamp:
                return timestamp
    elif isinstance(payload, list):
        for item in payload:
            timestamp = find_timestamp_in_payload(item)
            if timestamp:
                return timestamp
    return None


def format_feu_timestamp(date_value: str, time_value: str, offset_value: str | None) -> str:
    cleaned_date = date_value.strip()
    cleaned_time = time_value.strip()
    if len(cleaned_date) != 8 or len(cleaned_time) not in {4, 6}:
        return f"{cleaned_date}T{cleaned_time}"
    normalized_time = cleaned_time if len(cleaned_time) == 6 else f"{cleaned_time}00"
    timestamp = (
        f"{cleaned_date[0:4]}-{cleaned_date[4:6]}-{cleaned_date[6:8]}"
        f"T{normalized_time[0:2]}:{normalized_time[2:4]}:{normalized_time[4:6]}"
    )
    if not offset_value:
        return timestamp
    cleaned_offset = offset_value.strip()
    if len(cleaned_offset) == 5:
        return f"{timestamp}{cleaned_offset[0:3]}:{cleaned_offset[3:5]}"
    return f"{timestamp}{cleaned_offset}"


def normalize_coordinate(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except ValueError:
        return None
    if abs(parsed) > 1000:
        return parsed / 1_000_000.0
    return parsed


def ensure_unique_source_name(
    session: Session,
    name: str,
    *,
    source_id: int | None = None,
) -> None:
    statement = select(SourceDefinitionORM).where(SourceDefinitionORM.name == name)
    existing = session.scalar(statement)
    if existing is None:
        return
    if source_id is not None and existing.source_id == source_id:
        return
    raise ValueError(f"Source name '{name}' already exists.")


def apply_changes(record: SourceDefinitionORM, changes: dict[str, Any]) -> dict[str, dict[str, Any]]:
    details: dict[str, dict[str, Any]] = {}
    for field_name, new_value in changes.items():
        old_value = getattr(record, field_name)
        if old_value == new_value:
            continue
        setattr(record, field_name, new_value)
        details[field_name] = {
            "old": old_value,
            "new": new_value,
        }
    return details
