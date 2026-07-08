from __future__ import annotations

import base64
import hashlib
import json
import logging
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
from src.metrics import inc_counter
from src.models import (
    CustodyLogORM,
    SourceCheckpointORM,
    SourceDeadLetterORM,
    ScheduledTaskORM,
    ScheduledTaskRunORM,
    SourceDefinitionORM,
    SourceRunORM,
    StorageObjectORM,
)
from src.observability import log_event
from src.schemas import SourceDefinitionCreate, SourceDefinitionUpdate
from src.services.import_service import import_local_path
from src.services.ingestion_service import persist_envelopes_as_import_run
from src.services.layer_service import ensure_data_layer
from src.services.source_adapter_service import (
    AdapterContext,
    DeadLetterCandidate,
    get_adapter_for_source,
    ingest_webhook_source,
    list_supported_source_kinds,
    replay_source_dead_letter,
    run_source_adapter,
)
from src.services.redaction_service import sanitize_for_observability, sanitize_url
from src.services.storage_service import register_source_run_storage_object, register_storage_object

logger = logging.getLogger(__name__)


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


SOURCE_SCHEDULE_TASK_TYPES = ("source_sync", "source_runtime")


def create_source_definition(session: Session, payload: SourceDefinitionCreate) -> SourceDefinitionORM:
    ensure_unique_source_name(session, payload.name)
    validate_source_kind(payload.source_kind)
    ensure_target_uri_has_no_embedded_credentials(payload.target_uri)
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
                **sanitize_for_observability(payload.model_dump()),
                "source_id": record.source_id,
            },
        )
    )
    session.commit()
    session.refresh(record)
    log_event(
        logger,
        logging.INFO,
        "source_created",
        source_id=record.source_id,
        source_kind=record.source_kind,
        target_uri=record.target_uri,
        layer_key=record.layer_key,
    )
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
    if "source_kind" in changes and changes["source_kind"]:
        validate_source_kind(str(changes["source_kind"]))
    if "target_uri" in changes and changes["target_uri"]:
        ensure_target_uri_has_no_embedded_credentials(str(changes["target_uri"]))
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
                "changes": sanitize_for_observability(change_details),
            },
        )
    )
    session.commit()
    session.refresh(record)
    log_event(
        logger,
        logging.INFO,
        "source_updated",
        source_id=record.source_id,
        changes=change_details,
    )
    return record


def list_source_definitions(session: Session) -> list[SourceDefinitionORM]:
    statement = select(SourceDefinitionORM).order_by(SourceDefinitionORM.name.asc())
    return list(session.scalars(statement))


def list_source_runs(session: Session) -> list[SourceRunORM]:
    statement = select(SourceRunORM).order_by(SourceRunORM.source_run_id.desc())
    return list(session.scalars(statement))


def list_source_checkpoints(session: Session) -> list[SourceCheckpointORM]:
    statement = select(SourceCheckpointORM).order_by(SourceCheckpointORM.source_id.asc())
    return list(session.scalars(statement))


def list_source_dead_letters(
    session: Session,
    *,
    source_id: int | None = None,
    status: str | None = None,
    limit: int = 200,
) -> list[SourceDeadLetterORM]:
    statement = select(SourceDeadLetterORM).order_by(SourceDeadLetterORM.source_dead_letter_id.desc())
    if source_id is not None:
        statement = statement.where(SourceDeadLetterORM.source_id == source_id)
    if status is not None:
        statement = statement.where(SourceDeadLetterORM.status == status)
    return list(session.scalars(statement.limit(limit)))


def run_source_runtime_cycle(
    session: Session,
    *,
    source_id: int | None = None,
    actor: str = "source_runtime",
) -> dict[str, object]:
    statement = select(SourceDefinitionORM).where(SourceDefinitionORM.enabled.is_(True))
    if source_id is not None:
        statement = statement.where(SourceDefinitionORM.source_id == source_id)
    candidate_sources = list(session.scalars(statement.order_by(SourceDefinitionORM.source_id.asc())))
    sources = [
        source
        for source in candidate_sources
        if get_adapter_for_source(source.source_kind).fetch_mode == "stream"
    ]
    if source_id is not None and not sources:
        source = session.get(SourceDefinitionORM, source_id)
        if source is None:
            raise ValueError(f"Source {source_id} does not exist.")
        raise ValueError(f"Source {source_id} is not a stream-native runtime source.")
    runs: list[SourceRunORM] = []
    for source in sources:
        runs.append(run_source_definition(session, source.source_id, actor=actor))
    return {
        "source_count": len(sources),
        "source_run_ids": [run.source_run_id for run in runs],
        "records_seen": sum(run.records_seen for run in runs),
        "records_imported": sum(run.records_imported for run in runs),
        "records_failed": sum(run.records_failed for run in runs),
    }


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
    checkpoint = session.scalar(
        select(SourceCheckpointORM).where(SourceCheckpointORM.source_id == source.source_id).limit(1)
    )
    dead_letters = list(
        session.scalars(
            select(SourceDeadLetterORM)
            .where(SourceDeadLetterORM.source_id == source.source_id)
            .order_by(SourceDeadLetterORM.source_dead_letter_id.desc())
            .limit(50)
        )
    )
    sync_tasks = list(
        session.scalars(
            select(ScheduledTaskORM)
            .where(ScheduledTaskORM.source_id == source.source_id)
            .order_by(ScheduledTaskORM.task_id.asc())
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
        "report_status": build_source_ops_status(
            source,
            latest_run=recent_runs[0] if recent_runs else None,
            latest_success_at=first_timestamp(
                normalize_timestamp(run.finished_at)
                for run in recent_runs
                if run.status in {"completed", "skipped"} and run.finished_at is not None
            ),
            sync_tasks=sync_tasks,
            storage_stats={
                "count": len(storage_objects),
                "latest_observed_at": first_timestamp(
                    normalize_timestamp(row.observed_at)
                    for row in storage_objects
                    if row.observed_at is not None
                ),
            },
            checkpoint=checkpoint,
            dead_letter_stats={
                "pending_count": sum(1 for row in dead_letters if row.status == "pending"),
                "replayed_count": sum(1 for row in dead_letters if row.status == "replayed"),
                "latest_created_at": max((row.created_at for row in dead_letters), default=None),
            },
            stale_before=source_now() - timedelta(hours=24),
        ),
        "recent_runs": recent_runs,
        "checkpoint": checkpoint,
        "dead_letters": dead_letters,
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
        "runtime_active_count": sum(1 for status in statuses if status["runtime_state"] == "active"),
        "runtime_degraded_count": sum(1 for status in statuses if status["runtime_state"] == "degraded"),
        "dead_letter_pending_count": sum(int(status["pending_dead_letter_count"]) for status in statuses),
        "scheduled_count": sum(1 for status in statuses if status["has_schedule"]),
        "unscheduled_count": sum(1 for status in statuses if not status["has_schedule"]),
        "source_kind_counts": build_source_summary_buckets(
            statuses,
            lambda status: status["source"].source_kind,
        ),
        "fetch_mode_counts": build_source_summary_buckets(
            statuses,
            lambda status: status["fetch_mode"],
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
            .where(ScheduledTaskORM.task_type.in_(SOURCE_SCHEDULE_TASK_TYPES))
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
        "pending_dead_letter_count": sum(int(status["pending_dead_letter_count"]) for status in statuses),
        "runtime_degraded_count": sum(1 for status in statuses if status["runtime_state"] == "degraded"),
        "sync_tasks": sync_tasks,
        "recent_runs": sync_runs,
        "stale_sources": [status for status in statuses if status["is_stale"]][:stale_source_limit],
        "failing_sources": [status for status in statuses if status["is_failing"]][:stale_source_limit],
        "unscheduled_sources": [status for status in statuses if not status["has_schedule"]][:stale_source_limit],
        "runtime_degraded_sources": [
            status for status in statuses if status["runtime_state"] == "degraded"
        ][:stale_source_limit],
        "pending_dead_letter_sources": [
            status for status in statuses if int(status["pending_dead_letter_count"]) > 0
        ][:stale_source_limit],
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
    checkpoints_by_source = build_checkpoints_by_source(session)
    dead_letter_stats_by_source = build_dead_letter_stats_by_source(session)

    statuses = [
        build_source_ops_status(
            source,
            latest_run=latest_runs_by_source.get(source.source_id),
            latest_success_at=latest_success_at_by_source.get(source.source_id),
            sync_tasks=sync_tasks_by_source.get(source.source_id, []),
            storage_stats=storage_stats_by_source.get(source.source_id, {"count": 0, "latest_observed_at": None}),
            checkpoint=checkpoints_by_source.get(source.source_id),
            dead_letter_stats=dead_letter_stats_by_source.get(
                source.source_id,
                {"pending_count": 0, "replayed_count": 0, "latest_created_at": None},
            ),
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
                ScheduledTaskORM.task_type.in_(SOURCE_SCHEDULE_TASK_TYPES),
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


def build_checkpoints_by_source(session: Session) -> dict[int, SourceCheckpointORM]:
    rows = list(
        session.scalars(
            select(SourceCheckpointORM).order_by(SourceCheckpointORM.source_id.asc())
        )
    )
    return {row.source_id: row for row in rows}


def build_dead_letter_stats_by_source(session: Session) -> dict[int, dict[str, Any]]:
    rows = list(
        session.scalars(
            select(SourceDeadLetterORM)
            .order_by(SourceDeadLetterORM.source_id.asc(), SourceDeadLetterORM.source_dead_letter_id.desc())
        )
    )
    stats: dict[int, dict[str, Any]] = {}
    for row in rows:
        bucket = stats.setdefault(
            row.source_id,
            {"pending_count": 0, "replayed_count": 0, "latest_created_at": None},
        )
        if row.status == "pending":
            bucket["pending_count"] += 1
        if row.status == "replayed":
            bucket["replayed_count"] += 1
        if bucket["latest_created_at"] is None:
            bucket["latest_created_at"] = row.created_at
    return stats


def build_source_ops_status(
    source: SourceDefinitionORM,
    *,
    latest_run: SourceRunORM | None,
    latest_success_at: datetime | None,
    sync_tasks: list[ScheduledTaskORM],
    storage_stats: dict[str, Any],
    checkpoint: SourceCheckpointORM | None,
    dead_letter_stats: dict[str, Any],
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
    pending_dead_letters = int(dead_letter_stats.get("pending_count", 0))
    runtime_state = determine_source_runtime_state(source.source_kind, checkpoint, latest_run, pending_dead_letters)
    return {
        "source": source,
        "latest_run": latest_run,
        "checkpoint": checkpoint,
        "has_schedule": bool(enabled_tasks),
        "next_run_at": next_run_at,
        "latest_success_at": latest_success_at,
        "is_stale": is_stale,
        "is_failing": is_failing,
        "runtime_state": runtime_state,
        "fetch_mode": checkpoint.fetch_mode if checkpoint is not None else infer_fetch_mode_for_kind(source.source_kind),
        "pending_dead_letter_count": pending_dead_letters,
        "replayed_dead_letter_count": int(dead_letter_stats.get("replayed_count", 0)),
        "last_dead_letter_at": dead_letter_stats.get("latest_created_at"),
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


def infer_fetch_mode_for_kind(source_kind: str) -> str:
    if source_kind in {"sse_stream", "websocket_stream"}:
        return "stream"
    if source_kind == "webhook_ingest":
        return "push"
    return "pull"


def determine_source_runtime_state(
    source_kind: str,
    checkpoint: SourceCheckpointORM | None,
    latest_run: SourceRunORM | None,
    pending_dead_letters: int,
) -> str:
    if source_kind == "webhook_ingest":
        return "listening"
    if source_kind in {"sse_stream", "websocket_stream"}:
        if pending_dead_letters > 0:
            return "degraded"
        if checkpoint is not None and checkpoint.status == "active":
            return "active"
        if latest_run is not None and latest_run.status == "failed":
            return "degraded"
        return "idle"
    if pending_dead_letters > 0:
        return "degraded"
    return "idle"


def run_source_definition(session: Session, source_id: int, actor: str = "source_runner") -> SourceRunORM:
    source = session.get(SourceDefinitionORM, source_id)
    if source is None:
        raise ValueError(f"Source {source_id} does not exist.")
    if not source.enabled:
        raise ValueError(f"Source {source_id} is disabled.")
    adapter = get_adapter_for_source(source.source_kind)
    checkpoint = get_or_create_source_checkpoint(
        session,
        source,
        adapter_kind=adapter.source_kind,
        fetch_mode=adapter.fetch_mode,
    )
    checkpoint.status = "active" if adapter.fetch_mode == "stream" else "running"
    checkpoint.last_seen_at = source_now()
    session.flush()

    run = SourceRunORM(
        source_id=source.source_id,
        status="running",
        adapter_kind=adapter.source_kind,
        fetch_mode=adapter.fetch_mode,
        cursor_text=checkpoint.cursor_text,
        last_event_id=checkpoint.last_event_id,
        last_offset=checkpoint.last_offset,
        checkpoint_json=dict(checkpoint.checkpoint_json or {}),
    )
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
                "adapter_kind": adapter.source_kind,
                "fetch_mode": adapter.fetch_mode,
                "target_uri": sanitize_url(source.target_uri),
            },
        )
    )
    log_event(
        logger,
        logging.INFO,
        "source_run_started",
        source_id=source.source_id,
        source_run_id=run.source_run_id,
        source_kind=source.source_kind,
        adapter_kind=adapter.source_kind,
        fetch_mode=adapter.fetch_mode,
        target_uri=source.target_uri,
    )

    materialized_path: str | None = None
    materialization_metadata: dict[str, Any] | None = None
    try:
        adapter_result = run_source_adapter(
            source,
            AdapterContext(
                cursor_text=checkpoint.cursor_text,
                last_event_id=checkpoint.last_event_id,
                last_offset=checkpoint.last_offset,
                checkpoint_json=dict(checkpoint.checkpoint_json or {}),
                actor=actor,
            ),
        )
        if adapter_result.materialized_payload is not None:
            materialized_path = write_source_materialization(
                source.source_id,
                adapter_result.materialized_suffix,
                adapter_result.materialized_payload,
            )
            materialization_metadata = {
                "materialization_kind": "adapter_cache",
                "cached_path": materialized_path,
                **sanitize_for_observability(adapter_result.metadata_json),
            }
            register_source_run_storage_object(
                session,
                source,
                run,
                materialized_path,
                materialization_metadata,
                run_status="materialized",
                actor=actor,
            )
            session.add(
                CustodyLogORM(
                    object_type="source_run",
                    object_id=str(run.source_run_id),
                    action="source_payload_materialized",
                    actor=actor,
                    details_json={
                        "cached_path": materialized_path,
                        **sanitize_for_observability(adapter_result.metadata_json),
                    },
                )
            )

        run.records_seen = adapter_result.records_seen
        run.records_failed = len(adapter_result.dead_letters)
        run.cursor_text = adapter_result.cursor_text
        run.last_event_id = adapter_result.last_event_id
        run.last_offset = adapter_result.last_offset
        run.checkpoint_json = dict(adapter_result.checkpoint_json or {})

        checkpoint.cursor_text = adapter_result.cursor_text
        checkpoint.last_event_id = adapter_result.last_event_id
        checkpoint.last_offset = adapter_result.last_offset
        checkpoint.checkpoint_json = dict(adapter_result.checkpoint_json or {})
        checkpoint.last_seen_at = source_now()

        created_dead_letters = capture_dead_letters(
            session,
            source=source,
            run=run,
            candidates=adapter_result.dead_letters,
            actor=actor,
        )

        if should_skip_unchanged_source(session, source, adapter_result.metadata_json):
            finished_at = source_now()
            run.status = "skipped"
            run.records_imported = 0
            run.records_skipped = 0
            run.finished_at = finished_at
            run.output_json = {
                "source_kind": source.source_kind,
                "adapter_kind": adapter.source_kind,
                "fetch_mode": adapter.fetch_mode,
                "target_uri": sanitize_url(source.target_uri),
                "skip_reason": "payload_unchanged",
                "dead_letter_count": len(created_dead_letters),
                **sanitize_for_observability(adapter_result.metadata_json),
            }
            if materialized_path is not None:
                run.output_json["cached_path"] = materialized_path
            checkpoint.status = "idle" if adapter.fetch_mode != "stream" else "active"
            checkpoint.last_success_at = finished_at
            register_checkpoint_storage_object(session, source, checkpoint, actor=actor)
            if materialized_path is not None and materialization_metadata is not None:
                register_source_run_storage_object(
                    session,
                    source,
                    run,
                    materialized_path,
                    materialization_metadata,
                    run_status="skipped",
                    actor=actor,
                )
            session.add(
                CustodyLogORM(
                    object_type="source_run",
                    object_id=str(run.source_run_id),
                    action="source_run_skipped",
                    actor=actor,
                    details_json={
                        "source_id": source.source_id,
                        "skip_reason": "payload_unchanged",
                        "payload_sha256": adapter_result.metadata_json.get("payload_sha256"),
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
                        "payload_sha256": adapter_result.metadata_json.get("payload_sha256"),
                    },
                )
            )
            session.commit()
            session.refresh(run)
            inc_counter(
                "elevenwriter_source_runs_total",
                status="skipped",
                source_kind=source.source_kind,
            )
            log_event(
                logger,
                logging.INFO,
                "source_run_skipped",
                source_id=source.source_id,
                source_run_id=run.source_run_id,
                source_kind=source.source_kind,
                adapter_kind=adapter.source_kind,
                skip_reason="payload_unchanged",
            )
            return run

        import_run = persist_envelopes_as_import_run(
            session,
            source_path=build_source_import_path(source, adapter_result.fetch_mode),
            source_format=resolve_source_import_format(source.source_kind),
            layer_key=source.layer_key,
            notes=source.notes,
            actor=actor,
            source_type=source.source_kind,
            envelopes=adapter_result.envelopes,
        )
        finished_at = source_now()
        run.status = "completed"
        run.import_run_id = import_run.import_run_id
        run.records_imported = import_run.records_imported
        run.records_skipped = import_run.records_skipped
        run.finished_at = finished_at
        run.output_json = {
            "import_run_id": import_run.import_run_id,
            "source_kind": source.source_kind,
            "adapter_kind": adapter.source_kind,
            "fetch_mode": adapter.fetch_mode,
            "target_uri": sanitize_url(source.target_uri),
            "dead_letter_count": len(created_dead_letters),
            **sanitize_for_observability(adapter_result.metadata_json),
        }
        if materialized_path is not None:
            run.output_json["cached_path"] = materialized_path
        checkpoint.status = "idle" if adapter.fetch_mode != "stream" else "active"
        checkpoint.last_success_at = finished_at
        register_checkpoint_storage_object(session, source, checkpoint, actor=actor)
        if materialized_path is not None and materialization_metadata is not None:
            register_source_run_storage_object(
                session,
                source,
                run,
                materialized_path,
                materialization_metadata,
                import_run_id=import_run.import_run_id,
                run_status="completed",
                actor=actor,
            )
        session.add(
            CustodyLogORM(
                object_type="source_run",
                object_id=str(run.source_run_id),
                action="source_run_completed",
                actor=actor,
                details_json={
                    "source_id": source.source_id,
                    "records_seen": run.records_seen,
                    "records_imported": run.records_imported,
                    "records_skipped": run.records_skipped,
                    "records_failed": run.records_failed,
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
                    "records_seen": run.records_seen,
                    "records_imported": run.records_imported,
                    "records_skipped": run.records_skipped,
                    "records_failed": run.records_failed,
                    "import_run_id": import_run.import_run_id,
                },
            )
        )
        session.commit()
        inc_counter(
            "elevenwriter_source_runs_total",
            status="completed",
            source_kind=source.source_kind,
        )
        log_event(
            logger,
            logging.INFO,
            "source_run_completed",
            source_id=source.source_id,
            source_run_id=run.source_run_id,
            source_kind=source.source_kind,
            adapter_kind=adapter.source_kind,
            import_run_id=import_run.import_run_id,
            records_seen=run.records_seen,
            records_imported=run.records_imported,
            records_skipped=run.records_skipped,
            records_failed=run.records_failed,
        )
    except Exception as exc:
        finished_at = source_now()
        run.status = "failed"
        run.error_text = str(exc)
        run.finished_at = finished_at
        checkpoint.status = "degraded"
        checkpoint.last_failure_at = finished_at
        checkpoint.failure_count += 1
        register_checkpoint_storage_object(session, source, checkpoint, actor=actor)
        session.add(
            CustodyLogORM(
                object_type="source_run",
                object_id=str(run.source_run_id),
                action="source_run_failed",
                actor=actor,
                details_json={
                    "source_id": source.source_id,
                    "adapter_kind": adapter.source_kind,
                    "fetch_mode": adapter.fetch_mode,
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
                    "adapter_kind": adapter.source_kind,
                    "fetch_mode": adapter.fetch_mode,
                    "error_text": str(exc),
                },
            )
        )
        session.commit()
        inc_counter(
            "elevenwriter_source_runs_total",
            status="failed",
            source_kind=source.source_kind,
        )
        log_event(
            logger,
            logging.WARNING,
            "source_run_failed",
            source_id=source.source_id,
            source_run_id=run.source_run_id,
            source_kind=source.source_kind,
            adapter_kind=adapter.source_kind,
            fetch_mode=adapter.fetch_mode,
            error=str(exc),
        )
        raise

    session.refresh(run)
    return run


def ingest_webhook_payload(
    session: Session,
    source_id: int,
    *,
    payload: bytes,
    content_type: str | None,
    actor: str = "source_webhook",
) -> SourceRunORM:
    source = session.get(SourceDefinitionORM, source_id)
    if source is None:
        raise ValueError(f"Source {source_id} does not exist.")
    if source.source_kind != "webhook_ingest":
        raise ValueError(f"Source {source_id} is not configured for webhook ingestion.")
    if not source.enabled:
        raise ValueError(f"Source {source_id} is disabled.")

    checkpoint = get_or_create_source_checkpoint(
        session,
        source,
        adapter_kind="webhook_ingest",
        fetch_mode="push",
    )
    checkpoint.status = "active"
    checkpoint.last_seen_at = source_now()
    run = SourceRunORM(
        source_id=source.source_id,
        status="running",
        adapter_kind="webhook_ingest",
        fetch_mode="push",
        cursor_text=checkpoint.cursor_text,
        last_event_id=checkpoint.last_event_id,
        last_offset=checkpoint.last_offset,
        checkpoint_json=dict(checkpoint.checkpoint_json or {}),
    )
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
                "adapter_kind": "webhook_ingest",
                "fetch_mode": "push",
                "target_uri": sanitize_url(source.target_uri),
            },
        )
    )
    log_event(
        logger,
        logging.INFO,
        "source_webhook_ingest_started",
        source_id=source.source_id,
        source_run_id=run.source_run_id,
        source_kind=source.source_kind,
        adapter_kind="webhook_ingest",
        fetch_mode="push",
        target_uri=source.target_uri,
    )

    try:
        adapter_result = ingest_webhook_source(
            source,
            payload=payload,
            content_type=content_type,
            context=AdapterContext(
                cursor_text=checkpoint.cursor_text,
                last_event_id=checkpoint.last_event_id,
                last_offset=checkpoint.last_offset,
                checkpoint_json=dict(checkpoint.checkpoint_json or {}),
                actor=actor,
            ),
        )
        run.records_seen = adapter_result.records_seen
        run.records_failed = len(adapter_result.dead_letters)
        run.cursor_text = adapter_result.cursor_text
        run.last_event_id = adapter_result.last_event_id
        run.last_offset = adapter_result.last_offset
        run.checkpoint_json = dict(adapter_result.checkpoint_json or {})
        checkpoint.cursor_text = adapter_result.cursor_text
        checkpoint.last_event_id = adapter_result.last_event_id
        checkpoint.last_offset = adapter_result.last_offset
        checkpoint.checkpoint_json = dict(adapter_result.checkpoint_json or {})
        checkpoint.last_seen_at = source_now()
        created_dead_letters = capture_dead_letters(
            session,
            source=source,
            run=run,
            candidates=adapter_result.dead_letters,
            actor=actor,
        )
        import_run = persist_envelopes_as_import_run(
            session,
            source_path=build_source_import_path(source, "push"),
            source_format="webhook",
            layer_key=source.layer_key,
            notes=source.notes,
            actor=actor,
            source_type=source.source_kind,
            envelopes=adapter_result.envelopes,
        )
        run.status = "completed"
        run.import_run_id = import_run.import_run_id
        run.records_imported = import_run.records_imported
        run.records_skipped = import_run.records_skipped
        run.finished_at = source_now()
        run.output_json = {
            "import_run_id": import_run.import_run_id,
            "adapter_kind": "webhook_ingest",
            "fetch_mode": "push",
            "dead_letter_count": len(created_dead_letters),
            **sanitize_for_observability(adapter_result.metadata_json),
        }
        checkpoint.status = "idle"
        checkpoint.last_success_at = run.finished_at
        register_checkpoint_storage_object(session, source, checkpoint, actor=actor)
        if adapter_result.materialized_payload is not None:
            materialized_path = write_source_materialization(
                source.source_id,
                adapter_result.materialized_suffix,
                adapter_result.materialized_payload,
            )
            register_source_run_storage_object(
                session,
                source,
                run,
                materialized_path,
                {
                    "materialization_kind": "webhook_cache",
                    "cached_path": materialized_path,
                    **sanitize_for_observability(adapter_result.metadata_json),
                },
                import_run_id=import_run.import_run_id,
                run_status="completed",
                actor=actor,
            )
        session.add(
            CustodyLogORM(
                object_type="source_run",
                object_id=str(run.source_run_id),
                action="source_run_completed",
                actor=actor,
                details_json={
                    "source_id": source.source_id,
                    "records_seen": run.records_seen,
                    "records_imported": run.records_imported,
                    "records_skipped": run.records_skipped,
                    "records_failed": run.records_failed,
                    "import_run_id": import_run.import_run_id,
                },
            )
        )
        session.commit()
        session.refresh(run)
        inc_counter(
            "elevenwriter_source_runs_total",
            status="completed",
            source_kind=source.source_kind,
        )
        log_event(
            logger,
            logging.INFO,
            "source_webhook_ingest_completed",
            source_id=source.source_id,
            source_run_id=run.source_run_id,
            source_kind=source.source_kind,
            adapter_kind="webhook_ingest",
            import_run_id=import_run.import_run_id,
            records_seen=run.records_seen,
            records_imported=run.records_imported,
            records_skipped=run.records_skipped,
            records_failed=run.records_failed,
        )
        return run
    except Exception as exc:
        run.status = "failed"
        run.error_text = str(exc)
        run.finished_at = source_now()
        checkpoint.status = "degraded"
        checkpoint.last_failure_at = run.finished_at
        checkpoint.failure_count += 1
        register_checkpoint_storage_object(session, source, checkpoint, actor=actor)
        session.add(
            CustodyLogORM(
                object_type="source_run",
                object_id=str(run.source_run_id),
                action="source_run_failed",
                actor=actor,
                details_json={
                    "source_id": source.source_id,
                    "adapter_kind": "webhook_ingest",
                    "fetch_mode": "push",
                    "error_text": str(exc),
                },
            )
        )
        session.commit()
        inc_counter(
            "elevenwriter_source_runs_total",
            status="failed",
            source_kind=source.source_kind,
        )
        log_event(
            logger,
            logging.WARNING,
            "source_webhook_ingest_failed",
            source_id=source.source_id,
            source_run_id=run.source_run_id,
            source_kind=source.source_kind,
            adapter_kind="webhook_ingest",
            fetch_mode="push",
            error=str(exc),
        )
        raise


def replay_dead_letter_record(
    session: Session,
    source_dead_letter_id: int,
    *,
    actor: str = "source_replay",
) -> SourceDeadLetterORM:
    record = session.get(SourceDeadLetterORM, source_dead_letter_id)
    if record is None:
        raise ValueError(f"Source dead-letter {source_dead_letter_id} does not exist.")
    source = session.get(SourceDefinitionORM, record.source_id)
    if source is None:
        raise ValueError(f"Source {record.source_id} does not exist.")
    candidate = dead_letter_candidate_from_row(record)
    try:
        result = replay_source_dead_letter(source, candidate)
        import_run = persist_envelopes_as_import_run(
            session,
            source_path=f"dead-letter://source/{source.source_id}/{record.source_dead_letter_id}",
            source_format="dead_letter_replay",
            layer_key=source.layer_key,
            notes=f"Replay dead-letter {record.source_dead_letter_id}",
            actor=actor,
            source_type=source.source_kind,
            envelopes=result.envelopes,
        )
        record.status = "replayed"
        record.replay_count += 1
        record.last_replayed_at = source_now()
        record.last_error_text = None
        register_dead_letter_storage_object(session, source, record, actor=actor)
        session.add(
            CustodyLogORM(
                object_type="source_dead_letter",
                object_id=str(record.source_dead_letter_id),
                action="source_dead_letter_replayed",
                actor=actor,
                details_json={
                    "source_id": source.source_id,
                    "import_run_id": import_run.import_run_id,
                    "replay_count": record.replay_count,
                },
            )
        )
        session.commit()
        log_event(
            logger,
            logging.INFO,
            "source_dead_letter_replayed",
            source_id=source.source_id,
            source_dead_letter_id=record.source_dead_letter_id,
            replay_count=record.replay_count,
            import_run_id=import_run.import_run_id,
        )
    except Exception as exc:
        record.last_error_text = str(exc)
        session.commit()
        log_event(
            logger,
            logging.WARNING,
            "source_dead_letter_replay_failed",
            source_id=source.source_id,
            source_dead_letter_id=record.source_dead_letter_id,
            error=str(exc),
        )
        raise
    session.refresh(record)
    return record


def get_or_create_source_checkpoint(
    session: Session,
    source: SourceDefinitionORM,
    *,
    adapter_kind: str,
    fetch_mode: str,
) -> SourceCheckpointORM:
    checkpoint = session.scalar(
        select(SourceCheckpointORM).where(SourceCheckpointORM.source_id == source.source_id).limit(1)
    )
    if checkpoint is None:
        checkpoint = SourceCheckpointORM(
            source_id=source.source_id,
            adapter_kind=adapter_kind,
            fetch_mode=fetch_mode,
            status="idle",
            checkpoint_json={},
        )
        session.add(checkpoint)
        session.flush()
    else:
        checkpoint.adapter_kind = adapter_kind
        checkpoint.fetch_mode = fetch_mode
    return checkpoint


def capture_dead_letters(
    session: Session,
    *,
    source: SourceDefinitionORM,
    run: SourceRunORM,
    candidates: list[DeadLetterCandidate],
    actor: str,
) -> list[SourceDeadLetterORM]:
    created: list[SourceDeadLetterORM] = []
    for candidate in candidates:
        record = SourceDeadLetterORM(
            source_id=source.source_id,
            source_run_id=run.source_run_id,
            adapter_kind=run.adapter_kind,
            source_kind=source.source_kind,
            stage=candidate.stage,
            status="pending",
            failure_reason=candidate.failure_reason,
            record_key=candidate.record_key,
            record_hash=candidate.record_hash
            or (
                hashlib.sha256(candidate.raw_payload_text.encode("utf-8")).hexdigest()
                if candidate.raw_payload_text
                else None
            ),
            raw_payload_text=candidate.raw_payload_text,
            payload_json=dict(candidate.payload_json or {}),
            cursor_text=candidate.cursor_text,
            checkpoint_json=dict(candidate.checkpoint_json or {}),
        )
        session.add(record)
        session.flush()
        register_dead_letter_storage_object(session, source, record, actor=actor)
        session.add(
            CustodyLogORM(
                object_type="source_dead_letter",
                object_id=str(record.source_dead_letter_id),
                action="source_dead_letter_captured",
                actor=actor,
                details_json={
                    "source_id": source.source_id,
                    "source_run_id": run.source_run_id,
                    "stage": record.stage,
                    "record_key": record.record_key,
                },
            )
        )
        log_event(
            logger,
            logging.WARNING,
            "source_dead_letter_captured",
            source_id=source.source_id,
            source_run_id=run.source_run_id,
            source_dead_letter_id=record.source_dead_letter_id,
            adapter_kind=run.adapter_kind,
            stage=record.stage,
            record_key=record.record_key,
            failure_reason=record.failure_reason,
        )
        created.append(record)
    return created


def dead_letter_candidate_from_row(row: SourceDeadLetterORM) -> DeadLetterCandidate:
    return DeadLetterCandidate(
        stage=row.stage,
        failure_reason=row.failure_reason,
        raw_payload_text=row.raw_payload_text,
        payload_json=dict(row.payload_json or {}),
        record_key=row.record_key,
        record_hash=row.record_hash,
        cursor_text=row.cursor_text,
        checkpoint_json=dict(row.checkpoint_json or {}),
    )


def register_checkpoint_storage_object(
    session: Session,
    source: SourceDefinitionORM,
    checkpoint: SourceCheckpointORM,
    *,
    actor: str,
) -> StorageObjectORM:
    return register_storage_object(
        session,
        object_key=f"source_checkpoint:{source.source_id}",
        object_kind="source_checkpoint_state",
        owner_type="source_definition",
        owner_id=str(source.source_id),
        object_uri=f"checkpoint://source/{source.source_id}",
        content_hash=hashlib.sha256(
            json.dumps(
                {
                    "cursor_text": checkpoint.cursor_text,
                    "last_event_id": checkpoint.last_event_id,
                    "last_offset": checkpoint.last_offset,
                    "checkpoint_json": checkpoint.checkpoint_json,
                },
                sort_keys=True,
                default=str,
            ).encode("utf-8")
        ).hexdigest(),
        media_type="application/json",
        storage_tier="warm",
        retention_class="operational",
        lifecycle_status="active",
        source_uri=source.target_uri,
        observed_at=checkpoint.last_seen_at or checkpoint.updated_at,
        metadata_json={
            "adapter_kind": checkpoint.adapter_kind,
            "fetch_mode": checkpoint.fetch_mode,
            "status": checkpoint.status,
            "cursor_text": checkpoint.cursor_text,
            "last_event_id": checkpoint.last_event_id,
            "last_offset": checkpoint.last_offset,
            "failure_count": checkpoint.failure_count,
            "checkpoint_json": sanitize_for_observability(checkpoint.checkpoint_json),
        },
        actor=actor,
    )


def register_dead_letter_storage_object(
    session: Session,
    source: SourceDefinitionORM,
    record: SourceDeadLetterORM,
    *,
    actor: str,
) -> StorageObjectORM:
    return register_storage_object(
        session,
        object_key=f"source_dead_letter:{record.source_dead_letter_id}",
        object_kind="source_dead_letter_payload",
        owner_type="source_dead_letter",
        owner_id=str(record.source_dead_letter_id),
        object_uri=f"dead-letter://source/{source.source_id}/{record.source_dead_letter_id}",
        content_hash=record.record_hash,
        media_type="application/json" if record.payload_json else "text/plain",
        storage_tier="warm",
        retention_class="investigative",
        lifecycle_status="active",
        source_uri=source.target_uri,
        observed_at=record.created_at,
        metadata_json={
            "adapter_kind": record.adapter_kind,
            "source_kind": record.source_kind,
            "stage": record.stage,
            "status": record.status,
            "record_key": record.record_key,
            "failure_reason": record.failure_reason,
        },
        actor=actor,
    )


def write_source_materialization(source_id: int, suffix: str, payload: bytes) -> str:
    settings = get_settings()
    cache_dir = settings.data_dir / "source_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    target = cache_dir / f"source-{source_id}{suffix}"
    target.write_bytes(payload)
    return str(target)


def build_source_import_path(source: SourceDefinitionORM, fetch_mode: str) -> str:
    if fetch_mode == "push":
        return f"webhook://source/{source.source_id}"
    return source.target_uri


def resolve_source_import_format(source_kind: str) -> str:
    return {
        "local_file": "local_file",
        "sqlite_file": "sqlite",
        "http_json": "json",
        "http_jsonl": "jsonl",
        "http_text": "txt",
        "http_xml": "xml",
        "rss": "rss",
        "webhook_ingest": "webhook",
        "sse_stream": "sse",
        "websocket_stream": "websocket",
    }.get(source_kind, source_kind)


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
                **sanitize_for_observability(fetch_metadata),
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
    if source.source_kind in {"webhook_ingest", "sse_stream", "websocket_stream"}:
        return bool(metadata.get("skip_unchanged", False))
    return bool(metadata.get("skip_unchanged", True))


def fetch_http_source(
    source: SourceDefinitionORM,
    fetch_config: SourceFetchConfig,
) -> tuple[bytes, dict[str, Any]]:
    last_error: Exception | None = None
    for attempt in range(1, fetch_config.retry_attempts + 1):
        request = Request(source.target_uri, headers=fetch_config.headers)
        log_event(
            logger,
            logging.INFO,
            "source_fetch_attempt_started",
            source_id=source.source_id,
            source_kind=source.source_kind,
            target_uri=source.target_uri,
            attempt=attempt,
            timeout_seconds=fetch_config.timeout_seconds,
            headers=fetch_config.headers,
        )
        try:
            with urlopen(request, timeout=fetch_config.timeout_seconds) as response:
                payload = response.read()
                content_type = response.headers.get("Content-Type")
                status_code = getattr(response, "status", None) or getattr(response, "code", None) or 200
                log_event(
                    logger,
                    logging.INFO,
                    "source_fetch_completed",
                    source_id=source.source_id,
                    source_kind=source.source_kind,
                    target_uri=source.target_uri,
                    attempt=attempt,
                    status_code=int(status_code),
                    byte_count=len(payload),
                    content_type=content_type,
                )
                return payload, {
                    "attempt_count": attempt,
                    "http_status": int(status_code),
                    "content_type": content_type,
                    "byte_count": len(payload),
                    "payload_sha256": hashlib.sha256(payload).hexdigest(),
                    "request_timeout_seconds": fetch_config.timeout_seconds,
                    "retry_attempts": fetch_config.retry_attempts,
                    "headers": sanitize_for_observability(fetch_config.headers),
                    "host": urlparse(source.target_uri).netloc,
                }
        except HTTPError as exc:
            last_error = exc
            inc_counter(
                "elevenwriter_source_fetch_failures_total",
                source_kind=source.source_kind,
                error_type=f"http_{exc.code}",
            )
            log_event(
                logger,
                logging.WARNING,
                "source_fetch_attempt_failed",
                source_id=source.source_id,
                source_kind=source.source_kind,
                target_uri=source.target_uri,
                attempt=attempt,
                status_code=exc.code,
                error=str(exc),
            )
            if not should_retry_http_error(exc.code) or attempt >= fetch_config.retry_attempts:
                break
            apply_retry_backoff(fetch_config, attempt)
        except URLError as exc:
            last_error = exc
            inc_counter(
                "elevenwriter_source_fetch_failures_total",
                source_kind=source.source_kind,
                error_type="url_error",
            )
            log_event(
                logger,
                logging.WARNING,
                "source_fetch_attempt_failed",
                source_id=source.source_id,
                source_kind=source.source_kind,
                target_uri=source.target_uri,
                attempt=attempt,
                error=str(exc),
            )
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


def validate_source_kind(source_kind: str) -> None:
    if source_kind not in list_supported_source_kinds():
        raise ValueError(
            "Unsupported source kind. Supported kinds: " + ", ".join(list_supported_source_kinds())
        )


def ensure_target_uri_has_no_embedded_credentials(target_uri: str) -> None:
    parsed = urlparse(target_uri)
    if parsed.username or parsed.password:
        raise ValueError("Source target_uri must not embed credentials; use env-backed auth settings instead.")


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
