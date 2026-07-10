from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.config import get_settings
from src.models import WorkerStatusORM


def evaluate_worker_health(
    session: Session,
    *,
    worker_type: str | None = None,
    actor: str | None = None,
    worker_key: str | None = None,
    stale_after_seconds: float | None = None,
    require_active: bool = False,
) -> dict[str, object]:
    generated_at = worker_status_now()
    threshold_seconds = stale_after_seconds if stale_after_seconds is not None else default_worker_stale_after_seconds()
    stale_before = generated_at - timedelta(seconds=max(1.0, threshold_seconds))
    workers = list_worker_statuses(session)
    matching_workers = [
        worker
        for worker in workers
        if (worker_type is None or worker.worker_type == worker_type)
        and (actor is None or worker.actor == actor)
        and (worker_key is None or worker.worker_key == worker_key)
    ]
    healthy_workers = [
        worker
        for worker in matching_workers
        if worker_is_recently_available(
            worker,
            stale_before=stale_before,
            require_active=require_active,
        )
    ]
    return {
        "generated_at": generated_at,
        "stale_before": stale_before,
        "stale_after_seconds": max(1.0, threshold_seconds),
        "worker_type": worker_type,
        "actor": actor,
        "worker_key": worker_key,
        "require_active": require_active,
        "matching_count": len(matching_workers),
        "healthy_count": len(healthy_workers),
        "healthy": len(healthy_workers) > 0,
        "matching_workers": matching_workers,
        "healthy_workers": healthy_workers,
    }


def worker_status_now() -> datetime:
    return datetime.now(timezone.utc)


def default_worker_stale_after_seconds() -> float:
    return max(30.0, get_settings().scheduler_poll_seconds * 3.0)


def list_worker_statuses(session: Session) -> list[WorkerStatusORM]:
    statement = select(WorkerStatusORM).order_by(WorkerStatusORM.worker_type.asc(), WorkerStatusORM.worker_key.asc())
    return list(session.scalars(statement))


def build_worker_status_summary(
    session: Session,
    *,
    stale_after_seconds: float | None = None,
) -> dict[str, object]:
    generated_at = worker_status_now()
    threshold_seconds = stale_after_seconds if stale_after_seconds is not None else default_worker_stale_after_seconds()
    stale_before = generated_at - timedelta(seconds=max(1.0, threshold_seconds))
    workers = list_worker_statuses(session)
    active_count = 0
    stale_count = 0
    status_counts: dict[str, dict[str, object]] = {}
    worker_type_counts: dict[str, dict[str, object]] = {}
    for worker in workers:
        is_active = worker.status == "active"
        normalized_last_seen_at = normalize_worker_timestamp(worker.last_seen_at)
        is_stale = bool(normalized_last_seen_at is None or normalized_last_seen_at < stale_before)
        if is_active:
            active_count += 1
        if is_stale:
            stale_count += 1
        update_worker_bucket(status_counts, worker.status or "unknown", worker, is_active=is_active, is_stale=is_stale)
        update_worker_bucket(worker_type_counts, worker.worker_type or "unknown", worker, is_active=is_active, is_stale=is_stale)
    return {
        "generated_at": generated_at,
        "stale_before": stale_before,
        "total_count": len(workers),
        "active_count": active_count,
        "stale_count": stale_count,
        "status_counts": sort_worker_buckets(status_counts),
        "worker_type_counts": sort_worker_buckets(worker_type_counts),
        "workers": workers,
    }


def mark_worker_started(
    session: Session,
    *,
    worker_key: str,
    worker_type: str,
    actor: str,
    metadata_json: dict[str, Any] | None = None,
    process_token: str | None = None,
) -> WorkerStatusORM:
    token = process_token or uuid.uuid4().hex
    now = worker_status_now()
    record = get_or_create_worker_status(session, worker_key, worker_type=worker_type, actor=actor)
    record.worker_type = worker_type
    record.actor = actor
    record.status = "active"
    record.process_token = token
    record.last_started_at = now
    record.last_seen_at = now
    record.last_iteration_at = None
    if metadata_json is not None:
        record.metadata_json = dict(metadata_json)
    session.commit()
    session.refresh(record)
    return record


def publish_worker_heartbeat(
    session: Session,
    *,
    worker_key: str,
    worker_type: str,
    actor: str,
    process_token: str,
    metadata_json: dict[str, Any] | None = None,
) -> WorkerStatusORM:
    now = worker_status_now()
    record = get_or_create_worker_status(session, worker_key, worker_type=worker_type, actor=actor)
    record.worker_type = worker_type
    record.actor = actor
    record.status = "active"
    record.process_token = process_token
    record.last_seen_at = now
    record.last_iteration_at = now
    if metadata_json is not None:
        record.metadata_json = dict(metadata_json)
    session.commit()
    session.refresh(record)
    return record


def mark_worker_stopped(
    session: Session,
    *,
    worker_key: str,
    worker_type: str,
    actor: str,
    process_token: str,
    metadata_json: dict[str, Any] | None = None,
) -> WorkerStatusORM:
    now = worker_status_now()
    record = get_or_create_worker_status(session, worker_key, worker_type=worker_type, actor=actor)
    record.worker_type = worker_type
    record.actor = actor
    record.status = "idle"
    record.process_token = process_token
    record.last_seen_at = now
    record.last_stopped_at = now
    if metadata_json is not None:
        record.metadata_json = dict(metadata_json)
    session.commit()
    session.refresh(record)
    return record


def mark_worker_failed(
    session: Session,
    *,
    worker_key: str,
    worker_type: str,
    actor: str,
    process_token: str,
    error_text: str,
    metadata_json: dict[str, Any] | None = None,
) -> WorkerStatusORM:
    now = worker_status_now()
    record = get_or_create_worker_status(session, worker_key, worker_type=worker_type, actor=actor)
    record.worker_type = worker_type
    record.actor = actor
    record.status = "failed"
    record.process_token = process_token
    record.last_seen_at = now
    record.last_stopped_at = now
    record.failure_count += 1
    updated_metadata = dict(metadata_json or {})
    updated_metadata["error_text"] = error_text
    record.metadata_json = updated_metadata
    session.commit()
    session.refresh(record)
    return record


def get_or_create_worker_status(
    session: Session,
    worker_key: str,
    *,
    worker_type: str,
    actor: str,
) -> WorkerStatusORM:
    record = session.scalar(select(WorkerStatusORM).where(WorkerStatusORM.worker_key == worker_key).limit(1))
    if record is None:
        record = WorkerStatusORM(
            worker_key=worker_key,
            worker_type=worker_type,
            actor=actor,
            status="idle",
            metadata_json={},
        )
        session.add(record)
        session.flush()
    return record


def update_worker_bucket(
    buckets: dict[str, dict[str, object]],
    key: str,
    worker: WorkerStatusORM,
    *,
    is_active: bool,
    is_stale: bool,
) -> None:
    bucket = buckets.setdefault(
        key,
        {
            "key": key,
            "total_count": 0,
            "active_count": 0,
            "stale_count": 0,
        },
    )
    bucket["total_count"] += 1
    if is_active:
        bucket["active_count"] += 1
    if is_stale:
        bucket["stale_count"] += 1


def sort_worker_buckets(buckets: dict[str, dict[str, object]]) -> list[dict[str, object]]:
    return sorted(
        buckets.values(),
        key=lambda item: (-int(item["total_count"]), str(item["key"]).lower()),
    )


def normalize_worker_timestamp(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def worker_is_recently_available(
    worker: WorkerStatusORM,
    *,
    stale_before: datetime,
    require_active: bool = False,
) -> bool:
    last_seen_at = normalize_worker_timestamp(worker.last_seen_at)
    if last_seen_at is None or last_seen_at < stale_before:
        return False
    if require_active:
        return worker.status == "active"
    return worker.status in {"active", "idle"}
