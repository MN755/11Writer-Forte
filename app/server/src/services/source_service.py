from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import math
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse, urlsplit, urlunsplit
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
from src.services.discovery_fetch import FetchPolicy, fetch_url, read_bounded


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


RAW_HTTP_SOURCE_KINDS = {"http_json", "http_jsonl", "http_text"}
STRUCTURED_HTTP_SOURCE_KINDS = {
    "http_xml",
    "http_csv",
    "rss",
    "arcgis_feature_json",
    "ckan_package_search",
}
PAGINATED_STRUCTURED_HTTP_SOURCE_KINDS = {"arcgis_feature_json", "ckan_package_search"}
DISCOVERY_HTTP_SOURCE_KINDS = {"web_search", "web_crawl", "web_discovery"}
HTTP_SOURCE_KINDS = RAW_HTTP_SOURCE_KINDS | STRUCTURED_HTTP_SOURCE_KINDS | DISCOVERY_HTTP_SOURCE_KINDS


@dataclass(frozen=True)
class SourcePaginationConfig:
    enabled: bool
    initial_offset: int
    page_size: int
    max_pages: int


@dataclass(frozen=True)
class ParsedSourcePage:
    records: list[dict[str, Any]]
    item_count: int
    total_available: int | None = None
    has_more: bool | None = None


def source_now() -> datetime:
    return datetime.now(timezone.utc)


def create_source_definition(
    session: Session,
    payload: SourceDefinitionCreate,
    *,
    actor: str = "system",
    commit: bool = True,
) -> SourceDefinitionORM:
    ensure_unique_source_name(session, payload.name)
    ensure_data_layer(session, payload.layer_key, actor=actor)
    record = SourceDefinitionORM(**payload.model_dump())
    session.add(record)
    session.flush()
    session.add(
        CustodyLogORM(
            object_type="source_definition",
            object_id=str(record.source_id),
            action="source_created",
            actor=actor,
            details_json={
                **payload.model_dump(),
                "source_id": record.source_id,
            },
        )
    )
    if commit:
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

    if source.source_kind in HTTP_SOURCE_KINDS:
        fetch_config = parse_fetch_config(source)
        if source.source_kind in PAGINATED_STRUCTURED_HTTP_SOURCE_KINDS:
            pagination = parse_source_pagination_config(source)
            if pagination.enabled:
                return materialize_paginated_http_source_payload(
                    source,
                    fetch_config,
                    pagination,
                )
        payload, fetch_metadata = fetch_http_source(source, fetch_config)
        return materialize_http_source_payload(source, payload, fetch_metadata)

    if source.source_kind in {
        "websocket_stream",
        "sse_stream",
        "webhook_ingest",
        "camera_image",
        "camera_stream",
    }:
        raise ValueError(
            f"Source kind {source.source_kind} is reference-only and cannot be run by the batch source runner."
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
        "Accept": resolve_source_accept_header(source.source_kind),
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


def parse_source_pagination_config(source: SourceDefinitionORM) -> SourcePaginationConfig:
    metadata = source.metadata_json if isinstance(source.metadata_json, dict) else {}
    pagination_meta = metadata.get("pagination")
    pagination_dict = pagination_meta if isinstance(pagination_meta, dict) else {}
    query = parse_qs(urlsplit(source.target_uri).query, keep_blank_values=True)

    if source.source_kind == "ckan_package_search":
        default_page_size = 100
        offset_key = "start"
        size_key = "rows"
    elif source.source_kind == "arcgis_feature_json":
        default_page_size = 1000
        offset_key = "resultOffset"
        size_key = "resultRecordCount"
    else:
        return SourcePaginationConfig(enabled=False, initial_offset=0, page_size=0, max_pages=0)

    enabled = bool(pagination_dict.get("enabled", True))
    initial_offset = first_int_value(
        pagination_dict.get("initial_offset"),
        metadata.get("pagination_initial_offset"),
        first_query_value(query, offset_key),
        default=0,
    )
    page_size = first_int_value(
        pagination_dict.get("page_size"),
        metadata.get("pagination_page_size"),
        first_query_value(query, size_key),
        default=default_page_size,
    )
    max_pages = first_int_value(
        pagination_dict.get("max_pages"),
        metadata.get("pagination_max_pages"),
        default=10,
    )
    return SourcePaginationConfig(
        enabled=enabled,
        initial_offset=max(0, initial_offset),
        page_size=max(1, min(page_size, 5000)),
        max_pages=max(1, min(max_pages, 100)),
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
    *,
    target_uri: str | None = None,
) -> tuple[bytes, dict[str, Any]]:
    metadata = source.metadata_json if isinstance(source.metadata_json, dict) else {}
    request_uri = target_uri or source.target_uri
    max_response_bytes = max(1024, int(metadata.get("max_response_bytes", 20 * 1024 * 1024)))
    discovery_managed = isinstance(metadata.get("discovery"), dict)
    block_private_networks = bool(metadata.get("block_private_networks", False)) or discovery_managed
    allow_private_networks = bool(metadata.get("allow_private_networks", False)) and bool(
        get_settings().discovery_allow_private_networks
    )
    if block_private_networks:
        result = fetch_url(
            request_uri,
            policy=FetchPolicy(
                timeout_seconds=fetch_config.timeout_seconds,
                retry_attempts=fetch_config.retry_attempts,
                retry_backoff_seconds=fetch_config.retry_backoff_seconds,
                max_response_bytes=max_response_bytes,
                allow_private_networks=allow_private_networks,
                user_agent=fetch_config.headers.get("User-Agent", "11Writer-Forte/0.1"),
            ),
            request_headers={
                key: value
                for key, value in fetch_config.headers.items()
                if key.lower() != "user-agent"
            },
        )
        return result.payload, {
            "attempt_count": result.attempt_count,
            "http_status": result.status_code,
            "content_type": result.headers.get("content-type"),
            "byte_count": len(result.payload),
            "payload_sha256": hashlib.sha256(result.payload).hexdigest(),
            "request_timeout_seconds": fetch_config.timeout_seconds,
            "retry_attempts": fetch_config.retry_attempts,
            "request_headers": redact_request_headers(fetch_config.headers),
            "host": urlparse(result.final_url).netloc,
            "final_url": result.final_url,
            "max_response_bytes": max_response_bytes,
            "private_network_blocking": not allow_private_networks,
            "private_network_override_enabled": allow_private_networks,
        }

    last_error: Exception | None = None
    for attempt in range(1, fetch_config.retry_attempts + 1):
        request = Request(request_uri, headers=fetch_config.headers)
        try:
            with urlopen(request, timeout=fetch_config.timeout_seconds) as response:
                payload = read_bounded(response, max_response_bytes)
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
                    "headers": redact_request_headers(fetch_config.headers),
                    "host": urlparse(request_uri).netloc,
                    "max_response_bytes": max_response_bytes,
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


def resolve_source_accept_header(source_kind: str) -> str:
    if source_kind in {"http_json", "arcgis_feature_json", "ckan_package_search"}:
        return "application/json"
    if source_kind == "http_jsonl":
        return "application/x-ndjson, application/jsonl, application/json, text/plain, */*"
    if source_kind in {"http_xml", "rss"}:
        return "application/rss+xml, application/atom+xml, application/xml, text/xml, */*"
    if source_kind in DISCOVERY_HTTP_SOURCE_KINDS:
        return "text/html, application/xhtml+xml, */*"
    if source_kind == "http_csv":
        return "text/csv, application/csv, text/plain, */*"
    return "text/plain, */*"


def materialize_http_source_payload(
    source: SourceDefinitionORM,
    payload: bytes,
    fetch_metadata: dict[str, Any],
) -> MaterializedSourcePayload:
    if source.source_kind in DISCOVERY_HTTP_SOURCE_KINDS:
        return materialize_discovery_http_source_payload(source, payload, fetch_metadata)

    if source.source_kind in RAW_HTTP_SOURCE_KINDS:
        suffix = resolve_raw_http_source_suffix(source)
        destination = build_cached_path(source.source_id, suffix)
        destination.write_bytes(payload)
        return MaterializedSourcePayload(
            path=str(destination),
            metadata={
                "materialization_kind": "http_fetch",
                "cached_path": str(destination),
                **fetch_metadata,
            },
        )

    records = parse_structured_http_source_payload(source, payload)
    destination = build_cached_path(source.source_id, ".json")
    destination.write_text(json.dumps(records), encoding="utf-8")
    return MaterializedSourcePayload(
        path=str(destination),
        metadata={
            "materialization_kind": "http_fetch",
            "cached_path": str(destination),
            "cached_record_count": len(records),
            "materialized_content_type": "application/json",
            "original_content_type": fetch_metadata.get("content_type"),
            **fetch_metadata,
        },
    )


def materialize_paginated_http_source_payload(
    source: SourceDefinitionORM,
    fetch_config: SourceFetchConfig,
    pagination: SourcePaginationConfig,
) -> MaterializedSourcePayload:
    records: list[dict[str, Any]] = []
    seen_signatures: set[str] = set()
    page_count = 0
    offset = pagination.initial_offset
    total_items = 0
    total_attempt_count = 0
    total_byte_count = 0
    total_available: int | None = None
    page_item_counts: list[int] = []
    page_record_counts: list[int] = []
    payload_hasher = hashlib.sha256()
    base_fetch_metadata: dict[str, Any] | None = None
    more_pages_available = False

    while page_count < pagination.max_pages:
        page_url = build_paginated_source_request_url(
            source.target_uri,
            source.source_kind,
            offset=offset,
            page_size=pagination.page_size,
        )
        payload, fetch_metadata = fetch_http_source(
            source,
            fetch_config,
            target_uri=page_url,
        )
        base_fetch_metadata = base_fetch_metadata or strip_payload_hash(fetch_metadata)
        page = parse_paginated_source_page(
            source,
            payload,
            page_url,
            page_size=pagination.page_size,
        )
        unique_record_count = 0
        for record in page.records:
            signature = stable_record_signature(record)
            if signature in seen_signatures:
                continue
            seen_signatures.add(signature)
            records.append(record)
            unique_record_count += 1

        page_count += 1
        total_items += page.item_count
        total_attempt_count += int(fetch_metadata.get("attempt_count") or 0)
        total_byte_count += len(payload)
        page_item_counts.append(page.item_count)
        page_record_counts.append(unique_record_count)
        total_available = page.total_available if page.total_available is not None else total_available
        payload_hasher.update(page_url.encode("utf-8"))
        payload_hasher.update(b"\0")
        payload_hasher.update(payload)
        payload_hasher.update(b"\0")

        offset += page.item_count
        more_pages_available = True
        if page.item_count <= 0:
            more_pages_available = False
        elif total_available is not None and offset >= total_available:
            more_pages_available = False
        elif page.has_more is False:
            more_pages_available = False
        elif (
            page.has_more is None
            and total_available is None
            and page.item_count < pagination.page_size
        ):
            more_pages_available = False
        if not more_pages_available:
            break

    pagination_truncated = more_pages_available and page_count >= pagination.max_pages

    destination = build_cached_path(source.source_id, ".json")
    destination.write_text(json.dumps(records), encoding="utf-8")
    metadata = {
        "materialization_kind": "http_fetch",
        "cached_path": str(destination),
        "cached_record_count": len(records),
        "materialized_content_type": "application/json",
        "original_content_type": (
            base_fetch_metadata.get("content_type") if isinstance(base_fetch_metadata, dict) else None
        ),
        **(base_fetch_metadata or {}),
        "attempt_count": total_attempt_count,
        "byte_count": total_byte_count,
        "payload_sha256": payload_hasher.hexdigest(),
        "page_count": page_count,
        "page_item_counts": page_item_counts,
        "page_record_counts": page_record_counts,
        "pagination": {
            "enabled": True,
            "initial_offset": pagination.initial_offset,
            "page_size": pagination.page_size,
            "max_pages": pagination.max_pages,
            "truncated": pagination_truncated,
            "total_available": total_available,
        },
    }
    return MaterializedSourcePayload(path=str(destination), metadata=metadata)


def materialize_discovery_http_source_payload(
    source: SourceDefinitionORM,
    payload: bytes,
    fetch_metadata: dict[str, Any],
) -> MaterializedSourcePayload:
    from src.services.discovery_analysis import analyze_document

    analysis = analyze_document(
        source.target_uri,
        payload,
        content_type=fetch_metadata.get("content_type"),
        headers={},
    )
    destination = build_cached_path(source.source_id, ".json")
    destination.write_text(
        json.dumps(
            [
                {
                    "url": source.target_uri,
                    "title": analysis.title,
                    "text": analysis.text_excerpt,
                    "candidate_type": analysis.candidate_type,
                    "format_hint": analysis.format_hint,
                    "geo_hints": analysis.geo_hints,
                    "temporal_hints": analysis.temporal_hints,
                    "links": [link.url for link in analysis.links],
                }
            ]
        ),
        encoding="utf-8",
    )
    return MaterializedSourcePayload(
        path=str(destination),
        metadata={
            "materialization_kind": "http_fetch",
            "cached_path": str(destination),
            "cached_record_count": 1,
            "materialized_content_type": "application/json",
            "original_content_type": fetch_metadata.get("content_type"),
            **fetch_metadata,
        },
    )


def resolve_raw_http_source_suffix(source: SourceDefinitionORM) -> str:
    if source.source_kind == "http_json":
        return ".json"
    if source.source_kind == "http_jsonl":
        return ".jsonl"
    if source.source_kind == "http_text":
        metadata = source.metadata_json if isinstance(source.metadata_json, dict) else {}
        discovery_metadata = metadata.get("discovery", {})
        format_hint = discovery_metadata.get("format_hint") if isinstance(discovery_metadata, dict) else None
        return ".csv" if format_hint == "csv" else ".txt"
    raise ValueError(f"Unsupported raw HTTP source kind: {source.source_kind}")


def parse_structured_http_source_payload(
    source: SourceDefinitionORM,
    payload: bytes,
) -> list[dict[str, Any]]:
    if source.source_kind == "http_xml":
        return parse_http_xml_payload(payload, source.target_uri)
    if source.source_kind == "http_csv":
        return parse_http_csv_payload(payload, source.target_uri)
    if source.source_kind == "rss":
        return parse_rss_payload(payload, source.target_uri)
    if source.source_kind == "arcgis_feature_json":
        return parse_arcgis_feature_payload(payload, source.target_uri)
    if source.source_kind == "ckan_package_search":
        return parse_ckan_package_search_payload(payload, source.target_uri)
    raise ValueError(f"Unsupported structured HTTP source kind: {source.source_kind}")


def parse_paginated_source_page(
    source: SourceDefinitionORM,
    payload: bytes,
    source_uri: str,
    *,
    page_size: int,
) -> ParsedSourcePage:
    if source.source_kind == "arcgis_feature_json":
        document = parse_json_payload(payload, "ArcGIS feature payload")
        features = document.get("features")
        if not isinstance(features, list):
            raise RuntimeError("ArcGIS feature payload did not contain a features list.")
        has_more = document.get("exceededTransferLimit")
        if not isinstance(has_more, bool):
            has_more = len(features) >= page_size
        return ParsedSourcePage(
            records=parse_arcgis_feature_document(document, source_uri),
            item_count=len(features),
            has_more=has_more,
        )
    if source.source_kind == "ckan_package_search":
        document = parse_json_payload(payload, "CKAN package search payload")
        result = document.get("result")
        if not isinstance(result, dict):
            raise RuntimeError("CKAN package search payload did not contain a result object.")
        packages = result.get("results")
        if not isinstance(packages, list):
            raise RuntimeError("CKAN package search payload did not contain a results list.")
        return ParsedSourcePage(
            records=parse_ckan_package_search_document(document, source_uri),
            item_count=len(packages),
            total_available=safe_int(result.get("count")),
            has_more=None,
        )
    raise ValueError(f"Unsupported paginated source kind: {source.source_kind}")


def build_paginated_source_request_url(
    source_uri: str,
    source_kind: str,
    *,
    offset: int,
    page_size: int,
) -> str:
    parts = urlsplit(source_uri)
    query = parse_qs(parts.query, keep_blank_values=True)
    if source_kind == "ckan_package_search":
        query["start"] = [str(offset)]
        query["rows"] = [str(page_size)]
    elif source_kind == "arcgis_feature_json":
        query["resultOffset"] = [str(offset)]
        query["resultRecordCount"] = [str(page_size)]
    else:
        return source_uri
    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            urlencode(query, doseq=True),
            parts.fragment,
        )
    )


def strip_payload_hash(metadata: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in metadata.items() if key not in {"payload_sha256", "byte_count", "attempt_count"}}


def stable_record_signature(record: dict[str, Any]) -> str:
    return json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def redact_request_headers(headers: dict[str, str]) -> dict[str, str]:
    sensitive_markers = ("authorization", "cookie", "token", "secret", "api-key", "apikey")
    return {
        key: "<redacted>" if any(marker in key.lower() for marker in sensitive_markers) else value
        for key, value in headers.items()
    }


def safe_parse_xml_payload(payload: bytes) -> ElementTree.Element:
    prefix = payload[:100_000].lower()
    if b"<!doctype" in prefix or b"<!entity" in prefix:
        raise ValueError("XML DTD and entity declarations are not allowed.")
    return ElementTree.fromstring(payload)


def parse_rss_atom_payload(payload: bytes, source_uri: str) -> list[dict[str, Any]]:
    return parse_rss_payload(payload, source_uri)


def parse_http_xml_payload(payload: bytes, source_uri: str) -> list[dict[str, Any]]:
    root = safe_parse_xml_payload(payload)
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


def parse_http_csv_payload(payload: bytes, source_uri: str) -> list[dict[str, Any]]:
    text = payload.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    records: list[dict[str, Any]] = []
    for index, row in enumerate(reader, start=1):
        normalized = {
            str(key).strip(): value.strip() if isinstance(value, str) else value
            for key, value in row.items()
            if key is not None
        }
        if not normalized:
            continue
        normalize_numeric_coordinates(normalized)
        title = first_present_string(
            normalized.get("title"),
            normalized.get("name"),
            normalized.get("id"),
            normalized.get("external_id"),
        ) or f"csv-record-{index}"
        record = {
            **normalized,
            "source_url": source_uri,
            "title": title,
            "text": build_record_text(normalized, fallback=title),
        }
        records.append(record)
    return records


def parse_rss_payload(payload: bytes, source_uri: str) -> list[dict[str, Any]]:
    root = safe_parse_xml_payload(payload)
    root_tag = strip_xml_namespace(root.tag).lower()
    feed_title = extract_feed_title(root)
    feed_link = extract_feed_link(root)
    records: list[dict[str, Any]] = []

    if root_tag == "feed":
        entries = [child for child in root if isinstance(child.tag, str) and strip_xml_namespace(child.tag) == "entry"]
        for index, entry in enumerate(entries, start=1):
            payload_json = xml_element_to_data(entry)
            if not isinstance(payload_json, dict):
                payload_json = {"value": payload_json}
            title = first_nested_value(payload_json, "title") or f"atom-entry-{index}"
            summary = first_nested_value(payload_json, "summary") or first_nested_value(payload_json, "content")
            record = {
                "source_url": source_uri,
                "feed_type": "atom",
                "feed_title": feed_title,
                "feed_link": feed_link,
                "title": title,
                "text": summary or title,
                "link": extract_atom_entry_link(entry) or feed_link,
                "published_at": first_nested_value(payload_json, "updated")
                or first_nested_value(payload_json, "published"),
                "entry_id": first_nested_value(payload_json, "id"),
                "author": first_nested_value(payload_json, "name") or first_nested_value(payload_json, "author"),
                "raw_feed_entry": payload_json,
            }
            records.append(record)
        return records

    channel = next(
        (child for child in root if isinstance(child.tag, str) and strip_xml_namespace(child.tag) == "channel"),
        root,
    )
    items = [child for child in channel if isinstance(child.tag, str) and strip_xml_namespace(child.tag) == "item"]
    for index, item in enumerate(items, start=1):
        payload_json = xml_element_to_data(item)
        if not isinstance(payload_json, dict):
            payload_json = {"value": payload_json}
        title = first_nested_value(payload_json, "title") or f"rss-item-{index}"
        description = first_nested_value(payload_json, "description")
        record = {
            "source_url": source_uri,
            "feed_type": "rss",
            "feed_title": feed_title,
            "feed_link": feed_link,
            "title": title,
            "text": description or title,
            "link": first_nested_value(payload_json, "link") or feed_link,
            "published_at": first_nested_value(payload_json, "pubDate"),
            "guid": first_nested_value(payload_json, "guid"),
            "author": first_nested_value(payload_json, "author"),
            "category": first_nested_value(payload_json, "category"),
            "raw_feed_entry": payload_json,
        }
        records.append(record)
    return records


def parse_arcgis_feature_payload(payload: bytes, source_uri: str) -> list[dict[str, Any]]:
    document = parse_json_payload(payload, "ArcGIS feature payload")
    return parse_arcgis_feature_document(document, source_uri)


def parse_arcgis_feature_document(document: dict[str, Any], source_uri: str) -> list[dict[str, Any]]:
    features = document.get("features")
    if not isinstance(features, list):
        raise RuntimeError("ArcGIS feature payload did not contain a features list.")
    spatial_reference = document.get("spatialReference")
    wkid = extract_arcgis_wkid(spatial_reference)
    records: list[dict[str, Any]] = []
    for index, feature in enumerate(features, start=1):
        if not isinstance(feature, dict):
            continue
        attributes = feature.get("attributes")
        record = dict(attributes) if isinstance(attributes, dict) else {}
        geometry = feature.get("geometry")
        geojson = arcgis_geometry_to_geojson(geometry, wkid)
        if geojson is not None:
            if geojson.get("type") == "Point":
                coordinates = geojson.get("coordinates")
                if (
                    isinstance(coordinates, list)
                    and len(coordinates) >= 2
                    and isinstance(coordinates[0], (int, float))
                    and isinstance(coordinates[1], (int, float))
                ):
                    record["longitude"] = float(coordinates[0])
                    record["latitude"] = float(coordinates[1])
            record["geometry"] = geojson
        title = first_present_string(
            record.get("title"),
            record.get("name"),
            record.get("site_name"),
            record.get("camera_name"),
            record.get("OBJECTID"),
            record.get("objectid"),
        ) or f"arcgis-feature-{index}"
        record.update(
            {
                "source_url": source_uri,
                "title": str(title),
                "text": build_record_text(record, fallback=str(title)),
                "arcgis_geometry_type": document.get("geometryType"),
                "arcgis_spatial_reference": spatial_reference,
            }
        )
        records.append(record)
    return records


def parse_ckan_package_search_payload(payload: bytes, source_uri: str) -> list[dict[str, Any]]:
    document = parse_json_payload(payload, "CKAN package search payload")
    return parse_ckan_package_search_document(document, source_uri)


def parse_ckan_package_search_document(
    document: dict[str, Any],
    source_uri: str,
) -> list[dict[str, Any]]:
    result = document.get("result")
    if not isinstance(result, dict):
        raise RuntimeError("CKAN package search payload did not contain a result object.")
    packages = result.get("results")
    if not isinstance(packages, list):
        raise RuntimeError("CKAN package search payload did not contain a results list.")

    records: list[dict[str, Any]] = []
    for package in packages:
        if not isinstance(package, dict):
            continue
        package_title = first_present_string(package.get("title"), package.get("name"), package.get("id")) or "ckan-package"
        package_url = first_present_string(package.get("url"), package.get("notes"))
        resources = package.get("resources")
        if isinstance(resources, list) and resources:
            for index, resource in enumerate(resources, start=1):
                if not isinstance(resource, dict):
                    continue
                resource_title = first_present_string(
                    resource.get("name"),
                    resource.get("description"),
                    resource.get("id"),
                ) or f"{package_title}-resource-{index}"
                records.append(
                    {
                        "source_url": source_uri,
                        "title": str(resource_title),
                        "text": build_record_text(
                            {
                                "package_title": package_title,
                                "resource_title": resource_title,
                                "format": resource.get("format"),
                                "url": resource.get("url"),
                            },
                            fallback=str(resource_title),
                        ),
                        "url": resource.get("url"),
                        "page_url": package_url,
                        "package_id": package.get("id"),
                        "package_name": package.get("name"),
                        "package_title": package_title,
                        "resource_id": resource.get("id"),
                        "resource_name": resource.get("name"),
                        "resource_format": resource.get("format"),
                        "resource_mimetype": resource.get("mimetype"),
                        "resource_created": resource.get("created"),
                        "resource_last_modified": resource.get("last_modified"),
                        "organization_title": nested_dict_value(package.get("organization"), "title"),
                        "organization_name": nested_dict_value(package.get("organization"), "name"),
                        "license_title": package.get("license_title"),
                        "tags": extract_named_values(package.get("tags")),
                        "groups": extract_named_values(package.get("groups")),
                    }
                )
            continue
        records.append(
            {
                "source_url": source_uri,
                "title": str(package_title),
                "text": build_record_text(package, fallback=str(package_title)),
                "page_url": package_url,
                "package_id": package.get("id"),
                "package_name": package.get("name"),
                "package_title": package_title,
                "license_title": package.get("license_title"),
                "tags": extract_named_values(package.get("tags")),
                "groups": extract_named_values(package.get("groups")),
            }
        )
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


def parse_json_payload(payload: bytes, label: str) -> dict[str, Any]:
    try:
        parsed = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"{label} could not be decoded as JSON.") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError(f"{label} did not decode to a JSON object.")
    return parsed


def normalize_numeric_coordinates(payload: dict[str, Any]) -> None:
    for key in ("latitude", "lat", "longitude", "lon"):
        value = payload.get(key)
        if isinstance(value, str):
            number = parse_float(value)
            if number is not None:
                payload[key] = number


def build_record_text(payload: dict[str, Any], *, fallback: str) -> str:
    parts = [
        first_present_string(
            payload.get("title"),
            payload.get("name"),
            payload.get("description"),
            payload.get("summary"),
            payload.get("format"),
            payload.get("url"),
        )
    ]
    values = [part for part in parts if part]
    return " | ".join(values) if values else fallback


def first_present_string(*values: Any) -> str | None:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, (int, float)):
            return str(value)
    return None


def extract_feed_title(root: ElementTree.Element) -> str | None:
    for child in root.iter():
        if not isinstance(child.tag, str):
            continue
        local_name = strip_xml_namespace(child.tag)
        if local_name == "title":
            text = (child.text or "").strip()
            if text:
                return text
    return None


def extract_feed_link(root: ElementTree.Element) -> str | None:
    for child in root.iter():
        if not isinstance(child.tag, str):
            continue
        local_name = strip_xml_namespace(child.tag)
        if local_name != "link":
            continue
        href = child.attrib.get("href")
        if href and href.strip():
            return href.strip()
        text = (child.text or "").strip()
        if text:
            return text
    return None


def extract_atom_entry_link(entry: ElementTree.Element) -> str | None:
    for child in entry:
        if not isinstance(child.tag, str) or strip_xml_namespace(child.tag) != "link":
            continue
        rel = child.attrib.get("rel", "alternate")
        href = child.attrib.get("href")
        if href and rel in {"alternate", ""}:
            return href.strip()
        text = (child.text or "").strip()
        if text:
            return text
    return None


def extract_arcgis_wkid(spatial_reference: Any) -> int | None:
    if isinstance(spatial_reference, dict):
        value = spatial_reference.get("latestWkid", spatial_reference.get("wkid"))
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


def arcgis_geometry_to_geojson(geometry: Any, wkid: int | None) -> dict[str, Any] | None:
    if not isinstance(geometry, dict):
        return None
    if "x" in geometry and "y" in geometry:
        point = transform_arcgis_position(geometry.get("x"), geometry.get("y"), wkid)
        if point is None:
            return None
        return {"type": "Point", "coordinates": list(point)}
    if isinstance(geometry.get("points"), list):
        points = [
            transformed
            for point in geometry["points"]
            for transformed in [transform_arcgis_point_list(point, wkid)]
            if transformed is not None
        ]
        if points:
            return {"type": "MultiPoint", "coordinates": points}
    if isinstance(geometry.get("paths"), list):
        paths = [
            path
            for raw_path in geometry["paths"]
            for path in [transform_arcgis_path(raw_path, wkid)]
            if path
        ]
        if paths:
            return {
                "type": "LineString" if len(paths) == 1 else "MultiLineString",
                "coordinates": paths[0] if len(paths) == 1 else paths,
            }
    if isinstance(geometry.get("rings"), list):
        rings = [
            ring
            for raw_ring in geometry["rings"]
            for ring in [transform_arcgis_path(raw_ring, wkid)]
            if ring
        ]
        if rings:
            return {"type": "Polygon", "coordinates": rings}
    return None


def transform_arcgis_point_list(point: Any, wkid: int | None) -> list[float] | None:
    if not isinstance(point, list) or len(point) < 2:
        return None
    transformed = transform_arcgis_position(point[0], point[1], wkid)
    if transformed is None:
        return None
    return [transformed[0], transformed[1]]


def transform_arcgis_path(points: Any, wkid: int | None) -> list[list[float]]:
    if not isinstance(points, list):
        return []
    transformed = [value for point in points for value in [transform_arcgis_point_list(point, wkid)] if value is not None]
    return transformed


def transform_arcgis_position(x: Any, y: Any, wkid: int | None) -> tuple[float, float] | None:
    x_value = parse_float(x)
    y_value = parse_float(y)
    if x_value is None or y_value is None:
        return None
    if wkid in {3857, 102100, 102113}:
        return web_mercator_to_wgs84(x_value, y_value)
    return float(x_value), float(y_value)


def web_mercator_to_wgs84(x: float, y: float) -> tuple[float, float]:
    lon = (x / 20037508.34) * 180.0
    lat = (y / 20037508.34) * 180.0
    lat = 180.0 / math.pi * (2.0 * math.atan(math.exp(lat * math.pi / 180.0)) - math.pi / 2.0)
    return lon, lat


def parse_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def safe_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return int(stripped)
        except ValueError:
            return None
    return None


def first_query_value(query: dict[str, list[str]], key: str) -> str | None:
    values = query.get(key)
    if not values:
        return None
    return values[0]


def first_int_value(*values: Any, default: int) -> int:
    for value in values:
        candidate = safe_int(value)
        if candidate is not None:
            return candidate
    return default


def nested_dict_value(value: Any, key: str) -> str | None:
    if isinstance(value, dict):
        nested = value.get(key)
        if isinstance(nested, str) and nested.strip():
            return nested.strip()
    return None


def extract_named_values(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    names: list[str] = []
    for item in value:
        if isinstance(item, dict):
            name = first_present_string(item.get("display_name"), item.get("title"), item.get("name"))
            if name:
                names.append(name)
    return names


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
