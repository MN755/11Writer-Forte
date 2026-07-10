from __future__ import annotations

import base64
import hashlib
import ipaddress
import json
import os
import re
import socket
import ssl
import struct
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote_plus, urlencode, urljoin, urlparse
from urllib.request import Request, urlopen
from xml.etree import ElementTree

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from src.config import get_settings
from src.models import (
    AlertORM,
    CustodyLogORM,
    ScheduledTaskORM,
    ScheduledTaskRunORM,
    SourceCheckpointORM,
    SourceDeadLetterORM,
    SourceDefinitionORM,
    SourceRunORM,
    StorageObjectORM,
)
from src.schemas import SourceDefinitionCreate, SourceDefinitionUpdate
from src.services.ingestion_service import (
    IngestionEnvelope,
    normalize_payload as normalize_ingestion_payload,
    parse_json_envelopes,
    persist_envelopes_as_import_run,
    read_path_as_envelopes,
)
from src.services.layer_service import ensure_data_layer
from src.services.observation_service import parse_timestamp_value
from src.services.storage_service import register_source_run_storage_object, register_storage_object


@dataclass(frozen=True)
class SourceFetchConfig:
    timeout_seconds: float
    retry_attempts: int
    retry_backoff_seconds: float
    headers: dict[str, str]
    max_payload_bytes: int
    allow_private_networks: bool
    min_request_interval_seconds: float


@dataclass(frozen=True)
class MaterializedSourcePayload:
    path: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class ExtractedLink:
    url: str
    text: str


@dataclass(frozen=True)
class WebSearchProviderProfile:
    provider_name: str
    target_uri: str
    query_param: str
    redirect_query_param: str | None
    search_page_param: str | None
    search_page_start: int
    search_page_step: int
    skip_provider_domain: bool


@dataclass(frozen=True)
class WebSearchCollectionResult:
    records: list[dict[str, Any]]
    metadata: dict[str, Any]
    seed_urls: list[str]


@dataclass(frozen=True)
class RobotsRule:
    pattern: str
    allow: bool


@dataclass(frozen=True)
class RobotsPolicy:
    origin: str
    source_url: str
    matched_user_agent: str
    crawl_delay_seconds: float | None
    rules: tuple[RobotsRule, ...]


@dataclass
class RobotsRuntimeContext:
    enabled: bool
    user_agents: list[str]
    cache: dict[str, RobotsPolicy | None]
    checked_origins: set[str]
    blocked_url_count: int = 0
    missing_policy_count: int = 0
    blocked_urls_sample: list[str] | None = None

    def __post_init__(self) -> None:
        if self.blocked_urls_sample is None:
            self.blocked_urls_sample = []


@dataclass
class FetchRuntimeContext:
    last_request_started_at_by_origin: dict[str, float] = field(default_factory=dict)
    paced_request_count: int = 0
    pacing_delay_applied_seconds: float = 0.0
    private_network_blocked_count: int = 0
    private_network_blocked_urls_sample: list[str] = field(default_factory=list)


WEB_SEARCH_PROVIDER_DEFAULTS: dict[str, dict[str, Any]] = {
    "generic_html": {
        "query_param": "q",
        "redirect_query_param": None,
        "search_page_param": None,
        "search_page_start": 0,
        "search_page_step": 0,
        "skip_provider_domain": True,
    },
    "duckduckgo_html": {
        "target_uri": "https://html.duckduckgo.com/html/",
        "query_param": "q",
        "redirect_query_param": "uddg",
        "search_page_param": "s",
        "search_page_start": 0,
        "search_page_step": 30,
        "skip_provider_domain": True,
    },
    "duckduckgo_lite": {
        "target_uri": "https://lite.duckduckgo.com/lite/",
        "query_param": "q",
        "redirect_query_param": "uddg",
        "search_page_param": "s",
        "search_page_start": 0,
        "search_page_step": 30,
        "skip_provider_domain": True,
    },
    "bing_html": {
        "target_uri": "https://www.bing.com/search",
        "query_param": "q",
        "redirect_query_param": None,
        "search_page_param": "first",
        "search_page_start": 1,
        "search_page_step": 10,
        "skip_provider_domain": True,
    },
}

WEB_COLLECTION_SOURCE_KINDS = {"web_search", "web_crawl", "web_discovery"}
WEB_COLLECTION_STAT_KEYS = (
    "search_page_count",
    "search_result_candidate_count",
    "search_page_fetch_count",
    "search_page_fetch_error_count",
    "search_result_crawl_page_count",
    "search_result_crawl_queued_count",
    "sitemap_fetch_count",
    "sitemap_url_count",
    "crawl_page_count",
    "crawl_queued_count",
    "crawl_fetch_error_count",
    "robots_blocked_count",
    "fetch_private_network_blocked_count",
)


class SourceExecutionError(RuntimeError):
    def __init__(
        self,
        *,
        source_run_id: int,
        source_id: int,
        source_kind: str,
        cause: Exception,
    ) -> None:
        super().__init__(str(cause))
        self.source_run_id = source_run_id
        self.source_id = source_id
        self.source_kind = source_kind
        self.cause = cause


class HTMLDocumentParser(HTMLParser):
    def __init__(self, *, base_url: str, text_limit: int = 4000) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.text_limit = max(256, text_limit)
        self.links: list[ExtractedLink] = []
        self.title_parts: list[str] = []
        self.text_parts: list[str] = []
        self.meta_description: str | None = None
        self.meta_tags: dict[str, str] = {}
        self.time_values: list[str] = []
        self._anchor_href: str | None = None
        self._anchor_text_parts: list[str] = []
        self._capture_title = False
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {key.lower(): value for key, value in attrs}
        lowered = tag.lower()
        if lowered in {"script", "style", "noscript"}:
            self._skip_depth += 1
            return
        if lowered == "title":
            self._capture_title = True
            return
        if lowered == "meta":
            content = attr_map.get("content")
            if isinstance(content, str) and content.strip():
                normalized_content = content.strip()
                for meta_key in ("name", "property", "itemprop"):
                    raw_key = attr_map.get(meta_key)
                    if isinstance(raw_key, str) and raw_key.strip():
                        self.meta_tags[raw_key.strip().lower()] = normalized_content
                if (attr_map.get("name") or "").lower() == "description":
                    self.meta_description = normalized_content
            return
        if lowered == "time":
            datetime_value = attr_map.get("datetime")
            if isinstance(datetime_value, str) and datetime_value.strip():
                self.time_values.append(datetime_value.strip())
            return
        if lowered == "a":
            href = attr_map.get("href")
            if isinstance(href, str) and href.strip():
                self._anchor_href = href.strip()
                self._anchor_text_parts = []

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if lowered in {"script", "style", "noscript"}:
            if self._skip_depth > 0:
                self._skip_depth -= 1
            return
        if lowered == "title":
            self._capture_title = False
            return
        if lowered == "a" and self._anchor_href:
            normalized = normalize_html_link(self.base_url, self._anchor_href)
            if normalized:
                anchor_text = " ".join(part for part in self._anchor_text_parts if part).strip()
                self.links.append(ExtractedLink(url=normalized, text=anchor_text or normalized))
            self._anchor_href = None
            self._anchor_text_parts = []

    def handle_data(self, data: str) -> None:
        if self._skip_depth > 0:
            return
        stripped = data.strip()
        if not stripped:
            return
        if self._capture_title:
            self.title_parts.append(stripped)
        if self._anchor_href:
            self._anchor_text_parts.append(stripped)
        if len(" ".join(self.text_parts)) < self.text_limit:
            self.text_parts.append(stripped)

    def as_summary(self) -> dict[str, Any]:
        title = " ".join(self.title_parts).strip() or None
        text = " ".join(self.text_parts).strip()
        if len(text) > self.text_limit:
            text = text[: self.text_limit].rstrip()
        return {
            "title": title,
            "meta_description": self.meta_description,
            "meta_tags": dict(self.meta_tags),
            "time_values": list(self.time_values),
            "text_excerpt": text,
            "links": self.links,
        }


@dataclass(frozen=True)
class RuntimeStreamBatch:
    path: str
    metadata: dict[str, Any]
    envelopes: list[IngestionEnvelope]
    records_seen: int
    records_failed: int
    cursor_text: str | None
    last_event_id: str | None
    last_offset: int | None
    checkpoint_json: dict[str, Any]


def source_now() -> datetime:
    return datetime.now(timezone.utc)


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
                "changes": change_details,
            },
        )
    )
    session.commit()
    session.refresh(record)
    return record


def upsert_source_definition(
    session: Session,
    payload: SourceDefinitionCreate,
    *,
    actor: str = "system",
    allow_update: bool = True,
) -> tuple[SourceDefinitionORM, str]:
    existing = session.scalar(select(SourceDefinitionORM).where(SourceDefinitionORM.name == payload.name).limit(1))
    if existing is None:
        return create_source_definition(session, payload), "created"
    if not allow_update:
        raise ValueError(f"Source named '{payload.name}' already exists.")
    updated = update_source_definition(
        session,
        existing.source_id,
        SourceDefinitionUpdate(
            name=payload.name,
            source_kind=payload.source_kind,
            layer_key=payload.layer_key,
            target_uri=payload.target_uri,
            enabled=payload.enabled,
            integrity_source=payload.integrity_source,
            notes=payload.notes,
            metadata_json=payload.metadata_json,
        ),
        actor=actor,
    )
    return updated, "updated"


def upsert_and_run_source_definition(
    session: Session,
    payload: SourceDefinitionCreate,
    *,
    actor: str = "source_runner",
    allow_update: bool = True,
) -> dict[str, Any]:
    source, action = upsert_source_definition(
        session,
        payload,
        actor=actor,
        allow_update=allow_update,
    )
    source_run = run_source_definition(session, source.source_id, actor=actor)
    return {
        "action": action,
        "source": source,
        "source_run": source_run,
    }


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
    sources = list(session.scalars(statement.order_by(SourceDefinitionORM.source_id.asc())))
    runtime_sources = [source for source in sources if source.source_kind in {"sse_stream", "websocket_stream"}]
    if source_id is not None and not runtime_sources:
        source = session.get(SourceDefinitionORM, source_id)
        if source is None:
            raise ValueError(f"Source {source_id} does not exist.")
        raise ValueError(f"Source {source_id} is not a stream-native runtime source.")
    if source_id is not None:
        runs = [run_source_definition(session, runtime_sources[0].source_id, actor=actor)]
    else:
        runs: list[SourceRunORM] = []
        for source in runtime_sources:
            try:
                runs.append(run_source_definition(session, source.source_id, actor=actor))
            except Exception:
                latest_run = session.scalar(
                    select(SourceRunORM)
                    .where(SourceRunORM.source_id == source.source_id)
                    .order_by(SourceRunORM.source_run_id.desc())
                    .limit(1)
                )
                if latest_run is not None:
                    runs.append(latest_run)
    return {
        "source_count": len(runtime_sources),
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
    web_collection_summary = build_web_collection_fleet_summary(statuses)
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
        "web_collection_summary": web_collection_summary,
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
        "web_collection_summary": summary["web_collection_summary"],
        "web_collection_issue_sources": [
            status for status in statuses if source_status_has_web_collection_issue(status)
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


def perform_source_maintenance(
    session: Session,
    *,
    stale_after_hours: float = 24.0,
    source_limit: int = 25,
    dead_letter_limit: int = 100,
    replay_dead_letters: bool = True,
    run_stale_sources: bool = True,
    run_failing_sources: bool = True,
    source_kind: str | None = None,
    actor: str = "source_maintenance",
) -> dict[str, object]:
    generated_at = source_now()
    stale_before = generated_at - timedelta(hours=max(0.0, stale_after_hours))
    statuses = collect_source_ops_statuses(session, stale_before=stale_before)
    selected_statuses: list[dict[str, Any]] = []
    normalized_source_kind = source_kind.strip().lower() if isinstance(source_kind, str) and source_kind.strip() else None

    for status in statuses:
        source = status["source"]
        if not source.enabled:
            continue
        if normalized_source_kind is not None and source.source_kind != normalized_source_kind:
            continue
        should_select = False
        if replay_dead_letters and int(status["pending_dead_letter_count"]) > 0:
            should_select = True
        if run_failing_sources and bool(status["is_failing"]):
            should_select = True
        if run_stale_sources and bool(status["is_stale"]) and source.source_kind != "webhook_ingest":
            should_select = True
        if should_select:
            selected_statuses.append(status)
        if len(selected_statuses) >= max(1, source_limit):
            break

    replayed_records: list[SourceDeadLetterORM] = []
    rerun_records: list[SourceRunORM] = []
    rerun_failures: list[dict[str, Any]] = []
    remaining_dead_letter_budget = max(0, dead_letter_limit)

    for status in selected_statuses:
        source = status["source"]
        pending_dead_letters = int(status["pending_dead_letter_count"])
        if replay_dead_letters and pending_dead_letters > 0 and remaining_dead_letter_budget > 0:
            dead_letters = list_source_dead_letters(
                session,
                source_id=source.source_id,
                status="pending",
                limit=min(remaining_dead_letter_budget, pending_dead_letters),
            )
            for record in dead_letters:
                replayed_records.append(
                    replay_dead_letter_record(
                        session,
                        record.source_dead_letter_id,
                        actor=actor,
                    )
                )
                remaining_dead_letter_budget -= 1
                if remaining_dead_letter_budget <= 0:
                    break

        should_rerun = False
        if source.source_kind != "webhook_ingest":
            if run_failing_sources and bool(status["is_failing"]):
                should_rerun = True
            if run_stale_sources and bool(status["is_stale"]):
                should_rerun = True
        if not should_rerun:
            continue
        try:
            rerun_records.append(run_source_definition(session, source.source_id, actor=actor))
        except Exception as exc:
            rerun_failures.append(
                {
                    "source_id": source.source_id,
                    "source_name": source.name,
                    "source_kind": source.source_kind,
                    "error": str(exc),
                }
            )

    return {
        "generated_at": generated_at.isoformat().replace("+00:00", "Z"),
        "stale_after_hours": stale_after_hours,
        "source_limit": source_limit,
        "dead_letter_limit": dead_letter_limit,
        "replay_dead_letters": replay_dead_letters,
        "run_stale_sources": run_stale_sources,
        "run_failing_sources": run_failing_sources,
        "source_kind": normalized_source_kind,
        "selected_source_count": len(selected_statuses),
        "selected_source_ids": [status["source"].source_id for status in selected_statuses],
        "replayed_dead_letter_count": len(replayed_records),
        "replayed_dead_letter_ids": [record.source_dead_letter_id for record in replayed_records],
        "rerun_source_count": len(rerun_records),
        "rerun_source_ids": [run.source_id for run in rerun_records],
        "source_run_ids": [run.source_run_id for run in rerun_records],
        "rerun_failure_count": len(rerun_failures),
        "rerun_failures": rerun_failures,
    }


def scan_source_health_alerts(
    session: Session,
    *,
    stale_after_hours: float = 24.0,
    source_limit: int = 100,
    source_kind: str | None = None,
    alert_on_stale: bool = True,
    alert_on_failed: bool = True,
    alert_on_dead_letters: bool = True,
    actor: str = "source_health_scan",
) -> dict[str, object]:
    generated_at = source_now()
    stale_before = generated_at - timedelta(hours=max(0.0, stale_after_hours))
    statuses = collect_source_ops_statuses(session, stale_before=stale_before)
    selected_statuses = statuses[: max(1, source_limit)]
    normalized_source_kind = source_kind.strip().lower() if isinstance(source_kind, str) and source_kind.strip() else None

    created_alert_ids: list[int] = []
    active_alert_ids: list[int] = []
    closed_alert_ids: list[int] = []
    scanned_source_ids: list[int] = []

    for status in selected_statuses:
        source = status["source"]
        scanned_source_ids.append(source.source_id)
        if normalized_source_kind is not None and source.source_kind != normalized_source_kind:
            continue

        active_conditions: list[tuple[str, str, str, dict[str, Any]]] = []
        if source.enabled:
            source_is_failing = bool(status["is_failing"])
            if alert_on_failed and bool(status["is_failing"]):
                active_conditions.append(
                    (
                        "failed",
                        "warning",
                        f"Source {source.name} has a failed latest run.",
                        build_source_health_trigger_basis(source, status, "failed"),
                    )
                )
            if (
                alert_on_stale
                and bool(status["is_stale"])
                and not source_is_failing
                and source.source_kind != "webhook_ingest"
            ):
                active_conditions.append(
                    (
                        "stale",
                        "info" if status["runtime_state"] != "degraded" else "warning",
                        f"Source {source.name} is stale and has not produced a recent successful run.",
                        build_source_health_trigger_basis(source, status, "stale"),
                    )
                )
            if alert_on_dead_letters and int(status["pending_dead_letter_count"]) > 0:
                active_conditions.append(
                    (
                        "dead_letters",
                        "warning",
                        f"Source {source.name} has pending source dead letters awaiting remediation.",
                        build_source_health_trigger_basis(source, status, "dead_letters"),
                    )
                )

        active_dedupe_keys = {f"source-health:{source.source_id}:{condition}" for condition, _, _, _ in active_conditions}
        for condition, severity, message, trigger_basis in active_conditions:
            alert, created_new = ensure_source_health_alert(
                session,
                dedupe_key=f"source-health:{source.source_id}:{condition}",
                severity=severity,
                message=message,
                trigger_basis_json=trigger_basis,
                actor=actor,
            )
            active_alert_ids.append(alert.alert_id)
            if created_new:
                created_alert_ids.append(alert.alert_id)

        open_alerts = list(
            session.scalars(
                select(AlertORM)
                .where(
                    AlertORM.dedupe_key.like(f"source-health:{source.source_id}:%"),
                    AlertORM.status.in_(("open", "acknowledged")),
                )
                .order_by(AlertORM.alert_id.asc())
            )
        )
        for alert in open_alerts:
            if alert.dedupe_key in active_dedupe_keys:
                continue
            alert.status = "closed"
            alert.disposition_note = "Automatically closed after source health recovered."
            session.add(
                CustodyLogORM(
                    object_type="alert",
                    object_id=str(alert.alert_id),
                    action="alert_auto_closed",
                    actor=actor,
                    details_json={
                        "dedupe_key": alert.dedupe_key,
                        "reason": "source_health_recovered",
                    },
                )
            )
            closed_alert_ids.append(alert.alert_id)

    session.commit()
    return {
        "generated_at": generated_at.isoformat().replace("+00:00", "Z"),
        "stale_after_hours": stale_after_hours,
        "source_limit": source_limit,
        "source_kind": normalized_source_kind,
        "alert_on_stale": alert_on_stale,
        "alert_on_failed": alert_on_failed,
        "alert_on_dead_letters": alert_on_dead_letters,
        "scanned_source_ids": scanned_source_ids,
        "created_alert_count": len(created_alert_ids),
        "created_alert_ids": created_alert_ids,
        "active_alert_count": len(active_alert_ids),
        "active_alert_ids": active_alert_ids,
        "closed_alert_count": len(closed_alert_ids),
        "closed_alert_ids": closed_alert_ids,
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
            .where(SourceRunORM.status.in_(("completed", "skipped")))
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


def build_checkpoints_by_source(session: Session) -> dict[int, SourceCheckpointORM]:
    rows = list(session.scalars(select(SourceCheckpointORM).order_by(SourceCheckpointORM.source_id.asc())))
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
    web_collection_enabled = source.source_kind in WEB_COLLECTION_SOURCE_KINDS
    web_collection_stats = extract_web_collection_stats(source, latest_run)
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
        "web_collection_enabled": web_collection_enabled,
        "web_collection_kind": source.source_kind if web_collection_enabled else None,
        "web_collection_stats": web_collection_stats,
    }


def extract_web_collection_stats(
    source: SourceDefinitionORM,
    latest_run: SourceRunORM | None,
) -> dict[str, Any]:
    if source.source_kind not in WEB_COLLECTION_SOURCE_KINDS:
        return {}
    if latest_run is None or not isinstance(latest_run.output_json, dict):
        return {}
    stats: dict[str, Any] = {}
    for key in WEB_COLLECTION_STAT_KEYS:
        value = latest_run.output_json.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            stats[key] = int(value)
    provider = latest_run.output_json.get("search_provider")
    if isinstance(provider, str) and provider:
        stats["search_provider"] = provider
    providers = latest_run.output_json.get("search_providers")
    if isinstance(providers, list):
        stats["search_providers"] = [str(item) for item in providers if isinstance(item, str)]
    stats["has_issue"] = any(
        int(stats.get(key, 0)) > 0
        for key in (
            "robots_blocked_count",
            "fetch_private_network_blocked_count",
            "search_page_fetch_error_count",
            "crawl_fetch_error_count",
        )
    )
    return stats


def source_status_has_web_collection_issue(status: dict[str, Any]) -> bool:
    if not bool(status.get("web_collection_enabled")):
        return False
    stats = status.get("web_collection_stats")
    if not isinstance(stats, dict):
        return False
    return bool(stats.get("has_issue"))


def build_web_collection_fleet_summary(statuses: list[dict[str, Any]]) -> dict[str, Any]:
    web_statuses = [status for status in statuses if status.get("web_collection_enabled")]
    return {
        "total_source_count": len(web_statuses),
        "enabled_source_count": sum(1 for status in web_statuses if status["source"].enabled),
        "search_source_count": sum(1 for status in web_statuses if status["source"].source_kind == "web_search"),
        "crawl_source_count": sum(1 for status in web_statuses if status["source"].source_kind == "web_crawl"),
        "discovery_source_count": sum(
            1 for status in web_statuses if status["source"].source_kind == "web_discovery"
        ),
        "latest_run_count": sum(1 for status in web_statuses if status["latest_run"] is not None),
        "problem_source_count": sum(1 for status in web_statuses if source_status_has_web_collection_issue(status)),
        "robots_blocked_source_count": sum(
            1
            for status in web_statuses
            if int(status["web_collection_stats"].get("robots_blocked_count", 0)) > 0
        ),
        "private_network_blocked_source_count": sum(
            1
            for status in web_statuses
            if int(status["web_collection_stats"].get("fetch_private_network_blocked_count", 0)) > 0
        ),
        "fetch_error_source_count": sum(
            1
            for status in web_statuses
            if (
                int(status["web_collection_stats"].get("search_page_fetch_error_count", 0))
                + int(status["web_collection_stats"].get("crawl_fetch_error_count", 0))
            )
            > 0
        ),
        "total_search_page_count": sum(
            int(status["web_collection_stats"].get("search_page_count", 0)) for status in web_statuses
        ),
        "total_search_result_candidate_count": sum(
            int(status["web_collection_stats"].get("search_result_candidate_count", 0))
            for status in web_statuses
        ),
        "total_search_page_fetch_count": sum(
            int(status["web_collection_stats"].get("search_page_fetch_count", 0)) for status in web_statuses
        ),
        "total_search_page_fetch_error_count": sum(
            int(status["web_collection_stats"].get("search_page_fetch_error_count", 0))
            for status in web_statuses
        ),
        "total_search_result_crawl_page_count": sum(
            int(status["web_collection_stats"].get("search_result_crawl_page_count", 0))
            for status in web_statuses
        ),
        "total_sitemap_fetch_count": sum(
            int(status["web_collection_stats"].get("sitemap_fetch_count", 0)) for status in web_statuses
        ),
        "total_crawl_page_count": sum(
            int(status["web_collection_stats"].get("crawl_page_count", 0)) for status in web_statuses
        ),
        "total_crawl_queued_count": sum(
            int(status["web_collection_stats"].get("crawl_queued_count", 0)) for status in web_statuses
        ),
        "total_crawl_fetch_error_count": sum(
            int(status["web_collection_stats"].get("crawl_fetch_error_count", 0)) for status in web_statuses
        ),
        "total_robots_blocked_count": sum(
            int(status["web_collection_stats"].get("robots_blocked_count", 0)) for status in web_statuses
        ),
        "total_private_network_blocked_count": sum(
            int(status["web_collection_stats"].get("fetch_private_network_blocked_count", 0))
            for status in web_statuses
        ),
    }


def build_source_health_trigger_basis(
    source: SourceDefinitionORM,
    status: dict[str, Any],
    condition: str,
) -> dict[str, Any]:
    latest_run = status.get("latest_run")
    checkpoint = status.get("checkpoint")
    return {
        "condition": condition,
        "source_id": source.source_id,
        "source_name": source.name,
        "source_kind": source.source_kind,
        "layer_key": source.layer_key,
        "target_uri": source.target_uri,
        "runtime_state": status.get("runtime_state"),
        "is_stale": bool(status.get("is_stale")),
        "is_failing": bool(status.get("is_failing")),
        "pending_dead_letter_count": int(status.get("pending_dead_letter_count", 0)),
        "latest_success_at": format_datetime_for_json(status.get("latest_success_at")),
        "last_dead_letter_at": format_datetime_for_json(status.get("last_dead_letter_at")),
        "latest_run_status": latest_run.status if latest_run is not None else None,
        "latest_run_finished_at": format_datetime_for_json(latest_run.finished_at) if latest_run is not None else None,
        "checkpoint_status": checkpoint.status if checkpoint is not None else None,
        "checkpoint_last_failure_at": format_datetime_for_json(checkpoint.last_failure_at) if checkpoint is not None else None,
        "checkpoint_failure_count": checkpoint.failure_count if checkpoint is not None else 0,
    }


def ensure_source_health_alert(
    session: Session,
    *,
    dedupe_key: str,
    severity: str,
    message: str,
    trigger_basis_json: dict[str, Any],
    actor: str,
) -> tuple[AlertORM, bool]:
    existing = session.scalar(
        select(AlertORM)
        .where(
            AlertORM.dedupe_key == dedupe_key,
            AlertORM.status.in_(("open", "acknowledged")),
        )
        .order_by(AlertORM.alert_id.desc())
        .limit(1)
    )
    if existing is not None:
        existing.severity = severity
        existing.message = message
        existing.trigger_basis_json = trigger_basis_json
        return existing, False

    record = AlertORM(
        severity=severity,
        status="open",
        dedupe_key=dedupe_key,
        message=message,
        trigger_basis_json=trigger_basis_json,
    )
    session.add(record)
    session.flush()
    session.add(
        CustodyLogORM(
            object_type="alert",
            object_id=str(record.alert_id),
            action="alert_created",
            actor=actor,
            details_json={
                "alert_id": record.alert_id,
                "dedupe_key": dedupe_key,
                "severity": severity,
                "message": message,
                "trigger_basis_json": trigger_basis_json,
            },
        )
    )
    return record, True


def format_datetime_for_json(value: datetime | None) -> str | None:
    if value is None:
        return None
    normalized = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return normalized.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


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

    fetch_mode = infer_fetch_mode_for_kind(source.source_kind)
    checkpoint = get_or_create_source_checkpoint(
        session,
        source,
        adapter_kind=source.source_kind,
        fetch_mode=fetch_mode,
    )
    checkpoint.status = "active" if fetch_mode in {"stream", "push"} else "running"
    checkpoint.last_seen_at = source_now()

    run = SourceRunORM(
        source_id=source.source_id,
        status="running",
        adapter_kind=source.source_kind,
        fetch_mode=fetch_mode,
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
                "adapter_kind": run.adapter_kind,
                "fetch_mode": run.fetch_mode,
                "target_uri": source.target_uri,
            },
        )
    )

    try:
        if source.source_kind in {"sse_stream", "websocket_stream"}:
            return execute_runtime_stream_source_run(
                session,
                source=source,
                checkpoint=checkpoint,
                run=run,
                actor=actor,
            )
        materialized = materialize_source_payload(source, checkpoint=checkpoint)
        apply_materialized_checkpoint_state(run, checkpoint, materialized.metadata)
        envelopes = read_path_as_envelopes(
            Path(materialized.path),
            infer_source_format_for_kind(source.source_kind, materialized.path),
            source_type=source.source_kind,
        )
        run.records_seen = len(envelopes)
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
            run.records_skipped = 0
            run.finished_at = finished_at
            checkpoint.status = "idle"
            checkpoint.last_success_at = finished_at
            run.output_json = {
                "source_kind": source.source_kind,
                "adapter_kind": run.adapter_kind,
                "fetch_mode": run.fetch_mode,
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
            register_checkpoint_storage_object(session, source, checkpoint, actor=actor)
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
        import_run = persist_envelopes_as_import_run(
            session,
            source_path=build_source_import_path(source, fetch_mode),
            source_format=resolve_source_import_format(source.source_kind, materialized.path),
            layer_key=source.layer_key,
            notes=source.notes,
            actor=actor,
            source_type=source.source_kind,
            envelopes=envelopes,
        )
        finished_at = source_now()
        run.status = "completed"
        run.import_run_id = import_run.import_run_id
        run.records_seen = import_run.records_seen
        run.records_imported = import_run.records_imported
        run.records_skipped = import_run.records_skipped
        run.finished_at = finished_at
        checkpoint.status = "idle"
        checkpoint.last_success_at = finished_at
        run.output_json = {
            "import_run_id": import_run.import_run_id,
            "source_kind": source.source_kind,
            "adapter_kind": run.adapter_kind,
            "fetch_mode": run.fetch_mode,
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
        register_checkpoint_storage_object(session, source, checkpoint, actor=actor)
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
    except SourceExecutionError:
        raise
    except Exception as exc:
        run.status = "failed"
        run.error_text = str(exc)
        run.finished_at = source_now()
        checkpoint.status = "degraded"
        checkpoint.last_failure_at = run.finished_at
        checkpoint.failure_count += 1
        session.add(
            CustodyLogORM(
                object_type="source_run",
                object_id=str(run.source_run_id),
                action="source_run_failed",
                actor=actor,
                details_json={
                    "source_id": source.source_id,
                    "adapter_kind": run.adapter_kind,
                    "fetch_mode": run.fetch_mode,
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
                    "adapter_kind": run.adapter_kind,
                    "fetch_mode": run.fetch_mode,
                    "error_text": str(exc),
                },
            )
        )
        register_checkpoint_storage_object(session, source, checkpoint, actor=actor)
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
        raise SourceExecutionError(
            source_run_id=run.source_run_id,
            source_id=source.source_id,
            source_kind=source.source_kind,
            cause=exc,
        ) from exc

    session.refresh(run)
    return run


def execute_runtime_stream_source_run(
    session: Session,
    *,
    source: SourceDefinitionORM,
    checkpoint: SourceCheckpointORM,
    run: SourceRunORM,
    actor: str,
) -> SourceRunORM:
    try:
        batch = collect_runtime_stream_batch(
            session,
            source=source,
            checkpoint=checkpoint,
            run=run,
            actor=actor,
        )
        run.records_seen = batch.records_seen
        run.records_failed = batch.records_failed
        run.cursor_text = batch.cursor_text
        run.last_event_id = batch.last_event_id
        run.last_offset = batch.last_offset
        run.checkpoint_json = dict(batch.checkpoint_json)
        checkpoint.cursor_text = batch.cursor_text
        checkpoint.last_event_id = batch.last_event_id
        checkpoint.last_offset = batch.last_offset
        checkpoint.checkpoint_json = dict(batch.checkpoint_json)

        register_source_run_storage_object(
            session,
            source,
            run,
            batch.path,
            batch.metadata,
            run_status="materialized",
            actor=actor,
        )
        session.add(
            CustodyLogORM(
                object_type="source_run",
                object_id=str(run.source_run_id),
                action="source_payload_materialized",
                actor=actor,
                details_json=batch.metadata,
            )
        )
        if should_skip_unchanged_source(session, source, batch.metadata):
            finished_at = source_now()
            run.status = "skipped"
            run.finished_at = finished_at
            checkpoint.status = "idle"
            checkpoint.last_success_at = finished_at
            run.output_json = {
                "source_kind": source.source_kind,
                "adapter_kind": run.adapter_kind,
                "fetch_mode": run.fetch_mode,
                "target_uri": source.target_uri,
                "skip_reason": "payload_unchanged",
                **batch.metadata,
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
                        "payload_sha256": batch.metadata.get("payload_sha256"),
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
                        "payload_sha256": batch.metadata.get("payload_sha256"),
                    },
                )
            )
            register_checkpoint_storage_object(session, source, checkpoint, actor=actor)
            register_source_run_storage_object(
                session,
                source,
                run,
                batch.path,
                batch.metadata,
                run_status="skipped",
                actor=actor,
            )
            session.commit()
            session.refresh(run)
            return run

        import_run = None
        if batch.envelopes:
            import_run = persist_envelopes_as_import_run(
                session,
                source_path=build_source_import_path(source, run.fetch_mode),
                source_format="json",
                layer_key=source.layer_key,
                notes=source.notes,
                actor=actor,
                source_type=source.source_kind,
                envelopes=batch.envelopes,
            )
        finished_at = source_now()
        run.status = "completed"
        run.import_run_id = import_run.import_run_id if import_run is not None else None
        run.records_imported = import_run.records_imported if import_run is not None else 0
        run.records_skipped = import_run.records_skipped if import_run is not None else 0
        run.finished_at = finished_at
        checkpoint.status = "idle"
        checkpoint.last_success_at = finished_at
        run.output_json = {
            "import_run_id": import_run.import_run_id if import_run is not None else None,
            "source_kind": source.source_kind,
            "adapter_kind": run.adapter_kind,
            "fetch_mode": run.fetch_mode,
            "target_uri": source.target_uri,
            **batch.metadata,
        }
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
                    "import_run_id": run.import_run_id,
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
                    "import_run_id": run.import_run_id,
                },
            )
        )
        register_checkpoint_storage_object(session, source, checkpoint, actor=actor)
        register_source_run_storage_object(
            session,
            source,
            run,
            batch.path,
            batch.metadata,
            import_run_id=run.import_run_id,
            run_status="completed",
            actor=actor,
        )
        session.commit()
    except Exception as exc:
        run.status = "failed"
        run.error_text = str(exc)
        run.finished_at = source_now()
        checkpoint.status = "degraded"
        checkpoint.last_failure_at = run.finished_at
        checkpoint.failure_count += 1
        session.add(
            CustodyLogORM(
                object_type="source_run",
                object_id=str(run.source_run_id),
                action="source_run_failed",
                actor=actor,
                details_json={
                    "source_id": source.source_id,
                    "adapter_kind": run.adapter_kind,
                    "fetch_mode": run.fetch_mode,
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
                    "adapter_kind": run.adapter_kind,
                    "fetch_mode": run.fetch_mode,
                    "error_text": str(exc),
                },
            )
        )
        register_checkpoint_storage_object(session, source, checkpoint, actor=actor)
        if "batch" in locals():
            register_source_run_storage_object(
                session,
                source,
                run,
                batch.path,
                batch.metadata,
                run_status="failed",
                actor=actor,
            )
        session.commit()
        raise SourceExecutionError(
            source_run_id=run.source_run_id,
            source_id=source.source_id,
            source_kind=source.source_kind,
            cause=exc,
        ) from exc

    session.refresh(run)
    return run


def materialize_source_payload(
    source: SourceDefinitionORM,
    checkpoint: SourceCheckpointORM | None = None,
) -> MaterializedSourcePayload:
    if source.source_kind in {"local_file", "sqlite_file"}:
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

    if source.source_kind in {"http_json", "http_jsonl", "http_text", "http_xml", "rss"}:
        suffix = ".jsonl" if source.source_kind == "http_jsonl" else ".json" if source.source_kind in {"http_json", "http_xml", "rss"} else ".txt"
        destination = build_cached_path(source.source_id, suffix)
        fetch_config = parse_fetch_config(source)
        payload, fetch_metadata = fetch_http_source(source, fetch_config)
        if source.source_kind in {"http_xml", "rss"}:
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

    if source.source_kind in {"web_search", "web_crawl", "web_discovery"}:
        return materialize_web_discovery_source(source, checkpoint=checkpoint)

    if source.source_kind in {"sse_stream", "websocket_stream"}:
        raise ValueError(f"Source kind '{source.source_kind}' requires the runtime worker path, not a pull run.")

    if source.source_kind == "webhook_ingest":
        raise ValueError("Webhook sources must be ingested through the webhook endpoint.")

    raise ValueError(f"Unsupported source kind: {source.source_kind}")


def build_cached_path(source_id: int, suffix: str) -> Path:
    settings = get_settings()
    cache_dir = settings.data_dir_effective / "source_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / f"source-{source_id}{suffix}"


def write_source_materialization(source_id: int, suffix: str, payload: bytes) -> str:
    target = build_cached_path(source_id, suffix)
    target.write_bytes(payload)
    return str(target)


def parse_fetch_config(source: SourceDefinitionORM) -> SourceFetchConfig:
    metadata = source.metadata_json if isinstance(source.metadata_json, dict) else {}
    settings = get_settings()
    timeout_seconds = float(metadata.get("request_timeout_seconds", 30.0))
    retry_attempts = max(1, int(metadata.get("retry_attempts", 3)))
    retry_backoff_seconds = max(0.0, float(metadata.get("retry_backoff_seconds", 0.0)))
    max_payload_bytes = max(
        1,
        int(metadata.get("max_payload_bytes", settings.source_fetch_max_payload_bytes)),
    )
    allow_private_networks = bool(
        metadata.get("allow_private_networks", settings.source_fetch_allow_private_networks)
    )
    min_request_interval_seconds = max(
        0.0,
        float(
            metadata.get(
                "min_request_interval_seconds",
                settings.source_fetch_min_request_interval_seconds,
            )
        ),
    )
    user_headers = metadata.get("headers", {})
    headers = {
        "User-Agent": str(metadata.get("user_agent", "11Writer-Forte/0.1 (+headless-source-fetch)")),
        "Accept": (
            "application/json"
            if source.source_kind in {"http_json", "http_jsonl"}
            else "text/html, application/xhtml+xml, application/xml;q=0.9, */*;q=0.8"
            if source.source_kind in {"web_search", "web_crawl", "web_discovery"}
            else "application/xml, text/xml, */*"
            if source.source_kind in {"http_xml", "rss"}
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
        max_payload_bytes=max_payload_bytes,
        allow_private_networks=allow_private_networks,
        min_request_interval_seconds=min_request_interval_seconds,
    )


def materialize_web_discovery_source(
    source: SourceDefinitionORM,
    *,
    checkpoint: SourceCheckpointORM | None = None,
) -> MaterializedSourcePayload:
    fetch_config = parse_fetch_config(source)
    if source.source_kind == "web_search":
        records, discovery_metadata = collect_web_search_records(source, fetch_config)
    elif source.source_kind == "web_crawl":
        records, discovery_metadata = collect_web_crawl_records(source, fetch_config, checkpoint=checkpoint)
    elif source.source_kind == "web_discovery":
        records, discovery_metadata = collect_generic_web_discovery_records(
            source,
            fetch_config,
            checkpoint=checkpoint,
        )
    else:
        raise ValueError(f"Unsupported web discovery source kind '{source.source_kind}'.")

    destination = build_cached_path(source.source_id, ".json")
    serialized = json.dumps(records, indent=2, default=str)
    destination.write_text(serialized, encoding="utf-8")
    payload_bytes = serialized.encode("utf-8")
    return MaterializedSourcePayload(
        path=str(destination),
        metadata={
            "materialization_kind": "web_discovery",
            "cached_path": str(destination),
            "byte_count": len(payload_bytes),
            "payload_sha256": hashlib.sha256(payload_bytes).hexdigest(),
            "materialized_content_type": "application/json",
            "cached_record_count": len(records),
            **discovery_metadata,
        },
    )


def collect_web_search_records(
    source: SourceDefinitionORM,
    fetch_config: SourceFetchConfig,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    result = collect_web_search_campaign(source, fetch_config)
    return result.records, result.metadata


def collect_web_search_campaign(
    source: SourceDefinitionORM,
    fetch_config: SourceFetchConfig,
    *,
    metadata_override: dict[str, Any] | None = None,
    robots_context: RobotsRuntimeContext | None = None,
    fetch_runtime_context: FetchRuntimeContext | None = None,
) -> WebSearchCollectionResult:
    metadata = source.metadata_json if isinstance(source.metadata_json, dict) else {}
    if metadata_override:
        metadata = {**metadata, **metadata_override}
    effective_robots_context = robots_context or build_robots_runtime_context(metadata, fetch_config)
    effective_fetch_runtime_context = fetch_runtime_context or build_fetch_runtime_context()
    providers = resolve_web_search_providers(source, metadata)
    queries = resolve_search_queries(metadata)
    search_page_limit = max(1, int(metadata.get("search_page_limit", 1)))
    search_result_limit = max(1, int(metadata.get("search_result_limit", 25)))
    page_fetch_limit = max(1, int(metadata.get("page_fetch_limit", search_result_limit)))
    fetch_result_pages = bool(metadata.get("fetch_result_pages", True))
    allowed_domains = normalize_domain_list(metadata.get("allowed_domains"))
    include_url_patterns = normalize_string_list(metadata.get("include_url_patterns"))
    exclude_url_patterns = normalize_string_list(metadata.get("exclude_url_patterns"))
    crawl_from_results = bool(metadata.get("crawl_from_results", False))
    result_crawl_depth = max(0, int(metadata.get("result_crawl_depth", metadata.get("crawl_depth", 1))))
    result_crawl_page_limit = max(
        1,
        int(metadata.get("result_crawl_page_limit", metadata.get("crawl_page_limit", 25))),
    )
    result_crawl_link_limit = max(
        1,
        int(metadata.get("result_crawl_link_limit", metadata.get("crawl_link_limit", 50))),
    )
    result_crawl_same_domain_only = bool(
        metadata.get("result_crawl_same_domain_only", metadata.get("same_domain_only", True))
    )
    crawl_allowed_domains = normalize_domain_list(metadata.get("crawl_allowed_domains")) or allowed_domains
    crawl_include_url_patterns = (
        normalize_string_list(metadata.get("crawl_include_url_patterns")) or include_url_patterns
    )
    crawl_exclude_url_patterns = (
        normalize_string_list(metadata.get("crawl_exclude_url_patterns")) or exclude_url_patterns
    )
    page_text_limit = max(1000, int(metadata.get("page_text_limit", 5000)))

    records: list[dict[str, Any]] = []
    global_result_urls: set[str] = set()
    global_seed_urls: list[str] = []
    global_seed_url_set: set[str] = set()
    search_page_count = 0
    result_candidate_count = 0
    fetched_page_count = 0
    search_page_fetch_error_count = 0
    crawled_page_count = 0
    crawl_queued_count = 0
    provider_hosts: set[str] = set()
    discovered_hosts: set[str] = set()
    crawl_hosts: set[str] = set()
    resolved_provider_names: list[str] = []

    for provider in providers:
        resolved_provider_names.append(provider.provider_name)
        result_redirect_query_param = (
            as_optional_string(metadata.get("result_redirect_query_param"))
            or provider.redirect_query_param
        )
        skip_provider_domain = bool(metadata.get("skip_provider_domain", provider.skip_provider_domain))

        for query in queries:
            candidate_rank = 0
            fetched_for_query = 0
            query_result_urls: set[str] = set()
            query_result_seeds: list[str] = []

            for page_index in range(search_page_limit):
                if len(query_result_urls) >= search_result_limit:
                    break
                search_url = build_search_request_url(
                    provider.target_uri,
                    query,
                    metadata,
                    provider=provider,
                    page_index=page_index,
                )
                payload, fetch_metadata = fetch_http_url_with_runtime_policy(
                    search_url,
                    fetch_config,
                    fetch_runtime_context=effective_fetch_runtime_context,
                    allow_robots_lookup=False,
                )
                search_page_count += 1
                provider_host = normalize_host(urlparse(search_url).netloc)
                if provider_host:
                    provider_hosts.add(provider_host)
                summary = parse_html_document(
                    decode_http_payload(payload, fetch_metadata.get("content_type")),
                    search_url,
                    text_limit=max(1000, int(metadata.get("search_page_excerpt_limit", 3000))),
                )
                remaining_results = max(0, search_result_limit - len(query_result_urls))
                candidates = filter_discovered_links(
                    summary["links"],
                    provider_host=provider_host,
                    allowed_domains=allowed_domains,
                    include_url_patterns=include_url_patterns,
                    exclude_url_patterns=exclude_url_patterns,
                    skip_provider_domain=skip_provider_domain,
                    limit=max(1, remaining_results),
                    redirect_query_param=result_redirect_query_param,
                )
                for candidate in candidates:
                    if candidate.url in query_result_urls or candidate.url in global_result_urls:
                        continue
                    candidate_rank += 1
                    query_result_urls.add(candidate.url)
                    global_result_urls.add(candidate.url)
                    result_candidate_count += 1
                    page_url = candidate.url
                    discovered_host = normalize_host(urlparse(page_url).netloc)
                    if discovered_host:
                        discovered_hosts.add(discovered_host)
                    fetch_allowed = robots_policy_allows_url(page_url, fetch_config, effective_robots_context)
                    if fetch_allowed:
                        query_result_seeds.append(page_url)
                        if page_url not in global_seed_url_set:
                            global_seed_urls.append(page_url)
                            global_seed_url_set.add(page_url)
                    else:
                        record_robots_block(effective_robots_context, page_url)
                        continue
                    if fetch_result_pages and fetched_for_query < page_fetch_limit:
                        try:
                            page_payload, page_metadata = fetch_http_url_with_runtime_policy(
                                page_url,
                                fetch_config,
                                fetch_runtime_context=effective_fetch_runtime_context,
                                robots_context=effective_robots_context,
                                extra_headers={"Referer": search_url},
                            )
                            page_summary = parse_html_document(
                                decode_http_payload(page_payload, page_metadata.get("content_type")),
                                page_url,
                                text_limit=page_text_limit,
                            )
                            fetched_page_count += 1
                            fetched_for_query += 1
                            records.append(
                                build_web_search_record(
                                    query=query,
                                    rank=candidate_rank,
                                    search_url=search_url,
                                    search_provider=provider.provider_name,
                                    search_page_number=page_index + 1,
                                    page_url=page_url,
                                    page_summary=page_summary,
                                    link_text=candidate.text,
                                    fetch_metadata=page_metadata,
                                )
                            )
                        except Exception as exc:
                            search_page_fetch_error_count += 1
                            records.append(
                                {
                                    "title": candidate.text or page_url,
                                    "text": f"Search result discovered but page fetch failed: {exc}",
                                    "url": page_url,
                                    "page_url": page_url,
                                    "source_url": search_url,
                                    "search_query": query,
                                    "search_provider": provider.provider_name,
                                    "search_rank": candidate_rank,
                                    "search_page_number": page_index + 1,
                                    "discovery_kind": "web_search_result",
                                    "fetch_error": str(exc),
                                }
                            )
                    else:
                        records.append(
                            {
                                "title": candidate.text or page_url,
                                "text": candidate.text or f"Discovered search result for query '{query}'.",
                                "url": page_url,
                                "page_url": page_url,
                                "source_url": search_url,
                                "search_query": query,
                                "search_provider": provider.provider_name,
                                "search_rank": candidate_rank,
                                "search_page_number": page_index + 1,
                                "discovery_kind": "web_search_result",
                            }
                        )
                if not candidates:
                    break

            if crawl_from_results and query_result_seeds:
                crawl_records, crawl_metadata = collect_crawl_records_from_seed_urls(
                    fetch_config=fetch_config,
                    seed_urls=query_result_seeds,
                    crawl_depth=result_crawl_depth,
                    crawl_page_limit=result_crawl_page_limit,
                    crawl_link_limit=result_crawl_link_limit,
                    same_domain_only=result_crawl_same_domain_only,
                    allowed_domains=crawl_allowed_domains,
                    include_url_patterns=crawl_include_url_patterns,
                    exclude_url_patterns=crawl_exclude_url_patterns,
                    page_text_limit=page_text_limit,
                    discovery_kind="web_search_crawl_page",
                    extra_record_fields={
                        "search_query": query,
                        "search_provider": provider.provider_name,
                    },
                    record_seed_pages=False,
                    robots_context=effective_robots_context,
                    fetch_runtime_context=effective_fetch_runtime_context,
                )
                records.extend(crawl_records)
                crawled_page_count += len(crawl_records)
                crawl_queued_count += int(crawl_metadata.get("crawl_queued_count", 0))
                crawl_hosts.update(str(item) for item in crawl_metadata.get("crawled_hosts", []))

    result_metadata = {
        "discovery_kind": "web_search",
        "search_query_count": len(queries),
        "search_provider_count": len(resolved_provider_names),
        "search_page_count": search_page_count,
        "search_provider_page_limit": search_page_limit,
        "search_result_candidate_count": result_candidate_count,
        "search_page_fetch_count": fetched_page_count,
        "search_page_fetch_error_count": search_page_fetch_error_count,
        "search_result_crawl_enabled": crawl_from_results,
        "search_result_crawl_page_count": crawled_page_count,
        "search_result_crawl_queued_count": crawl_queued_count,
        "provider_hosts": sorted(provider_hosts),
        "discovered_hosts": sorted(discovered_hosts),
        "crawl_hosts": sorted(crawl_hosts),
    }
    result_metadata.update(merge_fetch_runtime_metadata(effective_fetch_runtime_context, fetch_config))
    result_metadata.update(merge_robots_metadata(effective_robots_context))
    if len(resolved_provider_names) == 1:
        result_metadata["search_provider"] = resolved_provider_names[0]
    else:
        result_metadata["search_providers"] = sorted(set(resolved_provider_names))
    return WebSearchCollectionResult(
        records=records,
        metadata=result_metadata,
        seed_urls=global_seed_urls,
    )


def collect_web_crawl_records(
    source: SourceDefinitionORM,
    fetch_config: SourceFetchConfig,
    *,
    checkpoint: SourceCheckpointORM | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    metadata = source.metadata_json if isinstance(source.metadata_json, dict) else {}
    robots_context = build_robots_runtime_context(metadata, fetch_config)
    fetch_runtime_context = build_fetch_runtime_context()
    seed_urls = resolve_web_crawl_seed_urls(source, metadata)
    crawl_depth = max(0, int(metadata.get("crawl_depth", 1)))
    crawl_page_limit = max(0, int(metadata.get("crawl_page_limit", 25)))
    crawl_link_limit = max(1, int(metadata.get("crawl_link_limit", 50)))
    same_domain_only = bool(metadata.get("same_domain_only", True))
    allowed_domains = normalize_domain_list(metadata.get("allowed_domains"))
    include_url_patterns = normalize_string_list(metadata.get("include_url_patterns"))
    exclude_url_patterns = normalize_string_list(metadata.get("exclude_url_patterns"))
    page_text_limit = max(1000, int(metadata.get("page_text_limit", 5000)))
    resume_frontier = bool(metadata.get("resume_frontier", True))
    checkpoint_state = checkpoint.checkpoint_json if checkpoint is not None and isinstance(checkpoint.checkpoint_json, dict) else {}
    frontier = (
        parse_checkpoint_frontier_entries(checkpoint_state.get("pending_frontier"))
        if resume_frontier
        else []
    )
    seen_urls = (
        parse_checkpoint_seen_urls(checkpoint_state.get("seen_urls"))
        if resume_frontier
        else set()
    )

    records, crawl_metadata = collect_crawl_records_from_seed_urls(
        fetch_config=fetch_config,
        seed_urls=seed_urls,
        crawl_depth=crawl_depth,
        crawl_page_limit=crawl_page_limit,
        crawl_link_limit=crawl_link_limit,
        same_domain_only=same_domain_only,
        allowed_domains=allowed_domains,
        include_url_patterns=include_url_patterns,
        exclude_url_patterns=exclude_url_patterns,
        page_text_limit=page_text_limit,
        discovery_kind="web_crawl_page",
        record_seed_pages=True,
        initial_frontier=frontier,
        initial_seen_urls=seen_urls,
        robots_context=robots_context,
        fetch_runtime_context=fetch_runtime_context,
    )
    checkpoint_payload = build_discovery_checkpoint_payload(
        crawl_metadata.get("remaining_frontier", []),
        crawl_metadata.get("seen_urls", []),
        metadata,
    )
    checkpoint_cursor_text = None
    remaining_frontier = checkpoint_payload.get("pending_frontier", [])
    if remaining_frontier:
        checkpoint_cursor_text = as_optional_string(remaining_frontier[0].get("url"))
    elif records:
        checkpoint_cursor_text = as_optional_string(records[-1].get("page_url"))
    result_metadata = {
        "discovery_kind": "web_crawl",
        "crawl_depth": crawl_depth,
        "crawl_page_limit": crawl_page_limit,
        "crawl_seed_count": len(seed_urls),
        "crawl_queued_count": crawl_metadata["crawl_queued_count"],
        "crawl_page_count": len(records),
        "crawl_fetch_error_count": int(crawl_metadata.get("crawl_fetch_error_count", 0)),
        "crawled_hosts": crawl_metadata["crawled_hosts"],
        "resume_frontier": resume_frontier,
        "source_checkpoint_cursor_text": checkpoint_cursor_text,
        "source_checkpoint_json": checkpoint_payload,
        "checkpoint_state_sha256": build_checkpoint_state_hash(
            cursor_text=checkpoint_cursor_text,
            last_event_id=None,
            last_offset=None,
            checkpoint_json=checkpoint_payload,
        ),
    }
    result_metadata.update(merge_fetch_runtime_metadata(fetch_runtime_context, fetch_config))
    result_metadata.update(merge_robots_metadata(robots_context))
    return records, result_metadata


def collect_generic_web_discovery_records(
    source: SourceDefinitionORM,
    fetch_config: SourceFetchConfig,
    *,
    checkpoint: SourceCheckpointORM | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    metadata = source.metadata_json if isinstance(source.metadata_json, dict) else {}
    robots_context = build_robots_runtime_context(metadata, fetch_config)
    fetch_runtime_context = build_fetch_runtime_context()
    crawl_depth = max(0, int(metadata.get("crawl_depth", 1)))
    crawl_page_limit = max(0, int(metadata.get("crawl_page_limit", 50)))
    crawl_link_limit = max(1, int(metadata.get("crawl_link_limit", 50)))
    same_domain_only = bool(metadata.get("same_domain_only", False))
    allowed_domains = normalize_domain_list(metadata.get("allowed_domains"))
    include_url_patterns = normalize_string_list(metadata.get("include_url_patterns"))
    exclude_url_patterns = normalize_string_list(metadata.get("exclude_url_patterns"))
    page_text_limit = max(1000, int(metadata.get("page_text_limit", 5000)))
    resume_frontier = bool(metadata.get("resume_frontier", True))
    discover_sitemaps_from_seeds = bool(metadata.get("discover_sitemaps_from_seeds", True))
    sitemap_fetch_limit = max(0, int(metadata.get("sitemap_fetch_limit", 10)))
    sitemap_url_limit = max(0, int(metadata.get("sitemap_url_limit", 250)))
    configured_seed_urls = resolve_web_crawl_seed_urls(source, metadata, include_source_target=False)
    explicit_sitemap_urls = normalize_string_list(metadata.get("sitemap_urls"))
    checkpoint_state = checkpoint.checkpoint_json if checkpoint is not None and isinstance(checkpoint.checkpoint_json, dict) else {}
    prior_frontier = (
        parse_checkpoint_frontier_entries(checkpoint_state.get("pending_frontier"))
        if resume_frontier
        else []
    )
    seen_urls = (
        parse_checkpoint_seen_urls(checkpoint_state.get("seen_urls"))
        if resume_frontier
        else set()
    )

    records: list[dict[str, Any]] = []
    crawled_hosts: set[str] = set()
    crawl_queued_count = 0
    search_seed_urls: list[str] = []
    search_metadata: dict[str, Any] = {
        "search_query_count": 0,
        "search_provider_count": 0,
        "search_page_count": 0,
        "search_result_candidate_count": 0,
        "search_page_fetch_count": 0,
        "search_page_fetch_error_count": 0,
    }
    queries_present = bool(normalize_string_list(metadata.get("queries")) or as_optional_string(metadata.get("query")))
    if queries_present:
        search_result = collect_web_search_campaign(
            source,
            fetch_config,
            metadata_override={"crawl_from_results": False},
            robots_context=robots_context,
            fetch_runtime_context=fetch_runtime_context,
        )
        records.extend(search_result.records)
        search_seed_urls = list(search_result.seed_urls)
        search_metadata = dict(search_result.metadata)

    sitemap_urls = list(explicit_sitemap_urls)
    if discover_sitemaps_from_seeds:
        sitemap_urls.extend(
            discover_sitemap_urls_from_seeds(
                [*configured_seed_urls, *search_seed_urls],
                fetch_config,
                fetch_runtime_context=fetch_runtime_context,
            )
        )
    sitemap_seed_urls, sitemap_metadata = expand_sitemap_seed_urls(
        sitemap_urls,
        fetch_config,
        allowed_domains=allowed_domains,
        include_url_patterns=include_url_patterns,
        exclude_url_patterns=exclude_url_patterns,
        max_sitemap_fetches=sitemap_fetch_limit,
        max_url_count=sitemap_url_limit,
        fetch_runtime_context=fetch_runtime_context,
    )

    combined_seed_urls = dedupe_preserving_order(
        [
            *configured_seed_urls,
            *search_seed_urls,
            *sitemap_seed_urls,
        ]
    )
    crawl_records, crawl_metadata = collect_crawl_records_from_seed_urls(
        fetch_config=fetch_config,
        seed_urls=combined_seed_urls,
        crawl_depth=crawl_depth,
        crawl_page_limit=crawl_page_limit,
        crawl_link_limit=crawl_link_limit,
        same_domain_only=same_domain_only,
        allowed_domains=allowed_domains,
        include_url_patterns=include_url_patterns,
        exclude_url_patterns=exclude_url_patterns,
        page_text_limit=page_text_limit,
        discovery_kind="web_discovery_page",
        record_seed_pages=True,
        initial_frontier=prior_frontier,
        initial_seen_urls=seen_urls,
        robots_context=robots_context,
        fetch_runtime_context=fetch_runtime_context,
    )
    records.extend(crawl_records)
    crawled_hosts.update(str(item) for item in crawl_metadata.get("crawled_hosts", []))
    crawl_queued_count += int(crawl_metadata.get("crawl_queued_count", 0))

    checkpoint_payload = build_discovery_checkpoint_payload(
        crawl_metadata.get("remaining_frontier", []),
        crawl_metadata.get("seen_urls", []),
        metadata,
    )
    checkpoint_cursor_text = None
    remaining_frontier = checkpoint_payload.get("pending_frontier", [])
    if remaining_frontier:
        checkpoint_cursor_text = as_optional_string(remaining_frontier[0].get("url"))
    elif crawl_records:
        checkpoint_cursor_text = as_optional_string(crawl_records[-1].get("page_url"))
    elif records:
        checkpoint_cursor_text = as_optional_string(records[-1].get("page_url") or records[-1].get("url"))

    discovery_metadata: dict[str, Any] = {
        "discovery_kind": "web_discovery",
        "resume_frontier": resume_frontier,
        "configured_seed_count": len(configured_seed_urls),
        "search_seed_count": len(search_seed_urls),
        "sitemap_seed_count": len(sitemap_seed_urls),
        "sitemap_url_count": len(dedupe_preserving_order(sitemap_urls)),
        "sitemap_fetch_count": sitemap_metadata.get("sitemap_fetch_count", 0),
        "crawl_depth": crawl_depth,
        "crawl_page_limit": crawl_page_limit,
        "crawl_seed_count": len(combined_seed_urls),
        "crawl_queued_count": crawl_queued_count,
        "crawl_page_count": len(crawl_records),
        "crawl_fetch_error_count": int(crawl_metadata.get("crawl_fetch_error_count", 0)),
        "crawled_hosts": sorted(crawled_hosts),
        "discovered_hosts": sorted(
            {
                normalize_host(urlparse(url).netloc)
                for url in combined_seed_urls
                if normalize_host(urlparse(url).netloc)
            }
        ),
        "source_checkpoint_cursor_text": checkpoint_cursor_text,
        "source_checkpoint_json": checkpoint_payload,
        "checkpoint_state_sha256": build_checkpoint_state_hash(
            cursor_text=checkpoint_cursor_text,
            last_event_id=None,
            last_offset=None,
            checkpoint_json=checkpoint_payload,
        ),
    }
    discovery_metadata.update(
        {
            key: value
            for key, value in search_metadata.items()
            if key not in {"discovery_kind"}
        }
    )
    if "search_provider" in search_metadata:
        discovery_metadata["search_provider"] = search_metadata["search_provider"]
    if "search_providers" in search_metadata:
        discovery_metadata["search_providers"] = search_metadata["search_providers"]
    discovery_metadata.update(merge_fetch_runtime_metadata(fetch_runtime_context, fetch_config))
    discovery_metadata.update(merge_robots_metadata(robots_context))
    return records, discovery_metadata


def resolve_web_crawl_seed_urls(
    source: SourceDefinitionORM,
    metadata: dict[str, Any],
    *,
    include_source_target: bool = True,
) -> list[str]:
    seed_urls: list[str] = []
    if include_source_target and is_http_url(source.target_uri):
        seed_urls.append(source.target_uri)
    seed_urls.extend(normalize_string_list(metadata.get("seed_urls")))
    return dedupe_preserving_order(
        [
            normalized
            for item in seed_urls
            if (normalized := normalize_html_link(item, item)) is not None
        ]
    )


def collect_crawl_records_from_seed_urls(
    *,
    fetch_config: SourceFetchConfig,
    seed_urls: list[str],
    crawl_depth: int,
    crawl_page_limit: int,
    crawl_link_limit: int,
    same_domain_only: bool,
    allowed_domains: set[str],
    include_url_patterns: list[str],
    exclude_url_patterns: list[str],
    page_text_limit: int,
    discovery_kind: str,
    extra_record_fields: dict[str, Any] | None = None,
    record_seed_pages: bool,
    initial_frontier: list[dict[str, Any]] | None = None,
    initial_seen_urls: set[str] | None = None,
    robots_context: RobotsRuntimeContext | None = None,
    fetch_runtime_context: FetchRuntimeContext | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    effective_allowed_domains = set(allowed_domains)
    if same_domain_only and not effective_allowed_domains:
        effective_allowed_domains = {
            normalize_host(urlparse(seed_url).netloc)
            for seed_url in seed_urls
            if normalize_host(urlparse(seed_url).netloc)
        }

    queue: list[dict[str, Any]] = []
    seen_urls: set[str] = set(initial_seen_urls or set())
    if initial_frontier:
        for entry in initial_frontier:
            normalized_entry = normalize_frontier_entry(entry)
            if normalized_entry is None:
                continue
            queue.append(normalized_entry)
            seen_urls.add(str(normalized_entry["url"]))
    for seed_url in seed_urls:
        normalized = normalize_html_link(seed_url, seed_url)
        if normalized and normalized not in seen_urls:
            seen_urls.add(normalized)
            queue.append(
                {
                    "url": normalized,
                    "depth": 0,
                    "parent_url": None,
                    "discovered_via": "seed_url",
                }
            )

    records: list[dict[str, Any]] = []
    crawled_hosts: set[str] = set()
    queued_count = len(queue)
    fetch_error_count = 0
    effective_robots_context = robots_context or RobotsRuntimeContext(
        enabled=False,
        user_agents=["*"],
        cache={},
        checked_origins=set(),
    )
    effective_fetch_runtime_context = fetch_runtime_context or build_fetch_runtime_context()

    while queue and len(records) < crawl_page_limit:
        current = queue.pop(0)
        page_url = str(current["url"])
        depth = int(current.get("depth", 0))
        parent_url = as_optional_string(current.get("parent_url"))
        discovered_via = as_optional_string(current.get("discovered_via"))
        host = normalize_host(urlparse(page_url).netloc)
        if host:
            crawled_hosts.add(host)
        try:
            if not robots_policy_allows_url(page_url, fetch_config, effective_robots_context):
                record_robots_block(effective_robots_context, page_url)
                continue
            payload, page_metadata = fetch_http_url_with_runtime_policy(
                page_url,
                fetch_config,
                fetch_runtime_context=effective_fetch_runtime_context,
                robots_context=effective_robots_context,
                extra_headers={"Referer": parent_url} if parent_url else None,
            )
            page_summary = parse_html_document(
                decode_http_payload(payload, page_metadata.get("content_type")),
                page_url,
                text_limit=page_text_limit,
            )
            outbound_links = filter_discovered_links(
                page_summary["links"],
                provider_host=None,
                allowed_domains=effective_allowed_domains,
                include_url_patterns=include_url_patterns,
                exclude_url_patterns=exclude_url_patterns,
                skip_provider_domain=False,
                limit=crawl_link_limit,
                redirect_query_param=None,
            )
            if record_seed_pages or depth > 0:
                record = build_web_crawl_record(
                    page_url=page_url,
                    parent_url=parent_url,
                    depth=depth,
                    page_summary=page_summary,
                    fetch_metadata=page_metadata,
                    outbound_link_count=len(outbound_links),
                    discovery_kind=discovery_kind,
                )
                if discovered_via:
                    record["discovered_via"] = discovered_via
                if extra_record_fields:
                    record.update(extra_record_fields)
                records.append(record)
            if depth >= crawl_depth:
                continue
            for link in outbound_links:
                if link.url in seen_urls:
                    continue
                seen_urls.add(link.url)
                queue.append(
                    {
                        "url": link.url,
                        "depth": depth + 1,
                        "parent_url": page_url,
                        "discovered_via": "crawl_link",
                    }
                )
                queued_count += 1
        except Exception as exc:
            fetch_error_count += 1
            record: dict[str, Any] = {
                "title": page_url,
                "text": f"Crawl fetch failed: {exc}",
                "url": page_url,
                "page_url": page_url,
                "source_url": parent_url or page_url,
                "crawl_depth": depth,
                "discovery_kind": discovery_kind,
                "fetch_error": str(exc),
            }
            if discovered_via:
                record["discovered_via"] = discovered_via
            if extra_record_fields:
                record.update(extra_record_fields)
            records.append(record)
    return records, {
        "crawl_queued_count": queued_count,
        "crawl_fetch_error_count": fetch_error_count,
        "crawled_hosts": sorted(crawled_hosts),
        "remaining_frontier": queue,
        "seen_urls": sorted(seen_urls),
        **merge_fetch_runtime_metadata(effective_fetch_runtime_context, fetch_config),
        **merge_robots_metadata(effective_robots_context),
    }


def build_search_request_url(
    target_uri: str,
    query: str,
    metadata: dict[str, Any],
    *,
    provider: WebSearchProviderProfile,
    page_index: int,
) -> str:
    offset_value = provider.search_page_start + (page_index * provider.search_page_step)
    if any(token in target_uri for token in ("{query}", "{offset}", "{page_index}", "{page_number}")):
        return (
            target_uri.replace("{query}", quote_plus(query))
            .replace("{offset}", str(offset_value))
            .replace("{page_index}", str(page_index))
            .replace("{page_number}", str(page_index + 1))
        )
    parsed = urlparse(target_uri)
    query_values = parse_qs(parsed.query, keep_blank_values=True)
    query_values[provider.query_param] = [query]
    if provider.search_page_param and (page_index > 0 or provider.search_page_start != 0):
        query_values[provider.search_page_param] = [str(offset_value)]
    query_text = urlencode(query_values, doseq=True)
    return parsed._replace(query=query_text).geturl()


def resolve_web_search_provider(
    source: SourceDefinitionORM,
    metadata: dict[str, Any],
) -> WebSearchProviderProfile:
    provider_name = resolve_web_search_provider_name(source.target_uri, metadata)
    return resolve_named_web_search_provider(
        provider_name,
        source,
        metadata,
    )


def resolve_web_search_providers(
    source: SourceDefinitionORM,
    metadata: dict[str, Any],
) -> list[WebSearchProviderProfile]:
    provider_names = normalize_string_list(metadata.get("search_providers"))
    if not provider_names:
        explicit_provider = as_optional_string(metadata.get("search_provider"))
        if explicit_provider:
            provider_names = [explicit_provider]
        elif target_uri_is_symbolic_search(source.target_uri) or source.source_kind == "web_search":
            provider_names = [resolve_web_search_provider_name(source.target_uri, metadata)]
        else:
            provider_names = ["duckduckgo_html"]
    resolved: list[WebSearchProviderProfile] = []
    seen_names: set[str] = set()
    for provider_name in provider_names:
        normalized = provider_name.strip()
        if not normalized or normalized in seen_names:
            continue
        resolved.append(resolve_named_web_search_provider(normalized, source, metadata))
        seen_names.add(normalized)
    if not resolved:
        raise ValueError("No usable web search providers were configured.")
    return resolved


def resolve_named_web_search_provider(
    provider_name: str,
    source: SourceDefinitionORM,
    metadata: dict[str, Any],
) -> WebSearchProviderProfile:
    if provider_name not in WEB_SEARCH_PROVIDER_DEFAULTS:
        raise ValueError(
            f"Unsupported web search provider '{provider_name}'. "
            f"Use one of: {', '.join(sorted(WEB_SEARCH_PROVIDER_DEFAULTS))}."
        )
    defaults = WEB_SEARCH_PROVIDER_DEFAULTS[provider_name]
    provider_target_overrides = parse_string_mapping(metadata.get("search_provider_targets"))
    configured_target = provider_target_overrides.get(provider_name) or as_optional_string(metadata.get("search_target_uri"))
    resolved_target_uri = configured_target or (
        defaults.get("target_uri") if target_uri_is_symbolic_search(source.target_uri) else source.target_uri
    )
    if not resolved_target_uri:
        raise ValueError(f"Web search provider '{provider_name}' requires a target URI.")
    return WebSearchProviderProfile(
        provider_name=provider_name,
        target_uri=resolved_target_uri,
        query_param=as_optional_string(metadata.get("query_param")) or str(defaults["query_param"]),
        redirect_query_param=as_optional_string(metadata.get("result_redirect_query_param"))
        or as_optional_string(defaults.get("redirect_query_param")),
        search_page_param=as_optional_string(metadata.get("search_page_param"))
        or as_optional_string(defaults.get("search_page_param")),
        search_page_start=int(metadata.get("search_page_start", defaults.get("search_page_start", 0))),
        search_page_step=int(metadata.get("search_page_step", defaults.get("search_page_step", 0))),
        skip_provider_domain=bool(metadata.get("skip_provider_domain", defaults.get("skip_provider_domain", True))),
    )


def list_supported_web_search_providers() -> list[dict[str, Any]]:
    providers: list[dict[str, Any]] = []
    for provider_name in sorted(WEB_SEARCH_PROVIDER_DEFAULTS):
        defaults = WEB_SEARCH_PROVIDER_DEFAULTS[provider_name]
        providers.append(
            {
                "provider_name": provider_name,
                "default_target_uri": defaults.get("target_uri"),
                "query_param": defaults["query_param"],
                "redirect_query_param": defaults.get("redirect_query_param"),
                "search_page_param": defaults.get("search_page_param"),
                "search_page_start": int(defaults.get("search_page_start", 0)),
                "search_page_step": int(defaults.get("search_page_step", 0)),
                "skip_provider_domain": bool(defaults.get("skip_provider_domain", True)),
            }
        )
    return providers


def resolve_web_search_provider_name(target_uri: str, metadata: dict[str, Any]) -> str:
    explicit = as_optional_string(metadata.get("search_provider"))
    if explicit:
        return explicit
    if not target_uri_is_symbolic_search(target_uri):
        return "generic_html"
    parsed = urlparse(target_uri)
    symbolic_name = parsed.path.strip("/")
    return symbolic_name or "duckduckgo_html"


def target_uri_is_symbolic_search(target_uri: str) -> bool:
    parsed = urlparse(target_uri)
    return parsed.scheme == "search" and parsed.netloc == "web"


def resolve_search_queries(metadata: dict[str, Any]) -> list[str]:
    queries = normalize_string_list(metadata.get("queries"))
    if queries:
        return queries
    query = as_optional_string(metadata.get("query"))
    if query:
        return [query]
    raise ValueError("Web search sources require metadata_json.query or metadata_json.queries.")


def normalize_frontier_entry(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    url = as_optional_string(value.get("url"))
    if not url:
        return None
    normalized_url = normalize_html_link(url, url)
    if normalized_url is None:
        return None
    depth_value = value.get("depth", 0)
    try:
        depth = max(0, int(depth_value))
    except (TypeError, ValueError):
        depth = 0
    return {
        "url": normalized_url,
        "depth": depth,
        "parent_url": as_optional_string(value.get("parent_url")),
        "discovered_via": as_optional_string(value.get("discovered_via")),
    }


def parse_checkpoint_frontier_entries(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    entries: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for item in value:
        normalized = normalize_frontier_entry(item)
        if normalized is None or str(normalized["url"]) in seen_urls:
            continue
        entries.append(normalized)
        seen_urls.add(str(normalized["url"]))
    return entries


def parse_checkpoint_seen_urls(value: Any) -> set[str]:
    if not isinstance(value, list):
        return set()
    normalized_urls: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            continue
        normalized = normalize_html_link(item, item)
        if normalized is not None:
            normalized_urls.add(normalized)
    return normalized_urls


def build_discovery_checkpoint_payload(
    remaining_frontier: Any,
    seen_urls: Any,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    frontier_entries = parse_checkpoint_frontier_entries(remaining_frontier)
    seen_url_set = parse_checkpoint_seen_urls(seen_urls)
    pending_limit = max(50, int(metadata.get("checkpoint_pending_url_limit", 1000)))
    seen_limit = max(100, int(metadata.get("checkpoint_seen_url_limit", 5000)))
    return {
        "pending_frontier": frontier_entries[:pending_limit],
        "seen_urls": sorted(seen_url_set)[:seen_limit],
    }


def build_checkpoint_state_hash(
    *,
    cursor_text: str | None,
    last_event_id: str | None,
    last_offset: int | None,
    checkpoint_json: dict[str, Any],
) -> str:
    return hashlib.sha256(
        json.dumps(
            {
                "cursor_text": cursor_text,
                "last_event_id": last_event_id,
                "last_offset": last_offset,
                "checkpoint_json": checkpoint_json,
            },
            sort_keys=True,
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def apply_materialized_checkpoint_state(
    run: SourceRunORM,
    checkpoint: SourceCheckpointORM,
    metadata: dict[str, Any],
) -> None:
    if "source_checkpoint_cursor_text" in metadata:
        cursor_text = as_optional_string(metadata.get("source_checkpoint_cursor_text"))
        run.cursor_text = cursor_text
        checkpoint.cursor_text = cursor_text
    if "source_checkpoint_last_event_id" in metadata:
        last_event_id = as_optional_string(metadata.get("source_checkpoint_last_event_id"))
        run.last_event_id = last_event_id
        checkpoint.last_event_id = last_event_id
    if "source_checkpoint_last_offset" in metadata:
        raw_last_offset = metadata.get("source_checkpoint_last_offset")
        last_offset = int(raw_last_offset) if raw_last_offset is not None else None
        run.last_offset = last_offset
        checkpoint.last_offset = last_offset
    checkpoint_json = metadata.get("source_checkpoint_json")
    if isinstance(checkpoint_json, dict):
        run.checkpoint_json = dict(checkpoint_json)
        checkpoint.checkpoint_json = dict(checkpoint_json)


def fetch_http_url(
    url: str,
    fetch_config: SourceFetchConfig,
    *,
    extra_headers: dict[str, str] | None = None,
    fetch_runtime_context: FetchRuntimeContext | None = None,
) -> tuple[bytes, dict[str, Any]]:
    last_error: Exception | None = None
    request_headers = dict(fetch_config.headers)
    if isinstance(extra_headers, dict):
        request_headers.update({str(key): str(value) for key, value in extra_headers.items()})
    enforce_fetch_target_network_policy(url, fetch_config, fetch_runtime_context=fetch_runtime_context)
    for attempt in range(1, fetch_config.retry_attempts + 1):
        request = Request(url, headers=request_headers)
        try:
            with urlopen(request, timeout=fetch_config.timeout_seconds) as response:
                payload = response.read(fetch_config.max_payload_bytes + 1)
                if len(payload) > fetch_config.max_payload_bytes:
                    raise RuntimeError(
                        f"HTTP source payload exceeded max_payload_bytes={fetch_config.max_payload_bytes}."
                    )
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
                    "max_payload_bytes": fetch_config.max_payload_bytes,
                    "allow_private_networks": fetch_config.allow_private_networks,
                    "headers": request_headers,
                    "host": urlparse(url).netloc,
                    "requested_url": url,
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
    raise RuntimeError(f"HTTP source fetch failed after {fetch_config.retry_attempts} attempts: {last_error}") from last_error


def fetch_http_url_with_runtime_policy(
    url: str,
    fetch_config: SourceFetchConfig,
    *,
    fetch_runtime_context: FetchRuntimeContext,
    robots_context: RobotsRuntimeContext | None = None,
    allow_robots_lookup: bool = True,
    extra_headers: dict[str, str] | None = None,
) -> tuple[bytes, dict[str, Any]]:
    apply_fetch_request_pacing(
        url,
        fetch_config,
        fetch_runtime_context,
        robots_context=robots_context,
        allow_robots_lookup=allow_robots_lookup,
    )
    return fetch_http_url(
        url,
        fetch_config,
        extra_headers=extra_headers,
        fetch_runtime_context=fetch_runtime_context,
    )


def decode_http_payload(payload: bytes, content_type: Any) -> str:
    if isinstance(content_type, str):
        lower = content_type.lower()
        if "charset=" in lower:
            charset = lower.split("charset=", 1)[1].split(";", 1)[0].strip()
            try:
                return payload.decode(charset, errors="replace")
            except LookupError:
                pass
    return payload.decode("utf-8", errors="replace")


def parse_html_document(html_text: str, base_url: str, *, text_limit: int) -> dict[str, Any]:
    parser = HTMLDocumentParser(base_url=base_url, text_limit=text_limit)
    parser.feed(html_text)
    parser.close()
    return parser.as_summary()


def normalize_html_link(base_url: str, href: str) -> str | None:
    stripped = href.strip()
    if not stripped or stripped.startswith("#") or stripped.startswith("javascript:") or stripped.startswith("mailto:"):
        return None
    resolved = urljoin(base_url, stripped)
    parsed = urlparse(resolved)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return parsed._replace(fragment="").geturl()


def filter_discovered_links(
    links: list[ExtractedLink],
    *,
    provider_host: str | None,
    allowed_domains: set[str],
    include_url_patterns: list[str],
    exclude_url_patterns: list[str],
    skip_provider_domain: bool,
    limit: int,
    redirect_query_param: str | None,
) -> list[ExtractedLink]:
    filtered: list[ExtractedLink] = []
    seen_urls: set[str] = set()
    for link in links:
        candidate_url = unwrap_redirect_target(link.url, redirect_query_param)
        if candidate_url in seen_urls:
            continue
        host = normalize_host(urlparse(candidate_url).netloc)
        if host is None:
            continue
        if skip_provider_domain and provider_host and host == provider_host:
            continue
        if allowed_domains and host not in allowed_domains:
            continue
        if include_url_patterns and not any(re.search(pattern, candidate_url) for pattern in include_url_patterns):
            continue
        if exclude_url_patterns and any(re.search(pattern, candidate_url) for pattern in exclude_url_patterns):
            continue
        filtered.append(ExtractedLink(url=candidate_url, text=link.text))
        seen_urls.add(candidate_url)
        if len(filtered) >= limit:
            break
    return filtered


def normalize_domain_list(value: Any) -> set[str]:
    return {item for item in (normalize_host(entry) for entry in normalize_string_list(value)) if item}


def normalize_string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        stripped = value.strip()
        return [stripped] if stripped else []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def build_robots_runtime_context(
    metadata: dict[str, Any],
    fetch_config: SourceFetchConfig,
) -> RobotsRuntimeContext:
    configured_user_agents = normalize_string_list(metadata.get("robots_user_agents"))
    request_user_agent = as_optional_string(fetch_config.headers.get("User-Agent"))
    normalized_agents: list[str] = []
    if configured_user_agents:
        normalized_agents.extend(configured_user_agents)
    elif request_user_agent:
        normalized_agents.append(request_user_agent.split(" ", 1)[0].strip())
    normalized_agents.append("*")
    deduped_agents = dedupe_preserving_order([agent for agent in normalized_agents if agent])
    return RobotsRuntimeContext(
        enabled=bool(metadata.get("respect_robots_txt", True)),
        user_agents=deduped_agents or ["*"],
        cache={},
        checked_origins=set(),
    )


def build_fetch_runtime_context() -> FetchRuntimeContext:
    return FetchRuntimeContext()


def merge_fetch_runtime_metadata(
    context: FetchRuntimeContext,
    fetch_config: SourceFetchConfig,
) -> dict[str, Any]:
    return {
        "fetch_max_payload_bytes": fetch_config.max_payload_bytes,
        "fetch_private_networks_allowed": fetch_config.allow_private_networks,
        "fetch_min_request_interval_seconds": fetch_config.min_request_interval_seconds,
        "fetch_paced_request_count": context.paced_request_count,
        "fetch_pacing_delay_seconds": round(context.pacing_delay_applied_seconds, 6),
        "fetch_private_network_blocked_count": context.private_network_blocked_count,
        "fetch_private_network_blocked_urls_sample": list(context.private_network_blocked_urls_sample),
    }


def merge_robots_metadata(context: RobotsRuntimeContext) -> dict[str, Any]:
    return {
        "robots_policy_enabled": context.enabled,
        "robots_origin_count": len(context.checked_origins),
        "robots_blocked_count": context.blocked_url_count,
        "robots_missing_policy_count": context.missing_policy_count,
        "robots_user_agents": list(context.user_agents),
        "robots_blocked_urls_sample": list(context.blocked_urls_sample or []),
    }


def parse_string_mapping(value: Any) -> dict[str, str]:
    if isinstance(value, dict):
        return {
            str(key).strip(): str(item).strip()
            for key, item in value.items()
            if str(key).strip() and str(item).strip()
        }
    if isinstance(value, list):
        parsed: dict[str, str] = {}
        for item in value:
            if not isinstance(item, str) or "=" not in item:
                continue
            key, mapped_value = item.split("=", 1)
            normalized_key = key.strip()
            normalized_value = mapped_value.strip()
            if normalized_key and normalized_value:
                parsed[normalized_key] = normalized_value
        return parsed
    return {}


def normalize_host(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip().lower()
    if not normalized:
        return None
    if ":" in normalized:
        normalized = normalized.split(":", 1)[0]
    return normalized


def dedupe_preserving_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def is_http_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def unwrap_redirect_target(url: str, redirect_query_param: str | None) -> str:
    if not redirect_query_param:
        return url
    parsed = urlparse(url)
    query_values = parse_qs(parsed.query).get(redirect_query_param, [])
    if not query_values:
        return url
    candidate = query_values[0].strip()
    normalized = normalize_html_link(url, candidate)
    return normalized or url


def fetch_robots_policy(
    origin: str,
    fetch_config: SourceFetchConfig,
    *,
    user_agents: list[str],
) -> RobotsPolicy | None:
    robots_url = f"{origin.rstrip('/')}/robots.txt"
    try:
        payload, fetch_metadata = fetch_http_url(robots_url, fetch_config)
    except Exception:
        return None
    content_text = decode_http_payload(payload, fetch_metadata.get("content_type"))
    return parse_robots_policy_document(
        content_text,
        origin=origin,
        robots_url=robots_url,
        user_agents=user_agents,
    )


def parse_robots_policy_document(
    content_text: str,
    *,
    origin: str,
    robots_url: str,
    user_agents: list[str],
) -> RobotsPolicy | None:
    groups: list[dict[str, Any]] = []
    current_agents: list[str] = []
    current_rules: list[RobotsRule] = []
    current_crawl_delay: float | None = None

    def flush_group() -> None:
        nonlocal current_agents, current_rules, current_crawl_delay
        if not current_agents:
            current_rules = []
            current_crawl_delay = None
            return
        groups.append(
            {
                "user_agents": [agent.lower() for agent in current_agents if agent.strip()],
                "rules": tuple(current_rules),
                "crawl_delay_seconds": current_crawl_delay,
            }
        )
        current_agents = []
        current_rules = []
        current_crawl_delay = None

    for raw_line in content_text.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        normalized_key = key.strip().lower()
        normalized_value = value.strip()
        if normalized_key == "user-agent":
            if current_agents and current_rules:
                flush_group()
            current_agents.append(normalized_value)
            continue
        if not current_agents:
            continue
        if normalized_key == "allow":
            current_rules.append(RobotsRule(pattern=normalized_value, allow=True))
        elif normalized_key == "disallow":
            current_rules.append(RobotsRule(pattern=normalized_value, allow=False))
        elif normalized_key == "crawl-delay":
            try:
                current_crawl_delay = float(normalized_value)
            except ValueError:
                continue
    flush_group()

    if not groups:
        return None

    requested_agents = [agent.lower() for agent in user_agents if agent.strip()]
    matched_groups: list[dict[str, Any]] = []
    matched_user_agent = "*"
    best_specificity = -1
    for requested_agent in requested_agents:
        for group in groups:
            for group_agent in group["user_agents"]:
                if group_agent == "*":
                    if best_specificity < 0:
                        matched_groups.append(group)
                    continue
                if requested_agent.startswith(group_agent):
                    specificity = len(group_agent)
                    if specificity > best_specificity:
                        matched_groups = [group]
                        matched_user_agent = group_agent
                        best_specificity = specificity
                    elif specificity == best_specificity:
                        matched_groups.append(group)
    if best_specificity < 0:
        matched_groups = [group for group in groups if "*" in group["user_agents"]]

    merged_rules: list[RobotsRule] = []
    crawl_delay_seconds: float | None = None
    for group in matched_groups:
        merged_rules.extend(list(group["rules"]))
        if crawl_delay_seconds is None and isinstance(group.get("crawl_delay_seconds"), (int, float)):
            crawl_delay_seconds = float(group["crawl_delay_seconds"])
    if not matched_groups:
        return None
    return RobotsPolicy(
        origin=origin,
        source_url=robots_url,
        matched_user_agent=matched_user_agent,
        crawl_delay_seconds=crawl_delay_seconds,
        rules=tuple(merged_rules),
    )


def get_robots_policy_for_url(
    url: str,
    fetch_config: SourceFetchConfig,
    context: RobotsRuntimeContext,
) -> RobotsPolicy | None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    origin = f"{parsed.scheme}://{parsed.netloc}"
    if origin in context.cache:
        return context.cache[origin]
    context.checked_origins.add(origin)
    policy = fetch_robots_policy(origin, fetch_config, user_agents=context.user_agents)
    if policy is None:
        context.missing_policy_count += 1
    context.cache[origin] = policy
    return policy


def robots_policy_allows_url(
    url: str,
    fetch_config: SourceFetchConfig,
    context: RobotsRuntimeContext,
) -> bool:
    if not context.enabled:
        return True
    policy = get_robots_policy_for_url(url, fetch_config, context)
    if policy is None:
        return True
    return evaluate_robots_policy_match(url, policy)


def evaluate_robots_policy_match(url: str, policy: RobotsPolicy) -> bool:
    parsed = urlparse(url)
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    winning_rule: tuple[int, bool] | None = None
    for rule in policy.rules:
        pattern = rule.pattern.strip()
        if not pattern:
            continue
        if not robots_pattern_matches(path, pattern):
            continue
        specificity = len(pattern.rstrip("$"))
        if winning_rule is None or specificity > winning_rule[0] or (
            specificity == winning_rule[0] and rule.allow
        ):
            winning_rule = (specificity, rule.allow)
    if winning_rule is None:
        return True
    return winning_rule[1]


def robots_pattern_matches(path: str, pattern: str) -> bool:
    normalized_pattern = pattern.strip()
    if not normalized_pattern:
        return False
    anchored_to_end = normalized_pattern.endswith("$")
    if anchored_to_end:
        normalized_pattern = normalized_pattern[:-1]
    expression = "^" + re.escape(normalized_pattern).replace(r"\*", ".*")
    if anchored_to_end:
        expression += "$"
    return re.match(expression, path) is not None


def record_robots_block(
    context: RobotsRuntimeContext,
    url: str,
) -> None:
    context.blocked_url_count += 1
    if len(context.blocked_urls_sample or []) < 10:
        assert context.blocked_urls_sample is not None
        context.blocked_urls_sample.append(url)


def apply_fetch_request_pacing(
    url: str,
    fetch_config: SourceFetchConfig,
    context: FetchRuntimeContext,
    *,
    robots_context: RobotsRuntimeContext | None = None,
    allow_robots_lookup: bool = True,
) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return
    origin = f"{parsed.scheme}://{parsed.netloc}"
    min_interval_seconds = fetch_config.min_request_interval_seconds
    if allow_robots_lookup and robots_context is not None and robots_context.enabled:
        policy = get_robots_policy_for_url(url, fetch_config, robots_context)
        if policy is not None and policy.crawl_delay_seconds is not None:
            min_interval_seconds = max(min_interval_seconds, policy.crawl_delay_seconds)
    if min_interval_seconds <= 0:
        context.last_request_started_at_by_origin[origin] = time.monotonic()
        return
    last_started_at = context.last_request_started_at_by_origin.get(origin)
    now_monotonic = time.monotonic()
    if last_started_at is not None:
        elapsed = now_monotonic - last_started_at
        remaining = min_interval_seconds - elapsed
        if remaining > 0:
            time.sleep(remaining)
            context.paced_request_count += 1
            context.pacing_delay_applied_seconds += remaining
    context.last_request_started_at_by_origin[origin] = time.monotonic()


def enforce_fetch_target_network_policy(
    url: str,
    fetch_config: SourceFetchConfig,
    *,
    fetch_runtime_context: FetchRuntimeContext | None = None,
) -> None:
    if fetch_config.allow_private_networks:
        return
    parsed = urlparse(url)
    host = parsed.hostname
    if not host:
        raise RuntimeError("HTTP source target URI is missing a host.")
    if host.strip().lower() == "localhost":
        record_private_network_block(fetch_runtime_context, url)
        raise RuntimeError(
            "HTTP source target resolves to a private or non-public network address while allow_private_networks=false."
        )
    try:
        addr_info = socket.getaddrinfo(host, parsed.port or 0, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise RuntimeError(f"HTTP source target host resolution failed for '{host}': {exc}") from exc
    candidate_ips: list[ipaddress.IPv4Address | ipaddress.IPv6Address] = []
    for entry in addr_info:
        sockaddr = entry[4]
        if not sockaddr:
            continue
        raw_ip = sockaddr[0]
        try:
            candidate_ips.append(ipaddress.ip_address(raw_ip))
        except ValueError:
            continue
    if not candidate_ips:
        raise RuntimeError(f"HTTP source target host '{host}' did not resolve to a usable IP address.")
    if any(not candidate_ip.is_global for candidate_ip in candidate_ips):
        record_private_network_block(fetch_runtime_context, url)
        raise RuntimeError(
            "HTTP source target resolves to a private or non-public network address while allow_private_networks=false."
        )


def record_private_network_block(
    context: FetchRuntimeContext | None,
    url: str,
) -> None:
    if context is None:
        return
    context.private_network_blocked_count += 1
    if len(context.private_network_blocked_urls_sample) < 10:
        context.private_network_blocked_urls_sample.append(url)


def discover_sitemap_urls_from_seeds(
    seed_urls: list[str],
    fetch_config: SourceFetchConfig,
    *,
    fetch_runtime_context: FetchRuntimeContext | None = None,
) -> list[str]:
    sitemap_urls: list[str] = []
    seen_urls: set[str] = set()
    origins = dedupe_preserving_order(
        [
            f"{parsed.scheme}://{parsed.netloc}"
            for seed_url in seed_urls
            if is_http_url(seed_url)
            for parsed in [urlparse(seed_url)]
        ]
    )
    for origin in origins:
        parsed_origin = urlparse(origin)
        candidate_urls = [
            f"{origin.rstrip('/')}/robots.txt",
            f"{origin.rstrip('/')}/sitemap.xml",
        ]
        for candidate_url in candidate_urls:
            if candidate_url in seen_urls:
                continue
            seen_urls.add(candidate_url)
            try:
                if fetch_runtime_context is None:
                    payload, fetch_metadata = fetch_http_url(candidate_url, fetch_config)
                else:
                    payload, fetch_metadata = fetch_http_url_with_runtime_policy(
                        candidate_url,
                        fetch_config,
                        fetch_runtime_context=fetch_runtime_context,
                        allow_robots_lookup=False,
                    )
            except Exception:
                continue
            content_text = decode_http_payload(payload, fetch_metadata.get("content_type"))
            if candidate_url.endswith("/robots.txt"):
                for line in content_text.splitlines():
                    if ":" not in line:
                        continue
                    key, value = line.split(":", 1)
                    if key.strip().lower() != "sitemap":
                        continue
                    sitemap_url = normalize_html_link(
                        f"{parsed_origin.scheme}://{parsed_origin.netloc}/",
                        value.strip(),
                    )
                    if sitemap_url is not None:
                        sitemap_urls.append(sitemap_url)
            else:
                sitemap_url = normalize_html_link(candidate_url, candidate_url)
                if sitemap_url is not None:
                    sitemap_urls.append(sitemap_url)
    return dedupe_preserving_order(sitemap_urls)


def expand_sitemap_seed_urls(
    sitemap_urls: list[str],
    fetch_config: SourceFetchConfig,
    *,
    allowed_domains: set[str],
    include_url_patterns: list[str],
    exclude_url_patterns: list[str],
    max_sitemap_fetches: int,
    max_url_count: int,
    fetch_runtime_context: FetchRuntimeContext | None = None,
) -> tuple[list[str], dict[str, Any]]:
    if max_sitemap_fetches <= 0 or max_url_count <= 0:
        return [], {"sitemap_fetch_count": 0}
    queue = dedupe_preserving_order(
        [
            normalized
            for item in sitemap_urls
            if (normalized := normalize_html_link(item, item)) is not None
        ]
    )
    seen_sitemaps: set[str] = set()
    discovered_page_urls: list[str] = []
    fetch_count = 0
    while queue and fetch_count < max_sitemap_fetches and len(discovered_page_urls) < max_url_count:
        sitemap_url = queue.pop(0)
        if sitemap_url in seen_sitemaps:
            continue
        seen_sitemaps.add(sitemap_url)
        try:
            if fetch_runtime_context is None:
                payload, fetch_metadata = fetch_http_url(sitemap_url, fetch_config)
            else:
                payload, fetch_metadata = fetch_http_url_with_runtime_policy(
                    sitemap_url,
                    fetch_config,
                    fetch_runtime_context=fetch_runtime_context,
                    allow_robots_lookup=False,
                )
            fetch_count += 1
            xml_text = decode_http_payload(payload, fetch_metadata.get("content_type"))
            child_sitemaps, page_urls = parse_sitemap_document(xml_text, sitemap_url)
            for child_sitemap in child_sitemaps:
                if child_sitemap not in seen_sitemaps:
                    queue.append(child_sitemap)
            remaining = max(0, max_url_count - len(discovered_page_urls))
            discovered_links = filter_discovered_links(
                [ExtractedLink(url=url, text=url) for url in page_urls],
                provider_host=None,
                allowed_domains=allowed_domains,
                include_url_patterns=include_url_patterns,
                exclude_url_patterns=exclude_url_patterns,
                skip_provider_domain=False,
                limit=max(1, remaining),
                redirect_query_param=None,
            )
            for link in discovered_links:
                if link.url not in discovered_page_urls:
                    discovered_page_urls.append(link.url)
                if len(discovered_page_urls) >= max_url_count:
                    break
        except Exception:
            fetch_count += 1
            continue
    return discovered_page_urls, {"sitemap_fetch_count": fetch_count}


def parse_sitemap_document(xml_text: str, base_url: str) -> tuple[list[str], list[str]]:
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError:
        return [], []
    tag_name = root.tag.rsplit("}", 1)[-1].lower()
    if tag_name == "sitemapindex":
        sitemap_urls: list[str] = []
        for child in root:
            if child.tag.rsplit("}", 1)[-1].lower() != "sitemap":
                continue
            loc_text = first_xml_child_text(child, "loc")
            if not loc_text:
                continue
            normalized = normalize_html_link(base_url, loc_text)
            if normalized is not None:
                sitemap_urls.append(normalized)
        return dedupe_preserving_order(sitemap_urls), []
    if tag_name == "urlset":
        page_urls: list[str] = []
        for child in root:
            if child.tag.rsplit("}", 1)[-1].lower() != "url":
                continue
            loc_text = first_xml_child_text(child, "loc")
            if not loc_text:
                continue
            normalized = normalize_html_link(base_url, loc_text)
            if normalized is not None:
                page_urls.append(normalized)
        return [], dedupe_preserving_order(page_urls)
    return [], []


def first_xml_child_text(element: ElementTree.Element, child_name: str) -> str | None:
    target_name = child_name.lower()
    for child in element:
        if child.tag.rsplit("}", 1)[-1].lower() != target_name:
            continue
        if child.text and child.text.strip():
            return child.text.strip()
    return None


def extract_discovery_coordinates(page_summary: dict[str, Any]) -> tuple[float | None, float | None]:
    meta_tags = page_summary.get("meta_tags")
    if not isinstance(meta_tags, dict):
        return (None, None)

    latitude = first_coordinate_value(
        meta_tags,
        "geo.position.latitude",
        "place:location:latitude",
        "geo:lat",
        "geo.latitude",
        "latitude",
        "lat",
    )
    longitude = first_coordinate_value(
        meta_tags,
        "geo.position.longitude",
        "place:location:longitude",
        "geo:lon",
        "geo:lng",
        "geo.longitude",
        "longitude",
        "lon",
        "lng",
    )
    if latitude is not None and longitude is not None and valid_lat_lon(latitude, longitude):
        return (latitude, longitude)

    for compound_key in ("geo.position", "icbm", "coordinates", "geo.coordinates"):
        raw_value = meta_tags.get(compound_key)
        if not isinstance(raw_value, str):
            continue
        pair = parse_coordinate_pair(raw_value)
        if pair is not None:
            return pair
    return (None, None)


def first_coordinate_value(meta_tags: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        candidate = normalize_coordinate(meta_tags.get(key))
        if candidate is not None:
            return candidate
    return None


def parse_coordinate_pair(value: str) -> tuple[float, float] | None:
    tokens = [token.strip() for token in re.split(r"[;, ]+", value) if token.strip()]
    if len(tokens) < 2:
        return None
    latitude = normalize_coordinate(tokens[0])
    longitude = normalize_coordinate(tokens[1])
    if latitude is None or longitude is None or not valid_lat_lon(latitude, longitude):
        return None
    return (latitude, longitude)


def valid_lat_lon(latitude: float, longitude: float) -> bool:
    return -90.0 <= latitude <= 90.0 and -180.0 <= longitude <= 180.0


def extract_discovery_observed_at(page_summary: dict[str, Any]) -> str | None:
    meta_tags = page_summary.get("meta_tags")
    if isinstance(meta_tags, dict):
        for key in (
            "article:published_time",
            "article:modified_time",
            "og:updated_time",
            "published_at",
            "pubdate",
            "timestamp",
            "datetime",
            "datepublished",
        ):
            normalized = normalize_observed_at_value(meta_tags.get(key))
            if normalized:
                return normalized
    time_values = page_summary.get("time_values")
    if isinstance(time_values, list):
        for value in time_values:
            normalized = normalize_observed_at_value(value)
            if normalized:
                return normalized
    text_excerpt = page_summary.get("text_excerpt")
    if isinstance(text_excerpt, str):
        match = re.search(r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:\d{2})?\b", text_excerpt)
        if match:
            normalized = normalize_observed_at_value(match.group(0))
            if normalized:
                return normalized
    return None


def normalize_observed_at_value(value: Any) -> str | None:
    normalized = parse_timestamp_value(value)
    if normalized is None:
        return None
    return normalized.isoformat().replace("+00:00", "Z")


def build_web_search_record(
    *,
    query: str,
    rank: int,
    search_url: str,
    search_provider: str,
    search_page_number: int,
    page_url: str,
    page_summary: dict[str, Any],
    link_text: str,
    fetch_metadata: dict[str, Any],
) -> dict[str, Any]:
    title = as_optional_string(page_summary.get("title")) or link_text or page_url
    text = (
        as_optional_string(page_summary.get("meta_description"))
        or as_optional_string(page_summary.get("text_excerpt"))
        or link_text
        or page_url
    )
    latitude, longitude = extract_discovery_coordinates(page_summary)
    observed_at = extract_discovery_observed_at(page_summary)
    record = {
        "title": title,
        "text": text,
        "url": page_url,
        "page_url": page_url,
        "source_url": search_url,
        "search_query": query,
        "search_provider": search_provider,
        "search_rank": rank,
        "search_page_number": search_page_number,
        "discovery_kind": "web_search_result",
        "meta_description": as_optional_string(page_summary.get("meta_description")),
        "page_title": as_optional_string(page_summary.get("title")),
        "content_type": fetch_metadata.get("content_type"),
        "http_status": fetch_metadata.get("http_status"),
    }
    if latitude is not None and longitude is not None:
        record["lat"] = latitude
        record["lon"] = longitude
    if observed_at is not None:
        record["observed_at"] = observed_at
    return record


def build_web_crawl_record(
    *,
    page_url: str,
    parent_url: str | None,
    depth: int,
    page_summary: dict[str, Any],
    fetch_metadata: dict[str, Any],
    outbound_link_count: int,
    discovery_kind: str = "web_crawl_page",
) -> dict[str, Any]:
    title = as_optional_string(page_summary.get("title")) or page_url
    text = (
        as_optional_string(page_summary.get("meta_description"))
        or as_optional_string(page_summary.get("text_excerpt"))
        or page_url
    )
    latitude, longitude = extract_discovery_coordinates(page_summary)
    observed_at = extract_discovery_observed_at(page_summary)
    record = {
        "title": title,
        "text": text,
        "url": page_url,
        "page_url": page_url,
        "source_url": parent_url or page_url,
        "crawl_depth": depth,
        "discovery_kind": discovery_kind,
        "meta_description": as_optional_string(page_summary.get("meta_description")),
        "page_title": as_optional_string(page_summary.get("title")),
        "content_type": fetch_metadata.get("content_type"),
        "http_status": fetch_metadata.get("http_status"),
        "outbound_link_count": outbound_link_count,
    }
    if latitude is not None and longitude is not None:
        record["lat"] = latitude
        record["lon"] = longitude
    if observed_at is not None:
        record["observed_at"] = observed_at
    return record


def collect_runtime_stream_batch(
    session: Session,
    *,
    source: SourceDefinitionORM,
    checkpoint: SourceCheckpointORM,
    run: SourceRunORM,
    actor: str,
) -> RuntimeStreamBatch:
    metadata = source.metadata_json if isinstance(source.metadata_json, dict) else {}
    fetch_config = parse_fetch_config(source)
    max_records = max(1, int(metadata.get("stream_max_records", 100)))
    idle_timeout_seconds = max(0.1, float(metadata.get("stream_idle_timeout_seconds", 1.0)))
    write_format = str(metadata.get("stream_write_format", "json")).strip().lower()
    if write_format not in {"json", "jsonl"}:
        write_format = "json"

    if source.source_kind == "sse_stream":
        records, transport_metadata = consume_sse_records(
            source=source,
            checkpoint=checkpoint,
            fetch_config=fetch_config,
            max_records=max_records,
            idle_timeout_seconds=idle_timeout_seconds,
        )
    elif source.source_kind == "websocket_stream":
        records, transport_metadata = consume_websocket_records(
            source=source,
            checkpoint=checkpoint,
            fetch_config=fetch_config,
            max_records=max_records,
            idle_timeout_seconds=idle_timeout_seconds,
        )
    else:
        raise ValueError(f"Unsupported runtime stream kind '{source.source_kind}'.")

    envelopes: list[IngestionEnvelope] = []
    serialized_records: list[dict[str, Any]] = []
    last_event_id = checkpoint.last_event_id
    last_offset = checkpoint.last_offset if checkpoint.last_offset is not None else -1
    records_seen = 0
    records_failed = 0
    latest_cursor = checkpoint.cursor_text

    for item in records:
        raw_text = item["raw_text"]
        event_id = as_optional_string(item.get("event_id"))
        event_name = as_optional_string(item.get("event_name"))
        latest_offset = int(item["offset"])
        latest_cursor = event_id or str(latest_offset)
        records_seen += 1
        try:
            payload = normalize_stream_record_payload(
                raw_text,
                source=source,
                event_id=event_id,
                event_name=event_name,
                offset=latest_offset,
            )
            envelopes.append(normalize_ingestion_payload(payload, "json", source_type=source.source_kind))
            serialized_records.append(payload)
            last_offset = latest_offset
            if event_id:
                last_event_id = event_id
        except Exception as exc:
            records_failed += 1
            last_offset = latest_offset
            if event_id:
                last_event_id = event_id
            record_source_dead_letter(
                session,
                source=source,
                run=run,
                stage="stream_parse",
                failure_reason=str(exc),
                raw_payload_text=raw_text,
                payload_json={
                    "event_id": event_id,
                    "event_name": event_name,
                    "offset": latest_offset,
                },
                actor=actor,
            )

    suffix = ".jsonl" if write_format == "jsonl" else ".json"
    destination = build_cached_path(source.source_id, suffix)
    if write_format == "jsonl":
        serialized_text = "\n".join(json.dumps(item, sort_keys=True, default=str) for item in serialized_records)
        if serialized_text:
            serialized_text += "\n"
    else:
        serialized_text = json.dumps(serialized_records, indent=2, default=str)
    destination.write_text(serialized_text, encoding="utf-8")
    payload_bytes = serialized_text.encode("utf-8")
    batch_checkpoint = {
        **dict(checkpoint.checkpoint_json or {}),
        "last_batch_transport": source.source_kind,
        "last_batch_completed_at": source_now().isoformat(),
        "last_batch_count": records_seen,
        "last_batch_failed_count": records_failed,
        "last_batch_sha256": hashlib.sha256(payload_bytes).hexdigest(),
    }
    if transport_metadata.get("close_reason"):
        batch_checkpoint["close_reason"] = transport_metadata["close_reason"]

    return RuntimeStreamBatch(
        path=str(destination),
        metadata={
            "materialization_kind": "stream_batch",
            "cached_path": str(destination),
            "byte_count": len(payload_bytes),
            "payload_sha256": hashlib.sha256(payload_bytes).hexdigest(),
            "stream_record_count": records_seen,
            "stream_failed_count": records_failed,
            "stream_max_records": max_records,
            "stream_idle_timeout_seconds": idle_timeout_seconds,
            "stream_write_format": write_format,
            "stream_last_event_id": last_event_id,
            "stream_last_offset": last_offset,
            **transport_metadata,
        },
        envelopes=envelopes,
        records_seen=records_seen,
        records_failed=records_failed,
        cursor_text=latest_cursor,
        last_event_id=last_event_id,
        last_offset=last_offset,
        checkpoint_json=batch_checkpoint,
    )


def consume_sse_records(
    *,
    source: SourceDefinitionORM,
    checkpoint: SourceCheckpointORM,
    fetch_config: SourceFetchConfig,
    max_records: int,
    idle_timeout_seconds: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    headers = dict(fetch_config.headers)
    headers["Accept"] = "text/event-stream"
    if checkpoint.last_event_id:
        headers["Last-Event-ID"] = checkpoint.last_event_id
    request = Request(source.target_uri, headers=headers)
    records: list[dict[str, Any]] = []
    current_data: list[str] = []
    current_event_id: str | None = None
    current_event_name: str | None = None
    current_retry: str | None = None
    offset = checkpoint.last_offset if checkpoint.last_offset is not None else -1
    bytes_read = 0

    def finalize_event() -> None:
        nonlocal current_data, current_event_id, current_event_name, current_retry, offset
        if not current_data:
            current_event_id = None
            current_event_name = None
            current_retry = None
            return
        offset += 1
        records.append(
            {
                "offset": offset,
                "event_id": current_event_id,
                "event_name": current_event_name,
                "retry": current_retry,
                "raw_text": "\n".join(current_data),
            }
        )
        current_data = []
        current_event_id = None
        current_event_name = None
        current_retry = None

    enforce_fetch_target_network_policy(source.target_uri, fetch_config)
    with urlopen(request, timeout=fetch_config.timeout_seconds) as response:
        apply_stream_read_timeout(response, idle_timeout_seconds)
        while len(records) < max_records:
            try:
                line_bytes = response.readline()
            except TimeoutError:
                break
            except socket.timeout:
                break
            if not line_bytes:
                break
            bytes_read += len(line_bytes)
            if bytes_read > fetch_config.max_payload_bytes:
                raise RuntimeError(
                    f"SSE stream exceeded max_payload_bytes={fetch_config.max_payload_bytes} before completion."
                )
            line = line_bytes.decode("utf-8", errors="replace").rstrip("\r\n")
            if not line:
                finalize_event()
                continue
            if line.startswith(":"):
                continue
            field, _, value = line.partition(":")
            if value.startswith(" "):
                value = value[1:]
            if field == "data":
                current_data.append(value)
            elif field == "id":
                current_event_id = value
            elif field == "event":
                current_event_name = value
            elif field == "retry":
                current_retry = value
        finalize_event()
        status_code = getattr(response, "status", None) or getattr(response, "code", None) or 200
        return records, {
            "transport_kind": "sse_stream",
            "host": urlparse(source.target_uri).netloc,
            "http_status": int(status_code),
            "content_type": response.headers.get("Content-Type"),
        }


def consume_websocket_records(
    *,
    source: SourceDefinitionORM,
    checkpoint: SourceCheckpointORM,
    fetch_config: SourceFetchConfig,
    max_records: int,
    idle_timeout_seconds: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    parsed = urlparse(source.target_uri)
    if parsed.scheme not in {"ws", "wss"}:
        raise ValueError("WebSocket sources must use ws:// or wss:// target URIs.")
    if parsed.hostname is None:
        raise ValueError("WebSocket source target URI is missing a host.")
    metadata = source.metadata_json if isinstance(source.metadata_json, dict) else {}
    port = parsed.port or (443 if parsed.scheme == "wss" else 80)
    resource = parsed.path or "/"
    if parsed.query:
        resource = f"{resource}?{parsed.query}"
    websocket_key = base64.b64encode(os.urandom(16)).decode("ascii")
    headers = {
        "Host": parsed.netloc,
        "Upgrade": "websocket",
        "Connection": "Upgrade",
        "Sec-WebSocket-Version": "13",
        "Sec-WebSocket-Key": websocket_key,
        **fetch_config.headers,
    }
    if checkpoint.last_event_id:
        headers.setdefault("Last-Event-ID", checkpoint.last_event_id)

    enforce_fetch_target_network_policy(source.target_uri, fetch_config)
    raw_socket = socket.create_connection((parsed.hostname, port), timeout=fetch_config.timeout_seconds)
    wrapped_socket: socket.socket
    if parsed.scheme == "wss":
        wrapped_socket = ssl.create_default_context().wrap_socket(raw_socket, server_hostname=parsed.hostname)
    else:
        wrapped_socket = raw_socket
    try:
        wrapped_socket.settimeout(fetch_config.timeout_seconds)
        handshake = [f"GET {resource} HTTP/1.1"]
        handshake.extend(f"{key}: {value}" for key, value in headers.items())
        request_bytes = ("\r\n".join(handshake) + "\r\n\r\n").encode("utf-8")
        wrapped_socket.sendall(request_bytes)
        status_code, response_headers = read_websocket_handshake_response(wrapped_socket)
        if status_code != 101:
            raise RuntimeError(f"WebSocket handshake failed with status {status_code}.")
        validate_websocket_accept(response_headers, websocket_key)
        wrapped_socket.settimeout(idle_timeout_seconds)

        for message in metadata.get("send_messages", []):
            if isinstance(message, dict):
                send_websocket_text_frame(wrapped_socket, json.dumps(message))
            elif isinstance(message, str):
                send_websocket_text_frame(wrapped_socket, message)

        records: list[dict[str, Any]] = []
        offset = checkpoint.last_offset if checkpoint.last_offset is not None else -1
        close_reason: str | None = None
        payload_bytes_received = 0
        while len(records) < max_records:
            try:
                opcode, payload = read_websocket_frame(wrapped_socket)
            except TimeoutError:
                break
            except socket.timeout:
                break
            if opcode == 0x8:
                close_reason = payload.decode("utf-8", errors="replace") if payload else "peer_closed"
                break
            if opcode == 0x9:
                send_websocket_control_frame(wrapped_socket, opcode=0xA, payload=payload)
                continue
            if opcode == 0xA:
                continue
            if opcode not in {0x1, 0x2}:
                continue
            payload_bytes_received += len(payload)
            if payload_bytes_received > fetch_config.max_payload_bytes:
                raise RuntimeError(
                    f"WebSocket stream exceeded max_payload_bytes={fetch_config.max_payload_bytes} before completion."
                )
            offset += 1
            raw_text = (
                payload.decode("utf-8", errors="replace")
                if opcode == 0x1
                else base64.b64encode(payload).decode("ascii")
            )
            parsed_payload = try_parse_json_object(raw_text)
            event_id = None
            if isinstance(parsed_payload, dict):
                event_id = as_optional_string(parsed_payload.get("event_id")) or as_optional_string(
                    parsed_payload.get("id")
                )
            records.append(
                {
                    "offset": offset,
                    "event_id": event_id,
                    "event_name": "message",
                    "raw_text": raw_text,
                }
            )
        send_websocket_control_frame(wrapped_socket, opcode=0x8, payload=b"")
        return records, {
            "transport_kind": "websocket_stream",
            "host": parsed.netloc,
            "http_status": status_code,
            "content_type": "application/websocket",
            "close_reason": close_reason,
        }
    finally:
        wrapped_socket.close()


def normalize_stream_record_payload(
    raw_text: str,
    *,
    source: SourceDefinitionORM,
    event_id: str | None,
    event_name: str | None,
    offset: int,
) -> dict[str, Any]:
    parsed = try_parse_json_object(raw_text)
    if isinstance(parsed, dict):
        payload = dict(parsed)
    elif isinstance(parsed, list):
        payload = {"items": parsed}
    elif parsed is not None:
        payload = {"value": parsed}
    else:
        payload = {"text": raw_text}
    payload.setdefault("text", raw_text)
    payload.setdefault("source_url", source.target_uri)
    payload.setdefault("stream_offset", offset)
    payload.setdefault("transport", source.source_kind)
    if event_id:
        payload.setdefault("event_id", event_id)
    if event_name:
        payload.setdefault("event_name", event_name)
    return payload


def try_parse_json_object(raw_text: str) -> Any | None:
    stripped = raw_text.strip()
    if not stripped:
        return None
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return None


def apply_stream_read_timeout(response: Any, timeout_seconds: float) -> None:
    candidates = [
        getattr(response, "fp", None),
        getattr(getattr(response, "fp", None), "raw", None),
        getattr(getattr(getattr(response, "fp", None), "raw", None), "_sock", None),
    ]
    for candidate in candidates:
        if candidate is None:
            continue
        try:
            candidate.settimeout(timeout_seconds)
            return
        except AttributeError:
            continue


def read_websocket_handshake_response(sock: socket.socket) -> tuple[int, dict[str, str]]:
    buffer = b""
    while b"\r\n\r\n" not in buffer:
        chunk = sock.recv(4096)
        if not chunk:
            raise RuntimeError("WebSocket handshake ended before headers were received.")
        buffer += chunk
        if len(buffer) > 65536:
            raise RuntimeError("WebSocket handshake headers exceeded 64 KiB.")
    header_block = buffer.split(b"\r\n\r\n", 1)[0].decode("utf-8", errors="replace")
    lines = header_block.split("\r\n")
    if not lines:
        raise RuntimeError("WebSocket handshake response was empty.")
    status_line = lines[0].split()
    if len(status_line) < 2:
        raise RuntimeError("WebSocket handshake response did not include a status code.")
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        headers[key.strip().lower()] = value.strip()
    return int(status_line[1]), headers


def validate_websocket_accept(headers: dict[str, str], websocket_key: str) -> None:
    accept = headers.get("sec-websocket-accept")
    expected = base64.b64encode(
        hashlib.sha1(
            f"{websocket_key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11".encode("utf-8")
        ).digest()
    ).decode("ascii")
    if accept != expected:
        raise RuntimeError("WebSocket handshake accept key did not match.")


def read_websocket_frame(sock: socket.socket) -> tuple[int, bytes]:
    first_two = read_exact(sock, 2)
    first_byte, second_byte = first_two[0], first_two[1]
    opcode = first_byte & 0x0F
    masked = (second_byte & 0x80) != 0
    payload_length = second_byte & 0x7F
    if payload_length == 126:
        payload_length = struct.unpack("!H", read_exact(sock, 2))[0]
    elif payload_length == 127:
        payload_length = struct.unpack("!Q", read_exact(sock, 8))[0]
    masking_key = read_exact(sock, 4) if masked else b""
    payload = read_exact(sock, payload_length) if payload_length else b""
    if masked and payload:
        payload = bytes(byte ^ masking_key[index % 4] for index, byte in enumerate(payload))
    return opcode, payload


def send_websocket_text_frame(sock: socket.socket, text: str) -> None:
    send_websocket_control_frame(sock, opcode=0x1, payload=text.encode("utf-8"))


def send_websocket_control_frame(sock: socket.socket, *, opcode: int, payload: bytes) -> None:
    fin_and_opcode = 0x80 | (opcode & 0x0F)
    mask = os.urandom(4)
    payload_length = len(payload)
    if payload_length < 126:
        header = bytes([fin_and_opcode, 0x80 | payload_length])
    elif payload_length <= 0xFFFF:
        header = bytes([fin_and_opcode, 0x80 | 126]) + struct.pack("!H", payload_length)
    else:
        header = bytes([fin_and_opcode, 0x80 | 127]) + struct.pack("!Q", payload_length)
    masked_payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    try:
        sock.sendall(header + mask + masked_payload)
    except OSError:
        if opcode in {0x8, 0x9, 0xA}:
            return
        raise


def read_exact(sock: socket.socket, size: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        chunk = sock.recv(size - len(chunks))
        if not chunk:
            raise RuntimeError("WebSocket connection closed before the frame was fully read.")
        chunks.extend(chunk)
    return bytes(chunks)


def as_optional_string(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return str(value)


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
    if previous_hash != payload_sha256:
        return False
    checkpoint_state_sha256 = metadata.get("checkpoint_state_sha256")
    if isinstance(checkpoint_state_sha256, str) and checkpoint_state_sha256:
        previous_checkpoint_hash = previous_run.output_json.get("checkpoint_state_sha256")
        return previous_checkpoint_hash == checkpoint_state_sha256
    return True


def source_skip_unchanged_enabled(source: SourceDefinitionORM) -> bool:
    metadata = source.metadata_json if isinstance(source.metadata_json, dict) else {}
    if source.source_kind in {"webhook_ingest", "sse_stream", "websocket_stream"}:
        return bool(metadata.get("skip_unchanged", False))
    return bool(metadata.get("skip_unchanged", True))


def infer_source_format_for_kind(source_kind: str, materialized_path: str) -> str:
    if source_kind == "sqlite_file":
        return "sqlite"
    suffix = Path(materialized_path).suffix.lower()
    if source_kind in {
        "http_json",
        "http_jsonl",
        "webhook_ingest",
        "sse_stream",
        "websocket_stream",
        "web_search",
        "web_crawl",
        "web_discovery",
    }:
        return "json" if suffix != ".txt" else "txt"
    if source_kind in {"http_xml", "rss"}:
        return "json"
    if suffix in {".sqlite", ".sqlite3", ".db"}:
        return "sqlite"
    if suffix in {".json", ".jsonl"}:
        return "json"
    return "txt"


def build_source_import_path(source: SourceDefinitionORM, fetch_mode: str) -> str:
    if fetch_mode == "push":
        return f"webhook://source/{source.source_id}"
    if fetch_mode == "stream":
        return f"stream://source/{source.source_id}"
    return source.target_uri


def resolve_source_import_format(source_kind: str, materialized_path: str) -> str:
    if source_kind == "sqlite_file":
        return "sqlite"
    suffix = Path(materialized_path).suffix.lower()
    if source_kind in {
        "http_json",
        "http_xml",
        "rss",
        "webhook_ingest",
        "sse_stream",
        "websocket_stream",
        "web_search",
        "web_crawl",
        "web_discovery",
    }:
        return "json"
    if source_kind == "http_jsonl" or suffix == ".jsonl":
        return "jsonl"
    if suffix in {".sqlite", ".sqlite3", ".db"}:
        return "sqlite"
    if suffix == ".json":
        return "json"
    return "txt"


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
            "checkpoint_json": checkpoint.checkpoint_json,
        },
        actor=actor,
    )


def record_source_dead_letter(
    session: Session,
    *,
    source: SourceDefinitionORM,
    run: SourceRunORM,
    stage: str,
    failure_reason: str,
    raw_payload_text: str | None = None,
    payload_json: dict[str, Any] | None = None,
    actor: str,
) -> SourceDeadLetterORM:
    record_hash = None
    if raw_payload_text:
        record_hash = hashlib.sha256(raw_payload_text.encode("utf-8")).hexdigest()
    elif payload_json:
        record_hash = hashlib.sha256(json.dumps(payload_json, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    record = SourceDeadLetterORM(
        source_id=source.source_id,
        source_run_id=run.source_run_id,
        adapter_kind=run.adapter_kind,
        source_kind=source.source_kind,
        stage=stage,
        status="pending",
        failure_reason=failure_reason,
        record_hash=record_hash,
        raw_payload_text=raw_payload_text,
        payload_json=payload_json or {},
        cursor_text=run.cursor_text,
        checkpoint_json=dict(run.checkpoint_json or {}),
    )
    session.add(record)
    session.flush()
    register_storage_object(
        session,
        object_key=f"source_dead_letter:{record.source_dead_letter_id}",
        object_kind="source_dead_letter_payload",
        owner_type="source_dead_letter",
        owner_id=str(record.source_dead_letter_id),
        object_uri=f"dead-letter://source/{source.source_id}/{record.source_dead_letter_id}",
        content_hash=record_hash,
        media_type="application/json" if payload_json else "text/plain",
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
            "failure_reason": record.failure_reason,
        },
        actor=actor,
    )
    session.add(
        CustodyLogORM(
            object_type="source_dead_letter",
            object_id=str(record.source_dead_letter_id),
            action="source_dead_letter_captured",
            actor=actor,
            details_json={
                "source_id": source.source_id,
                "source_run_id": run.source_run_id,
                "stage": stage,
            },
        )
    )
    return record


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
        checkpoint_json=dict(checkpoint.checkpoint_json or {}),
    )
    session.add(run)
    session.flush()
    try:
        suffix = ".json" if (content_type or "").lower().startswith("application/json") else ".txt"
        path = write_source_materialization(source.source_id, suffix, payload)
        metadata = {
            "materialization_kind": "webhook_cache",
            "cached_path": path,
            "content_type": content_type,
            "byte_count": len(payload),
            "payload_sha256": hashlib.sha256(payload).hexdigest(),
        }
        envelopes = read_path_as_envelopes(
            Path(path),
            "json" if suffix == ".json" else "txt",
            source_type=source.source_kind,
        )
        run.records_seen = len(envelopes)
        import_run = persist_envelopes_as_import_run(
            session,
            source_path=f"webhook://source/{source.source_id}",
            source_format="json" if suffix == ".json" else "txt",
            layer_key=source.layer_key,
            notes=source.notes,
            actor=actor,
            source_type=source.source_kind,
            envelopes=envelopes,
        )
        run.status = "completed"
        run.import_run_id = import_run.import_run_id
        run.records_imported = import_run.records_imported
        run.records_skipped = import_run.records_skipped
        run.finished_at = source_now()
        run.output_json = {"import_run_id": import_run.import_run_id, **metadata}
        checkpoint.status = "idle"
        checkpoint.last_success_at = run.finished_at
        register_checkpoint_storage_object(session, source, checkpoint, actor=actor)
        register_source_run_storage_object(
            session,
            source,
            run,
            path,
            metadata,
            import_run_id=import_run.import_run_id,
            run_status="completed",
            actor=actor,
        )
        session.commit()
        session.refresh(run)
        return run
    except Exception as exc:
        run.status = "failed"
        run.error_text = str(exc)
        run.finished_at = source_now()
        checkpoint.status = "degraded"
        checkpoint.last_failure_at = run.finished_at
        checkpoint.failure_count += 1
        payload_text = None
        try:
            payload_text = payload.decode("utf-8")
        except UnicodeDecodeError:
            payload_text = None
        record_source_dead_letter(
            session,
            source=source,
            run=run,
            stage="webhook_ingest",
            failure_reason=str(exc),
            raw_payload_text=payload_text,
            actor=actor,
        )
        register_checkpoint_storage_object(session, source, checkpoint, actor=actor)
        session.commit()
        raise SourceExecutionError(
            source_run_id=run.source_run_id,
            source_id=source.source_id,
            source_kind=source.source_kind,
            cause=exc,
        ) from exc


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
    if record.payload_json:
        envelopes = [normalize_ingestion_payload(record.payload_json, "json", source_type=source.source_kind)]
    elif record.raw_payload_text:
        envelopes = [
            normalize_ingestion_payload({"text": record.raw_payload_text}, "txt", source_type=source.source_kind)
        ]
    else:
        raise ValueError("Dead-letter record does not contain replayable payload data.")
    import_run = persist_envelopes_as_import_run(
        session,
        source_path=f"dead-letter://source/{source.source_id}/{record.source_dead_letter_id}",
        source_format="dead_letter_replay",
        layer_key=source.layer_key,
        notes=f"Replay dead-letter {record.source_dead_letter_id}",
        actor=actor,
        source_type=source.source_kind,
        envelopes=envelopes,
    )
    record.status = "replayed"
    record.replay_count += 1
    record.last_replayed_at = source_now()
    record.last_error_text = None
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
    session.refresh(record)
    return record


def fetch_http_source(
    source: SourceDefinitionORM,
    fetch_config: SourceFetchConfig,
) -> tuple[bytes, dict[str, Any]]:
    return fetch_http_url(source.target_uri, fetch_config)


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
    supported = {
        "local_file",
        "sqlite_file",
        "http_json",
        "http_jsonl",
        "http_text",
        "http_xml",
        "rss",
        "web_search",
        "web_crawl",
        "web_discovery",
        "webhook_ingest",
        "sse_stream",
        "websocket_stream",
    }
    if source_kind not in supported:
        raise ValueError(f"Unsupported source kind '{source_kind}'.")


def ensure_target_uri_has_no_embedded_credentials(target_uri: str) -> None:
    parsed = urlparse(target_uri)
    if parsed.scheme and (parsed.username or parsed.password):
        raise ValueError("Source target_uri must not embed credentials. Use metadata or environment variables instead.")


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
