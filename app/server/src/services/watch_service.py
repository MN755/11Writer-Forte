from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from xml.etree import ElementTree

from pydantic import TypeAdapter, ValidationError
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from src.config import get_settings
from src.models import (
    AlertORM,
    CameraInventoryORM,
    CameraSourceInventoryORM,
    CustodyLogORM,
    DataLayerORM,
    EventORM,
    GeofenceORM,
    ObservationORM,
    ScheduledTaskORM,
    ScheduledTaskRunORM,
    SourceCheckpointORM,
    SourceDefinitionORM,
    SourceRunORM,
    StorageObjectORM,
    WatchORM,
    WatchRunORM,
)
from src.schemas import (
    ImageChangeRule,
    NotificationPolicy,
    ObservationPredicate,
    ObservationRule,
    ScheduledTaskCreate,
    ScheduledTaskUpdate,
    SourceDeltaRule,
    SourceHealthRule,
    WatchCreate,
    WatchRule,
    WatchScheduleCreate,
    WatchUpdate,
    ensure_watch_rule_matches_type,
)
from src.services.geospatial_service import point_in_geometry
from src.services.source_service import (
    SourceFetchConfig,
    fetch_http_url,
    parse_fetch_config,
    run_source_definition,
)
from src.services.storage_service import register_storage_object


RUNNABLE_PULL_SOURCE_KINDS = {
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
}
HTTP_PULL_SOURCE_KINDS = {"http_json", "http_jsonl", "http_text", "http_xml", "rss"}
WATCH_TYPES = {"source_delta", "image_change", "observation_rule", "source_health"}
WATCH_STATES = {"enabled", "paused"}
WATCH_SEVERITIES = {"info", "warning", "critical"}
MAX_DEDUPE_FINGERPRINTS = 256
MAX_PREDICATE_COLLECTION_SIZE = 100
_MISSING = object()
_WATCH_RULE_ADAPTER = TypeAdapter(WatchRule)
_NOTIFICATION_POLICY_ADAPTER = TypeAdapter(NotificationPolicy)


class WatchEvaluationError(RuntimeError):
    def __init__(
        self,
        *,
        watch_id: int,
        watch_run_id: int,
        cause: Exception,
    ) -> None:
        super().__init__(str(cause))
        self.watch_id = watch_id
        self.watch_run_id = watch_run_id
        self.cause = cause


@dataclass
class _EvaluationResult:
    outcome: str
    fingerprint: str | None
    message: str
    evidence_json: dict[str, Any]
    baseline_after_json: dict[str, Any]
    dedupe_after_json: dict[str, Any]
    baseline_initialized: bool = False
    source_run_id: int | None = None
    storage_object_id: int | None = None


def watch_now() -> datetime:
    return datetime.now(timezone.utc)


def create_watch(
    session: Session,
    payload: WatchCreate,
    *,
    actor: str = "api_watch",
) -> WatchORM:
    ensure_unique_watch_identity(session, name=payload.name, slug=payload.slug)
    values = payload.model_dump(mode="python")
    validate_watch_configuration(session, values)
    record = WatchORM(**values)
    session.add(record)
    session.flush()
    bind_attached_task(session, record, actor=actor)
    session.add(
        CustodyLogORM(
            object_type="watch",
            object_id=str(record.watch_id),
            action="watch_created",
            actor=actor,
            details_json={
                "watch_id": record.watch_id,
                "name": record.name,
                "slug": record.slug,
                "watch_type": record.watch_type,
                "state": record.state,
                "references": watch_reference_details(record),
                "rule_json": dict(record.rule_json or {}),
                "notification_policy_json": dict(record.notification_policy_json or {}),
            },
        )
    )
    session.commit()
    session.refresh(record)
    return record


def list_watches(
    session: Session,
    *,
    watch_type: str | None = None,
    state: str | None = None,
    limit: int = 200,
) -> list[WatchORM]:
    statement = select(WatchORM).order_by(WatchORM.created_at.desc(), WatchORM.watch_id.desc())
    if watch_type is not None:
        statement = statement.where(WatchORM.watch_type == watch_type)
    if state is not None:
        statement = statement.where(WatchORM.state == state)
    return list(session.scalars(statement.limit(max(1, min(limit, 5000)))))


def get_watch(session: Session, watch_id: int) -> WatchORM:
    return require_watch(session, watch_id)


def update_watch(
    session: Session,
    watch_id: int,
    payload: WatchUpdate,
    *,
    actor: str = "api_watch",
) -> WatchORM:
    record = require_watch(session, watch_id, lock=True)
    previous_scheduled_task_id = record.scheduled_task_id
    changes = payload.model_dump(mode="python", exclude_unset=True)
    if not changes:
        return record

    if "name" in changes or "slug" in changes:
        ensure_unique_watch_identity(
            session,
            name=str(changes.get("name", record.name)),
            slug=str(changes.get("slug", record.slug)),
            watch_id=watch_id,
        )

    candidate = watch_configuration_values(record)
    candidate.update(changes)
    validate_watch_configuration(session, candidate)
    changed_fields: dict[str, dict[str, Any]] = {}
    reset_fields = {
        "watch_type",
        "rule_json",
        "source_id",
        "camera_inventory_id",
        "camera_source_inventory_id",
        "layer_key",
        "event_id",
        "geofence_id",
    }
    reset_evaluation_state = False
    for field_name, new_value in changes.items():
        old_value = getattr(record, field_name)
        if old_value == new_value:
            continue
        setattr(record, field_name, new_value)
        changed_fields[field_name] = {
            "old": json_safe(old_value),
            "new": json_safe(new_value),
        }
        if field_name in reset_fields:
            reset_evaluation_state = True

    if reset_evaluation_state:
        record.baseline_json = {}
        record.dedupe_json = {}
        changed_fields["evaluation_state"] = {
            "old": "preserved",
            "new": "reset_for_configuration_change",
        }

    if (
        "interval_seconds" in changes
        and record.state == "enabled"
        and record.scheduled_task_id is None
    ):
        record.next_run_at = (
            watch_now() + timedelta(seconds=record.interval_seconds)
            if record.interval_seconds is not None
            else None
        )

    if previous_scheduled_task_id != record.scheduled_task_id:
        deactivate_detached_task(session, previous_scheduled_task_id, record.watch_id, actor=actor)
        bind_attached_task(session, record, actor=actor)
    else:
        validate_attached_task(session, record)
    if "state" in changes and record.scheduled_task_id is not None:
        sync_attached_schedule_state(
            session, record, enabled=record.state == "enabled", actor=actor
        )
    elif "interval_seconds" in changes and record.scheduled_task_id is not None:
        sync_attached_schedule_interval(session, record, actor=actor)

    session.add(
        CustodyLogORM(
            object_type="watch",
            object_id=str(record.watch_id),
            action="watch_updated",
            actor=actor,
            details_json={
                "watch_id": record.watch_id,
                "changes": changed_fields,
                "baseline_reset": reset_evaluation_state,
            },
        )
    )
    session.commit()
    session.refresh(record)
    return record


def pause_watch(
    session: Session,
    watch_id: int,
    *,
    actor: str = "api_watch",
) -> WatchORM:
    record = require_watch(session, watch_id, lock=True)
    if record.state == "paused":
        return record
    record.state = "paused"
    record.next_run_at = None
    if record.scheduled_task_id is not None:
        sync_attached_schedule_state(session, record, enabled=False, actor=actor)
    session.add(
        CustodyLogORM(
            object_type="watch",
            object_id=str(record.watch_id),
            action="watch_paused",
            actor=actor,
            details_json={
                "watch_id": record.watch_id,
                "scheduled_task_id": record.scheduled_task_id,
            },
        )
    )
    session.commit()
    session.refresh(record)
    return record


def resume_watch(
    session: Session,
    watch_id: int,
    *,
    actor: str = "api_watch",
) -> WatchORM:
    record = require_watch(session, watch_id, lock=True)
    if record.state == "enabled":
        return record
    record.state = "enabled"
    if record.scheduled_task_id is not None:
        sync_attached_schedule_state(session, record, enabled=True, actor=actor)
        task = session.get(ScheduledTaskORM, record.scheduled_task_id)
        record.next_run_at = task.next_run_at if task is not None else None
    elif record.interval_seconds is not None:
        record.next_run_at = watch_now() + timedelta(seconds=record.interval_seconds)
    session.add(
        CustodyLogORM(
            object_type="watch",
            object_id=str(record.watch_id),
            action="watch_resumed",
            actor=actor,
            details_json={
                "watch_id": record.watch_id,
                "scheduled_task_id": record.scheduled_task_id,
                "next_run_at": isoformat_or_none(record.next_run_at),
            },
        )
    )
    session.commit()
    session.refresh(record)
    return record


def list_watch_runs(
    session: Session,
    watch_id: int | None = None,
    *,
    status: str | None = None,
    outcome: str | None = None,
    limit: int = 200,
) -> list[WatchRunORM]:
    statement = select(WatchRunORM).order_by(WatchRunORM.watch_run_id.desc())
    if watch_id is not None:
        require_watch(session, watch_id)
        statement = statement.where(WatchRunORM.watch_id == watch_id)
    if status is not None:
        statement = statement.where(WatchRunORM.status == status)
    if outcome is not None:
        statement = statement.where(WatchRunORM.outcome == outcome)
    return list(session.scalars(statement.limit(max(1, min(limit, 5000)))))


def list_watch_alerts(
    session: Session,
    watch_id: int | None = None,
    *,
    status: str | None = None,
    limit: int | None = 200,
) -> list[AlertORM]:
    if watch_id is not None:
        require_watch(session, watch_id)
    statement = select(AlertORM).where(AlertORM.dedupe_key.like("watch:%"))
    if watch_id is not None:
        statement = statement.where(AlertORM.dedupe_key.like(f"watch:{watch_id}:%"))
    if status is not None:
        statement = statement.where(AlertORM.status == status)
    statement = statement.order_by(AlertORM.created_at.desc(), AlertORM.alert_id.desc())
    if limit is not None:
        statement = statement.limit(max(1, min(limit, 5000)))
    return list(session.scalars(statement))


def list_watch_evidence(
    session: Session,
    watch_id: int,
    *,
    limit: int = 200,
) -> list[StorageObjectORM]:
    require_watch(session, watch_id)
    runs = list(
        session.scalars(
            select(WatchRunORM)
            .where(WatchRunORM.watch_id == watch_id)
            .order_by(WatchRunORM.watch_run_id.desc())
            .limit(max(1, min(limit * 4, 5000)))
        )
    )
    run_ids = [str(run.watch_run_id) for run in runs]
    linked_ids = {run.storage_object_id for run in runs if run.storage_object_id is not None}
    if not run_ids and not linked_ids:
        return []
    predicates = []
    if run_ids:
        predicates.append(
            (StorageObjectORM.owner_type == "watch_run") & StorageObjectORM.owner_id.in_(run_ids)
        )
    if linked_ids:
        predicates.append(StorageObjectORM.storage_object_id.in_(linked_ids))
    statement = (
        select(StorageObjectORM)
        .where(or_(*predicates))
        .order_by(StorageObjectORM.storage_object_id.desc())
        .limit(max(1, min(limit, 5000)))
    )
    rows = list(session.scalars(statement))
    seen: set[int] = set()
    deduped: list[StorageObjectORM] = []
    for row in rows:
        if row.storage_object_id in seen:
            continue
        seen.add(row.storage_object_id)
        deduped.append(row)
    return deduped


def attach_watch_schedule(
    session: Session,
    watch_id: int,
    payload: WatchScheduleCreate,
    *,
    actor: str = "api_watch",
) -> ScheduledTaskORM:
    from src.services.scheduler_service import create_scheduled_task, update_scheduled_task

    watch = require_watch(session, watch_id, lock=True)
    task_name = payload.name or f"watch-{watch.slug}-evaluate"
    effective_enabled = payload.enabled and watch.state == "enabled"
    task = (
        session.get(ScheduledTaskORM, watch.scheduled_task_id) if watch.scheduled_task_id else None
    )
    if task is None:
        task = session.scalar(
            select(ScheduledTaskORM).where(ScheduledTaskORM.name == task_name).limit(1)
        )

    action = "watch_schedule_attached"
    if task is None:
        task = create_scheduled_task(
            session,
            ScheduledTaskCreate(
                name=task_name,
                task_type="watch_evaluate",
                interval_seconds=payload.interval_seconds,
                enabled=effective_enabled,
                retry_attempts=payload.retry_attempts,
                retry_backoff_seconds=payload.retry_backoff_seconds,
                notes=payload.notes,
                payload_json={"watch_id": watch.watch_id, "force": False},
            ),
        )
    else:
        if task.task_type != "watch_evaluate":
            raise ValueError(f"Scheduled task {task.task_id} is not a watch_evaluate task.")
        linked_watch_id = (
            task.payload_json.get("watch_id") if isinstance(task.payload_json, dict) else None
        )
        if linked_watch_id not in {None, watch.watch_id}:
            raise ValueError(f"Scheduled task {task.task_id} belongs to watch {linked_watch_id}.")
        task = update_scheduled_task(
            session,
            task.task_id,
            ScheduledTaskUpdate(
                name=task_name,
                enabled=effective_enabled,
                interval_seconds=payload.interval_seconds,
                retry_attempts=payload.retry_attempts,
                retry_backoff_seconds=payload.retry_backoff_seconds,
                notes=payload.notes,
                payload_json={"watch_id": watch.watch_id, "force": False},
            ),
            actor=actor,
        )
        action = "watch_schedule_updated"

    watch = require_watch(session, watch_id, lock=True)
    watch.scheduled_task_id = task.task_id
    watch.interval_seconds = task.interval_seconds
    watch.next_run_at = task.next_run_at if watch.state == "enabled" else None
    session.add(
        CustodyLogORM(
            object_type="watch",
            object_id=str(watch.watch_id),
            action=action,
            actor=actor,
            details_json={
                "watch_id": watch.watch_id,
                "scheduled_task_id": task.task_id,
                "task_name": task.name,
                "interval_seconds": task.interval_seconds,
                "enabled": task.enabled,
                "retry_attempts": task.retry_attempts,
                "retry_backoff_seconds": task.retry_backoff_seconds,
            },
        )
    )
    session.commit()
    session.refresh(task)
    return task


def evaluate_watch(
    session: Session,
    watch_id: int,
    *,
    actor: str = "watch_evaluator",
    force: bool = False,
    scheduled_task_run_id: int | None = None,
) -> WatchRunORM:
    watch = require_watch(session, watch_id, lock=True)
    if watch.state != "enabled" and not force:
        raise ValueError(f"Watch {watch_id} is paused. Pass force=True to evaluate it manually.")
    if scheduled_task_run_id is not None:
        scheduled_task_run = session.get(ScheduledTaskRunORM, scheduled_task_run_id)
        if scheduled_task_run is None:
            raise ValueError(f"Scheduled task run {scheduled_task_run_id} does not exist.")
        if watch.scheduled_task_id is None or scheduled_task_run.task_id != watch.scheduled_task_id:
            raise ValueError(
                f"Scheduled task run {scheduled_task_run_id} is not attached to watch {watch_id}."
            )

    rule = parse_watch_rule(watch)
    started_at = watch_now()
    checkpoint_before = build_watch_checkpoint(watch)
    run = WatchRunORM(
        watch_id=watch.watch_id,
        scheduled_task_run_id=scheduled_task_run_id,
        status="running",
        outcome="pending",
        started_at=started_at,
        checkpoint_before_json=checkpoint_before,
        metadata_json={
            "watch_type": watch.watch_type,
            "force": force,
            "actor": actor,
        },
    )
    session.add(run)
    session.flush()
    initial_watch_run_id = run.watch_run_id
    session.add(
        CustodyLogORM(
            object_type="watch_run",
            object_id=str(run.watch_run_id),
            action="watch_evaluation_started",
            actor=actor,
            details_json={
                "watch_id": watch.watch_id,
                "watch_run_id": run.watch_run_id,
                "watch_type": watch.watch_type,
                "scheduled_task_run_id": scheduled_task_run_id,
                "force": force,
            },
        )
    )

    try:
        if isinstance(rule, SourceDeltaRule):
            result = evaluate_source_delta(session, watch, rule, actor=actor)
        elif isinstance(rule, ImageChangeRule):
            result = evaluate_image_change(session, watch, run, rule, actor=actor)
        elif isinstance(rule, ObservationRule):
            result = evaluate_observation_rule(session, watch, rule, reference_time=started_at)
        elif isinstance(rule, SourceHealthRule):
            result = evaluate_source_health(session, watch, rule, reference_time=started_at)
        else:  # pragma: no cover - the discriminated schema makes this unreachable.
            raise ValueError(f"Unsupported watch rule for type {watch.watch_type!r}.")

        # Source runs commit their own transaction. Re-lock and refresh the durable watch/run
        # before applying the watch result so a scheduler retry sees one consistent checkpoint.
        watch = require_watch(session, watch_id, lock=True)
        durable_run = session.get(WatchRunORM, initial_watch_run_id)
        if durable_run is not None:
            run = durable_run

        result = suppress_duplicate_change(session, watch, result)
        alert = None
        notification_policy = watch_notification_policy(watch)
        if result.outcome == "change":
            assert result.fingerprint is not None
            dedupe_key = watch_alert_dedupe_key(watch.watch_id, result.fingerprint)
            if notification_policy.api_enabled:
                alert = create_watch_alert(
                    session,
                    watch=watch,
                    run=run,
                    dedupe_key=dedupe_key,
                    message=result.message,
                    evidence_json=result.evidence_json,
                    source_run_id=result.source_run_id,
                    storage_object_id=result.storage_object_id,
                    actor=actor,
                )
            result.dedupe_after_json = remember_fingerprint(
                result.dedupe_after_json,
                result.fingerprint,
                alert_id=alert.alert_id if alert is not None else None,
                changed_at=started_at,
            )
            run.alert_id = alert.alert_id if alert is not None else None
            run.dedupe_key = dedupe_key
            session.add(
                CustodyLogORM(
                    object_type="watch",
                    object_id=str(watch.watch_id),
                    action="watch_triggered",
                    actor=actor,
                    details_json={
                        "watch_id": watch.watch_id,
                        "watch_run_id": run.watch_run_id,
                        "alert_id": alert.alert_id if alert is not None else None,
                        "dedupe_key": dedupe_key,
                        "fingerprint": result.fingerprint,
                        "notification_policy_json": notification_policy.model_dump(mode="json"),
                        "analysis_on_change_requested": notification_policy.analysis_on_change,
                        "source_run_id": result.source_run_id,
                        "storage_object_id": result.storage_object_id,
                        "evidence_json": result.evidence_json,
                    },
                )
            )

        finished_at = watch_now()
        watch.baseline_json = result.baseline_after_json
        watch.dedupe_json = result.dedupe_after_json
        watch.last_evaluated_at = finished_at
        if result.outcome == "change":
            watch.last_changed_at = finished_at
        watch.next_run_at = compute_watch_next_run(watch, finished_at)

        run.status = "completed"
        run.outcome = result.outcome
        run.finished_at = finished_at
        run.change_detected = result.outcome == "change"
        run.baseline_initialized = result.baseline_initialized
        run.source_run_id = result.source_run_id
        run.storage_object_id = result.storage_object_id
        run.evidence_json = result.evidence_json
        run.checkpoint_after_json = build_watch_checkpoint(watch)
        run.output_summary = result.message
        session.add(
            CustodyLogORM(
                object_type="watch_run",
                object_id=str(run.watch_run_id),
                action="watch_evaluation_completed",
                actor=actor,
                details_json={
                    "watch_id": watch.watch_id,
                    "watch_run_id": run.watch_run_id,
                    "outcome": run.outcome,
                    "change_detected": run.change_detected,
                    "baseline_initialized": run.baseline_initialized,
                    "source_run_id": run.source_run_id,
                    "storage_object_id": run.storage_object_id,
                    "alert_id": run.alert_id,
                    "finished_at": finished_at.isoformat(),
                },
            )
        )
        session.commit()
        session.refresh(run)
        return run
    except WatchEvaluationError:
        raise
    except Exception as exc:
        failed_run = commit_failed_watch_run(
            session,
            watch_id=watch_id,
            watch_run_id=initial_watch_run_id,
            scheduled_task_run_id=scheduled_task_run_id,
            checkpoint_before=checkpoint_before,
            started_at=started_at,
            actor=actor,
            cause=exc,
        )
        raise WatchEvaluationError(
            watch_id=watch_id,
            watch_run_id=failed_run.watch_run_id,
            cause=exc,
        ) from exc


def evaluate_source_delta(
    session: Session,
    watch: WatchORM,
    rule: SourceDeltaRule,
    *,
    actor: str,
) -> _EvaluationResult:
    if watch.source_id is None:
        raise ValueError("source_delta watch requires source_id.")
    source = session.get(SourceDefinitionORM, watch.source_id)
    if source is None:
        raise ValueError(f"Source {watch.source_id} does not exist.")
    if source.source_kind not in RUNNABLE_PULL_SOURCE_KINDS:
        raise ValueError(
            f"Source {source.source_id} kind {source.source_kind!r} is not a runnable pull source."
        )

    if rule.run_source:
        source_run = run_source_definition(session, source.source_id, actor=actor)
    else:
        source_run = session.scalar(
            select(SourceRunORM)
            .where(
                SourceRunORM.source_id == source.source_id,
                SourceRunORM.status.in_(("completed", "skipped")),
            )
            .order_by(SourceRunORM.source_run_id.desc())
            .limit(1)
        )
        if source_run is None:
            raise ValueError(f"Source {source.source_id} has no completed run to inspect.")

    fingerprint = source_run.output_json.get("payload_sha256")
    if not isinstance(fingerprint, str) or len(fingerprint) != 64:
        raise ValueError(f"Source run {source_run.source_run_id} did not produce payload_sha256.")
    storage_object = session.scalar(
        select(StorageObjectORM)
        .where(
            StorageObjectORM.owner_type == "source_run",
            StorageObjectORM.owner_id == str(source_run.source_run_id),
        )
        .order_by(StorageObjectORM.storage_object_id.desc())
        .limit(1)
    )

    baseline_before = dict(watch.baseline_json or {})
    dedupe_before = dict(watch.dedupe_json or {})
    prior_fingerprint = baseline_before.get("payload_sha256")
    initialized = not isinstance(prior_fingerprint, str) or not prior_fingerprint
    qualifying_change = (initialized and rule.alert_on_initial) or (
        not initialized and prior_fingerprint != fingerprint
    )
    outcome = "change" if qualifying_change else "baseline" if initialized else "no_change"
    evidence = {
        "source_id": source.source_id,
        "source_name": source.name,
        "source_kind": source.source_kind,
        "source_run_id": source_run.source_run_id,
        "source_run_status": source_run.status,
        "payload_sha256": fingerprint,
        "previous_payload_sha256": prior_fingerprint,
        "records_seen": source_run.records_seen,
        "records_imported": source_run.records_imported,
        "records_skipped": source_run.records_skipped,
        "storage_object_id": storage_object.storage_object_id
        if storage_object is not None
        else None,
    }
    baseline_after = {
        "payload_sha256": fingerprint,
        "source_run_id": source_run.source_run_id,
        "storage_object_id": storage_object.storage_object_id
        if storage_object is not None
        else None,
        "observed_at": isoformat_or_none(source_run.finished_at or source_run.started_at),
    }
    if outcome == "change":
        message = f"Watch {watch.name} detected a new source payload state."
    elif outcome == "baseline":
        message = f"Watch {watch.name} initialized source baseline {fingerprint}."
    else:
        message = f"Watch {watch.name} found no source payload change."
    return _EvaluationResult(
        outcome=outcome,
        fingerprint=fingerprint if qualifying_change else None,
        message=message,
        evidence_json=evidence,
        baseline_after_json=baseline_after,
        dedupe_after_json=dedupe_before,
        baseline_initialized=initialized,
        source_run_id=source_run.source_run_id,
        storage_object_id=storage_object.storage_object_id if storage_object is not None else None,
    )


def evaluate_image_change(
    session: Session,
    watch: WatchORM,
    run: WatchRunORM,
    rule: ImageChangeRule,
    *,
    actor: str,
) -> _EvaluationResult:
    source_uri, fetch_config, reference = resolve_image_fetch_target(session, watch)
    image_config = replace(
        fetch_config,
        headers={**fetch_config.headers, "Accept": ", ".join(rule.accepted_media_types)},
    )
    payload, fetch_metadata = fetch_http_url(
        source_uri,
        image_config,
        validate_redirects=True,
    )
    media_type = normalize_media_type(fetch_metadata.get("content_type"))
    if media_type is None or not media_type_is_accepted(media_type, rule.accepted_media_types):
        raise ValueError(
            f"Image watch expected one of {rule.accepted_media_types!r}, got {media_type or 'unknown'!r}."
        )
    fingerprint = hashlib.sha256(payload).hexdigest()
    retrieved_at = watch_now()
    storage_object, newly_retained = retain_image_evidence(
        session,
        watch=watch,
        run=run,
        payload=payload,
        source_uri=source_uri,
        media_type=media_type,
        fingerprint=fingerprint,
        retrieved_at=retrieved_at,
        retention_class=rule.retention_class,
        fetch_metadata=fetch_metadata,
        reference=reference,
        actor=actor,
    )

    baseline_before = dict(watch.baseline_json or {})
    dedupe_before = dict(watch.dedupe_json or {})
    prior_fingerprint = baseline_before.get("payload_sha256")
    initialized = not isinstance(prior_fingerprint, str) or not prior_fingerprint
    qualifying_change = (initialized and rule.alert_on_initial) or (
        not initialized and prior_fingerprint != fingerprint
    )
    outcome = "change" if qualifying_change else "baseline" if initialized else "no_change"
    safe_fetch_metadata = sanitize_fetch_metadata(fetch_metadata)
    evidence = {
        **reference,
        "source_uri": source_uri,
        "retrieved_at": retrieved_at.isoformat(),
        "payload_sha256": fingerprint,
        "previous_payload_sha256": prior_fingerprint,
        "media_type": media_type,
        "byte_size": len(payload),
        "storage_object_id": storage_object.storage_object_id,
        "artifact_uri": storage_object.object_uri,
        "newly_retained": newly_retained,
        "fetch": safe_fetch_metadata,
    }
    baseline_after = {
        "payload_sha256": fingerprint,
        "storage_object_id": storage_object.storage_object_id,
        "source_uri": source_uri,
        "retrieved_at": retrieved_at.isoformat(),
        "media_type": media_type,
        "byte_size": len(payload),
    }
    if outcome == "change":
        message = f"Watch {watch.name} detected a new image content hash."
    elif outcome == "baseline":
        message = f"Watch {watch.name} retained its initial image baseline."
    else:
        message = f"Watch {watch.name} found no image content change."
    return _EvaluationResult(
        outcome=outcome,
        fingerprint=fingerprint if qualifying_change else None,
        message=message,
        evidence_json=evidence,
        baseline_after_json=baseline_after,
        dedupe_after_json=dedupe_before,
        baseline_initialized=initialized,
        storage_object_id=storage_object.storage_object_id,
    )


def evaluate_observation_rule(
    session: Session,
    watch: WatchORM,
    rule: ObservationRule,
    *,
    reference_time: datetime,
) -> _EvaluationResult:
    baseline_before = dict(watch.baseline_json or {})
    dedupe_before = dict(watch.dedupe_json or {})
    initialized = "last_observation_id" not in baseline_before
    previous_high_water = int(baseline_before.get("last_observation_id", 0) or 0)
    current_high_water = int(session.scalar(select(func.max(ObservationORM.observation_id))) or 0)

    if initialized and not rule.include_existing_on_first_run:
        return _EvaluationResult(
            outcome="baseline",
            fingerprint=None,
            message=f"Watch {watch.name} baselined existing observations through {current_high_water}.",
            evidence_json={
                "observation_ids": [],
                "matching_observation_count": 0,
                "previous_high_water": previous_high_water,
                "new_high_water": current_high_water,
                "include_existing_on_first_run": False,
            },
            baseline_after_json={
                "last_observation_id": current_high_water,
                "evaluated_at": reference_time.isoformat(),
            },
            dedupe_after_json=dedupe_before,
            baseline_initialized=True,
        )

    statement = select(ObservationORM).where(
        ObservationORM.observation_id > previous_high_water,
        ObservationORM.observation_id <= current_high_water,
    )
    if watch.layer_key is not None:
        statement = statement.where(ObservationORM.layer_key == watch.layer_key)
    if rule.source_domain is not None:
        statement = statement.where(func.lower(ObservationORM.source_domain) == rule.source_domain)
    if rule.min_confidence > 0:
        statement = statement.where(ObservationORM.confidence_score >= rule.min_confidence)
    if rule.trust_levels:
        statement = statement.where(ObservationORM.trust_level.in_(tuple(rule.trust_levels)))
    if watch.event_id is not None:
        statement = statement.where(ObservationORM.event_id == watch.event_id)
    if rule.time_window_seconds is not None:
        since = reference_time - timedelta(seconds=rule.time_window_seconds)
        statement = statement.where(
            or_(
                ObservationORM.observed_at >= since,
                ObservationORM.observed_at.is_(None) & (ObservationORM.created_at >= since),
            )
        )
    candidates = list(
        session.scalars(
            statement.order_by(ObservationORM.observation_id.asc()).limit(rule.max_results)
        )
    )
    geofence = (
        session.get(GeofenceORM, watch.geofence_id) if watch.geofence_id is not None else None
    )
    matches = [
        observation
        for observation in candidates
        if observation_matches_geofence(observation, geofence)
        and observation_matches_predicates(observation, rule.predicates)
    ]
    observation_ids = [row.observation_id for row in matches]
    if len(candidates) >= rule.max_results and candidates:
        next_high_water = candidates[-1].observation_id
    else:
        next_high_water = current_high_water
    fingerprint = hash_json_value({"observation_ids": observation_ids}) if observation_ids else None
    evidence = {
        "observation_ids": observation_ids,
        "observation_hashes": [row.raw_hash for row in matches],
        "matching_observation_count": len(matches),
        "candidate_count": len(candidates),
        "previous_high_water": previous_high_water,
        "evaluated_through_observation_id": current_high_water,
        "new_high_water": next_high_water,
        "truncated": len(candidates) >= rule.max_results,
        "layer_key": watch.layer_key,
        "event_id": watch.event_id,
        "geofence_id": watch.geofence_id,
        "source_domain": rule.source_domain,
        "min_confidence": rule.min_confidence,
        "trust_levels": list(rule.trust_levels),
    }
    baseline_after = {
        "last_observation_id": next_high_water,
        "evaluated_at": reference_time.isoformat(),
    }
    if observation_ids:
        message = f"Watch {watch.name} matched {len(observation_ids)} new observations."
        outcome = "change"
    elif initialized:
        message = f"Watch {watch.name} initialized with no matching observations."
        outcome = "baseline"
    else:
        message = f"Watch {watch.name} found no new matching observations."
        outcome = "no_change"
    return _EvaluationResult(
        outcome=outcome,
        fingerprint=fingerprint,
        message=message,
        evidence_json=evidence,
        baseline_after_json=baseline_after,
        dedupe_after_json=dedupe_before,
        baseline_initialized=initialized,
    )


def evaluate_source_health(
    session: Session,
    watch: WatchORM,
    rule: SourceHealthRule,
    *,
    reference_time: datetime,
) -> _EvaluationResult:
    if watch.source_id is None:
        raise ValueError("source_health watch requires source_id.")
    source = session.get(SourceDefinitionORM, watch.source_id)
    if source is None:
        raise ValueError(f"Source {watch.source_id} does not exist.")
    latest_run = session.scalar(
        select(SourceRunORM)
        .where(SourceRunORM.source_id == source.source_id)
        .order_by(SourceRunORM.source_run_id.desc())
        .limit(1)
    )
    latest_success = session.scalar(
        select(SourceRunORM)
        .where(
            SourceRunORM.source_id == source.source_id,
            SourceRunORM.status.in_(("completed", "skipped")),
        )
        .order_by(SourceRunORM.source_run_id.desc())
        .limit(1)
    )
    checkpoint = session.scalar(
        select(SourceCheckpointORM)
        .where(SourceCheckpointORM.source_id == source.source_id)
        .limit(1)
    )
    state = resolve_source_health_state(
        source,
        latest_run=latest_run,
        latest_success=latest_success,
        checkpoint=checkpoint,
        reference_time=reference_time,
        stale_after_seconds=rule.stale_after_seconds,
    )
    baseline_before = dict(watch.baseline_json or {})
    dedupe_before = dict(watch.dedupe_json or {})
    previous_state = baseline_before.get("health_state")
    initialized = not isinstance(previous_state, str)
    transitioned = not initialized and previous_state != state
    unhealthy_trigger = state in set(rule.alert_states) and (
        (initialized and rule.alert_on_initial_unhealthy) or transitioned
    )
    recovery_trigger = (
        transitioned
        and state == "healthy"
        and previous_state in set(rule.alert_states)
        and rule.alert_on_recovery
    )
    qualifying_change = unhealthy_trigger or recovery_trigger
    fingerprint = None
    if qualifying_change:
        fingerprint = hash_json_value(
            {
                "previous_state": previous_state,
                "state": state,
                "source_run_id": latest_run.source_run_id if latest_run is not None else None,
                "source_updated_at": isoformat_or_none(source.updated_at),
                "checkpoint_failure_count": checkpoint.failure_count
                if checkpoint is not None
                else 0,
            }
        )
    outcome = "change" if qualifying_change else "baseline" if initialized else "no_change"
    evidence = {
        "source_id": source.source_id,
        "source_name": source.name,
        "source_kind": source.source_kind,
        "previous_health_state": previous_state,
        "health_state": state,
        "latest_source_run_id": latest_run.source_run_id if latest_run is not None else None,
        "latest_source_run_status": latest_run.status if latest_run is not None else None,
        "latest_source_run_finished_at": (
            isoformat_or_none(latest_run.finished_at or latest_run.started_at)
            if latest_run is not None
            else None
        ),
        "latest_success_at": (
            isoformat_or_none(latest_success.finished_at or latest_success.started_at)
            if latest_success is not None
            else None
        ),
        "checkpoint_status": checkpoint.status if checkpoint is not None else None,
        "checkpoint_failure_count": checkpoint.failure_count if checkpoint is not None else 0,
        "stale_after_seconds": rule.stale_after_seconds,
        "recovery": recovery_trigger,
    }
    baseline_after = {
        "health_state": state,
        "source_run_id": latest_run.source_run_id if latest_run is not None else None,
        "observed_at": reference_time.isoformat(),
    }
    if recovery_trigger:
        message = f"Watch {watch.name} detected source recovery to healthy."
    elif unhealthy_trigger:
        message = f"Watch {watch.name} detected source health state {state}."
    elif initialized:
        message = f"Watch {watch.name} initialized source health baseline {state}."
    else:
        message = f"Watch {watch.name} found source health state {state} unchanged."
    return _EvaluationResult(
        outcome=outcome,
        fingerprint=fingerprint,
        message=message,
        evidence_json=evidence,
        baseline_after_json=baseline_after,
        dedupe_after_json=dedupe_before,
        baseline_initialized=initialized,
        source_run_id=latest_run.source_run_id if latest_run is not None else None,
    )


def render_watch_alert_rss(
    session: Session,
    base_url: str,
    watch_id: int | None = None,
    *,
    status: str | None = "open",
    limit: int = 50,
) -> str:
    if watch_id is not None:
        require_watch(session, watch_id)
    watches_statement = select(WatchORM)
    if watch_id is not None:
        watches_statement = watches_statement.where(WatchORM.watch_id == watch_id)
    watches = list(session.scalars(watches_statement.order_by(WatchORM.watch_id.asc())))
    rss_watches = {
        watch.watch_id: watch for watch in watches if watch_notification_policy(watch).rss_enabled
    }
    alerts = list_watch_alerts(
        session,
        watch_id=watch_id,
        status=status,
        limit=None,
    )
    selected: list[tuple[AlertORM, WatchORM]] = []
    for alert in alerts:
        alert_watch_id = alert_watch_identity(alert)
        watch = rss_watches.get(alert_watch_id) if alert_watch_id is not None else None
        if watch is None:
            continue
        selected.append((alert, watch))
        if len(selected) >= max(1, min(limit, 500)):
            break

    normalized_base_url = base_url.rstrip("/")
    channel_url = f"{normalized_base_url}/api/watches/feed.rss"
    root = ElementTree.Element("rss", {"version": "2.0"})
    channel = ElementTree.SubElement(root, "channel")
    ElementTree.SubElement(channel, "title").text = (
        f"11Writer Forte Watch Alerts - {rss_watches[watch_id].name}"
        if watch_id is not None and watch_id in rss_watches
        else "11Writer Forte Watch Alerts"
    )
    ElementTree.SubElement(channel, "link").text = channel_url
    ElementTree.SubElement(
        channel, "description"
    ).text = "Deterministic local watch alerts and retained evidence from 11Writer Forte."
    ElementTree.SubElement(channel, "language").text = "en-us"
    ElementTree.SubElement(channel, "generator").text = "11Writer Forte Watch Engine"
    ElementTree.SubElement(channel, "lastBuildDate").text = rss_datetime(watch_now())

    for alert, watch in selected:
        trigger = alert.trigger_basis_json if isinstance(alert.trigger_basis_json, dict) else {}
        watch_run_id = trigger.get("watch_run_id")
        storage_object_id = trigger.get("storage_object_id")
        watch_url = f"{normalized_base_url}/api/watches/{watch.watch_id}"
        item = ElementTree.SubElement(channel, "item")
        ElementTree.SubElement(
            item, "title"
        ).text = f"[{alert.severity.upper()}] {watch.name}: {alert.message}"
        ElementTree.SubElement(item, "link").text = watch_url
        ElementTree.SubElement(
            item, "guid", {"isPermaLink": "false"}
        ).text = f"watch-alert-{alert.alert_id}"
        ElementTree.SubElement(item, "pubDate").text = rss_datetime(alert.created_at)
        ElementTree.SubElement(item, "category").text = alert.severity
        description_parts = [
            alert.message,
            f"Watch: {watch.name} ({watch.slug}, id={watch.watch_id})",
            f"Severity: {alert.severity}",
            f"Status: {alert.status}",
        ]
        if watch_run_id is not None:
            description_parts.append(
                f"Watch run: {watch_run_id} ({normalized_base_url}/api/watches/{watch.watch_id}/runs)"
            )
        if storage_object_id is not None:
            description_parts.append(
                f"Evidence storage object: {storage_object_id} "
                f"({normalized_base_url}/api/storage/objects/{storage_object_id}/manifest)"
            )
        observation_ids = trigger.get("observation_ids")
        if isinstance(observation_ids, list) and observation_ids:
            description_parts.append(
                "Observation evidence: " + ", ".join(str(value) for value in observation_ids[:50])
            )
        ElementTree.SubElement(item, "description").text = "\n".join(description_parts)

    return ElementTree.tostring(root, encoding="unicode", xml_declaration=True)


def require_watch(session: Session, watch_id: int, *, lock: bool = False) -> WatchORM:
    statement = select(WatchORM).where(WatchORM.watch_id == watch_id)
    if lock:
        statement = statement.with_for_update()
    record = session.scalar(statement)
    if record is None:
        raise ValueError(f"Watch {watch_id} does not exist.")
    return record


def ensure_unique_watch_identity(
    session: Session,
    *,
    name: str,
    slug: str,
    watch_id: int | None = None,
) -> None:
    name_statement = select(WatchORM).where(func.lower(WatchORM.name) == name.strip().lower())
    slug_statement = select(WatchORM).where(func.lower(WatchORM.slug) == slug.strip().lower())
    if watch_id is not None:
        name_statement = name_statement.where(WatchORM.watch_id != watch_id)
        slug_statement = slug_statement.where(WatchORM.watch_id != watch_id)
    if session.scalar(name_statement.limit(1)) is not None:
        raise ValueError(f"Watch name {name!r} already exists.")
    if session.scalar(slug_statement.limit(1)) is not None:
        raise ValueError(f"Watch slug {slug!r} already exists.")


def parse_watch_rule(watch: WatchORM) -> WatchRule:
    try:
        rule = _WATCH_RULE_ADAPTER.validate_python(watch.rule_json)
        ensure_watch_rule_matches_type(watch.watch_type, rule)
        return rule
    except (ValidationError, ValueError) as exc:
        raise ValueError(f"Watch {watch.watch_id} has invalid rule_json: {exc}") from exc


def watch_notification_policy(watch: WatchORM) -> NotificationPolicy:
    try:
        return _NOTIFICATION_POLICY_ADAPTER.validate_python(watch.notification_policy_json or {})
    except ValidationError as exc:
        raise ValueError(
            f"Watch {watch.watch_id} has invalid notification_policy_json: {exc}"
        ) from exc


def watch_configuration_values(watch: WatchORM) -> dict[str, Any]:
    return {
        "name": watch.name,
        "slug": watch.slug,
        "objective": watch.objective,
        "description": watch.description,
        "watch_type": watch.watch_type,
        "state": watch.state,
        "rule_json": dict(watch.rule_json or {}),
        "source_id": watch.source_id,
        "camera_inventory_id": watch.camera_inventory_id,
        "camera_source_inventory_id": watch.camera_source_inventory_id,
        "layer_key": watch.layer_key,
        "event_id": watch.event_id,
        "geofence_id": watch.geofence_id,
        "scheduled_task_id": watch.scheduled_task_id,
        "interval_seconds": watch.interval_seconds,
        "severity": watch.severity,
        "notification_policy_json": dict(watch.notification_policy_json or {}),
        "metadata_json": dict(watch.metadata_json or {}),
        "provenance_json": dict(watch.provenance_json or {}),
    }


def validate_watch_configuration(session: Session, values: dict[str, Any]) -> None:
    watch_type = str(values.get("watch_type") or "")
    if watch_type not in WATCH_TYPES:
        raise ValueError(f"Unsupported watch type {watch_type!r}.")
    state = str(values.get("state") or "")
    if state not in WATCH_STATES:
        raise ValueError(f"Unsupported watch state {state!r}.")
    severity = str(values.get("severity") or "")
    if severity not in WATCH_SEVERITIES:
        raise ValueError(f"Unsupported watch severity {severity!r}.")
    try:
        rule = _WATCH_RULE_ADAPTER.validate_python(values.get("rule_json"))
        ensure_watch_rule_matches_type(watch_type, rule)
        _NOTIFICATION_POLICY_ADAPTER.validate_python(values.get("notification_policy_json") or {})
    except (ValidationError, ValueError) as exc:
        raise ValueError(f"Invalid watch configuration: {exc}") from exc

    source = require_optional_reference(
        session,
        SourceDefinitionORM,
        values.get("source_id"),
        "Source",
    )
    camera = require_optional_reference(
        session,
        CameraInventoryORM,
        values.get("camera_inventory_id"),
        "Camera inventory record",
    )
    camera_source = require_optional_reference(
        session,
        CameraSourceInventoryORM,
        values.get("camera_source_inventory_id"),
        "Camera source inventory record",
    )
    if values.get("layer_key") is not None:
        layer = session.scalar(
            select(DataLayerORM).where(DataLayerORM.key == str(values["layer_key"])).limit(1)
        )
        if layer is None:
            raise ValueError(f"Data layer {values['layer_key']!r} does not exist.")
    require_optional_reference(session, EventORM, values.get("event_id"), "Event")
    require_optional_reference(session, GeofenceORM, values.get("geofence_id"), "Geofence")

    if watch_type == "source_delta":
        if source is None:
            raise ValueError("source_delta watch requires source_id.")
        if source.source_kind not in RUNNABLE_PULL_SOURCE_KINDS:
            raise ValueError(
                f"source_delta requires a runnable pull source, got {source.source_kind!r}."
            )
        ensure_no_camera_references(camera, camera_source, watch_type)
    elif watch_type == "source_health":
        if source is None:
            raise ValueError("source_health watch requires source_id.")
        ensure_no_camera_references(camera, camera_source, watch_type)
    elif watch_type == "image_change":
        references = [source is not None, camera is not None, camera_source is not None]
        if sum(references) != 1:
            raise ValueError(
                "image_change watch requires exactly one of source_id, camera_inventory_id, "
                "or camera_source_inventory_id."
            )
        if source is not None and source.source_kind not in HTTP_PULL_SOURCE_KINDS:
            raise ValueError(
                f"image_change source reference must be an HTTP pull source, got {source.source_kind!r}."
            )
        if camera is not None and not camera.image_url:
            raise ValueError(
                f"Camera inventory record {camera.camera_inventory_id} has no image_url."
            )
        if camera_source is not None and camera_source.endpoint_kind != "image":
            raise ValueError(
                f"Camera source inventory record {camera_source.camera_source_inventory_id} "
                "is not an image endpoint."
            )
    elif watch_type == "observation_rule":
        if any((source is not None, camera is not None, camera_source is not None)):
            raise ValueError(
                "observation_rule uses layer/domain/event/geofence filters and does not accept "
                "source or camera references."
            )
        assert isinstance(rule, ObservationRule)
        validate_observation_predicates(rule.predicates)

    scheduled_task_id = values.get("scheduled_task_id")
    if scheduled_task_id is not None:
        task = session.get(ScheduledTaskORM, int(scheduled_task_id))
        if task is None:
            raise ValueError(f"Scheduled task {scheduled_task_id} does not exist.")
        if task.task_type != "watch_evaluate":
            raise ValueError(f"Scheduled task {scheduled_task_id} is not a watch_evaluate task.")


def require_optional_reference(
    session: Session,
    model: type,
    object_id: Any,
    label: str,
) -> Any | None:
    if object_id is None:
        return None
    record = session.get(model, int(object_id))
    if record is None:
        raise ValueError(f"{label} {object_id} does not exist.")
    return record


def ensure_no_camera_references(
    camera: CameraInventoryORM | None,
    camera_source: CameraSourceInventoryORM | None,
    watch_type: str,
) -> None:
    if camera is not None or camera_source is not None:
        raise ValueError(f"{watch_type} watch does not accept camera references.")


def validate_observation_predicates(predicates: list[ObservationPredicate]) -> None:
    for predicate in predicates:
        if (
            predicate.field.startswith(".")
            or predicate.field.endswith(".")
            or ".." in predicate.field
        ):
            raise ValueError(f"Observation predicate field {predicate.field!r} is invalid.")
        if len(predicate.field.split(".")) > 12:
            raise ValueError("Observation predicate field nesting must not exceed 12 segments.")
        if (
            isinstance(predicate.value, list)
            and len(predicate.value) > MAX_PREDICATE_COLLECTION_SIZE
        ):
            raise ValueError(
                f"Observation predicate list values must not exceed {MAX_PREDICATE_COLLECTION_SIZE} items."
            )


def validate_attached_task(session: Session, watch: WatchORM) -> None:
    if watch.scheduled_task_id is None:
        return
    task = session.get(ScheduledTaskORM, watch.scheduled_task_id)
    if task is None:
        raise ValueError(f"Scheduled task {watch.scheduled_task_id} does not exist.")
    if task.task_type != "watch_evaluate":
        raise ValueError(f"Scheduled task {task.task_id} is not a watch_evaluate task.")
    payload_watch_id = (
        task.payload_json.get("watch_id") if isinstance(task.payload_json, dict) else None
    )
    if payload_watch_id != watch.watch_id:
        raise ValueError(f"Scheduled task {task.task_id} belongs to watch {payload_watch_id}.")


def bind_attached_task(session: Session, watch: WatchORM, *, actor: str) -> None:
    if watch.scheduled_task_id is None:
        return
    task = session.get(ScheduledTaskORM, watch.scheduled_task_id)
    if task is None:
        raise ValueError(f"Scheduled task {watch.scheduled_task_id} does not exist.")
    if task.task_type != "watch_evaluate":
        raise ValueError(f"Scheduled task {task.task_id} is not a watch_evaluate task.")
    payload = dict(task.payload_json or {})
    payload_watch_id = payload.get("watch_id")
    if payload_watch_id not in {None, watch.watch_id}:
        raise ValueError(f"Scheduled task {task.task_id} belongs to watch {payload_watch_id}.")
    payload["watch_id"] = watch.watch_id
    payload.setdefault("force", False)
    task.payload_json = payload
    if watch.interval_seconds is None:
        watch.interval_seconds = task.interval_seconds
    else:
        task.interval_seconds = watch.interval_seconds
    if watch.state == "paused":
        task.enabled = False
        task.next_run_at = None
    watch.next_run_at = task.next_run_at if task.enabled and watch.state == "enabled" else None
    session.add(
        CustodyLogORM(
            object_type="scheduled_task",
            object_id=str(task.task_id),
            action="watch_schedule_bound",
            actor=actor,
            details_json={"watch_id": watch.watch_id, "payload_json": payload},
        )
    )


def deactivate_detached_task(
    session: Session,
    task_id: int | None,
    watch_id: int,
    *,
    actor: str,
) -> None:
    if task_id is None:
        return
    task = session.get(ScheduledTaskORM, task_id)
    if task is None or task.task_type != "watch_evaluate":
        return
    payload = task.payload_json if isinstance(task.payload_json, dict) else {}
    if payload.get("watch_id") != watch_id:
        return
    task.enabled = False
    task.next_run_at = None
    session.add(
        CustodyLogORM(
            object_type="scheduled_task",
            object_id=str(task.task_id),
            action="watch_schedule_detached",
            actor=actor,
            details_json={"watch_id": watch_id},
        )
    )


def sync_attached_schedule_state(
    session: Session,
    watch: WatchORM,
    *,
    enabled: bool,
    actor: str,
) -> None:
    from src.services.scheduler_service import update_scheduled_task

    if watch.scheduled_task_id is None:
        return
    task = update_scheduled_task(
        session,
        watch.scheduled_task_id,
        ScheduledTaskUpdate(enabled=enabled),
        actor=actor,
    )
    watch.next_run_at = task.next_run_at if enabled else None


def sync_attached_schedule_interval(
    session: Session,
    watch: WatchORM,
    *,
    actor: str,
) -> None:
    from src.services.scheduler_service import update_scheduled_task

    if watch.scheduled_task_id is None or watch.interval_seconds is None:
        return
    task = update_scheduled_task(
        session,
        watch.scheduled_task_id,
        ScheduledTaskUpdate(interval_seconds=watch.interval_seconds),
        actor=actor,
    )
    watch.next_run_at = task.next_run_at if watch.state == "enabled" else None


def watch_reference_details(watch: WatchORM) -> dict[str, Any]:
    return {
        "source_id": watch.source_id,
        "camera_inventory_id": watch.camera_inventory_id,
        "camera_source_inventory_id": watch.camera_source_inventory_id,
        "layer_key": watch.layer_key,
        "event_id": watch.event_id,
        "geofence_id": watch.geofence_id,
        "scheduled_task_id": watch.scheduled_task_id,
    }


def build_watch_checkpoint(watch: WatchORM) -> dict[str, Any]:
    return {
        "baseline_json": dict(watch.baseline_json or {}),
        "dedupe_json": dict(watch.dedupe_json or {}),
        "last_evaluated_at": isoformat_or_none(watch.last_evaluated_at),
        "last_changed_at": isoformat_or_none(watch.last_changed_at),
        "next_run_at": isoformat_or_none(watch.next_run_at),
    }


def compute_watch_next_run(watch: WatchORM, reference: datetime) -> datetime | None:
    if watch.state != "enabled":
        return None
    interval_seconds = watch.interval_seconds
    if interval_seconds is None and watch.scheduled_task_id is not None:
        task = watch.__dict__.get("_scheduled_task_for_next_run")
        if isinstance(task, ScheduledTaskORM):
            interval_seconds = task.interval_seconds
    if interval_seconds is None:
        return None
    return reference + timedelta(seconds=interval_seconds)


def suppress_duplicate_change(
    session: Session,
    watch: WatchORM,
    result: _EvaluationResult,
) -> _EvaluationResult:
    if result.outcome != "change" or result.fingerprint is None:
        return result
    dedupe_key = watch_alert_dedupe_key(watch.watch_id, result.fingerprint)
    fingerprints = result.dedupe_after_json.get("fingerprints", [])
    remembered = result.fingerprint in fingerprints if isinstance(fingerprints, list) else False
    if not remembered:
        return result
    existing = session.scalar(
        select(AlertORM)
        .where(AlertORM.dedupe_key == dedupe_key)
        .order_by(AlertORM.alert_id.desc())
        .limit(1)
    )
    evidence = {
        **result.evidence_json,
        "dedupe_suppressed": True,
        "existing_alert_id": existing.alert_id if existing is not None else None,
    }
    return _EvaluationResult(
        outcome="no_change",
        fingerprint=None,
        message=f"{result.message} Alert suppressed because this state was already recorded.",
        evidence_json=evidence,
        baseline_after_json=result.baseline_after_json,
        dedupe_after_json=result.dedupe_after_json,
        baseline_initialized=result.baseline_initialized,
        source_run_id=result.source_run_id,
        storage_object_id=result.storage_object_id,
    )


def watch_alert_dedupe_key(watch_id: int, fingerprint: str) -> str:
    return f"watch:{watch_id}:{fingerprint}"


def remember_fingerprint(
    dedupe_json: dict[str, Any],
    fingerprint: str,
    *,
    alert_id: int | None,
    changed_at: datetime,
) -> dict[str, Any]:
    remembered = [fingerprint]
    return {
        **dedupe_json,
        "fingerprints": remembered[-MAX_DEDUPE_FINGERPRINTS:],
        "last_fingerprint": fingerprint,
        "last_alert_id": alert_id,
        "last_changed_at": changed_at.isoformat(),
    }


def create_watch_alert(
    session: Session,
    *,
    watch: WatchORM,
    run: WatchRunORM,
    dedupe_key: str,
    message: str,
    evidence_json: dict[str, Any],
    source_run_id: int | None,
    storage_object_id: int | None,
    actor: str,
) -> AlertORM:
    trigger_basis = {
        "watch_id": watch.watch_id,
        "watch_name": watch.name,
        "watch_slug": watch.slug,
        "watch_type": watch.watch_type,
        "watch_run_id": run.watch_run_id,
        "source_run_id": source_run_id,
        "storage_object_id": storage_object_id,
        "fingerprint": dedupe_key.rsplit(":", 1)[-1],
        "observation_ids": evidence_json.get("observation_ids", []),
        "evidence_json": evidence_json,
        "rule_json": dict(watch.rule_json or {}),
        "provenance_json": dict(watch.provenance_json or {}),
    }
    alert = AlertORM(
        event_id=watch.event_id,
        geofence_id=watch.geofence_id,
        severity=watch.severity,
        status="open",
        dedupe_key=dedupe_key,
        message=message,
        trigger_basis_json=trigger_basis,
    )
    session.add(alert)
    session.flush()
    session.add(
        CustodyLogORM(
            object_type="alert",
            object_id=str(alert.alert_id),
            action="alert_created",
            actor=actor,
            details_json={
                "alert_id": alert.alert_id,
                "watch_id": watch.watch_id,
                "watch_run_id": run.watch_run_id,
                "severity": alert.severity,
                "dedupe_key": dedupe_key,
                "source_run_id": source_run_id,
                "storage_object_id": storage_object_id,
            },
        )
    )
    return alert


def commit_failed_watch_run(
    session: Session,
    *,
    watch_id: int,
    watch_run_id: int,
    scheduled_task_run_id: int | None,
    checkpoint_before: dict[str, Any],
    started_at: datetime,
    actor: str,
    cause: Exception,
) -> WatchRunORM:
    session.rollback()
    watch = require_watch(session, watch_id, lock=True)
    run = session.get(WatchRunORM, watch_run_id)
    if run is None:
        run = WatchRunORM(
            watch_id=watch_id,
            scheduled_task_run_id=scheduled_task_run_id,
            started_at=started_at,
            checkpoint_before_json=checkpoint_before,
            metadata_json={
                "watch_type": watch.watch_type,
                "actor": actor,
            },
        )
        session.add(run)
        session.flush()
    finished_at = watch_now()
    run.status = "failed"
    run.outcome = "failure"
    run.finished_at = finished_at
    run.change_detected = False
    run.error_text = str(cause)
    run.output_summary = f"Watch evaluation failed: {cause}"
    run.checkpoint_after_json = build_watch_checkpoint(watch)
    run.evidence_json = {
        "error_type": cause.__class__.__name__,
        "message": str(cause),
    }
    watch.last_evaluated_at = finished_at
    watch.next_run_at = compute_watch_next_run(watch, finished_at)
    session.add(
        CustodyLogORM(
            object_type="watch_run",
            object_id=str(run.watch_run_id),
            action="watch_evaluation_failed",
            actor=actor,
            details_json={
                "watch_id": watch.watch_id,
                "watch_run_id": run.watch_run_id,
                "error_type": cause.__class__.__name__,
                "error_text": str(cause),
                "scheduled_task_run_id": scheduled_task_run_id,
            },
        )
    )
    session.commit()
    session.refresh(run)
    return run


def resolve_image_fetch_target(
    session: Session,
    watch: WatchORM,
) -> tuple[str, SourceFetchConfig, dict[str, Any]]:
    if watch.source_id is not None:
        source = session.get(SourceDefinitionORM, watch.source_id)
        if source is None:
            raise ValueError(f"Source {watch.source_id} does not exist.")
        if source.source_kind not in HTTP_PULL_SOURCE_KINDS:
            raise ValueError(
                f"Image watch source {source.source_id} kind {source.source_kind!r} is not HTTP pull."
            )
        ensure_http_url(source.target_uri)
        return (
            source.target_uri,
            parse_fetch_config(source),
            {
                "reference_type": "source_definition",
                "source_id": source.source_id,
                "source_name": source.name,
                "source_kind": source.source_kind,
            },
        )
    if watch.camera_inventory_id is not None:
        camera = session.get(CameraInventoryORM, watch.camera_inventory_id)
        if camera is None:
            raise ValueError(f"Camera inventory record {watch.camera_inventory_id} does not exist.")
        if not camera.image_url:
            raise ValueError(
                f"Camera inventory record {camera.camera_inventory_id} has no image_url."
            )
        ensure_http_url(camera.image_url)
        return (
            camera.image_url,
            build_camera_image_fetch_config(),
            {
                "reference_type": "camera_inventory",
                "camera_inventory_id": camera.camera_inventory_id,
                "camera_key": camera.camera_key,
                "camera_name": camera.name,
            },
        )
    if watch.camera_source_inventory_id is not None:
        camera_source = session.get(CameraSourceInventoryORM, watch.camera_source_inventory_id)
        if camera_source is None:
            raise ValueError(
                f"Camera source inventory record {watch.camera_source_inventory_id} does not exist."
            )
        if camera_source.endpoint_kind != "image":
            raise ValueError(
                f"Camera source inventory record {camera_source.camera_source_inventory_id} "
                "is not an image endpoint."
            )
        ensure_http_url(camera_source.endpoint_url)
        return (
            camera_source.endpoint_url,
            build_camera_image_fetch_config(),
            {
                "reference_type": "camera_source_inventory",
                "camera_source_inventory_id": camera_source.camera_source_inventory_id,
                "candidate_key": camera_source.candidate_key,
                "camera_inventory_id": camera_source.camera_inventory_id,
            },
        )
    raise ValueError("image_change watch has no image reference.")


def build_camera_image_fetch_config() -> SourceFetchConfig:
    settings = get_settings()
    return SourceFetchConfig(
        timeout_seconds=30.0,
        retry_attempts=3,
        retry_backoff_seconds=0.0,
        headers={
            "User-Agent": "11Writer-Forte/0.1 (+watch-image-fetch)",
            "Accept": "image/jpeg, image/png, image/webp, image/gif",
        },
        max_payload_bytes=max(1, int(settings.source_fetch_max_payload_bytes)),
        allow_private_networks=bool(settings.source_fetch_allow_private_networks),
        min_request_interval_seconds=max(
            0.0,
            float(settings.source_fetch_min_request_interval_seconds),
        ),
    )


def ensure_http_url(value: str) -> None:
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("Image watch targets must use an absolute http:// or https:// URL.")


def normalize_media_type(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.split(";", 1)[0].strip().lower()
    return candidate or None


def media_type_is_accepted(media_type: str, accepted_media_types: list[str]) -> bool:
    normalized = {value.strip().lower() for value in accepted_media_types}
    return media_type in normalized or "image/*" in normalized


def retain_image_evidence(
    session: Session,
    *,
    watch: WatchORM,
    run: WatchRunORM,
    payload: bytes,
    source_uri: str,
    media_type: str,
    fingerprint: str,
    retrieved_at: datetime,
    retention_class: str,
    fetch_metadata: dict[str, Any],
    reference: dict[str, Any],
    actor: str,
) -> tuple[StorageObjectORM, bool]:
    object_key = f"watch:{watch.watch_id}:image:{fingerprint}"
    existing = session.scalar(
        select(StorageObjectORM).where(StorageObjectORM.object_key == object_key).limit(1)
    )
    if existing is not None:
        return existing, False

    extension = image_file_extension(media_type)
    artifact_dir = (
        get_settings().data_dir_effective / "artifacts" / "watches" / watch.slug
    ).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = (artifact_dir / f"{fingerprint}{extension}").resolve()
    if artifact_dir not in artifact_path.parents:
        raise RuntimeError("Resolved watch artifact path escaped the configured watch directory.")
    if artifact_path.exists():
        existing_hash = hashlib.sha256(artifact_path.read_bytes()).hexdigest()
        if existing_hash != fingerprint:
            raise RuntimeError(f"Watch artifact path {artifact_path} contains unexpected bytes.")
    else:
        temporary_path = artifact_path.with_name(f".{artifact_path.name}.{run.watch_run_id}.tmp")
        try:
            temporary_path.write_bytes(payload)
            temporary_path.replace(artifact_path)
        finally:
            if temporary_path.exists():
                temporary_path.unlink()

    safe_fetch_metadata = sanitize_fetch_metadata(fetch_metadata)
    record = register_storage_object(
        session,
        object_key=object_key,
        object_kind="watch_image_evidence",
        owner_type="watch_run",
        owner_id=str(run.watch_run_id),
        object_uri=str(artifact_path),
        source_uri=source_uri,
        content_hash=fingerprint,
        media_type=media_type,
        storage_tier="warm",
        retention_class=retention_class,
        lifecycle_status="active",
        byte_size=len(payload),
        observed_at=retrieved_at,
        metadata_json={
            "watch_id": watch.watch_id,
            "watch_run_id": run.watch_run_id,
            "watch_slug": watch.slug,
            "comparison": "sha256",
            "retrieved_at": retrieved_at.isoformat(),
            "payload_sha256": fingerprint,
            "media_type": media_type,
            "storage_managed": True,
            "archive_eligible": True,
            "prune_eligible": True,
            "fetch": safe_fetch_metadata,
            **reference,
        },
        actor=actor,
    )
    session.add(
        CustodyLogORM(
            object_type="watch_run",
            object_id=str(run.watch_run_id),
            action="watch_evidence_retained",
            actor=actor,
            details_json={
                "watch_id": watch.watch_id,
                "watch_run_id": run.watch_run_id,
                "storage_object_id": record.storage_object_id,
                "object_key": object_key,
                "source_uri": source_uri,
                "retrieved_at": retrieved_at.isoformat(),
                "payload_sha256": fingerprint,
                "media_type": media_type,
                "byte_size": len(payload),
                "retention_class": retention_class,
            },
        )
    )
    return record, True


def image_file_extension(media_type: str) -> str:
    return {
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
        "image/gif": ".gif",
    }.get(media_type, ".img")


def sanitize_fetch_metadata(fetch_metadata: dict[str, Any]) -> dict[str, Any]:
    allowed_keys = {
        "attempt_count",
        "http_status",
        "content_type",
        "byte_count",
        "payload_sha256",
        "request_timeout_seconds",
        "retry_attempts",
        "max_payload_bytes",
        "allow_private_networks",
        "host",
        "requested_url",
    }
    return {key: json_safe(value) for key, value in fetch_metadata.items() if key in allowed_keys}


def observation_matches_geofence(
    observation: ObservationORM,
    geofence: GeofenceORM | None,
) -> bool:
    if geofence is None:
        return True
    coordinates = (observation.location_geojson or {}).get("coordinates")
    if not isinstance(coordinates, list) or len(coordinates) < 2:
        return False
    try:
        point = (float(coordinates[0]), float(coordinates[1]))
    except (TypeError, ValueError):
        return False
    return point_in_geometry(point, geofence.geometry_geojson)


def observation_matches_predicates(
    observation: ObservationORM,
    predicates: list[ObservationPredicate],
) -> bool:
    return all(observation_matches_predicate(observation, predicate) for predicate in predicates)


def observation_matches_predicate(
    observation: ObservationORM,
    predicate: ObservationPredicate,
) -> bool:
    actual = resolve_observation_field(observation, predicate.field)
    operator = predicate.operator
    expected = predicate.value
    if operator == "exists":
        exists = actual is not _MISSING
        expected_exists = True if expected is None else bool(expected)
        return exists is expected_exists
    if actual is _MISSING:
        return False
    if operator == "eq":
        return actual == expected
    if operator == "ne":
        return actual != expected
    if operator == "in":
        if not isinstance(expected, list):
            return False
        if isinstance(actual, list):
            return any(value in expected for value in actual)
        return actual in expected
    if operator == "contains":
        if isinstance(actual, str):
            return isinstance(expected, str) and expected in actual
        if isinstance(actual, (list, tuple, set)):
            return expected in actual
        if isinstance(actual, dict):
            return expected in actual
        return False
    if operator in {"gt", "gte", "lt", "lte"}:
        return compare_ordered_values(actual, expected, operator)
    return False


def resolve_observation_field(observation: ObservationORM, field: str) -> Any:
    direct_fields = {
        "observation_id",
        "import_run_id",
        "event_id",
        "layer_key",
        "source_domain",
        "source_type",
        "record_format",
        "trust_level",
        "approval_policy",
        "confidence_score",
        "observed_at",
        "content_text",
        "raw_hash",
        "created_at",
        "updated_at",
    }
    segments = field.split(".")
    if segments[0] in direct_fields:
        value: Any = getattr(observation, segments[0])
        segments = segments[1:]
    else:
        value = observation.content_json or {}
        if segments[0] == "content_json":
            segments = segments[1:]
    for segment in segments:
        if not isinstance(value, dict) or segment not in value:
            return _MISSING
        value = value[segment]
    return value


def compare_ordered_values(actual: Any, expected: Any, operator: str) -> bool:
    if isinstance(actual, bool) or isinstance(expected, bool):
        return False
    if isinstance(actual, (int, float)) and isinstance(expected, (int, float)):
        left: Any = float(actual)
        right: Any = float(expected)
    elif isinstance(actual, str) and isinstance(expected, str):
        left = actual
        right = expected
    else:
        return False
    if operator == "gt":
        return left > right
    if operator == "gte":
        return left >= right
    if operator == "lt":
        return left < right
    return left <= right


def resolve_source_health_state(
    source: SourceDefinitionORM,
    *,
    latest_run: SourceRunORM | None,
    latest_success: SourceRunORM | None,
    checkpoint: SourceCheckpointORM | None,
    reference_time: datetime,
    stale_after_seconds: int,
) -> str:
    if not source.enabled:
        return "disabled"
    if latest_run is not None and latest_run.status == "failed":
        return "failed"
    if checkpoint is not None and checkpoint.status == "degraded":
        return "failed"
    if latest_run is None:
        return "never_run"
    if latest_success is None:
        return "never_run"
    successful_at = normalize_timestamp(latest_success.finished_at or latest_success.started_at)
    if successful_at < reference_time - timedelta(seconds=stale_after_seconds):
        return "stale"
    return "healthy"


def hash_json_value(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def normalize_timestamp(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def isoformat_or_none(value: datetime | None) -> str | None:
    return normalize_timestamp(value).isoformat() if value is not None else None


def json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return normalize_timestamp(value).isoformat()
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    return value


def alert_watch_identity(alert: AlertORM) -> int | None:
    trigger = alert.trigger_basis_json if isinstance(alert.trigger_basis_json, dict) else {}
    value = trigger.get("watch_id")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    dedupe_key = alert.dedupe_key or ""
    parts = dedupe_key.split(":", 2)
    if len(parts) >= 3 and parts[0] == "watch" and parts[1].isdigit():
        return int(parts[1])
    return None


def rss_datetime(value: datetime) -> str:
    return format_datetime(normalize_timestamp(value), usegmt=True)
