from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from email.utils import format_datetime
from xml.etree import ElementTree

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import (
    AlertORM,
    CustodyLogORM,
    ObservationORM,
    ScheduledTaskORM,
    SourceDefinitionORM,
    SourceRunORM,
    StorageObjectORM,
    WatchORM,
    WatchRunORM,
)
from src.schemas import WatchCreate, WatchScheduleCreate, WatchUpdate
from src.config import get_settings
from src.services.discovery_fetch import FetchPolicy, fetch_url
from src.services.storage_service import register_storage_object
from src.services.scheduler_service import create_scheduled_task
from src.schemas import ScheduledTaskCreate
from src.services.source_service import run_source_definition


def now() -> datetime:
    return datetime.now(timezone.utc)


def require_watch(session: Session, watch_id: int) -> WatchORM:
    watch = session.get(WatchORM, watch_id)
    if watch is None:
        raise ValueError(f"Watch {watch_id} does not exist.")
    return watch


def create_watch(session: Session, payload: WatchCreate, *, actor: str = "api") -> WatchORM:
    if session.scalar(select(WatchORM).where(WatchORM.name == payload.name)) is not None:
        raise ValueError(f"Watch name {payload.name!r} already exists.")
    if session.scalar(select(WatchORM).where(WatchORM.slug == payload.slug)) is not None:
        raise ValueError(f"Watch slug {payload.slug!r} already exists.")
    watch = WatchORM(**payload.model_dump(mode="python"))
    session.add(watch)
    session.flush()
    session.add(
        CustodyLogORM(
            object_type="watch",
            object_id=str(watch.watch_id),
            action="watch_created",
            actor=actor,
            details_json={"watch_type": watch.watch_type, "rule_json": watch.rule_json},
        )
    )
    session.commit()
    session.refresh(watch)
    return watch


def list_watches(
    session: Session, *, state: str | None = None, watch_type: str | None = None, limit: int = 200
) -> list[WatchORM]:
    statement = select(WatchORM).order_by(WatchORM.watch_id.desc())
    if state:
        statement = statement.where(WatchORM.state == state)
    if watch_type:
        statement = statement.where(WatchORM.watch_type == watch_type)
    return list(session.scalars(statement.limit(limit)))


def get_watch(session: Session, watch_id: int) -> WatchORM:
    return require_watch(session, watch_id)


def update_watch(
    session: Session, watch_id: int, payload: WatchUpdate, *, actor: str = "api"
) -> WatchORM:
    watch = require_watch(session, watch_id)
    changes = payload.model_dump(mode="python", exclude_unset=True)
    for key, value in changes.items():
        setattr(watch, key, value)
    if any(key in changes for key in {"watch_type", "rule_json", "source_id", "layer_key"}):
        watch.baseline_json, watch.dedupe_json = {}, {}
    session.add(
        CustodyLogORM(
            object_type="watch",
            object_id=str(watch_id),
            action="watch_updated",
            actor=actor,
            details_json={"changes": list(changes)},
        )
    )
    session.commit()
    session.refresh(watch)
    return watch


def pause_watch(session: Session, watch_id: int, *, actor: str = "api") -> WatchORM:
    return set_watch_state(session, watch_id, "paused", actor=actor)


def resume_watch(session: Session, watch_id: int, *, actor: str = "api") -> WatchORM:
    return set_watch_state(session, watch_id, "enabled", actor=actor)


def set_watch_state(session: Session, watch_id: int, state: str, *, actor: str) -> WatchORM:
    watch = require_watch(session, watch_id)
    watch.state = state
    session.add(
        CustodyLogORM(
            object_type="watch",
            object_id=str(watch_id),
            action=f"watch_{state}",
            actor=actor,
            details_json={},
        )
    )
    session.commit()
    session.refresh(watch)
    return watch


def evaluate_watch(
    session: Session,
    watch_id: int,
    *,
    actor: str = "api",
    force: bool = False,
    scheduled_task_run_id: int | None = None,
) -> WatchRunORM:
    watch = require_watch(session, watch_id)
    if watch.state != "enabled" and not force:
        raise ValueError(f"Watch {watch_id} is paused.")
    run = WatchRunORM(
        watch_id=watch_id,
        scheduled_task_run_id=scheduled_task_run_id,
        checkpoint_before_json=dict(watch.baseline_json or {}),
    )
    session.add(run)
    session.flush()
    try:
        fingerprint, evidence, message, source_run_id, storage_object_id = evaluate_rule(
            session, watch, actor
        )
        baseline = dict(watch.baseline_json or {})
        initialized = "fingerprint" not in baseline
        changed = not initialized and baseline.get("fingerprint") != fingerprint
        baseline["fingerprint"] = fingerprint
        baseline["evaluated_at"] = now().isoformat()
        watch.baseline_json = baseline
        watch.last_evaluated_at = now()
        run.baseline_initialized = initialized
        run.change_detected = changed
        run.source_run_id = source_run_id
        run.storage_object_id = storage_object_id
        run.outcome = "baseline" if initialized else ("change" if changed else "no_change")
        run.status = "completed"
        run.evidence_json = evidence
        run.output_summary = message
        run.checkpoint_after_json = baseline
        run.finished_at = now()
        if changed:
            key = f"watch:{watch.watch_id}:{fingerprint}"
            alert = session.scalar(
                select(AlertORM).where(AlertORM.dedupe_key == key, AlertORM.status == "open")
            )
            if alert is None:
                alert = AlertORM(
                    severity=watch.severity,
                    status="open",
                    dedupe_key=key,
                    message=message,
                    trigger_basis_json={
                        "watch_id": watch.watch_id,
                        "watch_run_id": run.watch_run_id,
                        "evidence": evidence,
                    },
                )
                session.add(alert)
                session.flush()
                session.add(
                    CustodyLogORM(
                        object_type="alert",
                        object_id=str(alert.alert_id),
                        action="alert_created",
                        actor=actor,
                        details_json={"watch_id": watch.watch_id},
                    )
                )
            run.alert_id = alert.alert_id
            watch.last_changed_at = now()
        session.add(
            CustodyLogORM(
                object_type="watch_run",
                object_id=str(run.watch_run_id),
                action="watch_evaluated",
                actor=actor,
                details_json={"outcome": run.outcome, "change_detected": changed},
            )
        )
        session.commit()
        session.refresh(run)
        return run
    except Exception as exc:
        run.status, run.outcome, run.error_text, run.finished_at = (
            "failed",
            "failure",
            str(exc),
            now(),
        )
        session.commit()
        raise


def evaluate_rule(
    session: Session, watch: WatchORM, actor: str
) -> tuple[str, dict[str, object], str, int | None, int | None]:
    rule = watch.rule_json or {}
    if watch.watch_type == "image_change":
        if watch.source_id is None:
            raise ValueError("image_change watches require source_id.")
        source = session.get(SourceDefinitionORM, watch.source_id)
        if source is None or not source.enabled:
            raise ValueError("image_change watch source is missing or disabled.")
        result = fetch_url(source.target_uri, policy=FetchPolicy(max_response_bytes=20_000_000))
        media_type = str(result.headers.get("Content-Type", "")).split(";", 1)[0].lower()
        accepted = set(
            rule.get("accepted_media_types", ["image/jpeg", "image/png", "image/webp", "image/gif"])
        )
        if media_type not in accepted:
            raise ValueError(f"Image watch received unsupported media type {media_type!r}.")
        digest = hashlib.sha256(result.payload).hexdigest()
        artifact_dir = get_settings().data_dir / "watch_artifacts"
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = artifact_dir / f"watch-{watch.watch_id}-{digest[:16]}"
        if not artifact_path.exists():
            artifact_path.write_bytes(result.payload)
        storage = register_storage_object(
            session,
            object_key=f"watch:{watch.watch_id}:image:{digest}",
            object_kind="watch_image_artifact",
            owner_type="watch",
            owner_id=str(watch.watch_id),
            object_uri=artifact_path.resolve().as_uri(),
            content_hash=digest,
            media_type=media_type,
            retention_class=rule.get("retention_class", "permanent"),
            source_uri=result.final_url,
            byte_size=len(result.payload),
            metadata_json={"watch_id": watch.watch_id, "status_code": result.status_code},
            actor=actor,
        )
        return (
            digest,
            {"source_id": source.source_id, "source_uri": result.final_url, "sha256": digest},
            f"Watch {watch.name} detected an image update.",
            None,
            storage.storage_object_id,
        )
    if watch.watch_type == "source_delta":
        if watch.source_id is None:
            raise ValueError("source_delta watches require source_id.")
        source_run = (
            run_source_definition(session, watch.source_id, actor=actor)
            if rule.get("run_source", True)
            else session.scalar(
                select(SourceRunORM)
                .where(SourceRunORM.source_id == watch.source_id)
                .order_by(SourceRunORM.source_run_id.desc())
            )
        )
        if source_run is None:
            raise ValueError("Source has no run to evaluate.")
        fingerprint = str(
            (source_run.output_json or {}).get("payload_sha256") or source_run.source_run_id
        )
        return (
            fingerprint,
            {"source_id": watch.source_id, "source_run_id": source_run.source_run_id},
            f"Watch {watch.name} detected a source update.",
            source_run.source_run_id,
            None,
        )
    if watch.watch_type == "observation_rule":
        statement = (
            select(ObservationORM)
            .order_by(ObservationORM.observation_id.desc())
            .limit(int(rule.get("max_results", 200)))
        )
        if watch.layer_key:
            statement = statement.where(ObservationORM.layer_key == watch.layer_key)
        if rule.get("source_domain"):
            statement = statement.where(ObservationORM.source_domain == rule["source_domain"])
        rows = list(session.scalars(statement))
        ids = [
            row.observation_id
            for row in rows
            if row.confidence_score >= float(rule.get("min_confidence", 0.0))
        ]
        fingerprint = hashlib.sha256(",".join(map(str, ids)).encode()).hexdigest()
        return (
            fingerprint,
            {"observation_ids": ids},
            f"Watch {watch.name} detected {len(ids)} matching observations.",
            None,
            None,
        )
    if watch.watch_type == "source_health":
        if watch.source_id is None:
            raise ValueError("source_health watches require source_id.")
        source = session.get(SourceDefinitionORM, watch.source_id)
        latest = session.scalar(
            select(SourceRunORM)
            .where(SourceRunORM.source_id == watch.source_id)
            .order_by(SourceRunORM.source_run_id.desc())
        )
        state = (
            "disabled"
            if source is None or not source.enabled
            else ("never_run" if latest is None else latest.status)
        )
        return (
            state,
            {"source_id": watch.source_id, "state": state},
            f"Watch {watch.name} source health is {state}.",
            latest.source_run_id if latest else None,
            None,
        )
    raise ValueError(f"Watch type {watch.watch_type!r} is not yet wired to a canonical evaluator.")


def attach_watch_schedule(
    session: Session, watch_id: int, payload: WatchScheduleCreate, *, actor: str = "api"
) -> ScheduledTaskORM:
    watch = require_watch(session, watch_id)
    task = create_scheduled_task(
        session,
        ScheduledTaskCreate(
            name=payload.name or f"watch-{watch.slug}",
            task_type="watch_evaluate",
            interval_seconds=payload.interval_seconds,
            enabled=payload.enabled,
            retry_attempts=payload.retry_attempts,
            retry_backoff_seconds=payload.retry_backoff_seconds,
            notes=payload.notes,
            payload_json={"watch_id": watch_id},
        ),
    )
    watch.scheduled_task_id, watch.interval_seconds, watch.next_run_at = (
        task.task_id,
        task.interval_seconds,
        task.next_run_at,
    )
    session.commit()
    return task


def list_watch_runs(
    session: Session,
    *,
    watch_id: int | None = None,
    status: str | None = None,
    outcome: str | None = None,
    limit: int = 200,
) -> list[WatchRunORM]:
    statement = select(WatchRunORM).order_by(WatchRunORM.watch_run_id.desc())
    if watch_id:
        statement = statement.where(WatchRunORM.watch_id == watch_id)
    if status:
        statement = statement.where(WatchRunORM.status == status)
    if outcome:
        statement = statement.where(WatchRunORM.outcome == outcome)
    return list(session.scalars(statement.limit(limit)))


def list_watch_alerts(
    session: Session, *, watch_id: int | None = None, status: str | None = None, limit: int = 200
) -> list[AlertORM]:
    statement = select(AlertORM).order_by(AlertORM.alert_id.desc())
    if status:
        statement = statement.where(AlertORM.status == status)
    if watch_id:
        statement = statement.where(
            AlertORM.trigger_basis_json["watch_id"].as_integer() == watch_id
        )
    return list(session.scalars(statement.limit(limit)))


def list_watch_evidence(
    session: Session, watch_id: int, *, limit: int = 200
) -> list[StorageObjectORM]:
    require_watch(session, watch_id)
    ids = [
        str(run.storage_object_id)
        for run in list_watch_runs(session, watch_id=watch_id, limit=limit)
        if run.storage_object_id
    ]
    return (
        list(
            session.scalars(
                select(StorageObjectORM).where(StorageObjectORM.storage_object_id.in_(ids))
            )
        )
        if ids
        else []
    )


def render_watch_alert_rss(
    session: Session, *, base_url: str, watch_id: int | None, status: str | None, limit: int
) -> str:
    root = ElementTree.Element("rss", version="2.0")
    channel = ElementTree.SubElement(root, "channel")
    ElementTree.SubElement(channel, "title").text = "11Writer Forte Watch Alerts"
    ElementTree.SubElement(channel, "link").text = f"{base_url}/api/watches/feed.rss"
    ElementTree.SubElement(channel, "description").text = "Local rule-generated watch alerts"
    for alert in list_watch_alerts(session, watch_id=watch_id, status=status, limit=limit):
        item = ElementTree.SubElement(channel, "item")
        ElementTree.SubElement(item, "title").text = f"[{alert.severity}] {alert.message}"
        ElementTree.SubElement(item, "guid").text = f"alert:{alert.alert_id}"
        ElementTree.SubElement(item, "pubDate").text = format_datetime(alert.created_at)
    return ElementTree.tostring(root, encoding="unicode", xml_declaration=True)
