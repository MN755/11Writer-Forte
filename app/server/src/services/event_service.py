from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import AlertORM, EventORM, ScheduledTaskORM, ScheduledTaskRunORM, SituationProductORM


def event_ops_now() -> datetime:
    return datetime.now(timezone.utc)


def list_event_records(
    session: Session,
    *,
    status: str | None = None,
    redaction_level: str | None = None,
    limit: int | None = 200,
) -> list[EventORM]:
    statement = select(EventORM).order_by(EventORM.created_at.desc(), EventORM.event_id.desc())
    if status is not None:
        statement = statement.where(EventORM.status == status)
    if redaction_level is not None:
        statement = statement.where(EventORM.redaction_level == redaction_level)
    if limit is not None:
        statement = statement.limit(limit)
    return list(session.scalars(statement))


def build_event_inventory_summary(
    session: Session,
    *,
    status: str | None = None,
    redaction_level: str | None = None,
    stale_after_hours: float = 24.0,
) -> dict[str, object]:
    generated_at = event_ops_now()
    stale_before = generated_at - timedelta(hours=max(0.0, stale_after_hours))
    events = list_event_records(
        session,
        status=status,
        redaction_level=redaction_level,
        limit=None,
    )
    event_ids = [event.event_id for event in events]
    alerts_by_event_id = count_alerts_by_event_id(session, event_ids)
    products_by_event_id = count_products_by_event_id(session, event_ids)
    return {
        "generated_at": generated_at,
        "stale_before": stale_before,
        "total_count": len(events),
        "open_count": sum(1 for event in events if event.status == "open"),
        "closed_count": sum(1 for event in events if event.status == "closed"),
        "stale_open_count": sum(1 for event in events if is_stale_open_event(event, stale_before)),
        "alert_scoped_count": sum(1 for event in events if alerts_by_event_id.get(event.event_id, 0) > 0),
        "product_covered_count": sum(1 for event in events if products_by_event_id.get(event.event_id, 0) > 0),
        "status_counts": build_event_summary_buckets(
            events,
            key_fn=lambda event: event.status or "unknown",
            stale_before=stale_before,
        ),
        "redaction_level_counts": build_event_summary_buckets(
            events,
            key_fn=lambda event: event.redaction_level or "unknown",
            stale_before=stale_before,
        ),
        "confidence_band_counts": build_event_summary_buckets(
            events,
            key_fn=lambda event: event_confidence_band(event),
            stale_before=stale_before,
        ),
    }


def build_event_ops_report_index(
    session: Session,
    *,
    status: str | None = None,
    redaction_level: str | None = None,
    stale_after_hours: float = 24.0,
    limit: int = 25,
    stale_event_limit: int = 25,
) -> dict[str, object]:
    generated_at = event_ops_now()
    summary = build_event_inventory_summary(
        session,
        status=status,
        redaction_level=redaction_level,
        stale_after_hours=stale_after_hours,
    )
    stale_before = summary["stale_before"]
    scoped_events = list_event_records(
        session,
        status=status,
        redaction_level=redaction_level,
        limit=None,
    )
    event_ids = [event.event_id for event in scoped_events]
    fusion_tasks = list(
        session.scalars(
            select(ScheduledTaskORM)
            .where(ScheduledTaskORM.task_type == "event_fusion_refresh")
            .order_by(ScheduledTaskORM.task_id.asc())
        )
    )
    task_ids = [task.task_id for task in fusion_tasks]
    fusion_runs = (
        list(
            session.scalars(
                select(ScheduledTaskRunORM)
                .where(ScheduledTaskRunORM.task_id.in_(task_ids))
                .order_by(ScheduledTaskRunORM.task_run_id.desc())
            )
        )
        if task_ids
        else []
    )
    recent_products = (
        list(
            session.scalars(
                select(SituationProductORM)
                .where(SituationProductORM.event_id.in_(event_ids))
                .order_by(SituationProductORM.updated_at.desc(), SituationProductORM.product_id.desc())
                .limit(limit)
            )
        )
        if event_ids
        else []
    )
    recent_alerts = (
        list(
            session.scalars(
                select(AlertORM)
                .where(AlertORM.event_id.in_(event_ids))
                .order_by(AlertORM.created_at.desc(), AlertORM.alert_id.desc())
                .limit(limit)
            )
        )
        if event_ids
        else []
    )
    return {
        "generated_at": generated_at,
        "stale_after_hours": stale_after_hours,
        "latest_event_at": scoped_events[0].created_at if scoped_events else None,
        "inventory_summary": summary,
        "event_fusion_task_count": len(fusion_tasks),
        "event_fusion_run_count": len(fusion_runs),
        "event_fusion_failure_count": sum(1 for run in fusion_runs if run.status == "failed"),
        "event_fusion_tasks": fusion_tasks,
        "recent_fusion_runs": fusion_runs[:limit],
        "recent_events": scoped_events[:limit],
        "stale_open_events": [event for event in scoped_events if is_stale_open_event(event, stale_before)][:stale_event_limit],
        "recent_products": recent_products,
        "recent_alerts": recent_alerts,
    }


def build_event_ops_export_summary(
    session: Session,
    *,
    status: str | None = None,
    redaction_level: str | None = None,
    stale_after_hours: float = 24.0,
    event_limit: int = 500,
    report_limit: int = 25,
    stale_event_limit: int = 25,
) -> dict[str, object]:
    generated_at = event_ops_now()
    events = list_event_records(
        session,
        status=status,
        redaction_level=redaction_level,
        limit=event_limit,
    )
    return {
        "generated_at": generated_at,
        "filters_json": {
            "status": status,
            "redaction_level": redaction_level,
            "stale_after_hours": stale_after_hours,
            "event_limit": event_limit,
            "report_limit": report_limit,
            "stale_event_limit": stale_event_limit,
        },
        "report_index": build_event_ops_report_index(
            session,
            status=status,
            redaction_level=redaction_level,
            stale_after_hours=stale_after_hours,
            limit=report_limit,
            stale_event_limit=stale_event_limit,
        ),
        "events": events,
    }


def build_event_summary_buckets(
    events: list[EventORM],
    *,
    key_fn,
    stale_before: datetime,
) -> list[dict[str, object]]:
    buckets: dict[str, dict[str, object]] = {}
    for event in events:
        key = str(key_fn(event) or "unknown")
        bucket = buckets.setdefault(
            key,
            {
                "key": key,
                "total_count": 0,
                "open_count": 0,
                "closed_count": 0,
                "stale_open_count": 0,
            },
        )
        bucket["total_count"] += 1
        if event.status == "open":
            bucket["open_count"] += 1
        if event.status == "closed":
            bucket["closed_count"] += 1
        if is_stale_open_event(event, stale_before):
            bucket["stale_open_count"] += 1
    return sorted(buckets.values(), key=lambda item: (-int(item["total_count"]), str(item["key"]).lower()))


def is_stale_open_event(event: EventORM, stale_before: datetime) -> bool:
    return event.status == "open" and event_reference_timestamp(event) < stale_before


def event_reference_timestamp(event: EventORM) -> datetime:
    if event.occurred_at is not None:
        return normalize_timestamp(event.occurred_at)
    return normalize_timestamp(event.updated_at)


def normalize_timestamp(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def event_confidence_band(event: EventORM) -> str:
    metadata = event.metadata_json if isinstance(event.metadata_json, dict) else {}
    value = metadata.get("confidence_band")
    if isinstance(value, str) and value.strip():
        return value.strip().lower()
    score = metadata.get("verification_score")
    if isinstance(score, (int, float)):
        numeric = float(score)
        if numeric >= 0.85:
            return "high"
        if numeric >= 0.65:
            return "medium"
        return "low"
    return "unknown"


def count_alerts_by_event_id(session: Session, event_ids: list[int]) -> dict[int, int]:
    if not event_ids:
        return {}
    rows = list(
        session.scalars(
            select(AlertORM).where(AlertORM.event_id.in_(event_ids))
        )
    )
    counts: dict[int, int] = {}
    for row in rows:
        if row.event_id is None:
            continue
        counts[row.event_id] = counts.get(row.event_id, 0) + 1
    return counts


def count_products_by_event_id(session: Session, event_ids: list[int]) -> dict[int, int]:
    if not event_ids:
        return {}
    rows = list(
        session.scalars(
            select(SituationProductORM).where(SituationProductORM.event_id.in_(event_ids))
        )
    )
    counts: dict[int, int] = {}
    for row in rows:
        counts[row.event_id] = counts.get(row.event_id, 0) + 1
    return counts
