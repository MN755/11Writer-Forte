from __future__ import annotations

from datetime import datetime, timedelta, timezone
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import AlertORM, ScheduledTaskORM, ScheduledTaskRunORM


def alert_now() -> datetime:
    return datetime.now(timezone.utc)


def list_alert_records(
    session: Session,
    *,
    status: str | None = None,
    geofence_id: int | None = None,
    event_id: int | None = None,
    severity: str | None = None,
    limit: int | None = 200,
) -> list[AlertORM]:
    statement = select(AlertORM).order_by(AlertORM.created_at.desc())
    if status is not None:
        statement = statement.where(AlertORM.status == status)
    if geofence_id is not None:
        statement = statement.where(AlertORM.geofence_id == geofence_id)
    if event_id is not None:
        statement = statement.where(AlertORM.event_id == event_id)
    if severity is not None:
        statement = statement.where(AlertORM.severity == severity)
    if limit is not None:
        statement = statement.limit(limit)
    return list(session.scalars(statement))


def build_alert_inventory_summary(
    session: Session,
    *,
    status: str | None = None,
    geofence_id: int | None = None,
    event_id: int | None = None,
    severity: str | None = None,
    stale_after_hours: float = 24.0,
) -> dict[str, object]:
    generated_at = alert_now()
    stale_before = generated_at - timedelta(hours=max(0.0, stale_after_hours))
    alerts = list_alert_records(
        session,
        status=status,
        geofence_id=geofence_id,
        event_id=event_id,
        severity=severity,
        limit=None,
    )
    return {
        "generated_at": generated_at,
        "stale_before": stale_before,
        "total_count": len(alerts),
        "open_count": sum(1 for alert in alerts if alert.status == "open"),
        "acknowledged_count": sum(1 for alert in alerts if alert.status == "acknowledged"),
        "closed_count": sum(1 for alert in alerts if alert.status == "closed"),
        "stale_open_count": sum(1 for alert in alerts if is_stale_open_alert(alert, stale_before)),
        "geofence_scoped_count": sum(1 for alert in alerts if alert.geofence_id is not None),
        "event_scoped_count": sum(1 for alert in alerts if alert.event_id is not None),
        "unscoped_count": sum(1 for alert in alerts if alert.geofence_id is None and alert.event_id is None),
        "severity_counts": build_alert_summary_buckets(alerts, lambda alert: alert.severity, stale_before),
        "status_counts": build_alert_summary_buckets(alerts, lambda alert: alert.status, stale_before),
        "geofence_counts": build_alert_summary_buckets(
            alerts,
            lambda alert: str(alert.geofence_id) if alert.geofence_id is not None else "none",
            stale_before,
        ),
    }


def build_alert_ops_report_index(
    session: Session,
    *,
    status: str | None = None,
    geofence_id: int | None = None,
    event_id: int | None = None,
    severity: str | None = None,
    stale_after_hours: float = 24.0,
    limit: int = 25,
    stale_alert_limit: int = 25,
) -> dict[str, object]:
    generated_at = alert_now()
    stale_before = generated_at - timedelta(hours=max(0.0, stale_after_hours))
    alerts = list_alert_records(
        session,
        status=status,
        geofence_id=geofence_id,
        event_id=event_id,
        severity=severity,
        limit=None,
    )
    geofence_scan_tasks = list(
        session.scalars(
            select(ScheduledTaskORM)
            .where(ScheduledTaskORM.task_type == "geofence_scan")
            .order_by(ScheduledTaskORM.task_id.asc())
        )
    )
    matching_tasks = [
        task
        for task in geofence_scan_tasks
        if geofence_scan_task_matches_scope(task, geofence_id=geofence_id)
    ]
    task_ids = [task.task_id for task in matching_tasks]
    all_runs = (
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
    observation_watch_tasks = list(
        session.scalars(
            select(ScheduledTaskORM)
            .where(ScheduledTaskORM.task_type == "observation_watch_scan")
            .order_by(ScheduledTaskORM.task_id.asc())
        )
    )
    observation_watch_task_ids = [task.task_id for task in observation_watch_tasks]
    observation_watch_runs = (
        list(
            session.scalars(
                select(ScheduledTaskRunORM)
                .where(ScheduledTaskRunORM.task_id.in_(observation_watch_task_ids))
                .order_by(ScheduledTaskRunORM.task_run_id.desc())
            )
        )
        if observation_watch_task_ids
        else []
    )
    return {
        "generated_at": generated_at,
        "stale_after_hours": stale_after_hours,
        "latest_alert_at": alerts[0].created_at if alerts else None,
        "inventory_summary": build_alert_inventory_summary(
            session,
            status=status,
            geofence_id=geofence_id,
            event_id=event_id,
            severity=severity,
            stale_after_hours=stale_after_hours,
        ),
        "geofence_scan_task_count": len(matching_tasks),
        "geofence_scan_run_count": len(all_runs),
        "geofence_scan_failure_count": sum(1 for run in all_runs if run.status == "failed"),
        "geofence_scan_tasks": matching_tasks,
        "observation_watch_task_count": len(observation_watch_tasks),
        "observation_watch_run_count": len(observation_watch_runs),
        "observation_watch_failure_count": sum(
            1 for run in observation_watch_runs if run.status == "failed"
        ),
        "observation_watch_tasks": observation_watch_tasks,
        "recent_alerts": alerts[:limit],
        "stale_open_alerts": [alert for alert in alerts if is_stale_open_alert(alert, stale_before)][:stale_alert_limit],
        "unscoped_alerts": [
            alert for alert in alerts if alert.geofence_id is None and alert.event_id is None
        ][:stale_alert_limit],
    }


def build_alert_ops_export_summary(
    session: Session,
    *,
    status: str | None = None,
    geofence_id: int | None = None,
    event_id: int | None = None,
    severity: str | None = None,
    stale_after_hours: float = 24.0,
    alert_limit: int = 500,
    report_limit: int = 25,
    stale_alert_limit: int = 25,
) -> dict[str, object]:
    generated_at = alert_now()
    alerts = list_alert_records(
        session,
        status=status,
        geofence_id=geofence_id,
        event_id=event_id,
        severity=severity,
        limit=alert_limit,
    )
    return {
        "generated_at": generated_at,
        "filters_json": {
            "status": status,
            "geofence_id": geofence_id,
            "event_id": event_id,
            "severity": severity,
            "stale_after_hours": stale_after_hours,
            "alert_limit": alert_limit,
            "report_limit": report_limit,
            "stale_alert_limit": stale_alert_limit,
        },
        "report_index": build_alert_ops_report_index(
            session,
            status=status,
            geofence_id=geofence_id,
            event_id=event_id,
            severity=severity,
            stale_after_hours=stale_after_hours,
            limit=report_limit,
            stale_alert_limit=stale_alert_limit,
        ),
        "alerts": alerts,
    }


def build_alert_summary_buckets(
    alerts: list[AlertORM],
    key_fn,
    stale_before: datetime,
) -> list[dict[str, object]]:
    buckets: dict[str, dict[str, object]] = {}
    for alert in alerts:
        key = str(key_fn(alert) or "unknown")
        bucket = buckets.setdefault(
            key,
            {
                "key": key,
                "total_count": 0,
                "open_count": 0,
                "acknowledged_count": 0,
                "closed_count": 0,
                "stale_open_count": 0,
            },
        )
        bucket["total_count"] += 1
        if alert.status == "open":
            bucket["open_count"] += 1
        elif alert.status == "acknowledged":
            bucket["acknowledged_count"] += 1
        elif alert.status == "closed":
            bucket["closed_count"] += 1
        if is_stale_open_alert(alert, stale_before):
            bucket["stale_open_count"] += 1
    return sorted(buckets.values(), key=lambda item: (-int(item["total_count"]), str(item["key"]).lower()))


def geofence_scan_task_matches_scope(task: ScheduledTaskORM, *, geofence_id: int | None) -> bool:
    if geofence_id is None:
        return True
    if task.geofence_id is None:
        return True
    return task.geofence_id == geofence_id


def is_stale_open_alert(alert: AlertORM, stale_before: datetime) -> bool:
    return alert.status == "open" and normalize_timestamp(alert.created_at) < stale_before


def normalize_timestamp(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
