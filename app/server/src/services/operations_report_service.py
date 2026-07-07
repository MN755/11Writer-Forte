from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session

from src.models import (
    AlertORM,
    CustodyLogORM,
    EntityORM,
    EventORM,
    LocalImportRunORM,
    ObservationORM,
    ScheduledTaskRunORM,
    SourceRunORM,
)
from src.services.camera_service import build_camera_inventory_summary, build_camera_ops_report_index
from src.services.source_service import build_source_inventory_summary, build_source_ops_report_index


def report_now() -> datetime:
    return datetime.now(timezone.utc)


def build_operations_report(
    session: Session,
    *,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = 25,
) -> dict[str, object]:
    import_runs = list(
        session.scalars(
            apply_time_filters(
                select(LocalImportRunORM).order_by(LocalImportRunORM.created_at.desc()).limit(limit),
                LocalImportRunORM.created_at,
                since=since,
                until=until,
            )
        )
    )
    source_runs = list(
        session.scalars(
            apply_time_filters(
                select(SourceRunORM).order_by(SourceRunORM.source_run_id.desc()).limit(limit),
                SourceRunORM.started_at,
                since=since,
                until=until,
            )
        )
    )
    scheduled_task_runs = list(
        session.scalars(
            apply_time_filters(
                select(ScheduledTaskRunORM).order_by(ScheduledTaskRunORM.task_run_id.desc()).limit(limit),
                ScheduledTaskRunORM.started_at,
                since=since,
                until=until,
            )
        )
    )
    alerts = list(
        session.scalars(
            apply_time_filters(
                select(AlertORM).order_by(AlertORM.created_at.desc()).limit(limit),
                AlertORM.created_at,
                since=since,
                until=until,
            )
        )
    )
    custody_logs = list(
        session.scalars(
            apply_time_filters(
                select(CustodyLogORM).order_by(CustodyLogORM.created_at.desc()).limit(limit),
                CustodyLogORM.created_at,
                since=since,
                until=until,
            )
        )
    )
    camera_inventory_summary = build_camera_inventory_summary(session)
    camera_report_index = build_camera_ops_report_index(session, limit=limit)
    source_inventory_summary = build_source_inventory_summary(session)
    source_report_index = build_source_ops_report_index(session, limit=limit)

    return {
        "generated_at": report_now(),
        "scope_since": since,
        "scope_until": until,
        "summary": {
            "import_run_count": count_records(session, LocalImportRunORM, LocalImportRunORM.created_at, since, until),
            "imported_record_count": sum_integer_field(
                session,
                LocalImportRunORM,
                LocalImportRunORM.records_imported,
                LocalImportRunORM.created_at,
                since,
                until,
            ),
            "skipped_record_count": sum_integer_field(
                session,
                LocalImportRunORM,
                LocalImportRunORM.records_skipped,
                LocalImportRunORM.created_at,
                since,
                until,
            ),
            "source_run_count": count_records(session, SourceRunORM, SourceRunORM.started_at, since, until),
            "source_run_failure_count": count_status_records(
                session,
                SourceRunORM,
                SourceRunORM.status,
                "failed",
                SourceRunORM.started_at,
                since,
                until,
            ),
            "scheduled_task_run_count": count_records(
                session,
                ScheduledTaskRunORM,
                ScheduledTaskRunORM.started_at,
                since,
                until,
            ),
            "scheduled_task_run_failure_count": count_status_records(
                session,
                ScheduledTaskRunORM,
                ScheduledTaskRunORM.status,
                "failed",
                ScheduledTaskRunORM.started_at,
                since,
                until,
            ),
            "alert_count": count_records(session, AlertORM, AlertORM.created_at, since, until),
            "open_alert_count": count_status_records(
                session,
                AlertORM,
                AlertORM.status,
                "open",
                AlertORM.created_at,
                since,
                until,
            ),
            "acknowledged_alert_count": count_status_records(
                session,
                AlertORM,
                AlertORM.status,
                "acknowledged",
                AlertORM.created_at,
                since,
                until,
            ),
            "closed_alert_count": count_status_records(
                session,
                AlertORM,
                AlertORM.status,
                "closed",
                AlertORM.created_at,
                since,
                until,
            ),
            "event_count": count_records(session, EventORM, EventORM.created_at, since, until),
            "entity_count": count_records(session, EntityORM, EntityORM.created_at, since, until),
            "observation_count": count_records(session, ObservationORM, ObservationORM.created_at, since, until),
        },
        "source_inventory_summary": source_inventory_summary,
        "source_report_index": source_report_index,
        "camera_inventory_summary": camera_inventory_summary,
        "camera_report_index": camera_report_index,
        "import_runs": import_runs,
        "source_runs": source_runs,
        "scheduled_task_runs": scheduled_task_runs,
        "alerts": alerts,
        "custody_logs": custody_logs,
    }


def apply_time_filters(
    statement: Select,
    timestamp_column,
    *,
    since: datetime | None,
    until: datetime | None,
) -> Select:
    if since is not None:
        statement = statement.where(timestamp_column >= since)
    if until is not None:
        statement = statement.where(timestamp_column <= until)
    return statement


def count_records(session: Session, model, timestamp_column, since: datetime | None, until: datetime | None) -> int:
    statement = apply_time_filters(select(func.count()).select_from(model), timestamp_column, since=since, until=until)
    return int(session.scalar(statement) or 0)


def count_status_records(
    session: Session,
    model,
    status_column,
    status: str,
    timestamp_column,
    since: datetime | None,
    until: datetime | None,
) -> int:
    statement = apply_time_filters(
        select(func.count()).select_from(model).where(status_column == status),
        timestamp_column,
        since=since,
        until=until,
    )
    return int(session.scalar(statement) or 0)


def sum_integer_field(
    session: Session,
    model,
    value_column,
    timestamp_column,
    since: datetime | None,
    until: datetime | None,
) -> int:
    statement = apply_time_filters(
        select(func.coalesce(func.sum(value_column), 0)).select_from(model),
        timestamp_column,
        since=since,
        until=until,
    )
    return int(session.scalar(statement) or 0)
