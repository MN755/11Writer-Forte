from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from pydantic import TypeAdapter
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
from src.schemas import OperationsReportRead
from src.services.alert_service import build_alert_inventory_summary, build_alert_ops_report_index
from src.services.clickhouse_service import build_clickhouse_diagnostics
from src.services.camera_source_service import (
    build_camera_source_inventory_summary,
    build_camera_source_ops_report_index,
)
from src.services.camera_service import build_camera_inventory_summary, build_camera_ops_report_index
from src.services.entity_service import build_entity_inventory_summary, build_entity_ops_report_index
from src.services.event_service import build_event_inventory_summary, build_event_ops_report_index
from src.services.export_artifact_service import write_json_export_artifact
from src.services.runtime_readiness_service import build_runtime_readiness
from src.services.scheduler_service import build_scheduler_inventory_summary, build_scheduler_ops_report_index
from src.services.source_service import build_source_inventory_summary, build_source_ops_report_index
from src.services.storage_service import build_storage_report
from src.services.worker_status_service import build_worker_status_summary


def report_now() -> datetime:
    return datetime.now(timezone.utc)


def resolve_operations_report_since(
    hours: float | None,
    *,
    reference: datetime | None = None,
) -> datetime | None:
    if hours is None:
        return None
    return (reference or report_now()) - timedelta(hours=hours)


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
    alert_inventory_summary = build_alert_inventory_summary(session)
    alert_report_index = build_alert_ops_report_index(session, limit=limit)
    camera_inventory_summary = build_camera_inventory_summary(session)
    camera_report_index = build_camera_ops_report_index(session, limit=limit)
    camera_source_inventory_summary = build_camera_source_inventory_summary(session)
    camera_source_report_index = build_camera_source_ops_report_index(session, limit=limit)
    event_inventory_summary = build_event_inventory_summary(session)
    event_report_index = build_event_ops_report_index(session, limit=limit)
    entity_inventory_summary = build_entity_inventory_summary(session)
    entity_report_index = build_entity_ops_report_index(session, limit=limit)
    storage_report = build_storage_report(session, limit=limit)
    clickhouse_diagnostics = build_clickhouse_diagnostics()
    scheduler_inventory_summary = build_scheduler_inventory_summary(session)
    scheduler_report_index = build_scheduler_ops_report_index(session, limit=limit)
    worker_status_summary = build_worker_status_summary(session)
    source_inventory_summary = build_source_inventory_summary(session)
    source_report_index = build_source_ops_report_index(session, limit=limit)
    runtime_readiness = build_runtime_readiness(session)

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
        "runtime_readiness": runtime_readiness,
        "alert_inventory_summary": alert_inventory_summary,
        "alert_report_index": alert_report_index,
        "event_inventory_summary": event_inventory_summary,
        "event_report_index": event_report_index,
        "entity_inventory_summary": entity_inventory_summary,
        "entity_report_index": entity_report_index,
        "storage_report": storage_report,
        "clickhouse_diagnostics": clickhouse_diagnostics,
        "scheduler_inventory_summary": scheduler_inventory_summary,
        "scheduler_report_index": scheduler_report_index,
        "worker_status_summary": worker_status_summary,
        "source_inventory_summary": source_inventory_summary,
        "source_report_index": source_report_index,
        "camera_inventory_summary": camera_inventory_summary,
        "camera_report_index": camera_report_index,
        "camera_source_inventory_summary": camera_source_inventory_summary,
        "camera_source_report_index": camera_source_report_index,
        "import_runs": import_runs,
        "source_runs": source_runs,
        "scheduled_task_runs": scheduled_task_runs,
        "alerts": alerts,
        "custody_logs": custody_logs,
    }


def export_operations_report_artifact(
    session: Session,
    *,
    output_path: Path,
    hours: float | None = 24.0,
    limit: int = 25,
    actor: str = "cli_export",
) -> dict[str, object]:
    since = resolve_operations_report_since(hours)
    report = build_operations_report(session, since=since, limit=limit)
    serializable = TypeAdapter(OperationsReportRead).validate_python(report).model_dump(mode="json")
    record = write_json_export_artifact(
        session,
        payload=serializable,
        object_kind="operations_report_export",
        owner_type="operations_report",
        owner_id="scoped",
        output_path=output_path,
        source_uri="/api/operations/report",
        observed_at=report["generated_at"],
        metadata_json={
            "scope_since": serializable["scope_since"],
            "scope_until": serializable["scope_until"],
            "limit": limit,
            "hours": hours,
        },
        actor=actor,
    )
    return {
        "report": serializable,
        "output_path": str(output_path.expanduser().resolve()),
        "storage_object": record,
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
