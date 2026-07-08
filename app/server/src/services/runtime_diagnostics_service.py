from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.auth import build_api_auth_diagnostics
from src.config import get_settings
from src.models import ScheduledTaskRunORM, SourceCheckpointORM, SourceDeadLetterORM, SourceRunORM
from src.services.clickhouse_service import build_clickhouse_diagnostics
from src.services.database_diagnostics_service import build_database_diagnostics
from src.services.scheduler_service import build_scheduler_inventory_summary
from src.services.source_service import build_source_inventory_summary
from src.services.storage_service import build_storage_report


def diagnostics_now() -> datetime:
    return datetime.now(timezone.utc)


def build_runtime_diagnostics(session: Session, *, limit: int = 10) -> dict[str, object]:
    settings = get_settings()
    database = build_database_diagnostics(session)
    scheduler = build_scheduler_inventory_summary(session)
    sources = build_source_inventory_summary(session)
    storage = build_storage_report(session, limit=limit)
    clickhouse = build_clickhouse_diagnostics()
    return {
        "generated_at": diagnostics_now(),
        "app_env": settings.app_env,
        "api_auth": build_api_auth_diagnostics(),
        "observability": {
            "metrics_enabled": settings.metrics_enabled,
            "metrics_path": settings.metrics_path,
            "request_id_header": settings.request_id_header,
            "log_level": settings.log_level.upper(),
        },
        "database": database,
        "scheduler": scheduler,
        "sources": sources,
        "storage": storage,
        "clickhouse": clickhouse,
        "recent_failures": collect_recent_failures(session, limit=limit),
    }


def collect_recent_failures(session: Session, *, limit: int) -> list[dict[str, object]]:
    source_failures = list(
        session.scalars(
            select(SourceRunORM)
            .where(SourceRunORM.status == "failed")
            .order_by(SourceRunORM.finished_at.desc().nullslast(), SourceRunORM.source_run_id.desc())
            .limit(limit)
        )
    )
    scheduler_failures = list(
        session.scalars(
            select(ScheduledTaskRunORM)
            .where(ScheduledTaskRunORM.status == "failed")
            .order_by(ScheduledTaskRunORM.finished_at.desc().nullslast(), ScheduledTaskRunORM.task_run_id.desc())
            .limit(limit)
        )
    )
    checkpoint_failures = list(
        session.scalars(
            select(SourceCheckpointORM)
            .where(SourceCheckpointORM.status == "degraded")
            .order_by(
                SourceCheckpointORM.last_failure_at.desc().nullslast(),
                SourceCheckpointORM.source_checkpoint_id.desc(),
            )
            .limit(limit)
        )
    )
    dead_letters = list(
        session.scalars(
            select(SourceDeadLetterORM)
            .where(SourceDeadLetterORM.status == "pending")
            .order_by(
                SourceDeadLetterORM.created_at.desc(),
                SourceDeadLetterORM.source_dead_letter_id.desc(),
            )
            .limit(limit)
        )
    )

    failures = [
        {
            "subsystem": "source",
            "reference_id": f"source_run:{row.source_run_id}",
            "status": row.status,
            "message": row.error_text or "source run failed",
            "occurred_at": row.finished_at or row.started_at,
        }
        for row in source_failures
    ]
    failures.extend(
        {
            "subsystem": "scheduler",
            "reference_id": f"task_run:{row.task_run_id}",
            "status": row.status,
            "message": row.error_text or "scheduler task failed",
            "occurred_at": row.finished_at or row.started_at,
        }
        for row in scheduler_failures
    )
    failures.extend(
        {
            "subsystem": "source_checkpoint",
            "reference_id": f"source_checkpoint:{row.source_checkpoint_id}",
            "status": row.status,
            "message": (
                f"checkpoint degraded for source {row.source_id}"
                if row.failure_count <= 0
                else f"checkpoint degraded for source {row.source_id} after {row.failure_count} failures"
            ),
            "occurred_at": row.last_failure_at or row.updated_at,
        }
        for row in checkpoint_failures
    )
    failures.extend(
        {
            "subsystem": "source_dead_letter",
            "reference_id": f"source_dead_letter:{row.source_dead_letter_id}",
            "status": row.status,
            "message": row.failure_reason,
            "occurred_at": row.created_at,
        }
        for row in dead_letters
    )

    if clickhouse_warning := first_clickhouse_warning():
        failures.append(
            {
                "subsystem": "clickhouse",
                "reference_id": "clickhouse:current",
                "status": "degraded",
                "message": clickhouse_warning,
                "occurred_at": diagnostics_now(),
            }
        )

    failures.sort(key=lambda row: row["occurred_at"], reverse=True)
    return failures[:limit]


def first_clickhouse_warning() -> str | None:
    clickhouse = build_clickhouse_diagnostics()
    warnings = clickhouse.get("warnings", [])
    if not isinstance(warnings, list) or not warnings:
        return None
    first_warning = warnings[0]
    return str(first_warning) if first_warning else None
