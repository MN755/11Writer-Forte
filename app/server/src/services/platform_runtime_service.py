from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy.orm import Session

from src.services.scheduler_service import normalize_task_type_filters, run_enabled_tasks
from src.services.source_service import run_source_runtime_cycle


def platform_runtime_now() -> datetime:
    return datetime.now(timezone.utc)


def run_platform_runtime_cycle(
    session: Session,
    *,
    include_stream_runtime: bool = True,
    include_enabled_schedules: bool = True,
    task_types: list[str] | None = None,
    source_id: int | None = None,
    actor: str = "platform_runtime_cycle",
) -> dict[str, object]:
    normalized_task_types = normalize_task_type_filters(task_types)
    source_runtime_result = {
        "source_count": 0,
        "source_run_ids": [],
        "records_seen": 0,
        "records_imported": 0,
        "records_failed": 0,
    }
    if include_stream_runtime:
        source_runtime_result = run_source_runtime_cycle(
            session,
            source_id=source_id,
            actor=actor,
        )

    schedule_runs = []
    if include_enabled_schedules:
        schedule_runs = run_enabled_tasks(
            session,
            actor=actor,
            task_types=normalized_task_types or None,
        )

    return {
        "executed_at": platform_runtime_now(),
        "include_stream_runtime": include_stream_runtime,
        "include_enabled_schedules": include_enabled_schedules,
        "task_type_filters": normalized_task_types,
        "source_runtime": {
            "source_count": int(source_runtime_result.get("source_count", 0)),
            "source_run_ids": [int(item) for item in source_runtime_result.get("source_run_ids", [])],
            "records_seen": int(source_runtime_result.get("records_seen", 0)),
            "records_imported": int(source_runtime_result.get("records_imported", 0)),
            "records_failed": int(source_runtime_result.get("records_failed", 0)),
        },
        "schedules": {
            "runs_created": len(schedule_runs),
            "task_run_ids": [int(run.task_run_id) for run in schedule_runs],
            "completed_count": sum(1 for run in schedule_runs if run.status == "completed"),
            "failed_count": sum(1 for run in schedule_runs if run.status == "failed"),
            "task_types": sorted(
                {
                    str(run.task.task_type)
                    for run in schedule_runs
                    if getattr(run, "task", None) is not None and getattr(run.task, "task_type", None)
                }
            ),
        },
    }
