from __future__ import annotations

import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field

from sqlalchemy.orm import Session, sessionmaker

from src.services.platform_runtime_service import run_platform_runtime_cycle
from src.services.worker_status_service import (
    mark_worker_failed,
    mark_worker_started,
    mark_worker_stopped,
    publish_worker_heartbeat,
)


@dataclass
class PlatformRuntimeWorkerResult:
    iterations: int = 0
    runs_created: int = 0
    task_run_ids: list[int] = field(default_factory=list)
    source_run_ids: list[int] = field(default_factory=list)
    records_seen: int = 0
    records_imported: int = 0
    records_failed: int = 0


def run_platform_runtime_worker(
    session_factory: sessionmaker[Session],
    *,
    poll_seconds: float,
    actor: str = "platform_runtime_worker",
    once: bool = False,
    max_iterations: int | None = None,
    include_stream_runtime: bool = True,
    include_enabled_schedules: bool = True,
    task_types: list[str] | None = None,
    source_id: int | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    on_iteration: Callable[[int, dict[str, object]], None] | None = None,
) -> PlatformRuntimeWorkerResult:
    result = PlatformRuntimeWorkerResult()
    normalized_poll_seconds = max(0.0, poll_seconds)
    worker_key = f"platform_runtime_worker:{actor}:{source_id if source_id is not None else 'all'}"
    process_token = uuid.uuid4().hex
    failed = False

    startup_session = session_factory()
    try:
        mark_worker_started(
            startup_session,
            worker_key=worker_key,
            worker_type="platform_runtime_worker",
            actor=actor,
            process_token=process_token,
            metadata_json={
                "poll_seconds": normalized_poll_seconds,
                "once": once,
                "max_iterations": max_iterations,
                "include_stream_runtime": include_stream_runtime,
                "include_enabled_schedules": include_enabled_schedules,
                "task_types": list(task_types or []),
                "source_id": source_id,
            },
        )
    finally:
        startup_session.close()

    try:
        while True:
            result.iterations += 1
            session = session_factory()
            try:
                cycle = run_platform_runtime_cycle(
                    session,
                    include_stream_runtime=include_stream_runtime,
                    include_enabled_schedules=include_enabled_schedules,
                    task_types=task_types,
                    source_id=source_id,
                    actor=actor,
                )
                publish_worker_heartbeat(
                    session,
                    worker_key=worker_key,
                    worker_type="platform_runtime_worker",
                    actor=actor,
                    process_token=process_token,
                    metadata_json={
                        "poll_seconds": normalized_poll_seconds,
                        "iterations": result.iterations,
                        "include_stream_runtime": include_stream_runtime,
                        "include_enabled_schedules": include_enabled_schedules,
                        "task_types": list(task_types or []),
                        "source_id": source_id,
                        "last_runs_created": int(cycle["schedules"]["runs_created"]),
                        "runs_created_total": result.runs_created + int(cycle["schedules"]["runs_created"]),
                        "last_source_run_ids": [int(item) for item in cycle["source_runtime"]["source_run_ids"]],
                        "records_seen_total": result.records_seen + int(cycle["source_runtime"]["records_seen"]),
                        "records_imported_total": result.records_imported
                        + int(cycle["source_runtime"]["records_imported"]),
                        "records_failed_total": result.records_failed + int(cycle["source_runtime"]["records_failed"]),
                    },
                )
            except Exception as exc:
                failed = True
                mark_worker_failed(
                    session,
                    worker_key=worker_key,
                    worker_type="platform_runtime_worker",
                    actor=actor,
                    process_token=process_token,
                    error_text=str(exc),
                    metadata_json={
                        "poll_seconds": normalized_poll_seconds,
                        "iterations": result.iterations,
                        "include_stream_runtime": include_stream_runtime,
                        "include_enabled_schedules": include_enabled_schedules,
                        "task_types": list(task_types or []),
                        "source_id": source_id,
                        "runs_created_total": result.runs_created,
                        "source_run_ids_total": list(result.source_run_ids),
                        "records_seen_total": result.records_seen,
                        "records_imported_total": result.records_imported,
                        "records_failed_total": result.records_failed,
                    },
                )
                raise
            finally:
                session.close()

            task_run_ids = [int(item) for item in cycle["schedules"]["task_run_ids"]]
            source_run_ids = [int(item) for item in cycle["source_runtime"]["source_run_ids"]]
            result.runs_created += int(cycle["schedules"]["runs_created"])
            result.task_run_ids.extend(task_run_ids)
            result.source_run_ids.extend(source_run_ids)
            result.records_seen += int(cycle["source_runtime"]["records_seen"])
            result.records_imported += int(cycle["source_runtime"]["records_imported"])
            result.records_failed += int(cycle["source_runtime"]["records_failed"])

            if on_iteration is not None:
                on_iteration(result.iterations, cycle)
            if once:
                return result
            if max_iterations is not None and result.iterations >= max_iterations:
                return result
            sleep_fn(normalized_poll_seconds)
    finally:
        if not failed:
            shutdown_session = session_factory()
            try:
                mark_worker_stopped(
                    shutdown_session,
                    worker_key=worker_key,
                    worker_type="platform_runtime_worker",
                    actor=actor,
                    process_token=process_token,
                    metadata_json={
                        "poll_seconds": normalized_poll_seconds,
                        "iterations": result.iterations,
                        "include_stream_runtime": include_stream_runtime,
                        "include_enabled_schedules": include_enabled_schedules,
                        "task_types": list(task_types or []),
                        "source_id": source_id,
                        "runs_created_total": result.runs_created,
                        "source_run_ids_total": list(result.source_run_ids),
                        "records_seen_total": result.records_seen,
                        "records_imported_total": result.records_imported,
                        "records_failed_total": result.records_failed,
                    },
                )
            finally:
                shutdown_session.close()
