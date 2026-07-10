from __future__ import annotations

import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field

from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from src.models import ScheduledTaskRunORM
from src.services.scheduler_service import run_due_tasks
from src.services.worker_status_service import (
    mark_worker_failed,
    mark_worker_started,
    mark_worker_stopped,
    publish_worker_heartbeat,
)


@dataclass
class SchedulerWorkerResult:
    iterations: int = 0
    runs_created: int = 0
    task_run_ids: list[int] = field(default_factory=list)


def run_scheduler_worker(
    session_factory: sessionmaker[Session],
    *,
    poll_seconds: float,
    actor: str = "scheduler_worker",
    once: bool = False,
    max_iterations: int | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    on_iteration: Callable[[int, list[ScheduledTaskRunORM]], None] | None = None,
) -> SchedulerWorkerResult:
    result = SchedulerWorkerResult()
    normalized_poll_seconds = max(0.0, poll_seconds)
    worker_key = f"scheduler_worker:{actor}"
    process_token = uuid.uuid4().hex
    failed = False

    startup_session = session_factory()
    try:
        mark_worker_started(
            startup_session,
            worker_key=worker_key,
            worker_type="scheduler_worker",
            actor=actor,
            process_token=process_token,
            metadata_json={
                "poll_seconds": normalized_poll_seconds,
                "once": once,
                "max_iterations": max_iterations,
            },
        )
    finally:
        startup_session.close()

    try:
        while True:
            result.iterations += 1
            session = session_factory()
            try:
                runs = run_due_tasks(session, actor=actor)
                publish_worker_heartbeat(
                    session,
                    worker_key=worker_key,
                    worker_type="scheduler_worker",
                    actor=actor,
                    process_token=process_token,
                    metadata_json={
                        "poll_seconds": normalized_poll_seconds,
                        "iterations": result.iterations,
                        "last_runs_created": len(runs),
                        "runs_created_total": result.runs_created + len(runs),
                    },
                )
            except Exception as exc:
                failed = True
                mark_worker_failed(
                    session,
                    worker_key=worker_key,
                    worker_type="scheduler_worker",
                    actor=actor,
                    process_token=process_token,
                    error_text=str(exc),
                    metadata_json={
                        "poll_seconds": normalized_poll_seconds,
                        "iterations": result.iterations,
                        "runs_created_total": result.runs_created,
                    },
                )
                raise
            finally:
                session.close()

            result.runs_created += len(runs)
            result.task_run_ids.extend(run.task_run_id for run in runs)
            if on_iteration is not None:
                on_iteration(result.iterations, runs)

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
                    worker_type="scheduler_worker",
                    actor=actor,
                    process_token=process_token,
                    metadata_json={
                        "poll_seconds": normalized_poll_seconds,
                        "iterations": result.iterations,
                        "runs_created_total": result.runs_created,
                    },
                )
            finally:
                shutdown_session.close()
