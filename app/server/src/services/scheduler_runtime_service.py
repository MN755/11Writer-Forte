from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field

from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from src.models import ScheduledTaskRunORM
from src.services.scheduler_service import run_due_tasks


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

    while True:
        result.iterations += 1
        session = session_factory()
        try:
            runs = run_due_tasks(session, actor=actor)
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
