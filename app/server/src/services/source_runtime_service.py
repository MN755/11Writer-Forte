from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from src.metrics import inc_counter
from src.observability import log_event
from src.services.source_service import run_source_runtime_cycle

logger = logging.getLogger(__name__)


@dataclass
class SourceRuntimeWorkerResult:
    iterations: int = 0
    source_run_ids: list[int] = field(default_factory=list)
    records_seen: int = 0
    records_imported: int = 0
    records_failed: int = 0


def run_source_runtime_worker(
    session_factory: sessionmaker[Session],
    *,
    poll_seconds: float,
    actor: str = "source_runtime_worker",
    once: bool = False,
    max_iterations: int | None = None,
    source_id: int | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    on_iteration: Callable[[int, dict[str, object]], None] | None = None,
) -> SourceRuntimeWorkerResult:
    result = SourceRuntimeWorkerResult()
    normalized_poll_seconds = max(0.0, poll_seconds)

    while True:
        result.iterations += 1
        log_event(
            logger,
            logging.INFO,
            "source_runtime_worker_iteration_started",
            iteration=result.iterations,
            poll_seconds=normalized_poll_seconds,
            actor=actor,
            source_id=source_id,
        )
        session = session_factory()
        try:
            cycle = run_source_runtime_cycle(session, source_id=source_id, actor=actor)
        finally:
            session.close()

        source_run_ids = [int(item) for item in cycle.get("source_run_ids", [])]
        result.source_run_ids.extend(source_run_ids)
        result.records_seen += int(cycle.get("records_seen", 0))
        result.records_imported += int(cycle.get("records_imported", 0))
        result.records_failed += int(cycle.get("records_failed", 0))
        inc_counter("elevenwriter_source_runtime_worker_iterations_total")
        log_event(
            logger,
            logging.INFO,
            "source_runtime_worker_iteration_completed",
            iteration=result.iterations,
            actor=actor,
            source_id=source_id,
            source_count=int(cycle.get("source_count", 0)),
            source_run_ids=source_run_ids,
            records_seen=int(cycle.get("records_seen", 0)),
            records_imported=int(cycle.get("records_imported", 0)),
            records_failed=int(cycle.get("records_failed", 0)),
        )
        if on_iteration is not None:
            on_iteration(result.iterations, cycle)
        if once:
            return result
        if max_iterations is not None and result.iterations >= max_iterations:
            return result
        sleep_fn(normalized_poll_seconds)
