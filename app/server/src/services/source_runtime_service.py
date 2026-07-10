from __future__ import annotations

import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field

from sqlalchemy.orm import Session, sessionmaker

from src.services.source_service import run_source_runtime_cycle
from src.services.worker_status_service import (
    mark_worker_failed,
    mark_worker_started,
    mark_worker_stopped,
    publish_worker_heartbeat,
)


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
    worker_key = f"source_runtime_worker:{actor}:{source_id if source_id is not None else 'all'}"
    process_token = uuid.uuid4().hex
    failed = False

    startup_session = session_factory()
    try:
        mark_worker_started(
            startup_session,
            worker_key=worker_key,
            worker_type="source_runtime_worker",
            actor=actor,
            process_token=process_token,
            metadata_json={
                "poll_seconds": normalized_poll_seconds,
                "once": once,
                "max_iterations": max_iterations,
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
                cycle = run_source_runtime_cycle(session, source_id=source_id, actor=actor)
                source_run_ids = [int(item) for item in cycle.get("source_run_ids", [])]
                publish_worker_heartbeat(
                    session,
                    worker_key=worker_key,
                    worker_type="source_runtime_worker",
                    actor=actor,
                    process_token=process_token,
                    metadata_json={
                        "poll_seconds": normalized_poll_seconds,
                        "iterations": result.iterations,
                        "source_id": source_id,
                        "last_source_count": int(cycle.get("source_count", 0)),
                        "last_source_run_ids": source_run_ids,
                        "records_seen_total": result.records_seen + int(cycle.get("records_seen", 0)),
                        "records_imported_total": result.records_imported + int(cycle.get("records_imported", 0)),
                        "records_failed_total": result.records_failed + int(cycle.get("records_failed", 0)),
                    },
                )
            except Exception as exc:
                failed = True
                mark_worker_failed(
                    session,
                    worker_key=worker_key,
                    worker_type="source_runtime_worker",
                    actor=actor,
                    process_token=process_token,
                    error_text=str(exc),
                    metadata_json={
                        "poll_seconds": normalized_poll_seconds,
                        "iterations": result.iterations,
                        "source_id": source_id,
                        "records_seen_total": result.records_seen,
                        "records_imported_total": result.records_imported,
                        "records_failed_total": result.records_failed,
                    },
                )
                raise
            finally:
                session.close()
            source_run_ids = [int(item) for item in cycle.get("source_run_ids", [])]
            result.source_run_ids.extend(source_run_ids)
            result.records_seen += int(cycle.get("records_seen", 0))
            result.records_imported += int(cycle.get("records_imported", 0))
            result.records_failed += int(cycle.get("records_failed", 0))
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
                    worker_type="source_runtime_worker",
                    actor=actor,
                    process_token=process_token,
                    metadata_json={
                        "poll_seconds": normalized_poll_seconds,
                        "iterations": result.iterations,
                        "source_id": source_id,
                        "records_seen_total": result.records_seen,
                        "records_imported_total": result.records_imported,
                        "records_failed_total": result.records_failed,
                    },
                )
            finally:
                shutdown_session.close()
