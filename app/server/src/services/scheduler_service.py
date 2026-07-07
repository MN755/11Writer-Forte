from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from src.models import (
    AlertORM,
    CustodyLogORM,
    GeofenceORM,
    ObservationORM,
    ScheduledTaskORM,
    ScheduledTaskRunORM,
)
from src.schemas import ScheduledTaskCreate
from src.services.geospatial_service import build_contains_geometry_sql_filter, point_in_geometry, uses_postgis
from src.services.import_service import import_local_path
from src.services.source_service import run_source_definition
from src.services.trust_service import seed_default_integrity_sources


def scheduler_now() -> datetime:
    return datetime.now(timezone.utc)


def compute_next_run(interval_seconds: int, reference: datetime | None = None) -> datetime:
    return (reference or scheduler_now()) + timedelta(seconds=interval_seconds)


def create_scheduled_task(session: Session, payload: ScheduledTaskCreate) -> ScheduledTaskORM:
    record = ScheduledTaskORM(
        **payload.model_dump(),
        next_run_at=compute_next_run(payload.interval_seconds),
    )
    session.add(record)
    session.flush()
    session.add(
        CustodyLogORM(
            object_type="scheduled_task",
            object_id=str(record.task_id),
            action="task_created",
            actor="system",
            details_json={
                **payload.model_dump(),
                "task_id": record.task_id,
            },
        )
    )
    session.commit()
    session.refresh(record)
    return record


def run_due_tasks(session: Session, actor: str = "scheduler") -> list[ScheduledTaskRunORM]:
    now = scheduler_now()
    statement = select(ScheduledTaskORM).where(
        and_(
            ScheduledTaskORM.enabled.is_(True),
            ScheduledTaskORM.next_run_at.is_not(None),
            ScheduledTaskORM.next_run_at <= now,
        )
    )
    tasks = list(session.scalars(statement.order_by(ScheduledTaskORM.task_id.asc())))
    return [run_task(session, task.task_id, actor=actor) for task in tasks]


def run_task(session: Session, task_id: int, actor: str = "scheduler") -> ScheduledTaskRunORM:
    task = session.get(ScheduledTaskORM, task_id)
    if task is None:
        raise ValueError(f"Scheduled task {task_id} does not exist.")

    task_run = ScheduledTaskRunORM(task_id=task.task_id, status="running")
    session.add(task_run)
    session.flush()
    session.add(
        CustodyLogORM(
            object_type="scheduled_task_run",
            object_id=str(task_run.task_run_id),
            action="task_run_started",
            actor=actor,
            details_json={
                "task_id": task.task_id,
                "task_type": task.task_type,
            },
        )
    )

    max_attempts = max(1, task.retry_attempts)
    attempt_errors: list[dict[str, object]] = []

    for attempt in range(1, max_attempts + 1):
        try:
            records_affected, output_json = execute_task(session, task, actor=actor)
            finished_at = scheduler_now()
            task_run.status = "completed"
            task_run.records_affected = records_affected
            task_run.output_json = {
                **output_json,
                "attempt_count": attempt,
                "max_attempts": max_attempts,
                "attempt_errors": attempt_errors,
            }
            task_run.finished_at = finished_at
            task.last_run_at = finished_at
            task.next_run_at = compute_next_run(task.interval_seconds, finished_at)
            session.add(
                CustodyLogORM(
                    object_type="scheduled_task_run",
                    object_id=str(task_run.task_run_id),
                    action="task_run_completed",
                    actor=actor,
                    details_json={
                        "task_id": task.task_id,
                        "task_type": task.task_type,
                        "records_affected": records_affected,
                        "attempt_count": attempt,
                        "max_attempts": max_attempts,
                    },
                )
            )
            session.add(
                CustodyLogORM(
                    object_type="scheduled_task",
                    object_id=str(task.task_id),
                    action="task_run_completed",
                    actor=actor,
                    details_json={
                        "task_type": task.task_type,
                        "task_run_id": task_run.task_run_id,
                        "records_affected": records_affected,
                        "attempt_count": attempt,
                        "max_attempts": max_attempts,
                    },
                )
            )
            session.commit()
            session.refresh(task_run)
            return task_run
        except Exception as exc:
            error_details = {
                "attempt": attempt,
                "error_text": str(exc),
            }
            attempt_errors.append(error_details)
            session.add(
                CustodyLogORM(
                    object_type="scheduled_task_run",
                    object_id=str(task_run.task_run_id),
                    action="task_attempt_failed",
                    actor=actor,
                    details_json={
                        "task_id": task.task_id,
                        "task_type": task.task_type,
                        **error_details,
                    },
                )
            )
            if attempt < max_attempts:
                retry_delay = compute_retry_delay(task.retry_backoff_seconds, attempt)
                session.add(
                    CustodyLogORM(
                        object_type="scheduled_task_run",
                        object_id=str(task_run.task_run_id),
                        action="task_retry_scheduled",
                        actor=actor,
                        details_json={
                            "task_id": task.task_id,
                            "task_type": task.task_type,
                            "attempt": attempt,
                            "next_attempt": attempt + 1,
                            "retry_delay_seconds": retry_delay,
                        },
                    )
                )
                apply_task_retry_backoff(retry_delay)
                continue

            finished_at = scheduler_now()
            task_run.status = "failed"
            task_run.error_text = str(exc)
            task_run.output_json = {
                "attempt_count": attempt,
                "max_attempts": max_attempts,
                "attempt_errors": attempt_errors,
            }
            task_run.finished_at = finished_at
            task.last_run_at = finished_at
            task.next_run_at = compute_next_run(task.interval_seconds, finished_at)
            session.add(
                CustodyLogORM(
                    object_type="scheduled_task_run",
                    object_id=str(task_run.task_run_id),
                    action="task_run_failed",
                    actor=actor,
                    details_json={
                        "task_id": task.task_id,
                        "task_type": task.task_type,
                        "error_text": str(exc),
                        "attempt_count": attempt,
                        "max_attempts": max_attempts,
                    },
                )
            )
            session.add(
                CustodyLogORM(
                    object_type="scheduled_task",
                    object_id=str(task.task_id),
                    action="task_run_failed",
                    actor=actor,
                    details_json={
                        "task_type": task.task_type,
                        "task_run_id": task_run.task_run_id,
                        "error_text": str(exc),
                        "attempt_count": attempt,
                        "max_attempts": max_attempts,
                    },
                )
            )
            session.commit()
            raise

    session.refresh(task_run)
    return task_run


def execute_task(
    session: Session,
    task: ScheduledTaskORM,
    actor: str,
) -> tuple[int, dict[str, object]]:
    if task.task_type == "local_import":
        if not task.target_path:
            raise ValueError("Local import task requires target_path.")
        import_run = import_local_path(
            session,
            task.target_path,
            task.layer_key or "unassigned",
            task.notes,
            actor=actor,
        )
        return (
            import_run.records_imported,
            {
                "import_run_id": import_run.import_run_id,
                "source_format": import_run.source_format,
            },
        )
    if task.task_type == "geofence_scan":
        created = evaluate_geofence_alerts(session, geofence_id=task.geofence_id, actor=actor)
        return (created, {"alerts_created": created})
    if task.task_type == "integrity_seed":
        created = seed_default_integrity_sources(session)
        return (len(created), {"domains": created})
    if task.task_type == "source_sync":
        if task.source_id is None:
            raise ValueError("Source sync task requires source_id.")
        source_run = run_source_definition(session, task.source_id, actor=actor)
        return (
            source_run.records_imported,
            {
                "source_run_id": source_run.source_run_id,
                "import_run_id": source_run.import_run_id,
            },
        )
    raise ValueError(f"Unsupported task type: {task.task_type}")


def compute_retry_delay(retry_backoff_seconds: float, attempt: int) -> float:
    return max(0.0, retry_backoff_seconds) * attempt


def apply_task_retry_backoff(retry_delay_seconds: float) -> None:
    if retry_delay_seconds <= 0:
        return
    time.sleep(retry_delay_seconds)


def evaluate_geofence_alerts(
    session: Session,
    geofence_id: int | None = None,
    actor: str = "scheduler",
) -> int:
    geofence_statement = select(GeofenceORM).where(GeofenceORM.enabled.is_(True))
    if geofence_id is not None:
        geofence_statement = geofence_statement.where(GeofenceORM.geofence_id == geofence_id)
    geofences = list(session.scalars(geofence_statement.order_by(GeofenceORM.geofence_id.asc())))

    created = 0
    for geofence in geofences:
        observations = query_geofence_observations(session, geofence)
        for observation in observations:
            coordinates = (observation.location_geojson or {}).get("coordinates")
            if not isinstance(coordinates, list) or len(coordinates) < 2:
                continue
            point = (float(coordinates[0]), float(coordinates[1]))
            if not point_in_geometry(point, geofence.geometry_geojson):
                continue
            dedupe_key = f"geofence:{geofence.geofence_id}:observation:{observation.observation_id}"
            existing = session.scalar(
                select(AlertORM).where(AlertORM.dedupe_key == dedupe_key, AlertORM.status == "open")
            )
            if existing is not None:
                continue
            alert = AlertORM(
                geofence_id=geofence.geofence_id,
                severity="info" if observation.trust_level != "blocked" else "warning",
                status="open",
                dedupe_key=dedupe_key,
                message=(
                    f"Observation {observation.observation_id} entered geofence "
                    f"{geofence.name} on layer {observation.layer_key}."
                ),
                trigger_basis_json={
                    "observation_id": observation.observation_id,
                    "layer_key": observation.layer_key,
                    "source_domain": observation.source_domain,
                    "confidence_score": observation.confidence_score,
                },
            )
            session.add(alert)
            session.flush()
            session.add(
                CustodyLogORM(
                    object_type="alert",
                    object_id=str(alert.alert_id),
                    action="alert_created",
                    actor=actor,
                    details_json={
                        "geofence_id": geofence.geofence_id,
                        "observation_id": observation.observation_id,
                        "severity": alert.severity,
                        "dedupe_key": dedupe_key,
                    },
                )
            )
            created += 1

    session.add(
        CustodyLogORM(
            object_type="geofence_scan",
            object_id=str(geofence_id or "all"),
            action="alerts_evaluated",
            actor=actor,
            details_json={
                "alerts_created": created,
                "geofence_count": len(geofences),
                "observation_count": count_scanned_observations(session, geofences),
            },
        )
    )
    session.flush()
    return created


def query_geofence_observations(session: Session, geofence: GeofenceORM) -> list[ObservationORM]:
    if uses_postgis(session) and geofence.geometry_wkt:
        statement = (
            select(ObservationORM)
            .where(
                build_contains_geometry_sql_filter(
                    geofence.geometry_wkt,
                    ObservationORM.location_wkt,
                )
            )
            .order_by(ObservationORM.observation_id.asc())
        )
        return list(session.scalars(statement))

    observations = list(
        session.scalars(
            select(ObservationORM)
            .where(ObservationORM.location_geojson.is_not(None))
            .order_by(ObservationORM.observation_id.asc())
        )
    )
    matched: list[ObservationORM] = []
    for observation in observations:
        coordinates = (observation.location_geojson or {}).get("coordinates")
        if not isinstance(coordinates, list) or len(coordinates) < 2:
            continue
        point = (float(coordinates[0]), float(coordinates[1]))
        if point_in_geometry(point, geofence.geometry_geojson):
            matched.append(observation)
    return matched


def count_scanned_observations(session: Session, geofences: list[GeofenceORM]) -> int:
    if uses_postgis(session):
        total = 0
        for geofence in geofences:
            total += len(query_geofence_observations(session, geofence))
        return total
    return len(
        list(
            session.scalars(
                select(ObservationORM)
                .where(ObservationORM.location_geojson.is_not(None))
                .order_by(ObservationORM.observation_id.asc())
            )
        )
    )
