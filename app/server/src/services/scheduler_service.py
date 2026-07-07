from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

from pydantic import ValidationError
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
from src.schemas import EntityResolutionRequest, EventFusionRequest, ScheduledTaskCreate, ScheduledTaskUpdate
from src.services.camera_source_service import materialize_camera_source_inventory
from src.services.camera_service import materialize_camera_inventory
from src.services.clickhouse_service import archive_clickhouse_observations_to_r2, sync_runtime_to_clickhouse
from src.services.entity_resolution_service import materialize_entities
from src.services.event_fusion_service import materialize_fused_events
from src.services.geospatial_service import build_contains_geometry_sql_filter, point_in_geometry, uses_postgis
from src.services.import_service import import_local_path
from src.services.layer_service import ensure_data_layer
from src.services.storage_service import sweep_expired_storage_objects
from src.services.source_service import run_source_definition
from src.services.trust_service import seed_default_integrity_sources


def scheduler_now() -> datetime:
    return datetime.now(timezone.utc)


def compute_next_run(interval_seconds: int, reference: datetime | None = None) -> datetime:
    return (reference or scheduler_now()) + timedelta(seconds=interval_seconds)


MAINTENANCE_TASK_TYPES = {
    "storage_lifecycle",
    "clickhouse_sync",
    "clickhouse_archive",
}


def create_scheduled_task(session: Session, payload: ScheduledTaskCreate) -> ScheduledTaskORM:
    ensure_unique_task_name(session, payload.name)
    validate_task_configuration(
        payload.task_type,
        source_id=payload.source_id,
        target_path=payload.target_path,
        geofence_id=payload.geofence_id,
        layer_key=payload.layer_key,
        payload_json=payload.payload_json,
    )
    if payload.layer_key:
        ensure_data_layer(session, payload.layer_key, actor="scheduler_registry")
    record = ScheduledTaskORM(
        **payload.model_dump(),
        next_run_at=compute_next_run(payload.interval_seconds) if payload.enabled else None,
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


def build_scheduler_inventory_summary(
    session: Session,
    *,
    reference_time: datetime | None = None,
) -> dict[str, object]:
    now = reference_time or scheduler_now()
    statuses = collect_scheduled_task_statuses(session, reference_time=now)
    return {
        "generated_at": now,
        "reference_time": now,
        "total_count": len(statuses),
        "enabled_count": sum(1 for status in statuses if status["task"].enabled),
        "disabled_count": sum(1 for status in statuses if not status["task"].enabled),
        "due_count": sum(1 for status in statuses if status["is_due"]),
        "overdue_count": sum(1 for status in statuses if status["is_overdue"]),
        "failing_count": sum(1 for status in statuses if status["is_failing"]),
        "maintenance_task_count": sum(
            1 for status in statuses if status["task"].task_type in MAINTENANCE_TASK_TYPES
        ),
        "task_type_counts": build_scheduler_task_buckets(
            statuses,
            key_fn=lambda status: str(status["task"].task_type or "unknown"),
        ),
        "latest_status_counts": build_scheduler_task_buckets(
            statuses,
            key_fn=lambda status: latest_task_status_key(status),
        ),
    }


def build_scheduler_ops_report_index(
    session: Session,
    *,
    limit: int = 25,
    overdue_task_limit: int = 25,
    reference_time: datetime | None = None,
) -> dict[str, object]:
    now = reference_time or scheduler_now()
    inventory_summary = build_scheduler_inventory_summary(session, reference_time=now)
    statuses = collect_scheduled_task_statuses(session, reference_time=now)
    all_runs = list(
        session.scalars(
            select(ScheduledTaskRunORM).order_by(ScheduledTaskRunORM.started_at.desc(), ScheduledTaskRunORM.task_run_id.desc())
        )
    )
    recent_runs = all_runs[:limit]
    maintenance_task_ids = {
        int(status["task"].task_id)
        for status in statuses
        if status["task"].task_type in MAINTENANCE_TASK_TYPES
    }
    latest_run_at = recent_runs[0].started_at if recent_runs else None
    return {
        "generated_at": now,
        "latest_run_at": latest_run_at,
        "inventory_summary": inventory_summary,
        "task_run_count": len(all_runs),
        "task_run_failure_count": sum(1 for run in all_runs if run.status == "failed"),
        "maintenance_run_count": sum(1 for run in all_runs if run.task_id in maintenance_task_ids),
        "maintenance_failure_count": sum(
            1 for run in all_runs if run.task_id in maintenance_task_ids and run.status == "failed"
        ),
        "task_type_run_counts": build_scheduler_run_buckets(all_runs),
        "recent_runs": recent_runs,
        "overdue_tasks": [status for status in statuses if status["is_overdue"]][:overdue_task_limit],
        "failing_tasks": [status for status in statuses if status["is_failing"]][:overdue_task_limit],
        "maintenance_tasks": [
            status for status in statuses if status["task"].task_type in MAINTENANCE_TASK_TYPES
        ][:overdue_task_limit],
    }


def update_scheduled_task(
    session: Session,
    task_id: int,
    payload: ScheduledTaskUpdate,
    actor: str = "system",
) -> ScheduledTaskORM:
    record = session.get(ScheduledTaskORM, task_id)
    if record is None:
        raise ValueError(f"Scheduled task {task_id} does not exist.")

    changes = payload.model_dump(exclude_unset=True)
    if not changes:
        return record

    if "name" in changes and changes["name"] != record.name:
        ensure_unique_task_name(session, str(changes["name"]), task_id=task_id)

    task_type = record.task_type
    source_id = int(changes["source_id"]) if "source_id" in changes and changes["source_id"] is not None else (
        None if "source_id" in changes else record.source_id
    )
    target_path = str(changes["target_path"]) if "target_path" in changes and changes["target_path"] is not None else (
        None if "target_path" in changes else record.target_path
    )
    geofence_id = int(changes["geofence_id"]) if "geofence_id" in changes and changes["geofence_id"] is not None else (
        None if "geofence_id" in changes else record.geofence_id
    )
    validate_task_configuration(
        task_type,
        source_id=source_id,
        target_path=target_path,
        geofence_id=geofence_id,
        layer_key=str(changes["layer_key"]) if "layer_key" in changes and changes["layer_key"] is not None else (
            None if "layer_key" in changes else record.layer_key
        ),
        payload_json=changes["payload_json"] if "payload_json" in changes else record.payload_json,
    )

    if "layer_key" in changes and changes["layer_key"]:
        ensure_data_layer(session, str(changes["layer_key"]), actor=actor)

    change_details = apply_task_changes(record, changes)
    recompute_task_next_run(record, changes)
    session.add(
        CustodyLogORM(
            object_type="scheduled_task",
            object_id=str(record.task_id),
            action="task_updated",
            actor=actor,
            details_json={
                "task_id": record.task_id,
                "changes": change_details,
                "next_run_at": record.next_run_at.isoformat() if record.next_run_at else None,
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
    runs: list[ScheduledTaskRunORM] = []
    for task in tasks:
        try:
            runs.append(run_task(session, task.task_id, actor=actor))
        except Exception:
            failed_run = session.scalar(
                select(ScheduledTaskRunORM)
                .where(ScheduledTaskRunORM.task_id == task.task_id)
                .order_by(ScheduledTaskRunORM.task_run_id.desc())
                .limit(1)
            )
            if failed_run is not None:
                runs.append(failed_run)
    return runs


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
    if task.task_type == "storage_lifecycle":
        retention_class, limit = resolve_storage_lifecycle_payload(task.payload_json)
        result = sweep_expired_storage_objects(
            session,
            retention_class=retention_class,
            limit=limit,
            dry_run=False,
            actor=actor,
        )
        return (
            result.transitioned_count,
            {
                "retention_class": retention_class,
                "limit": limit,
                "expired_candidate_count": result.expired_candidate_count,
                "transitioned_count": result.transitioned_count,
                "storage_object_ids": [
                    candidate.storage_object_id for candidate in result.candidates
                ],
            },
        )
    if task.task_type == "clickhouse_sync":
        source_domain, limit = resolve_clickhouse_sync_payload(task.payload_json)
        result = sync_runtime_to_clickhouse(
            session,
            layer_key=task.layer_key,
            source_domain=source_domain,
            limit=limit,
            actor=actor,
        )
        records_affected = int(result["observation_count"]) + int(result["storage_object_count"])
        return (
            records_affected,
            {
                "layer_key": task.layer_key,
                "source_domain": source_domain,
                "limit": limit,
                "clickhouse_database": result["clickhouse_database"],
                "observation_count": int(result["observation_count"]),
                "storage_object_count": int(result["storage_object_count"]),
            },
        )
    if task.task_type == "clickhouse_archive":
        source_domain, limit = resolve_clickhouse_archive_payload(task.payload_json)
        result = archive_clickhouse_observations_to_r2(
            session,
            layer_key=task.layer_key,
            source_domain=source_domain,
            limit=limit,
            actor=actor,
        )
        return (
            int(result["exported_row_count"]),
            {
                "layer_key": task.layer_key,
                "source_domain": source_domain,
                "limit": limit,
                "clickhouse_database": result["clickhouse_database"],
                "archive_root_url": result["archive_root_url"],
                "exported_row_count": int(result["exported_row_count"]),
                "partition_strategy": result["partition_strategy"],
            },
        )
    if task.task_type == "camera_inventory_refresh":
        source_domain, limit = resolve_camera_inventory_refresh_payload(task.payload_json)
        result = materialize_camera_inventory(
            session,
            layer_key=task.layer_key,
            source_domain=source_domain,
            limit=limit,
            actor=actor,
        )
        source_result = materialize_camera_source_inventory(
            session,
            layer_key=task.layer_key,
            source_domain=source_domain,
            limit=limit,
            actor=actor,
        )
        cameras = result["cameras"]
        return (
            int(result["created_count"]) + int(result["updated_count"]),
            {
                "layer_key": task.layer_key,
                "source_domain": source_domain,
                "limit": limit,
                "scanned_count": int(result["scanned_count"]),
                "created_count": int(result["created_count"]),
                "updated_count": int(result["updated_count"]),
                "source_created_count": int(source_result["created_count"]),
                "source_updated_count": int(source_result["updated_count"]),
                "source_scanned_endpoint_count": int(source_result["scanned_endpoint_count"]),
                "camera_inventory_ids": [camera.camera_inventory_id for camera in cameras],
                "camera_keys": [camera.camera_key for camera in cameras],
            },
        )
    if task.task_type == "entity_resolution_refresh":
        request = build_entity_resolution_request(layer_key=task.layer_key, payload_json=task.payload_json)
        results = materialize_entities(session, request, actor=actor)
        return (
            len(results),
            {
                "layer_key": request.layer_key,
                "entity_type": request.entity_type,
                "redaction_level": request.redaction_level,
                "created_entity_count": sum(1 for result in results if result.created_new),
                "entity_ids": [result.entity.entity_id for result in results],
                "entity_slugs": [result.entity.slug for result in results],
            },
        )
    if task.task_type == "event_fusion_refresh":
        request = build_event_fusion_request(layer_key=task.layer_key, payload_json=task.payload_json)
        results = materialize_fused_events(session, request, actor=actor)
        return (
            len(results),
            {
                "layer_key": request.layer_key,
                "redaction_level": request.redaction_level,
                "created_event_count": sum(1 for result in results if result.created_new),
                "event_ids": [result.event.event_id for result in results],
                "event_slugs": [result.event.slug for result in results],
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


def validate_task_configuration(
    task_type: str,
    *,
    source_id: int | None,
    target_path: str | None,
    geofence_id: int | None,
    layer_key: str | None,
    payload_json: dict[str, object] | None,
) -> None:
    if task_type == "local_import" and not target_path:
        raise ValueError("Local import task requires target_path.")
    if task_type == "source_sync" and source_id is None:
        raise ValueError("Source sync task requires source_id.")
    if task_type == "integrity_seed" and any(value is not None for value in (source_id, target_path, geofence_id)):
        raise ValueError("Integrity seed task does not accept source_id, target_path, or geofence_id.")
    if task_type == "storage_lifecycle":
        if any(value is not None for value in (source_id, target_path, geofence_id)):
            raise ValueError("Storage lifecycle task does not accept source_id, target_path, or geofence_id.")
        resolve_storage_lifecycle_payload(payload_json)
    if task_type == "clickhouse_sync":
        if any(value is not None for value in (source_id, target_path, geofence_id)):
            raise ValueError("ClickHouse sync task does not accept source_id, target_path, or geofence_id.")
        resolve_clickhouse_sync_payload(payload_json)
    if task_type == "clickhouse_archive":
        if any(value is not None for value in (source_id, target_path, geofence_id)):
            raise ValueError("ClickHouse archive task does not accept source_id, target_path, or geofence_id.")
        resolve_clickhouse_archive_payload(payload_json)
    if task_type == "camera_inventory_refresh":
        if any(value is not None for value in (source_id, target_path, geofence_id)):
            raise ValueError(
                "Camera inventory refresh task does not accept source_id, target_path, or geofence_id."
            )
        resolve_camera_inventory_refresh_payload(payload_json)
    if task_type == "entity_resolution_refresh":
        if any(value is not None for value in (source_id, target_path, geofence_id)):
            raise ValueError(
                "Entity resolution refresh task does not accept source_id, target_path, or geofence_id."
            )
        build_entity_resolution_request(layer_key=layer_key, payload_json=payload_json)
    if task_type == "event_fusion_refresh":
        if any(value is not None for value in (source_id, target_path, geofence_id)):
            raise ValueError(
                "Event fusion refresh task does not accept source_id, target_path, or geofence_id."
            )
        build_event_fusion_request(layer_key=layer_key, payload_json=payload_json)


def ensure_unique_task_name(
    session: Session,
    name: str,
    *,
    task_id: int | None = None,
) -> None:
    statement = select(ScheduledTaskORM).where(ScheduledTaskORM.name == name)
    existing = session.scalar(statement)
    if existing is None:
        return
    if task_id is not None and existing.task_id == task_id:
        return
    raise ValueError(f"Scheduled task name '{name}' already exists.")


def apply_task_changes(
    record: ScheduledTaskORM,
    changes: dict[str, object],
) -> dict[str, dict[str, object]]:
    details: dict[str, dict[str, object]] = {}
    for field_name, new_value in changes.items():
        old_value = getattr(record, field_name)
        if old_value == new_value:
            continue
        setattr(record, field_name, new_value)
        details[field_name] = {
            "old": old_value,
            "new": new_value,
        }
    return details


def recompute_task_next_run(record: ScheduledTaskORM, changes: dict[str, object]) -> None:
    scheduling_fields = {"enabled", "interval_seconds"}
    if not scheduling_fields.intersection(changes):
        return
    if not record.enabled:
        record.next_run_at = None
        return
    reference = record.last_run_at if record.last_run_at is not None else None
    record.next_run_at = compute_next_run(record.interval_seconds, reference)


def resolve_camera_inventory_refresh_payload(
    payload_json: dict[str, object] | None,
) -> tuple[str | None, int]:
    payload = payload_json or {}
    if not isinstance(payload, dict):
        raise ValueError("Camera inventory refresh payload_json must be a JSON object.")

    source_domain_value = payload.get("source_domain")
    if source_domain_value is not None and not isinstance(source_domain_value, str):
        raise ValueError("Camera inventory refresh payload source_domain must be a string.")
    source_domain = source_domain_value.strip() if isinstance(source_domain_value, str) else None
    if source_domain == "":
        source_domain = None

    limit_value = payload.get("limit", 500)
    if isinstance(limit_value, bool) or not isinstance(limit_value, int):
        raise ValueError("Camera inventory refresh payload limit must be an integer.")
    if limit_value < 1 or limit_value > 5000:
        raise ValueError("Camera inventory refresh payload limit must be between 1 and 5000.")

    return source_domain, limit_value


def resolve_storage_lifecycle_payload(
    payload_json: dict[str, object] | None,
) -> tuple[str | None, int]:
    payload = resolve_scheduler_payload_json("Storage lifecycle", payload_json)

    retention_class_value = payload.get("retention_class")
    if retention_class_value is not None and not isinstance(retention_class_value, str):
        raise ValueError("Storage lifecycle payload retention_class must be a string.")
    retention_class = retention_class_value.strip() if isinstance(retention_class_value, str) else None
    if retention_class == "":
        retention_class = None
    if retention_class is not None and retention_class not in {
        "ephemeral",
        "operational",
        "investigative",
        "permanent",
    }:
        raise ValueError("Storage lifecycle payload retention_class is not supported.")

    limit_value = payload.get("limit", 100)
    if isinstance(limit_value, bool) or not isinstance(limit_value, int):
        raise ValueError("Storage lifecycle payload limit must be an integer.")
    if limit_value < 1 or limit_value > 1000:
        raise ValueError("Storage lifecycle payload limit must be between 1 and 1000.")

    return retention_class, limit_value


def resolve_clickhouse_sync_payload(
    payload_json: dict[str, object] | None,
) -> tuple[str | None, int]:
    payload = resolve_scheduler_payload_json("ClickHouse sync", payload_json)

    source_domain_value = payload.get("source_domain")
    if source_domain_value is not None and not isinstance(source_domain_value, str):
        raise ValueError("ClickHouse sync payload source_domain must be a string.")
    source_domain = source_domain_value.strip() if isinstance(source_domain_value, str) else None
    if source_domain == "":
        source_domain = None

    limit_value = payload.get("limit", 1000)
    if isinstance(limit_value, bool) or not isinstance(limit_value, int):
        raise ValueError("ClickHouse sync payload limit must be an integer.")
    if limit_value < 1 or limit_value > 20000:
        raise ValueError("ClickHouse sync payload limit must be between 1 and 20000.")

    return source_domain, limit_value


def resolve_clickhouse_archive_payload(
    payload_json: dict[str, object] | None,
) -> tuple[str | None, int | None]:
    payload = resolve_scheduler_payload_json("ClickHouse archive", payload_json)

    source_domain_value = payload.get("source_domain")
    if source_domain_value is not None and not isinstance(source_domain_value, str):
        raise ValueError("ClickHouse archive payload source_domain must be a string.")
    source_domain = source_domain_value.strip() if isinstance(source_domain_value, str) else None
    if source_domain == "":
        source_domain = None

    limit_value = payload.get("limit")
    if limit_value is None:
        return source_domain, None
    if isinstance(limit_value, bool) or not isinstance(limit_value, int):
        raise ValueError("ClickHouse archive payload limit must be an integer.")
    if limit_value < 1 or limit_value > 500000:
        raise ValueError("ClickHouse archive payload limit must be between 1 and 500000.")

    return source_domain, limit_value


def build_entity_resolution_request(
    *,
    layer_key: str | None,
    payload_json: dict[str, object] | None,
) -> EntityResolutionRequest:
    payload = resolve_scheduler_payload_json("Entity resolution refresh", payload_json)
    request_data = dict(payload)
    if layer_key is not None:
        request_data["layer_key"] = layer_key
    try:
        return EntityResolutionRequest(**request_data)
    except ValidationError as exc:
        raise ValueError(format_validation_error("Entity resolution refresh", exc)) from exc


def build_event_fusion_request(
    *,
    layer_key: str | None,
    payload_json: dict[str, object] | None,
) -> EventFusionRequest:
    payload = resolve_scheduler_payload_json("Event fusion refresh", payload_json)
    request_data = dict(payload)
    if layer_key is not None:
        request_data["layer_key"] = layer_key
    try:
        return EventFusionRequest(**request_data)
    except ValidationError as exc:
        raise ValueError(format_validation_error("Event fusion refresh", exc)) from exc


def resolve_scheduler_payload_json(
    task_label: str,
    payload_json: dict[str, object] | None,
) -> dict[str, object]:
    payload = payload_json or {}
    if not isinstance(payload, dict):
        raise ValueError(f"{task_label} payload_json must be a JSON object.")
    return dict(payload)


def format_validation_error(task_label: str, exc: ValidationError) -> str:
    issue = exc.errors()[0]
    location = ".".join(str(part) for part in issue.get("loc", ()))
    detail = issue.get("msg", "invalid payload")
    if location:
        return f"{task_label} payload {location}: {detail}."
    return f"{task_label} payload invalid: {detail}."


def collect_scheduled_task_statuses(
    session: Session,
    *,
    reference_time: datetime,
) -> list[dict[str, object]]:
    tasks = list(session.scalars(select(ScheduledTaskORM).order_by(ScheduledTaskORM.task_id.asc())))
    latest_runs_by_task_id = build_latest_runs_by_task_id(session)
    normalized_reference_time = normalize_scheduler_timestamp(reference_time) or scheduler_now()
    statuses: list[dict[str, object]] = []
    for task in tasks:
        latest_run = latest_runs_by_task_id.get(task.task_id)
        next_run_at = normalize_scheduler_timestamp(task.next_run_at)
        is_due = bool(task.enabled and next_run_at is not None and next_run_at <= normalized_reference_time)
        overdue_threshold = (
            next_run_at + timedelta(seconds=max(task.interval_seconds, 60))
            if next_run_at is not None
            else None
        )
        is_overdue = bool(
            task.enabled
            and overdue_threshold is not None
            and overdue_threshold <= normalized_reference_time
        )
        statuses.append(
            {
                "task": task,
                "latest_run": latest_run,
                "is_due": is_due,
                "is_overdue": is_overdue,
                "is_failing": bool(latest_run is not None and latest_run.status == "failed"),
            }
        )
    return statuses


def build_latest_runs_by_task_id(session: Session) -> dict[int, ScheduledTaskRunORM]:
    rows = list(
        session.scalars(
            select(ScheduledTaskRunORM).order_by(
                ScheduledTaskRunORM.task_id.asc(),
                ScheduledTaskRunORM.task_run_id.desc(),
            )
        )
    )
    latest: dict[int, ScheduledTaskRunORM] = {}
    for row in rows:
        latest.setdefault(row.task_id, row)
    return latest


def build_scheduler_task_buckets(
    statuses: list[dict[str, object]],
    *,
    key_fn,
) -> list[dict[str, object]]:
    buckets: dict[str, dict[str, object]] = {}
    for status in statuses:
        key = key_fn(status)
        bucket = buckets.setdefault(
            key,
            {
                "key": key,
                "total_count": 0,
                "enabled_count": 0,
                "disabled_count": 0,
                "due_count": 0,
                "failing_count": 0,
            },
        )
        bucket["total_count"] = int(bucket["total_count"]) + 1
        if status["task"].enabled:
            bucket["enabled_count"] = int(bucket["enabled_count"]) + 1
        else:
            bucket["disabled_count"] = int(bucket["disabled_count"]) + 1
        if status["is_due"]:
            bucket["due_count"] = int(bucket["due_count"]) + 1
        if status["is_failing"]:
            bucket["failing_count"] = int(bucket["failing_count"]) + 1
    return [buckets[key] for key in sorted(buckets)]


def build_scheduler_run_buckets(runs: list[ScheduledTaskRunORM]) -> list[dict[str, object]]:
    buckets: dict[str, dict[str, object]] = {}
    for run in runs:
        task_type = run.task.task_type if run.task is not None else "unknown"
        bucket = buckets.setdefault(
            task_type,
            {
                "key": task_type,
                "total_count": 0,
                "completed_count": 0,
                "failure_count": 0,
            },
        )
        bucket["total_count"] = int(bucket["total_count"]) + 1
        if run.status == "completed":
            bucket["completed_count"] = int(bucket["completed_count"]) + 1
        if run.status == "failed":
            bucket["failure_count"] = int(bucket["failure_count"]) + 1
    return [buckets[key] for key in sorted(buckets)]


def latest_task_status_key(status: dict[str, object]) -> str:
    latest_run = status["latest_run"]
    if latest_run is None:
        return "never_run"
    return str(latest_run.status or "unknown")


def normalize_scheduler_timestamp(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is not None:
        return value
    return value.replace(tzinfo=timezone.utc)
