from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.config import get_settings
from src.models import (
    CameraInventoryORM,
    CameraSourceInventoryORM,
    GeofenceORM,
    ObservationORM,
    ScheduledTaskORM,
    StorageObjectORM,
    SourceDefinitionORM,
    SourceTrustProfileORM,
    WatchORM,
    WorkerStatusORM,
)
from src.services.database_diagnostics_service import build_database_diagnostics
from src.services.import_service import collect_import_paths
from src.services.scheduler_service import collect_scheduled_task_statuses
from src.services.source_service import collect_source_ops_statuses
from src.services.worker_status_service import default_worker_stale_after_seconds, worker_is_recently_available


def readiness_now() -> datetime:
    return datetime.now(timezone.utc)


def build_runtime_readiness(session: Session) -> dict[str, object]:
    settings = get_settings()
    generated_at = readiness_now()
    diagnostics = build_database_diagnostics(session)
    checks: list[dict[str, object]] = []
    operator_actions: list[str] = []

    enabled_sources = list(
        session.scalars(
            select(SourceDefinitionORM)
            .where(SourceDefinitionORM.enabled.is_(True))
            .order_by(SourceDefinitionORM.source_id.asc())
        )
    )
    enabled_tasks = list(
        session.scalars(
            select(ScheduledTaskORM)
            .where(ScheduledTaskORM.enabled.is_(True))
            .order_by(ScheduledTaskORM.task_id.asc())
        )
    )
    integrity_source_count = int(
        session.scalar(
            select(func.count())
            .select_from(SourceTrustProfileORM)
            .where(SourceTrustProfileORM.integrity_source.is_(True))
        )
        or 0
    )
    observation_count = int(session.scalar(select(func.count()).select_from(ObservationORM)) or 0)
    camera_inventory_count = int(session.scalar(select(func.count()).select_from(CameraInventoryORM)) or 0)
    camera_source_inventory_count = int(session.scalar(select(func.count()).select_from(CameraSourceInventoryORM)) or 0)
    geofence_count = int(session.scalar(select(func.count()).select_from(GeofenceORM)) or 0)
    worker_stale_seconds = default_worker_stale_after_seconds()
    worker_stale_before = generated_at - timedelta(seconds=max(1.0, worker_stale_seconds))
    workers = list(session.scalars(select(WorkerStatusORM).order_by(WorkerStatusORM.worker_key.asc())))
    source_health_stale_before = generated_at - timedelta(hours=24)
    source_ops_statuses = collect_source_ops_statuses(session, stale_before=source_health_stale_before)
    scheduled_task_statuses = collect_scheduled_task_statuses(session, reference_time=generated_at)

    pull_source_ids = sorted(
        source.source_id
        for source in enabled_sources
        if source.source_kind not in {"webhook_ingest", "sse_stream", "websocket_stream"}
    )
    sync_task_source_ids = sorted(
        task.source_id
        for task in enabled_tasks
        if task.task_type == "source_sync" and task.source_id is not None
    )
    enabled_task_types = {task.task_type for task in enabled_tasks}
    enabled_watches = list(
        session.scalars(
            select(WatchORM)
            .where(WatchORM.state == "enabled")
            .order_by(WatchORM.watch_id.asc())
        )
    )
    scheduled_watch_ids = {
        int(task.payload_json["watch_id"])
        for task in enabled_tasks
        if task.task_type == "watch_evaluate"
        and isinstance(task.payload_json, dict)
        and isinstance(task.payload_json.get("watch_id"), int)
    }
    camera_enabled_sources = [source for source in enabled_sources if source_looks_camera_related(source)]
    camera_runtime_expected = bool(
        camera_enabled_sources or camera_inventory_count > 0 or camera_source_inventory_count > 0
    )

    add_readiness_check(
        checks,
        operator_actions,
        key="database_connectivity",
        status="pass" if diagnostics["database_connected"] else "action_required",
        summary="Database connectivity probe succeeded."
        if diagnostics["database_connected"]
        else "Database connectivity probe failed.",
        details_json={
            "database_connected": diagnostics["database_connected"],
            "database_backend": diagnostics["database_backend"],
            "database_url": diagnostics["database_url"],
        },
        operator_action="Fix database connectivity and rerun readiness checks."
        if not diagnostics["database_connected"]
        else None,
    )

    migration = diagnostics["migration"]
    add_readiness_check(
        checks,
        operator_actions,
        key="schema_revision",
        status="pass"
        if migration["version_table_present"] and migration["schema_up_to_date"]
        else "action_required",
        summary="Runtime schema revision is present and up to date."
        if migration["version_table_present"] and migration["schema_up_to_date"]
        else "Runtime schema revision is missing or behind head.",
        details_json=dict(migration),
        operator_action="Run additive schema migration or reinitialize the database so runtime schema matches head."
        if not (migration["version_table_present"] and migration["schema_up_to_date"])
        else None,
    )

    missing_columns = [
        f"{item['table_name']}.{item['column_name']}"
        for item in diagnostics["required_columns"]
        if not item["present"]
    ]
    add_readiness_check(
        checks,
        operator_actions,
        key="additive_schema_columns",
        status="pass" if not missing_columns else "action_required",
        summary="Required additive schema columns are present."
        if not missing_columns
        else "Required additive schema columns are missing.",
        details_json={"missing_columns": missing_columns},
        operator_action="Apply the latest additive schema changes before running the platform."
        if missing_columns
        else None,
    )

    spatial_status = "pass"
    spatial_summary = f"Spatial backend '{settings.spatial_backend}' is usable for this runtime."
    spatial_action = None
    if diagnostics["postgis_expected"]:
        missing_spatial_indexes = [
            item["index_name"] for item in diagnostics["spatial_indexes"] if not item["present"]
        ]
        if not diagnostics["postgis_extension_installed"] or missing_spatial_indexes:
            spatial_status = "action_required"
            spatial_summary = "PostGIS backend is selected but required extension or indexes are missing."
            spatial_action = "Install PostGIS and provision required spatial indexes on the PostgreSQL backend."
    add_readiness_check(
        checks,
        operator_actions,
        key="spatial_backend",
        status=spatial_status,
        summary=spatial_summary,
        details_json={
            "spatial_backend": diagnostics["spatial_backend"],
            "postgis_expected": diagnostics["postgis_expected"],
            "postgis_extension_installed": diagnostics["postgis_extension_installed"],
            "postgis_version": diagnostics["postgis_version"],
            "spatial_indexes": diagnostics["spatial_indexes"],
        },
        operator_action=spatial_action,
    )

    add_readiness_check(
        checks,
        operator_actions,
        key="integrity_registry",
        status="pass" if integrity_source_count > 0 else "action_required",
        summary="Integrity source registry is seeded."
        if integrity_source_count > 0
        else "No integrity sources are seeded.",
        details_json={"integrity_source_count": integrity_source_count},
        operator_action="Run integrity seeding so trust scoring has a baseline source registry."
        if integrity_source_count == 0
        else None,
    )

    add_readiness_check(
        checks,
        operator_actions,
        key="enabled_source_catalog",
        status="pass" if enabled_sources else "action_required",
        summary="Managed sources are enabled for unattended collection."
        if enabled_sources
        else "No enabled managed sources are configured.",
        details_json={
            "enabled_source_count": len(enabled_sources),
            "source_kinds": sorted({source.source_kind for source in enabled_sources}),
        },
        operator_action="Create or bootstrap managed sources so the runtime has something to collect."
        if not enabled_sources
        else None,
    )

    add_readiness_check(
        checks,
        operator_actions,
        key="enabled_schedule_catalog",
        status="pass" if enabled_tasks else "action_required",
        summary="Enabled schedules exist for unattended runtime work."
        if enabled_tasks
        else "No enabled schedules are configured.",
        details_json={
            "enabled_task_count": len(enabled_tasks),
            "task_types": sorted(enabled_task_types),
        },
        operator_action="Create or bootstrap enabled schedules for unattended runtime work."
        if not enabled_tasks
        else None,
    )

    local_import_target_health = evaluate_local_import_target_health(enabled_tasks)
    add_readiness_check(
        checks,
        operator_actions,
        key="local_import_target_health",
        status=local_import_target_health["status"],
        summary=local_import_target_health["summary"],
        details_json=local_import_target_health["details_json"],
        operator_action=local_import_target_health["operator_action"],
    )

    missing_source_ops_tasks = [
        task_type
        for task_type in ("source_maintenance", "source_health_scan")
        if task_type not in enabled_task_types
    ]
    add_readiness_check(
        checks,
        operator_actions,
        key="source_ops_coverage",
        status="pass" if not enabled_sources or not missing_source_ops_tasks else "action_required",
        summary="Source maintenance and health coverage are scheduled."
        if not enabled_sources or not missing_source_ops_tasks
        else "Managed sources exist without baseline maintenance/health coverage.",
        details_json={
            "enabled_source_count": len(enabled_sources),
            "missing_task_types": missing_source_ops_tasks,
        },
        operator_action="Add source maintenance and source health scan schedules so source failures do not rot silently."
        if enabled_sources and missing_source_ops_tasks
        else None,
    )

    uncovered_pull_sources = sorted(set(pull_source_ids) - set(sync_task_source_ids))
    add_readiness_check(
        checks,
        operator_actions,
        key="source_sync_coverage",
        status="pass" if not uncovered_pull_sources else "warning",
        summary="Pull-capable sources have direct sync schedule coverage."
        if not uncovered_pull_sources
        else "Some pull-capable sources do not have dedicated source sync schedules.",
        details_json={
            "pull_source_ids": pull_source_ids,
            "sync_task_source_ids": sync_task_source_ids,
            "uncovered_pull_source_ids": uncovered_pull_sources,
        },
        operator_action="Add source sync schedules for pull-capable sources that should be refreshed on a normal cadence."
        if uncovered_pull_sources
        else None,
    )

    source_runtime_health = evaluate_source_runtime_health(source_ops_statuses)
    add_readiness_check(
        checks,
        operator_actions,
        key="source_runtime_health",
        status=source_runtime_health["status"],
        summary=source_runtime_health["summary"],
        details_json=source_runtime_health["details_json"],
        operator_action=source_runtime_health["operator_action"],
    )

    scheduler_runtime_health = evaluate_scheduler_runtime_health(scheduled_task_statuses)
    add_readiness_check(
        checks,
        operator_actions,
        key="scheduler_runtime_health",
        status=scheduler_runtime_health["status"],
        summary=scheduler_runtime_health["summary"],
        details_json=scheduler_runtime_health["details_json"],
        operator_action=scheduler_runtime_health["operator_action"],
    )

    missing_analytic_tasks = [
        task_type
        for task_type in ("entity_resolution_refresh", "event_fusion_refresh")
        if task_type not in enabled_task_types
    ]
    add_readiness_check(
        checks,
        operator_actions,
        key="analytic_pipeline_coverage",
        status="pass" if not missing_analytic_tasks else "warning",
        summary="Entity-resolution and event-fusion pipeline coverage is scheduled."
        if not missing_analytic_tasks
        else "Analytic pipeline schedules are missing.",
        details_json={
            "observation_count": observation_count,
            "missing_task_types": missing_analytic_tasks,
        },
        operator_action="Add entity-resolution and event-fusion refresh schedules so collected observations promote into analyst-facing outputs."
        if missing_analytic_tasks
        else None,
    )

    missing_camera_tasks = [
        task_type
        for task_type in ("camera_inventory_refresh", "camera_source_verification")
        if task_type not in enabled_task_types
    ]
    add_readiness_check(
        checks,
        operator_actions,
        key="camera_pipeline_coverage",
        status="pass" if not camera_runtime_expected or not missing_camera_tasks else "action_required",
        summary="Camera inventory refresh and endpoint verification coverage are scheduled."
        if not camera_runtime_expected or not missing_camera_tasks
        else "Camera-related sources or inventory exist without full refresh/verification coverage.",
        details_json={
            "camera_runtime_expected": camera_runtime_expected,
            "camera_enabled_source_ids": [source.source_id for source in camera_enabled_sources],
            "camera_inventory_count": camera_inventory_count,
            "camera_source_inventory_count": camera_source_inventory_count,
            "missing_task_types": missing_camera_tasks,
        },
        operator_action=(
            "Add camera inventory refresh and camera source verification schedules so camera observations "
            "materialize and stay verified unattended."
        )
        if camera_runtime_expected and missing_camera_tasks
        else None,
    )

    add_readiness_check(
        checks,
        operator_actions,
        key="storage_lifecycle_coverage",
        status="pass" if "storage_lifecycle" in enabled_task_types else "warning",
        summary="Storage lifecycle sweep coverage is scheduled."
        if "storage_lifecycle" in enabled_task_types
        else "Storage lifecycle sweep schedule is missing.",
        details_json={"storage_lifecycle_scheduled": "storage_lifecycle" in enabled_task_types},
        operator_action="Add a storage lifecycle schedule so local artifacts do not accumulate unmanaged."
        if "storage_lifecycle" not in enabled_task_types
        else None,
    )

    unscheduled_watch_ids = [
        watch.watch_id for watch in enabled_watches if watch.watch_id not in scheduled_watch_ids
    ]
    add_readiness_check(
        checks,
        operator_actions,
        key="watch_schedule_coverage",
        status="pass" if not unscheduled_watch_ids else "warning",
        summary="Every enabled watch has an enabled deterministic evaluation schedule."
        if not unscheduled_watch_ids
        else "Enabled watches exist without enabled evaluation schedules.",
        details_json={
            "enabled_watch_count": len(enabled_watches),
            "scheduled_watch_count": len(enabled_watches) - len(unscheduled_watch_ids),
            "unscheduled_watch_ids": unscheduled_watch_ids,
        },
        operator_action="Attach an enabled watch schedule so every active watch runs unattended."
        if unscheduled_watch_ids
        else None,
    )

    runtime_snapshot_coverage = evaluate_runtime_snapshot_coverage(
        session,
        generated_at=generated_at,
        max_age_hours=settings.runtime_snapshot_max_age_hours,
        enabled_tasks=enabled_tasks,
        enabled_sources=enabled_sources,
        observation_count=observation_count,
        camera_inventory_count=camera_inventory_count,
        camera_source_inventory_count=camera_source_inventory_count,
        geofence_count=geofence_count,
    )
    add_readiness_check(
        checks,
        operator_actions,
        key="runtime_snapshot_coverage",
        status=runtime_snapshot_coverage["status"],
        summary=runtime_snapshot_coverage["summary"],
        details_json=runtime_snapshot_coverage["details_json"],
        operator_action=runtime_snapshot_coverage["operator_action"],
    )

    geofence_scan_scheduled = "geofence_scan" in enabled_task_types
    add_readiness_check(
        checks,
        operator_actions,
        key="geofence_alert_coverage",
        status="pass" if geofence_count == 0 or geofence_scan_scheduled else "warning",
        summary="Geofence alert scan coverage matches current geofence inventory."
        if geofence_count == 0 or geofence_scan_scheduled
        else "Geofences exist without any enabled geofence scan schedule.",
        details_json={
            "geofence_count": geofence_count,
            "geofence_scan_scheduled": geofence_scan_scheduled,
        },
        operator_action="Add a geofence scan schedule so configured geofences can emit alerts unattended."
        if geofence_count > 0 and not geofence_scan_scheduled
        else None,
    )

    stream_runtime_sources = [
        source
        for source in enabled_sources
        if source.source_kind in {"sse_stream", "websocket_stream"}
    ]
    schedule_executor_types = {"scheduler_worker", "platform_runtime_worker"}
    source_executor_types = {"source_runtime_worker", "platform_runtime_worker"}
    schedule_executor_coverage = any(
        worker_satisfies_scheduler_coverage(worker, stale_before=worker_stale_before)
        for worker in workers
    )
    source_executor_coverage = any(
        worker_satisfies_source_runtime_coverage(worker, stale_before=worker_stale_before)
        for worker in workers
    )
    add_readiness_check(
        checks,
        operator_actions,
        key="scheduler_executor_coverage",
        status="pass" if not enabled_tasks or schedule_executor_coverage else "action_required",
        summary="A recent scheduler-capable executor heartbeat is present."
        if not enabled_tasks or schedule_executor_coverage
        else "Enabled schedules exist without any recent scheduler-capable executor heartbeat.",
        details_json={
            "enabled_task_count": len(enabled_tasks),
            "required_worker_types": sorted(schedule_executor_types),
            "worker_stale_after_seconds": worker_stale_seconds,
            "available_worker_keys": [
                worker.worker_key
                for worker in workers
                if worker_satisfies_scheduler_coverage(worker, stale_before=worker_stale_before)
            ],
        },
        operator_action="Run scheduler-worker or platform-runtime-worker under a real supervisor so enabled schedules actually execute."
        if enabled_tasks and not schedule_executor_coverage
        else None,
    )
    add_readiness_check(
        checks,
        operator_actions,
        key="source_runtime_executor_coverage",
        status="pass" if not stream_runtime_sources or source_executor_coverage else "action_required",
        summary="A recent stream-source runtime executor heartbeat is present."
        if not stream_runtime_sources or source_executor_coverage
        else "Enabled stream-native sources exist without any recent runtime executor heartbeat.",
        details_json={
            "stream_runtime_source_count": len(stream_runtime_sources),
            "stream_runtime_source_ids": [source.source_id for source in stream_runtime_sources],
            "required_worker_types": sorted(source_executor_types),
            "worker_stale_after_seconds": worker_stale_seconds,
            "available_worker_keys": [
                worker.worker_key
                for worker in workers
                if worker_satisfies_source_runtime_coverage(worker, stale_before=worker_stale_before)
            ],
        },
        operator_action="Run source-runtime-worker or platform-runtime-worker under a real supervisor so enabled stream sources are actually collected."
        if stream_runtime_sources and not source_executor_coverage
        else None,
    )

    action_required_count = sum(1 for check in checks if check["status"] == "action_required")
    warning_count = sum(1 for check in checks if check["status"] == "warning")
    overall_status = "ready"
    if action_required_count > 0:
        overall_status = "not_ready"
    elif warning_count > 0:
        overall_status = "degraded"

    return {
        "generated_at": generated_at,
        "overall_status": overall_status,
        "ready": overall_status == "ready",
        "check_count": len(checks),
        "action_required_count": action_required_count,
        "warning_count": warning_count,
        "operator_actions": operator_actions,
        "checks": checks,
    }


def add_readiness_check(
    checks: list[dict[str, object]],
    operator_actions: list[str],
    *,
    key: str,
    status: str,
    summary: str,
    details_json: dict[str, object],
    operator_action: str | None,
) -> None:
    checks.append(
        {
            "key": key,
            "status": status,
            "summary": summary,
            "details_json": details_json,
        }
    )
    if operator_action and status in {"warning", "action_required"} and operator_action not in operator_actions:
        operator_actions.append(operator_action)


def worker_satisfies_scheduler_coverage(
    worker: WorkerStatusORM,
    *,
    stale_before: datetime,
) -> bool:
    if not worker_is_recently_available(worker, stale_before=stale_before):
        return False
    if worker.worker_type == "scheduler_worker":
        return True
    if worker.worker_type == "platform_runtime_worker":
        metadata = worker.metadata_json if isinstance(worker.metadata_json, dict) else {}
        return bool(metadata.get("include_enabled_schedules", True))
    return False


def worker_satisfies_source_runtime_coverage(
    worker: WorkerStatusORM,
    *,
    stale_before: datetime,
) -> bool:
    if not worker_is_recently_available(worker, stale_before=stale_before):
        return False
    if worker.worker_type == "source_runtime_worker":
        return True
    if worker.worker_type == "platform_runtime_worker":
        metadata = worker.metadata_json if isinstance(worker.metadata_json, dict) else {}
        return bool(metadata.get("include_stream_runtime", True))
    return False


def evaluate_runtime_snapshot_coverage(
    session: Session,
    *,
    generated_at: datetime,
    max_age_hours: float,
    enabled_tasks: list[ScheduledTaskORM],
    enabled_sources: list[SourceDefinitionORM],
    observation_count: int,
    camera_inventory_count: int,
    camera_source_inventory_count: int,
    geofence_count: int,
) -> dict[str, object]:
    snapshot_tasks = [task for task in enabled_tasks if task.task_type == "runtime_snapshot_export"]
    snapshot_rows = list(
        session.scalars(
            select(StorageObjectORM)
            .where(
                StorageObjectORM.owner_type == "runtime_snapshot",
                StorageObjectORM.object_kind == "runtime_snapshot_export",
            )
            .order_by(StorageObjectORM.observed_at.desc().nullslast(), StorageObjectORM.storage_object_id.desc())
        )
    )
    manifest_rows = list(
        session.scalars(
            select(StorageObjectORM)
            .where(
                StorageObjectORM.owner_type == "runtime_snapshot",
                StorageObjectORM.object_kind == "runtime_snapshot_manifest_export",
            )
            .order_by(StorageObjectORM.observed_at.desc().nullslast(), StorageObjectORM.storage_object_id.desc())
        )
    )
    manifests_by_owner_id = {row.owner_id: row for row in manifest_rows}
    latest_snapshot = snapshot_rows[0] if snapshot_rows else None
    latest_manifest = manifest_rows[0] if manifest_rows else None
    latest_paired_snapshot = next((row for row in snapshot_rows if row.owner_id in manifests_by_owner_id), None)
    latest_paired_manifest = (
        manifests_by_owner_id.get(latest_paired_snapshot.owner_id) if latest_paired_snapshot is not None else None
    )
    latest_paired_observed_at = (
        normalize_readiness_timestamp(latest_paired_snapshot.observed_at)
        if latest_paired_snapshot is not None
        else None
    )
    fresh_before = generated_at - timedelta(hours=max(1.0, max_age_hours))
    non_backup_tasks = [task for task in enabled_tasks if task.task_type != "runtime_snapshot_export"]
    backup_expected = bool(
        enabled_sources
        or non_backup_tasks
        or observation_count > 0
        or camera_inventory_count > 0
        or camera_source_inventory_count > 0
        or geofence_count > 0
    )
    recent_pair_available = latest_paired_observed_at is not None and latest_paired_observed_at >= fresh_before

    if not backup_expected:
        status = "pass"
        summary = "Runtime backup coverage is not required yet because the runtime has no meaningful operational state."
        operator_action = None
    elif snapshot_tasks and recent_pair_available:
        status = "pass"
        summary = "Recent runtime snapshot + manifest artifacts are present for local recovery."
        operator_action = None
    elif latest_paired_snapshot is not None:
        status = "action_required"
        summary = "Runtime snapshot artifacts exist, but the most recent verified recovery export is stale."
        operator_action = (
            "Run the runtime snapshot export now or repair the scheduled runtime snapshot task so local recovery "
            "artifacts stay fresh."
        )
    elif snapshot_tasks:
        status = "action_required"
        summary = "Runtime snapshot scheduling exists, but no verified snapshot artifact has been exported yet."
        operator_action = (
            "Run the runtime snapshot export task once and confirm it writes both snapshot and manifest artifacts."
        )
    else:
        status = "action_required"
        summary = "No runtime snapshot export coverage is configured for local recovery."
        operator_action = (
            "Add a runtime snapshot export schedule and keep verified snapshot + manifest artifacts under managed storage."
        )

    return {
        "status": status,
        "summary": summary,
        "operator_action": operator_action,
        "details_json": {
            "backup_expected": backup_expected,
            "runtime_snapshot_task_ids": [task.task_id for task in snapshot_tasks],
            "runtime_snapshot_task_count": len(snapshot_tasks),
            "max_age_hours": max_age_hours,
            "fresh_before": fresh_before,
            "snapshot_export_count": len(snapshot_rows),
            "manifest_export_count": len(manifest_rows),
            "latest_snapshot_storage_object_id": latest_snapshot.storage_object_id if latest_snapshot else None,
            "latest_snapshot_observed_at": latest_snapshot.observed_at if latest_snapshot else None,
            "latest_manifest_storage_object_id": latest_manifest.storage_object_id if latest_manifest else None,
            "latest_manifest_observed_at": latest_manifest.observed_at if latest_manifest else None,
            "latest_paired_snapshot_storage_object_id": (
                latest_paired_snapshot.storage_object_id if latest_paired_snapshot else None
            ),
            "latest_paired_manifest_storage_object_id": (
                latest_paired_manifest.storage_object_id if latest_paired_manifest else None
            ),
            "latest_paired_exported_at": latest_paired_observed_at,
            "recent_pair_available": recent_pair_available,
        },
    }


def source_looks_camera_related(source: SourceDefinitionORM) -> bool:
    return any(
        text_looks_camera_related(value)
        for value in (source.name, source.layer_key, source.notes, source.target_uri)
    )


def normalize_readiness_timestamp(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def evaluate_source_runtime_health(statuses: list[dict[str, object]]) -> dict[str, object]:
    enabled_statuses = [status for status in statuses if status["source"].enabled]
    stale_pull_statuses = [
        status for status in enabled_statuses if status["fetch_mode"] == "pull" and status["is_stale"]
    ]
    failing_statuses = [status for status in enabled_statuses if status["is_failing"]]
    degraded_runtime_statuses = [
        status for status in enabled_statuses if status["runtime_state"] == "degraded"
    ]
    pending_dead_letter_statuses = [
        status for status in enabled_statuses if int(status["pending_dead_letter_count"]) > 0
    ]

    affected_by_source_id: dict[int, dict[str, object]] = {}
    for bucket in (
        stale_pull_statuses,
        failing_statuses,
        degraded_runtime_statuses,
        pending_dead_letter_statuses,
    ):
        for status in bucket:
            source = status["source"]
            affected_by_source_id.setdefault(source.source_id, status)

    affected_statuses = [
        affected_by_source_id[source_id] for source_id in sorted(affected_by_source_id)
    ]
    status = "pass" if not affected_statuses else "action_required"
    summary = (
        "Enabled source fleet is healthy enough for unattended collection."
        if not affected_statuses
        else "Enabled source fleet currently has stale pull feeds, failed runs, or dead-letter backlog."
    )
    operator_action = (
        None
        if not affected_statuses
        else "Repair or rerun unhealthy sources, clear pending dead letters, and rerun readiness so collection health reflects reality."
    )

    return {
        "status": status,
        "summary": summary,
        "operator_action": operator_action,
        "details_json": {
            "enabled_source_count": len(enabled_statuses),
            "affected_source_count": len(affected_statuses),
            "stale_pull_source_ids": [status["source"].source_id for status in stale_pull_statuses],
            "failing_source_ids": [status["source"].source_id for status in failing_statuses],
            "runtime_degraded_source_ids": [
                status["source"].source_id for status in degraded_runtime_statuses
            ],
            "pending_dead_letter_source_ids": [
                status["source"].source_id for status in pending_dead_letter_statuses
            ],
            "pending_dead_letter_count": sum(
                int(status["pending_dead_letter_count"]) for status in pending_dead_letter_statuses
            ),
            "affected_sources": [
                {
                    "source_id": status["source"].source_id,
                    "name": status["source"].name,
                    "source_kind": status["source"].source_kind,
                    "fetch_mode": status["fetch_mode"],
                    "latest_run_status": status["latest_run"].status if status["latest_run"] is not None else None,
                    "runtime_state": status["runtime_state"],
                    "is_stale": bool(status["is_stale"]),
                    "is_failing": bool(status["is_failing"]),
                    "pending_dead_letter_count": int(status["pending_dead_letter_count"]),
                }
                for status in affected_statuses[:25]
            ],
        },
    }


def evaluate_scheduler_runtime_health(statuses: list[dict[str, object]]) -> dict[str, object]:
    enabled_statuses = [status for status in statuses if status["task"].enabled]
    overdue_statuses = [status for status in enabled_statuses if status["is_overdue"]]
    failing_statuses = [status for status in enabled_statuses if status["is_failing"]]

    affected_by_task_id: dict[int, dict[str, object]] = {}
    for bucket in (overdue_statuses, failing_statuses):
        for status in bucket:
            task = status["task"]
            affected_by_task_id.setdefault(task.task_id, status)

    affected_statuses = [affected_by_task_id[task_id] for task_id in sorted(affected_by_task_id)]
    status = "pass" if not affected_statuses else "action_required"
    summary = (
        "Enabled scheduler fleet is healthy enough for unattended execution."
        if not affected_statuses
        else "Enabled scheduler tasks are overdue or their latest run failed."
    )
    operator_action = (
        None
        if not affected_statuses
        else "Repair failing scheduled tasks or clear overdue task backlog before treating the runtime as operational."
    )

    return {
        "status": status,
        "summary": summary,
        "operator_action": operator_action,
        "details_json": {
            "enabled_task_count": len(enabled_statuses),
            "affected_task_count": len(affected_statuses),
            "overdue_task_ids": [status["task"].task_id for status in overdue_statuses],
            "failing_task_ids": [status["task"].task_id for status in failing_statuses],
            "affected_tasks": [
                {
                    "task_id": status["task"].task_id,
                    "name": status["task"].name,
                    "task_type": status["task"].task_type,
                    "next_run_at": status["task"].next_run_at,
                    "latest_run_status": status["latest_run"].status if status["latest_run"] is not None else None,
                    "is_overdue": bool(status["is_overdue"]),
                    "is_failing": bool(status["is_failing"]),
                }
                for status in affected_statuses[:25]
            ],
        },
    }


def evaluate_local_import_target_health(enabled_tasks: list[ScheduledTaskORM]) -> dict[str, object]:
    local_import_tasks = [task for task in enabled_tasks if task.task_type == "local_import"]
    affected_tasks: list[dict[str, object]] = []

    for task in local_import_tasks:
        target_path = str(task.target_path or "").strip()
        if not target_path:
            affected_tasks.append(
                {
                    "task_id": task.task_id,
                    "name": task.name,
                    "target_path": task.target_path,
                    "issue": "missing_target_path",
                }
            )
            continue

        path = Path(target_path).expanduser().resolve(strict=False)
        if not path.exists():
            affected_tasks.append(
                {
                    "task_id": task.task_id,
                    "name": task.name,
                    "target_path": str(path),
                    "issue": "missing_path",
                }
            )
            continue

        if path.is_dir():
            supported_paths, ignored_paths = collect_import_paths(path)
            if not supported_paths:
                affected_tasks.append(
                    {
                        "task_id": task.task_id,
                        "name": task.name,
                        "target_path": str(path),
                        "issue": "empty_supported_directory",
                        "ignored_file_count": len(ignored_paths),
                    }
                )

    status = "pass" if not affected_tasks else "action_required"
    summary = (
        "Enabled local-import schedules point at ingestible local targets."
        if not affected_tasks
        else "One or more enabled local-import schedules point at missing or non-ingestible local targets."
    )
    operator_action = (
        None
        if not affected_tasks
        else "Repair or disable broken local-import schedules so unattended local ingestion does not fail on startup."
    )

    return {
        "status": status,
        "summary": summary,
        "operator_action": operator_action,
        "details_json": {
            "enabled_local_import_task_count": len(local_import_tasks),
            "affected_task_count": len(affected_tasks),
            "affected_task_ids": [int(task["task_id"]) for task in affected_tasks],
            "affected_tasks": affected_tasks[:25],
        },
    }


def text_looks_camera_related(value: str | None) -> bool:
    if not isinstance(value, str):
        return False
    normalized = value.strip().lower()
    if not normalized:
        return False
    return any(term in normalized for term in ("camera", "webcam", "cctv"))
