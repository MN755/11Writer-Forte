from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from pydantic import BaseModel, TypeAdapter
from sqlalchemy import delete, func, insert, select
from sqlalchemy.orm import Session

from src.config import get_settings
from src.migrations import inspect_database_revision
from src.models import (
    AlertORM,
    CameraInventoryORM,
    CameraSourceInventoryORM,
    CustodyLogORM,
    DataLayerORM,
    EntityObservationLinkORM,
    EntityORM,
    EventObservationLinkORM,
    EventORM,
    GeofenceORM,
    LocalImportRunORM,
    ObservationORM,
    ScheduledTaskORM,
    ScheduledTaskRunORM,
    SituationProductORM,
    StorageObjectORM,
    SourceCheckpointORM,
    SourceDeadLetterORM,
    SourceDefinitionORM,
    SourceRunORM,
    SourceTrustProfileORM,
    WorkerStatusORM,
)
from src.schemas import (
    AlertRead,
    CameraInventoryRead,
    CameraSourceInventoryRead,
    CustodyLogRead,
    DataLayerRead,
    DatabaseTableCountRead,
    EntityObservationLinkRead,
    EntityRead,
    EventObservationLinkRead,
    EventRead,
    GeofenceRead,
    LocalImportRunSummaryRead,
    ObservationRead,
    RuntimeRestoreResultRead,
    RuntimeSnapshotArtifactManifestRead,
    RuntimeSnapshotRead,
    ScheduledTaskRead,
    ScheduledTaskRunRead,
    SituationProductRead,
    StorageObjectRead,
    SourceCheckpointRead,
    SourceDeadLetterRead,
    SourceDefinitionRead,
    SourceRunRead,
    SourceTrustProfileRead,
    WorkerStatusRead,
)
from src.services.database_diagnostics_service import collect_table_counts
from src.services.export_artifact_service import write_json_export_artifact
from src.services.storage_service import hash_file


def snapshot_now() -> datetime:
    return datetime.now(timezone.utc)


SNAPSHOT_SECTIONS: tuple[tuple[str, object, type[BaseModel], object], ...] = (
    ("data_layers", DataLayerORM, DataLayerRead, DataLayerORM.layer_id),
    (
        "source_trust_profiles",
        SourceTrustProfileORM,
        SourceTrustProfileRead,
        SourceTrustProfileORM.trust_profile_id,
    ),
    ("geofences", GeofenceORM, GeofenceRead, GeofenceORM.geofence_id),
    ("source_definitions", SourceDefinitionORM, SourceDefinitionRead, SourceDefinitionORM.source_id),
    ("source_checkpoints", SourceCheckpointORM, SourceCheckpointRead, SourceCheckpointORM.source_checkpoint_id),
    ("local_import_runs", LocalImportRunORM, LocalImportRunSummaryRead, LocalImportRunORM.import_run_id),
    ("events", EventORM, EventRead, EventORM.event_id),
    ("entities", EntityORM, EntityRead, EntityORM.entity_id),
    ("observations", ObservationORM, ObservationRead, ObservationORM.observation_id),
    ("camera_inventory", CameraInventoryORM, CameraInventoryRead, CameraInventoryORM.camera_inventory_id),
    (
        "camera_source_inventory",
        CameraSourceInventoryORM,
        CameraSourceInventoryRead,
        CameraSourceInventoryORM.camera_source_inventory_id,
    ),
    ("storage_objects", StorageObjectORM, StorageObjectRead, StorageObjectORM.storage_object_id),
    (
        "event_observation_links",
        EventObservationLinkORM,
        EventObservationLinkRead,
        EventObservationLinkORM.event_observation_link_id,
    ),
    (
        "entity_observation_links",
        EntityObservationLinkORM,
        EntityObservationLinkRead,
        EntityObservationLinkORM.entity_observation_link_id,
    ),
    ("alerts", AlertORM, AlertRead, AlertORM.alert_id),
    ("scheduled_tasks", ScheduledTaskORM, ScheduledTaskRead, ScheduledTaskORM.task_id),
    ("scheduled_task_runs", ScheduledTaskRunORM, ScheduledTaskRunRead, ScheduledTaskRunORM.task_run_id),
    ("source_runs", SourceRunORM, SourceRunRead, SourceRunORM.source_run_id),
    ("source_dead_letters", SourceDeadLetterORM, SourceDeadLetterRead, SourceDeadLetterORM.source_dead_letter_id),
    ("worker_statuses", WorkerStatusORM, WorkerStatusRead, WorkerStatusORM.worker_status_id),
    ("situation_products", SituationProductORM, SituationProductRead, SituationProductORM.product_id),
    ("custody_logs", CustodyLogORM, CustodyLogRead, CustodyLogORM.custody_log_id),
)

RESTORE_ORDER: tuple[tuple[str, object], ...] = (
    ("data_layers", DataLayerORM),
    ("source_trust_profiles", SourceTrustProfileORM),
    ("geofences", GeofenceORM),
    ("source_definitions", SourceDefinitionORM),
    ("source_checkpoints", SourceCheckpointORM),
    ("local_import_runs", LocalImportRunORM),
    ("events", EventORM),
    ("entities", EntityORM),
    ("observations", ObservationORM),
    ("camera_inventory", CameraInventoryORM),
    ("camera_source_inventory", CameraSourceInventoryORM),
    ("storage_objects", StorageObjectORM),
    ("event_observation_links", EventObservationLinkORM),
    ("entity_observation_links", EntityObservationLinkORM),
    ("alerts", AlertORM),
    ("scheduled_tasks", ScheduledTaskORM),
    ("source_runs", SourceRunORM),
    ("scheduled_task_runs", ScheduledTaskRunORM),
    ("source_dead_letters", SourceDeadLetterORM),
    ("worker_statuses", WorkerStatusORM),
    ("situation_products", SituationProductORM),
    ("custody_logs", CustodyLogORM),
)


def build_runtime_snapshot(session: Session) -> dict[str, object]:
    settings = get_settings()
    row_counts_before = collect_table_counts(session)
    export_log = log_runtime_snapshot_export(session, row_counts=row_counts_before)
    session.commit()
    revision_status = inspect_database_revision(session.get_bind())
    snapshot = {
        "exported_at": snapshot_now(),
        "app_name": settings.app_name,
        "app_version": settings.app_version,
        "database_backend": session.get_bind().dialect.name,
        "spatial_backend": settings.spatial_backend,
        "database_revision": revision_status.current_revision,
        "database_head_revision": revision_status.head_revision,
        "row_counts": collect_table_counts(session),
    }
    snapshot.update(serialize_snapshot_sections(session))
    if not any(log["custody_log_id"] == export_log.custody_log_id for log in snapshot["custody_logs"]):
        snapshot["custody_logs"].append(serialize_row(export_log, CustodyLogRead))
    return snapshot


def build_runtime_snapshot_section_counts(snapshot_payload: RuntimeSnapshotRead | dict[str, object]) -> dict[str, int]:
    snapshot = ensure_runtime_snapshot_model(snapshot_payload)
    return {
        section_name: len(getattr(snapshot, section_name))
        for section_name, *_ in SNAPSHOT_SECTIONS
    }


def build_runtime_snapshot_artifact_manifest(
    snapshot_payload: RuntimeSnapshotRead | dict[str, object],
    *,
    snapshot_file_name: str,
    snapshot_sha256: str,
    snapshot_byte_size: int,
    snapshot_storage_object_id: int | None = None,
    snapshot_object_key: str | None = None,
) -> dict[str, object]:
    snapshot = ensure_runtime_snapshot_model(snapshot_payload)
    return TypeAdapter(RuntimeSnapshotArtifactManifestRead).validate_python(
        {
            "generated_at": snapshot_now(),
            "snapshot_file_name": snapshot_file_name,
            "snapshot_sha256": snapshot_sha256,
            "snapshot_byte_size": snapshot_byte_size,
            "snapshot_storage_object_id": snapshot_storage_object_id,
            "snapshot_object_key": snapshot_object_key,
            "exported_at": snapshot.exported_at,
            "app_name": snapshot.app_name,
            "app_version": snapshot.app_version,
            "database_backend": snapshot.database_backend,
            "spatial_backend": snapshot.spatial_backend,
            "database_revision": snapshot.database_revision,
            "database_head_revision": snapshot.database_head_revision,
            "section_counts": build_runtime_snapshot_section_counts(snapshot),
            "row_counts": snapshot.row_counts,
        }
    ).model_dump(mode="python")


def default_runtime_snapshot_export_dir() -> Path:
    settings = get_settings()
    return (settings.data_dir_effective / "backups" / "runtime-snapshots").resolve()


def build_runtime_snapshot_export_paths(
    *,
    root_dir: Path | None = None,
    file_prefix: str = "runtime-snapshot",
    exported_at: datetime | None = None,
) -> tuple[Path, Path]:
    normalized_prefix = file_prefix.strip() or "runtime-snapshot"
    timestamp = (exported_at or snapshot_now()).strftime("%Y%m%dT%H%M%SZ")
    export_root = (root_dir or default_runtime_snapshot_export_dir()).expanduser().resolve()
    snapshot_path = export_root / f"{normalized_prefix}-{timestamp}.json"
    manifest_path = export_root / f"{normalized_prefix}-{timestamp}.manifest.json"
    return snapshot_path, manifest_path


def export_runtime_snapshot_artifacts(
    session: Session,
    *,
    output_path: Path,
    manifest_path: Path | None = None,
    actor: str = "runtime_snapshot_export",
) -> dict[str, object]:
    resolved_output_path = output_path.expanduser().resolve()
    resolved_manifest_path = (
        manifest_path.expanduser().resolve()
        if manifest_path is not None
        else resolved_output_path.with_name(f"{resolved_output_path.stem}.manifest.json")
    )
    snapshot = build_runtime_snapshot(session)
    serializable = TypeAdapter(RuntimeSnapshotRead).validate_python(snapshot).model_dump(mode="json")
    snapshot_record = write_json_export_artifact(
        session,
        payload=serializable,
        object_kind="runtime_snapshot_export",
        owner_type="runtime_snapshot",
        owner_id=serializable["exported_at"],
        output_path=resolved_output_path,
        source_uri="/api/operations/runtime/export",
        observed_at=snapshot["exported_at"],
        metadata_json={
            "database_backend": serializable["database_backend"],
            "spatial_backend": serializable["spatial_backend"],
        },
        actor=actor,
    )
    manifest_payload = build_runtime_snapshot_artifact_manifest(
        snapshot,
        snapshot_file_name=resolved_output_path.name,
        snapshot_sha256=hash_file(resolved_output_path),
        snapshot_byte_size=resolved_output_path.stat().st_size,
        snapshot_storage_object_id=snapshot_record.storage_object_id,
        snapshot_object_key=snapshot_record.object_key,
    )
    manifest_serializable = TypeAdapter(RuntimeSnapshotArtifactManifestRead).validate_python(
        manifest_payload
    ).model_dump(mode="json")
    manifest_record = write_json_export_artifact(
        session,
        payload=manifest_serializable,
        object_kind="runtime_snapshot_manifest_export",
        owner_type="runtime_snapshot",
        owner_id=serializable["exported_at"],
        output_path=resolved_manifest_path,
        source_uri="/api/operations/runtime/export#manifest",
        observed_at=snapshot["exported_at"],
        metadata_json={
            "snapshot_storage_object_id": snapshot_record.storage_object_id,
            "snapshot_object_key": snapshot_record.object_key,
            "snapshot_file_name": manifest_serializable["snapshot_file_name"],
            "snapshot_sha256": manifest_serializable["snapshot_sha256"],
            "snapshot_byte_size": manifest_serializable["snapshot_byte_size"],
            "database_backend": manifest_serializable["database_backend"],
            "spatial_backend": manifest_serializable["spatial_backend"],
        },
        actor=actor,
    )
    return {
        "snapshot": serializable,
        "manifest": manifest_serializable,
        "snapshot_record": snapshot_record,
        "manifest_record": manifest_record,
        "output_path": resolved_output_path,
        "manifest_path": resolved_manifest_path,
    }


def restore_runtime_snapshot(
    session: Session,
    snapshot_payload: dict[str, object],
    *,
    replace_existing: bool = False,
) -> dict[str, object]:
    snapshot = TypeAdapter(RuntimeSnapshotRead).validate_python(snapshot_payload)
    if runtime_has_records(session):
        if not replace_existing:
            raise ValueError("Runtime restore requires an empty database unless replace_existing is enabled.")
        clear_runtime_tables(session)

    for section_name, model in RESTORE_ORDER:
        rows = [row.model_dump(mode="python") for row in getattr(snapshot, section_name)]
        if not rows:
            continue
        session.execute(insert(model), rows)

    restored_at = snapshot_now()
    session.flush()
    session.add(
        CustodyLogORM(
            object_type="runtime_snapshot",
            object_id=restored_at.isoformat(),
            action="runtime_restored",
            actor="runtime_restore",
            details_json={
                "snapshot_exported_at": snapshot.exported_at.isoformat(),
                "replaced_existing": replace_existing,
                "restored_total_records": sum(len(getattr(snapshot, section_name)) for section_name, _ in RESTORE_ORDER),
            },
        )
    )
    session.commit()

    row_counts = collect_table_counts(session)
    return TypeAdapter(RuntimeRestoreResultRead).validate_python(
        {
            "restored_at": restored_at,
            "database_backend": session.get_bind().dialect.name,
            "replaced_existing": replace_existing,
            "total_records": sum(item["row_count"] for item in row_counts),
            "row_counts": row_counts,
        }
    ).model_dump(mode="python")


def ensure_runtime_snapshot_model(snapshot_payload: RuntimeSnapshotRead | dict[str, object]) -> RuntimeSnapshotRead:
    if isinstance(snapshot_payload, RuntimeSnapshotRead):
        return snapshot_payload
    return TypeAdapter(RuntimeSnapshotRead).validate_python(snapshot_payload)


def serialize_snapshot_sections(session: Session) -> dict[str, list[dict[str, object]]]:
    payload: dict[str, list[dict[str, object]]] = {}
    for section_name, model, schema_cls, order_column in SNAPSHOT_SECTIONS:
        rows = list(session.scalars(select(model).order_by(order_column.asc())))
        payload[section_name] = [serialize_row(row, schema_cls) for row in rows]
    return payload


def serialize_row(row: object, schema_cls: type[BaseModel]) -> dict[str, object]:
    return schema_cls.model_validate(row).model_dump(mode="python")


def runtime_has_records(session: Session) -> bool:
    return any(
        int(session.scalar(select(func.count()).select_from(model)) or 0) > 0
        for _, model in RESTORE_ORDER
    )


def clear_runtime_tables(session: Session) -> None:
    for _, model in reversed(RESTORE_ORDER):
        session.execute(delete(model))
    session.flush()


def log_runtime_snapshot_export(
    session: Session,
    *,
    row_counts: list[dict[str, object]],
) -> CustodyLogORM:
    normalized_row_counts = TypeAdapter(list[DatabaseTableCountRead]).validate_python(row_counts)
    record = CustodyLogORM(
        object_type="runtime_snapshot",
        object_id=snapshot_now().isoformat(),
        action="runtime_exported",
        actor="runtime_export",
        details_json={
            "row_counts": [row.model_dump(mode="json") for row in normalized_row_counts],
        },
    )
    session.add(record)
    session.flush()
    return record
