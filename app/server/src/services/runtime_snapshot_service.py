from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, TypeAdapter
from sqlalchemy import delete, func, insert, select
from sqlalchemy.orm import Session

from src.config import get_settings
from src.models import (
    AlertORM,
    CameraInventoryORM,
<<<<<<< HEAD
=======
    CameraSourceInventoryORM,
>>>>>>> 05aeee6 (chore: initialize repository)
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
    SourceDefinitionORM,
    SourceRunORM,
    SourceTrustProfileORM,
)
from src.schemas import (
    AlertRead,
    CameraInventoryRead,
<<<<<<< HEAD
=======
    CameraSourceInventoryRead,
>>>>>>> 05aeee6 (chore: initialize repository)
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
    RuntimeSnapshotRead,
    ScheduledTaskRead,
    ScheduledTaskRunRead,
    SituationProductRead,
    StorageObjectRead,
    SourceDefinitionRead,
    SourceRunRead,
    SourceTrustProfileRead,
)
from src.services.database_diagnostics_service import collect_table_counts


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
    ("local_import_runs", LocalImportRunORM, LocalImportRunSummaryRead, LocalImportRunORM.import_run_id),
    ("events", EventORM, EventRead, EventORM.event_id),
    ("entities", EntityORM, EntityRead, EntityORM.entity_id),
    ("observations", ObservationORM, ObservationRead, ObservationORM.observation_id),
    ("camera_inventory", CameraInventoryORM, CameraInventoryRead, CameraInventoryORM.camera_inventory_id),
<<<<<<< HEAD
=======
    (
        "camera_source_inventory",
        CameraSourceInventoryORM,
        CameraSourceInventoryRead,
        CameraSourceInventoryORM.camera_source_inventory_id,
    ),
>>>>>>> 05aeee6 (chore: initialize repository)
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
    ("situation_products", SituationProductORM, SituationProductRead, SituationProductORM.product_id),
    ("custody_logs", CustodyLogORM, CustodyLogRead, CustodyLogORM.custody_log_id),
)

RESTORE_ORDER: tuple[tuple[str, object], ...] = (
    ("data_layers", DataLayerORM),
    ("source_trust_profiles", SourceTrustProfileORM),
    ("geofences", GeofenceORM),
    ("source_definitions", SourceDefinitionORM),
    ("local_import_runs", LocalImportRunORM),
    ("events", EventORM),
    ("entities", EntityORM),
    ("observations", ObservationORM),
    ("camera_inventory", CameraInventoryORM),
<<<<<<< HEAD
=======
    ("camera_source_inventory", CameraSourceInventoryORM),
>>>>>>> 05aeee6 (chore: initialize repository)
    ("storage_objects", StorageObjectORM),
    ("event_observation_links", EventObservationLinkORM),
    ("entity_observation_links", EntityObservationLinkORM),
    ("alerts", AlertORM),
    ("scheduled_tasks", ScheduledTaskORM),
    ("source_runs", SourceRunORM),
    ("scheduled_task_runs", ScheduledTaskRunORM),
    ("situation_products", SituationProductORM),
    ("custody_logs", CustodyLogORM),
)


def build_runtime_snapshot(session: Session) -> dict[str, object]:
    settings = get_settings()
    row_counts_before = collect_table_counts(session)
    export_log = log_runtime_snapshot_export(session, row_counts=row_counts_before)
    session.commit()
    snapshot = {
        "exported_at": snapshot_now(),
        "app_name": settings.app_name,
        "app_version": settings.app_version,
        "database_backend": session.get_bind().dialect.name,
        "spatial_backend": settings.spatial_backend,
        "row_counts": collect_table_counts(session),
    }
    snapshot.update(serialize_snapshot_sections(session))
    if not any(log["custody_log_id"] == export_log.custody_log_id for log in snapshot["custody_logs"]):
        snapshot["custody_logs"].append(serialize_row(export_log, CustodyLogRead))
    return snapshot


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
