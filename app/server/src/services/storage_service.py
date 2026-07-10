from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

<<<<<<< HEAD
from src.models import CameraInventoryORM, CustodyLogORM, LocalImportRunORM, StorageObjectORM
from src.schemas import (
    StorageObjectCreate,
    StorageObjectPromoteRequest,
    StorageObjectTransitionRequest,
=======
from src.models import (
    CameraInventoryORM,
    CustodyLogORM,
    LocalImportRunORM,
    SourceDefinitionORM,
    SourceRunORM,
    StorageObjectORM,
)
from src.schemas import (
    StorageLifecycleSweepResultRead,
    StorageObjectCreate,
    StorageObjectPromoteRequest,
    StorageObjectTransitionRequest,
    StorageReportRead,
>>>>>>> 05aeee6 (chore: initialize repository)
)

RETENTION_WINDOWS_HOURS: dict[str, float | None] = {
    "ephemeral": 24.0,
    "operational": 24.0 * 7,
    "investigative": 24.0 * 30,
    "permanent": None,
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def list_storage_objects(
    session: Session,
    *,
    owner_type: str | None = None,
    owner_id: str | None = None,
    object_kind: str | None = None,
    lifecycle_status: str | None = None,
    retention_class: str | None = None,
    limit: int = 200,
) -> list[StorageObjectORM]:
    statement = select(StorageObjectORM).order_by(
        StorageObjectORM.observed_at.desc().nullslast(),
        StorageObjectORM.updated_at.desc(),
    )
    if owner_type:
        statement = statement.where(StorageObjectORM.owner_type == owner_type)
    if owner_id:
        statement = statement.where(StorageObjectORM.owner_id == owner_id)
    if object_kind:
        statement = statement.where(StorageObjectORM.object_kind == object_kind)
    if lifecycle_status:
        statement = statement.where(StorageObjectORM.lifecycle_status == lifecycle_status)
    if retention_class:
        statement = statement.where(StorageObjectORM.retention_class == retention_class)
    return list(session.scalars(statement.limit(limit)))


def create_storage_object(
    session: Session,
    payload: StorageObjectCreate,
    *,
    actor: str = "api_storage",
) -> StorageObjectORM:
    record = register_storage_object(
        session,
        object_key=payload.object_key,
        object_kind=payload.object_kind,
        owner_type=payload.owner_type,
        owner_id=payload.owner_id,
        object_uri=payload.object_uri,
        content_hash=payload.content_hash,
        media_type=payload.media_type,
        storage_tier=payload.storage_tier,
        retention_class=payload.retention_class,
        lifecycle_status=payload.lifecycle_status,
        source_uri=payload.source_uri,
        byte_size=payload.byte_size,
        observed_at=payload.observed_at,
        expires_at=payload.expires_at,
        degraded_from_storage_object_id=payload.degraded_from_storage_object_id,
        metadata_json=payload.metadata_json,
        actor=actor,
    )
    session.commit()
    session.refresh(record)
    return record


<<<<<<< HEAD
=======
def build_storage_report(
    session: Session,
    *,
    limit: int = 25,
    reference: datetime | None = None,
) -> StorageReportRead:
    now = normalize_timestamp(reference) or utcnow()
    rows = list(
        session.scalars(
            select(StorageObjectORM).order_by(
                StorageObjectORM.expires_at.asc().nullslast(),
                StorageObjectORM.updated_at.desc(),
            )
        )
    )
    return StorageReportRead.model_validate(
        {
            "generated_at": now,
            "total_count": len(rows),
            "active_count": sum(1 for row in rows if not is_storage_object_expired(row, now)),
            "expired_count": sum(1 for row in rows if is_storage_object_expired(row, now)),
            "promoted_count": sum(1 for row in rows if row.lifecycle_status == "promoted"),
            "archived_count": sum(1 for row in rows if row.lifecycle_status == "archived"),
            "next_expiration_at": first_timestamp(
                normalize_timestamp(row.expires_at)
                for row in rows
                if row.lifecycle_status != "expired" and normalize_timestamp(row.expires_at) is not None
            ),
            "oldest_expired_at": first_timestamp(
                normalize_timestamp(row.expires_at)
                for row in rows
                if is_storage_object_expired(row, now) and normalize_timestamp(row.expires_at) is not None
            ),
            "retention_class_counts": build_storage_buckets(
                rows,
                key_name="retention_class",
                reference=now,
            ),
            "storage_tier_counts": build_storage_buckets(
                rows,
                key_name="storage_tier",
                reference=now,
            ),
            "lifecycle_status_counts": build_storage_buckets(
                rows,
                key_name="lifecycle_status",
                reference=now,
            ),
            "expiring_objects": [
                row
                for row in rows
                if row.lifecycle_status != "expired" and normalize_timestamp(row.expires_at) is not None
            ][:limit],
        }
    )


def sweep_expired_storage_objects(
    session: Session,
    *,
    retention_class: str | None = None,
    limit: int = 100,
    dry_run: bool = False,
    actor: str = "storage_lifecycle",
    reference: datetime | None = None,
) -> StorageLifecycleSweepResultRead:
    now = normalize_timestamp(reference) or utcnow()
    candidates = query_expired_storage_candidates(
        session,
        retention_class=retention_class,
        limit=limit,
        reference=now,
    )
    transitioned_count = 0
    if not dry_run:
        for record in candidates:
            apply_storage_transition(
                session,
                record,
                lifecycle_status="expired",
                expires_at=normalize_timestamp(record.expires_at) or now,
                metadata_json={"expired_by": actor, "expired_at": now.isoformat()},
                actor=actor,
                action="storage_expired",
                extra_details={
                    "reason": "retention_window_elapsed",
                    "reference": now.isoformat(),
                },
            )
            transitioned_count += 1
        session.commit()
        for record in candidates:
            session.refresh(record)
    return StorageLifecycleSweepResultRead.model_validate(
        {
            "swept_at": now,
            "dry_run": dry_run,
            "filters_json": {
                "retention_class": retention_class,
                "limit": limit,
            },
            "expired_candidate_count": len(candidates),
            "transitioned_count": transitioned_count,
            "candidates": candidates,
        }
    )


>>>>>>> 05aeee6 (chore: initialize repository)
def promote_storage_object(
    session: Session,
    storage_object_id: int,
    payload: StorageObjectPromoteRequest,
    *,
    actor: str = "api_storage",
) -> StorageObjectORM:
    record = session.get(StorageObjectORM, storage_object_id)
    if record is None:
        raise ValueError(f"Storage object {storage_object_id} does not exist.")
<<<<<<< HEAD

    previous = {
        "storage_tier": record.storage_tier,
        "retention_class": record.retention_class,
        "lifecycle_status": record.lifecycle_status,
        "expires_at": normalize_timestamp(record.expires_at),
    }
    record.storage_tier = payload.storage_tier
    if payload.retention_class is not None:
        record.retention_class = payload.retention_class
    record.promoted_by_type = payload.promoted_by_type
    record.promoted_by_id = payload.promoted_by_id
    record.lifecycle_status = "promoted"
    record.expires_at = resolve_expiration(
        record.retention_class,
        observed_at=record.observed_at,
        explicit_expires_at=payload.expires_at,
    )
    if payload.metadata_json:
        record.metadata_json = merge_metadata(record.metadata_json, payload.metadata_json)
    session.add(
        CustodyLogORM(
            object_type="storage_object",
            object_id=str(record.storage_object_id),
            action="storage_promoted",
            actor=actor,
            details_json={
                "object_key": record.object_key,
                "previous": serialize_storage_values(previous),
                "current": {
                    "storage_tier": record.storage_tier,
                    "retention_class": record.retention_class,
                    "lifecycle_status": record.lifecycle_status,
                    "expires_at": normalize_timestamp(record.expires_at).isoformat()
                    if normalize_timestamp(record.expires_at)
                    else None,
                    "promoted_by_type": record.promoted_by_type,
                    "promoted_by_id": record.promoted_by_id,
                },
            },
        )
    )
=======
    apply_storage_promotion(session, record, payload, actor=actor)
>>>>>>> 05aeee6 (chore: initialize repository)
    session.commit()
    session.refresh(record)
    return record


def transition_storage_object(
    session: Session,
    storage_object_id: int,
    payload: StorageObjectTransitionRequest,
    *,
    actor: str = "api_storage",
) -> StorageObjectORM:
    record = session.get(StorageObjectORM, storage_object_id)
    if record is None:
        raise ValueError(f"Storage object {storage_object_id} does not exist.")
<<<<<<< HEAD

    previous = {
        "storage_tier": record.storage_tier,
        "lifecycle_status": record.lifecycle_status,
        "expires_at": normalize_timestamp(record.expires_at),
    }
    record.lifecycle_status = payload.lifecycle_status
    if payload.storage_tier is not None:
        record.storage_tier = payload.storage_tier
    if payload.expires_at is not None or payload.lifecycle_status == "expired":
        record.expires_at = payload.expires_at or utcnow()
    if payload.metadata_json:
        record.metadata_json = merge_metadata(record.metadata_json, payload.metadata_json)
    session.add(
        CustodyLogORM(
            object_type="storage_object",
            object_id=str(record.storage_object_id),
            action="storage_transitioned",
            actor=actor,
            details_json={
                "object_key": record.object_key,
                "previous": serialize_storage_values(previous),
                "current": {
                    "storage_tier": record.storage_tier,
                    "lifecycle_status": record.lifecycle_status,
                    "expires_at": normalize_timestamp(record.expires_at).isoformat()
                    if normalize_timestamp(record.expires_at)
                    else None,
                },
            },
        )
=======
    apply_storage_transition(
        session,
        record,
        lifecycle_status=payload.lifecycle_status,
        storage_tier=payload.storage_tier,
        expires_at=payload.expires_at,
        metadata_json=payload.metadata_json,
        actor=actor,
>>>>>>> 05aeee6 (chore: initialize repository)
    )
    session.commit()
    session.refresh(record)
    return record


def register_storage_object(
    session: Session,
    *,
    object_key: str,
    object_kind: str,
    owner_type: str,
    owner_id: str,
    object_uri: str,
    content_hash: str | None = None,
    media_type: str | None = None,
    storage_tier: str = "hot",
    retention_class: str = "operational",
    lifecycle_status: str = "active",
    source_uri: str | None = None,
    byte_size: int | None = None,
    observed_at: datetime | None = None,
    expires_at: datetime | None = None,
    degraded_from_storage_object_id: int | None = None,
    metadata_json: dict[str, Any] | None = None,
    actor: str = "system",
) -> StorageObjectORM:
    record = session.scalar(select(StorageObjectORM).where(StorageObjectORM.object_key == object_key))
    normalized_observed_at = normalize_timestamp(observed_at) or utcnow()
    resolved_expires_at = resolve_expiration(
        retention_class,
        observed_at=normalized_observed_at,
        explicit_expires_at=expires_at,
    )
    incoming_metadata = metadata_json or {}
    action = "storage_registered"
    details: dict[str, Any] = {
        "object_key": object_key,
        "object_kind": object_kind,
        "owner_type": owner_type,
        "owner_id": owner_id,
    }

    if record is None:
        record = StorageObjectORM(
            object_key=object_key,
            object_kind=object_kind,
            owner_type=owner_type,
            owner_id=owner_id,
            content_hash=content_hash,
            media_type=media_type,
            storage_tier=storage_tier,
            retention_class=retention_class,
            lifecycle_status=lifecycle_status,
            source_uri=source_uri,
            object_uri=object_uri,
            byte_size=byte_size,
            observed_at=normalized_observed_at,
            expires_at=resolved_expires_at,
            degraded_from_storage_object_id=degraded_from_storage_object_id,
            metadata_json=incoming_metadata,
        )
        session.add(record)
        session.flush()
    else:
        changed_fields: dict[str, dict[str, Any]] = {}
        field_mapping = {
            "object_kind": object_kind,
            "owner_type": owner_type,
            "owner_id": owner_id,
            "content_hash": content_hash,
            "media_type": media_type,
            "storage_tier": storage_tier,
            "retention_class": retention_class,
            "lifecycle_status": lifecycle_status,
            "source_uri": source_uri,
            "object_uri": object_uri,
            "byte_size": byte_size,
            "observed_at": normalized_observed_at,
            "expires_at": resolved_expires_at,
            "degraded_from_storage_object_id": degraded_from_storage_object_id,
        }
        for field_name, new_value in field_mapping.items():
            old_value = getattr(record, field_name)
            if values_equal(old_value, new_value):
                continue
            setattr(record, field_name, new_value)
            changed_fields[field_name] = {"old": old_value, "new": new_value}
        merged_metadata = merge_metadata(record.metadata_json, incoming_metadata)
        if merged_metadata != record.metadata_json:
            changed_fields["metadata_json"] = {"old": record.metadata_json, "new": merged_metadata}
            record.metadata_json = merged_metadata
        action = "storage_refreshed"
        details["changes"] = serialize_storage_values(changed_fields)

    session.add(
        CustodyLogORM(
            object_type="storage_object",
            object_id=str(record.storage_object_id),
            action=action,
            actor=actor,
            details_json=details,
        )
    )
    session.flush()
    return record


def register_import_storage_object(
    session: Session,
    run: LocalImportRunORM,
    *,
    actor: str,
) -> StorageObjectORM:
    path = Path(run.source_path)
    content_hash = hash_file(path) if path.exists() and path.is_file() else hash_text(run.source_path)
    byte_size = path.stat().st_size if path.exists() and path.is_file() else None
    return register_storage_object(
        session,
        object_key=f"local_import_run:{run.import_run_id}:source",
        object_kind="local_import_source",
        owner_type="local_import_run",
        owner_id=str(run.import_run_id),
        object_uri=run.source_path,
        source_uri=run.source_path,
        content_hash=content_hash,
        media_type=media_type_for_import_format(run.source_format),
        storage_tier="warm",
        retention_class="investigative",
        lifecycle_status="active",
        byte_size=byte_size,
        observed_at=run.created_at,
        metadata_json={
            "layer_key": run.layer_key,
            "source_format": run.source_format,
            "records_seen": run.records_seen,
            "records_imported": run.records_imported,
            "records_skipped": run.records_skipped,
        },
        actor=actor,
    )


def register_camera_storage_objects(
    session: Session,
    camera: CameraInventoryORM,
    *,
    actor: str,
) -> list[StorageObjectORM]:
    urls = [
        ("camera_image_ref", camera.image_url, "image/jpeg"),
        ("camera_stream_ref", camera.stream_url, "application/x-mpegURL"),
        ("camera_page_ref", camera.page_url, "text/html"),
    ]
    registered: list[StorageObjectORM] = []
    for object_kind, uri, media_type in urls:
        if not uri:
            continue
        registered.append(
            register_storage_object(
                session,
                object_key=build_camera_storage_key(camera.camera_inventory_id, object_kind, uri),
                object_kind=object_kind,
                owner_type="camera_inventory",
                owner_id=str(camera.camera_inventory_id),
                object_uri=uri,
                source_uri=uri,
                content_hash=hash_text(uri),
                media_type=media_type,
                storage_tier="hot",
                retention_class="operational",
                lifecycle_status="active",
                observed_at=camera.last_observed_at or camera.updated_at,
                metadata_json={
                    "camera_key": camera.camera_key,
                    "external_id": camera.external_id,
                    "layer_key": camera.layer_key,
                    "source_domain": camera.source_domain,
                    "provider": camera.provider,
                    "status": camera.status,
                    "active": camera.active,
                },
                actor=actor,
            )
        )
    return registered


<<<<<<< HEAD
=======
def register_source_run_storage_object(
    session: Session,
    source: SourceDefinitionORM,
    run: SourceRunORM,
    materialized_path: str,
    materialization_metadata: dict[str, Any],
    *,
    import_run_id: int | None = None,
    run_status: str,
    actor: str,
) -> StorageObjectORM:
    materialization_kind = str(materialization_metadata.get("materialization_kind") or "unknown")
    byte_size = (
        int(materialization_metadata["byte_count"])
        if isinstance(materialization_metadata.get("byte_count"), (int, float))
        else None
    )
    if materialization_kind == "local_file":
        object_kind = "source_local_payload"
    else:
        object_kind = "source_cached_payload"
    metadata_json = {
        "source_id": source.source_id,
        "source_name": source.name,
        "source_kind": source.source_kind,
        "layer_key": source.layer_key,
        "materialization_kind": materialization_kind,
        "run_status": run_status,
        **materialization_metadata,
    }
    if import_run_id is not None:
        metadata_json["import_run_id"] = import_run_id
    return register_storage_object(
        session,
        object_key=f"source_run:{run.source_run_id}:payload",
        object_kind=object_kind,
        owner_type="source_run",
        owner_id=str(run.source_run_id),
        object_uri=materialized_path,
        source_uri=source.target_uri,
        content_hash=as_optional_string(materialization_metadata.get("payload_sha256")),
        media_type=resolve_source_materialization_media_type(source, materialization_metadata),
        storage_tier="warm",
        retention_class="investigative",
        lifecycle_status="active",
        byte_size=byte_size,
        observed_at=run.started_at,
        metadata_json=metadata_json,
        actor=actor,
    )


def register_export_storage_object(
    session: Session,
    *,
    object_kind: str,
    owner_type: str,
    owner_id: str,
    output_path: str | Path,
    source_uri: str | None = None,
    storage_tier: str = "warm",
    retention_class: str = "investigative",
    observed_at: datetime | None = None,
    metadata_json: dict[str, Any] | None = None,
    actor: str = "cli_export",
) -> StorageObjectORM:
    resolved_path = Path(output_path).expanduser().resolve()
    if not resolved_path.exists() or not resolved_path.is_file():
        raise ValueError(f"Export artifact '{resolved_path}' does not exist.")
    return register_storage_object(
        session,
        object_key=build_export_storage_key(owner_type, owner_id, object_kind, resolved_path),
        object_kind=object_kind,
        owner_type=owner_type,
        owner_id=owner_id,
        object_uri=str(resolved_path),
        source_uri=source_uri,
        content_hash=hash_file(resolved_path),
        media_type=media_type_for_export_path(resolved_path),
        storage_tier=storage_tier,
        retention_class=retention_class,
        lifecycle_status="active",
        byte_size=resolved_path.stat().st_size,
        observed_at=observed_at,
        metadata_json={
            "file_name": resolved_path.name,
            "suffix": resolved_path.suffix.lower(),
            **(metadata_json or {}),
        },
        actor=actor,
    )


def query_expired_storage_candidates(
    session: Session,
    *,
    retention_class: str | None = None,
    limit: int = 100,
    reference: datetime | None = None,
) -> list[StorageObjectORM]:
    now = normalize_timestamp(reference) or utcnow()
    statement = (
        select(StorageObjectORM)
        .where(
            StorageObjectORM.lifecycle_status != "expired",
            StorageObjectORM.expires_at.is_not(None),
            StorageObjectORM.expires_at <= now,
        )
        .order_by(
            StorageObjectORM.expires_at.asc(),
            StorageObjectORM.storage_object_id.asc(),
        )
        .limit(limit)
    )
    if retention_class:
        statement = statement.where(StorageObjectORM.retention_class == retention_class)
    return list(session.scalars(statement))


def apply_storage_promotion(
    session: Session,
    record: StorageObjectORM,
    payload: StorageObjectPromoteRequest,
    *,
    actor: str,
) -> StorageObjectORM:
    previous = {
        "storage_tier": record.storage_tier,
        "retention_class": record.retention_class,
        "lifecycle_status": record.lifecycle_status,
        "expires_at": normalize_timestamp(record.expires_at),
    }
    record.storage_tier = payload.storage_tier
    if payload.retention_class is not None:
        record.retention_class = payload.retention_class
    record.promoted_by_type = payload.promoted_by_type
    record.promoted_by_id = payload.promoted_by_id
    record.lifecycle_status = "promoted"
    record.expires_at = resolve_expiration(
        record.retention_class,
        observed_at=record.observed_at,
        explicit_expires_at=payload.expires_at,
    )
    if payload.metadata_json:
        record.metadata_json = merge_metadata(record.metadata_json, payload.metadata_json)
    session.add(
        CustodyLogORM(
            object_type="storage_object",
            object_id=str(record.storage_object_id),
            action="storage_promoted",
            actor=actor,
            details_json={
                "object_key": record.object_key,
                "previous": serialize_storage_values(previous),
                "current": {
                    "storage_tier": record.storage_tier,
                    "retention_class": record.retention_class,
                    "lifecycle_status": record.lifecycle_status,
                    "expires_at": normalize_timestamp(record.expires_at).isoformat()
                    if normalize_timestamp(record.expires_at)
                    else None,
                    "promoted_by_type": record.promoted_by_type,
                    "promoted_by_id": record.promoted_by_id,
                },
            },
        )
    )
    return record


def apply_storage_transition(
    session: Session,
    record: StorageObjectORM,
    *,
    lifecycle_status: str,
    storage_tier: str | None = None,
    expires_at: datetime | None = None,
    metadata_json: dict[str, Any] | None = None,
    actor: str,
    action: str = "storage_transitioned",
    extra_details: dict[str, Any] | None = None,
) -> StorageObjectORM:
    previous = {
        "storage_tier": record.storage_tier,
        "lifecycle_status": record.lifecycle_status,
        "expires_at": normalize_timestamp(record.expires_at),
    }
    record.lifecycle_status = lifecycle_status
    if storage_tier is not None:
        record.storage_tier = storage_tier
    if expires_at is not None or lifecycle_status == "expired":
        record.expires_at = normalize_timestamp(expires_at) or utcnow()
    if metadata_json:
        record.metadata_json = merge_metadata(record.metadata_json, metadata_json)
    details_json = {
        "object_key": record.object_key,
        "previous": serialize_storage_values(previous),
        "current": {
            "storage_tier": record.storage_tier,
            "lifecycle_status": record.lifecycle_status,
            "expires_at": normalize_timestamp(record.expires_at).isoformat()
            if normalize_timestamp(record.expires_at)
            else None,
        },
    }
    if extra_details:
        details_json.update(extra_details)
    session.add(
        CustodyLogORM(
            object_type="storage_object",
            object_id=str(record.storage_object_id),
            action=action,
            actor=actor,
            details_json=details_json,
        )
    )
    return record


>>>>>>> 05aeee6 (chore: initialize repository)
def resolve_expiration(
    retention_class: str,
    *,
    observed_at: datetime | None,
    explicit_expires_at: datetime | None,
) -> datetime | None:
    if explicit_expires_at is not None:
        return normalize_timestamp(explicit_expires_at)
    window_hours = RETENTION_WINDOWS_HOURS.get(retention_class)
    if window_hours is None:
        return None
    base_time = normalize_timestamp(observed_at) or utcnow()
    return base_time + timedelta(hours=window_hours)


def normalize_timestamp(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is not None:
        return value
    return value.replace(tzinfo=timezone.utc)


def media_type_for_import_format(source_format: str) -> str:
    return {
        "json": "application/json",
        "sqlite": "application/vnd.sqlite3",
        "txt": "text/plain",
    }.get(source_format, "application/octet-stream")


def build_camera_storage_key(camera_inventory_id: int, object_kind: str, uri: str) -> str:
    return f"camera_inventory:{camera_inventory_id}:{object_kind}:{hash_text(uri)[:16]}"


<<<<<<< HEAD
=======
def build_export_storage_key(
    owner_type: str,
    owner_id: str,
    object_kind: str,
    output_path: Path,
) -> str:
    normalized_path = str(output_path).replace("\\", "/")
    return (
        f"export:{owner_type}:{owner_id}:{object_kind}:"
        f"{hash_text(normalized_path)[:16]}"
    )


>>>>>>> 05aeee6 (chore: initialize repository)
def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


<<<<<<< HEAD
=======
def media_type_for_export_path(path: Path) -> str:
    return {
        ".json": "application/json",
        ".jsonl": "application/jsonl",
        ".txt": "text/plain",
        ".md": "text/markdown",
        ".csv": "text/csv",
        ".html": "text/html",
    }.get(path.suffix.lower(), "application/octet-stream")


>>>>>>> 05aeee6 (chore: initialize repository)
def merge_metadata(
    current: dict[str, Any] | None,
    incoming: dict[str, Any] | None,
) -> dict[str, Any]:
    merged = dict(current or {})
    merged.update(incoming or {})
    return merged


def values_equal(old_value: Any, new_value: Any) -> bool:
    if isinstance(old_value, datetime) or isinstance(new_value, datetime):
        return normalize_timestamp(old_value) == normalize_timestamp(new_value)
    return old_value == new_value


def serialize_storage_values(value: Any) -> Any:
    if isinstance(value, datetime):
        normalized = normalize_timestamp(value)
        return normalized.isoformat() if normalized is not None else None
    if isinstance(value, dict):
        return {key: serialize_storage_values(item) for key, item in value.items()}
    if isinstance(value, list):
        return [serialize_storage_values(item) for item in value]
    return value
<<<<<<< HEAD
=======


def is_storage_object_expired(record: StorageObjectORM, reference: datetime | None = None) -> bool:
    if record.lifecycle_status == "expired":
        return True
    expires_at = normalize_timestamp(record.expires_at)
    now = normalize_timestamp(reference) or utcnow()
    return expires_at is not None and expires_at <= now


def build_storage_buckets(
    rows: list[StorageObjectORM],
    *,
    key_name: str,
    reference: datetime,
) -> list[dict[str, int | str]]:
    grouped: dict[str, dict[str, int | str]] = {}
    for row in rows:
        key = str(getattr(row, key_name) or "unknown")
        bucket = grouped.setdefault(
            key,
            {
                "key": key,
                "total_count": 0,
                "expired_count": 0,
                "active_count": 0,
            },
        )
        bucket["total_count"] = int(bucket["total_count"]) + 1
        if is_storage_object_expired(row, reference):
            bucket["expired_count"] = int(bucket["expired_count"]) + 1
        else:
            bucket["active_count"] = int(bucket["active_count"]) + 1
    return [grouped[key] for key in sorted(grouped)]


def first_timestamp(values: Any) -> datetime | None:
    for value in values:
        if value is not None:
            return value
    return None


def resolve_source_materialization_media_type(
    source: SourceDefinitionORM,
    metadata: dict[str, Any],
) -> str:
    materialized_content_type = as_optional_string(metadata.get("materialized_content_type"))
    if materialized_content_type:
        return materialized_content_type
    content_type = as_optional_string(metadata.get("content_type"))
    if content_type:
        return content_type.split(";", 1)[0].strip()
    return {
        "local_file": "application/octet-stream",
        "http_json": "application/json",
        "http_text": "text/plain",
        "http_xml": "application/json",
    }.get(source.source_kind, "application/octet-stream")


def as_optional_string(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None
>>>>>>> 05aeee6 (chore: initialize repository)
