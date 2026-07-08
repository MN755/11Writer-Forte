from __future__ import annotations

import hashlib
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.config import get_settings
from src.models import (
    CameraInventoryORM,
    CustodyLogORM,
    LocalImportRunORM,
    SourceDefinitionORM,
    SourceRunORM,
    StorageObjectORM,
)
from src.schemas import (
    StorageActionResultRead,
    StorageLifecycleSweepResultRead,
    StorageManifestRead,
    StorageObjectCreate,
    StorageObjectPromoteRequest,
    StorageObjectTransitionRequest,
    StorageReportRead,
)
from src.services.storage_backends import (
    StorageTransferResult,
    build_archive_backend,
    build_local_backend,
    hash_file as backend_hash_file,
    local_path_from_uri,
    local_path_to_uri,
)

RETENTION_WINDOWS_HOURS: dict[str, float | None] = {
    "ephemeral": 24.0,
    "operational": 24.0 * 7,
    "investigative": 24.0 * 30,
    "permanent": None,
}

DEFAULT_LIFECYCLE_OPERATIONS: tuple[str, ...] = (
    "archive",
    "verify",
    "rehydrate",
    "prune",
    "expire",
)

MANAGED_STORAGE_OBJECT_KINDS = {
    "source_cached_payload",
    "event_bundle_export",
    "situation_product_export",
    "camera_summary_export",
    "camera_source_summary_export",
    "operations_report_export",
    "runtime_snapshot_export",
    "scheduler_summary_export",
    "source_summary_export",
}

NON_ARCHIVEABLE_OBJECT_KINDS = {
    "local_import_source",
    "source_local_payload",
    "camera_image_ref",
    "camera_stream_ref",
    "camera_page_ref",
}

TRANSFER_FAILURE_STATES = {"failed", "verification_failed", "quarantined"}


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
    manifests = {row.storage_object_id: get_storage_manifest(row) for row in rows}
    problem_objects = [
        row
        for row in rows
        if row.lifecycle_status == "quarantined"
        or str(manifests[row.storage_object_id]["transfer_status"]) in TRANSFER_FAILURE_STATES
    ][:limit]
    return StorageReportRead.model_validate(
        {
            "generated_at": now,
            "total_count": len(rows),
            "active_count": sum(1 for row in rows if not is_storage_object_expired(row, now)),
            "expired_count": sum(1 for row in rows if is_storage_object_expired(row, now)),
            "promoted_count": sum(1 for row in rows if row.lifecycle_status == "promoted"),
            "archived_count": sum(1 for row in rows if row.lifecycle_status == "archived"),
            "quarantined_count": sum(1 for row in rows if row.lifecycle_status == "quarantined"),
            "archive_pending_count": sum(
                1
                for row in rows
                if bool(manifests[row.storage_object_id]["archive_eligible"])
                and not has_verified_archive_replica(manifests[row.storage_object_id])
            ),
            "verification_failure_count": sum(
                1
                for row in rows
                if str(manifests[row.storage_object_id]["transfer_status"]) == "verification_failed"
            ),
            "rehydration_pending_count": sum(
                1
                for row in rows
                if str(manifests[row.storage_object_id]["transfer_status"]) == "rehydration_requested"
            ),
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
            "transfer_status_counts": build_manifest_status_buckets(
                rows,
                manifests=manifests,
                reference=now,
            ),
            "expiring_objects": [
                row
                for row in rows
                if row.lifecycle_status != "expired" and normalize_timestamp(row.expires_at) is not None
            ][:limit],
            "problem_objects": problem_objects,
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
    operations: list[str] | None = None,
) -> StorageLifecycleSweepResultRead:
    now = normalize_timestamp(reference) or utcnow()
    selected_operations = normalize_lifecycle_operations(operations)
    operation_results: list[dict[str, object]] = []
    candidate_rows: dict[int, StorageObjectORM] = {}

    for operation in selected_operations:
        if operation == "archive":
            candidates = query_archive_candidates(session, retention_class=retention_class, limit=limit)
        elif operation == "verify":
            candidates = query_verification_candidates(session, retention_class=retention_class, limit=limit)
        elif operation == "rehydrate":
            candidates = query_rehydration_request_candidates(
                session,
                retention_class=retention_class,
                limit=limit,
            )
        elif operation == "prune":
            candidates = query_prune_candidates(
                session,
                retention_class=retention_class,
                limit=limit,
                reference=now,
            )
        elif operation == "expire":
            candidates = query_expired_storage_candidates(
                session,
                retention_class=retention_class,
                limit=limit,
                reference=now,
            )
        else:
            raise ValueError(f"Unsupported lifecycle operation '{operation}'.")

        for row in candidates:
            candidate_rows[row.storage_object_id] = row

        processed_count = 0
        failed_count = 0
        object_ids: list[int] = []
        failures: list[str] = []

        if not dry_run:
            for row in candidates:
                try:
                    if operation == "archive":
                        archive_storage_object_record(session, row, actor=actor)
                    elif operation == "verify":
                        verify_storage_object_record(session, row, actor=actor)
                    elif operation == "rehydrate":
                        rehydrate_storage_object_record(session, row, actor=actor)
                    elif operation == "prune":
                        prune_storage_object_record(session, row, actor=actor, reference=now)
                    else:
                        apply_storage_transition(
                            session,
                            row,
                            lifecycle_status="expired",
                            expires_at=normalize_timestamp(row.expires_at) or now,
                            metadata_json={"expired_by": actor, "expired_at": now.isoformat()},
                            actor=actor,
                            action="storage_expired",
                            extra_details={
                                "reason": "retention_window_elapsed",
                                "reference": now.isoformat(),
                            },
                        )
                    processed_count += 1
                    object_ids.append(row.storage_object_id)
                except Exception as exc:
                    failed_count += 1
                    failures.append(f"{row.storage_object_id}: {exc}")
                    record_transfer_failure(
                        session,
                        row,
                        action=f"storage_{operation}_failed",
                        actor=actor,
                        reason=str(exc),
                        quarantine=True,
                    )
            session.commit()
            for row in candidates:
                session.refresh(row)

        operation_results.append(
            {
                "operation": operation,
                "candidate_count": len(candidates),
                "processed_count": processed_count,
                "failed_count": failed_count,
                "object_ids": object_ids,
                "failures": failures,
            }
        )

    candidates = list(candidate_rows.values())
    expired_candidates = [row for row in candidates if is_storage_object_expired(row, now)]
    return StorageLifecycleSweepResultRead.model_validate(
        {
            "swept_at": now,
            "dry_run": dry_run,
            "filters_json": {
                "retention_class": retention_class,
                "limit": limit,
                "operations": list(selected_operations),
            },
            "expired_candidate_count": len(expired_candidates) if "expire" in selected_operations else 0,
            "transitioned_count": sum(
                int(item["processed_count"])
                for item in operation_results
                if item["operation"] == "expire"
            ),
            "processed_count": sum(int(item["processed_count"]) for item in operation_results),
            "failed_count": sum(int(item["failed_count"]) for item in operation_results),
            "operation_results": operation_results,
            "candidates": candidates,
        }
    )


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
    apply_storage_promotion(session, record, payload, actor=actor)
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
    apply_storage_transition(
        session,
        record,
        lifecycle_status=payload.lifecycle_status,
        storage_tier=payload.storage_tier,
        expires_at=payload.expires_at,
        metadata_json=payload.metadata_json,
        actor=actor,
    )
    session.commit()
    session.refresh(record)
    return record


def archive_storage_object(
    session: Session,
    storage_object_id: int,
    *,
    actor: str = "api_storage",
    prune_local: bool | None = None,
) -> dict[str, object]:
    record = require_storage_object(session, storage_object_id)
    result = archive_storage_object_record(session, record, actor=actor, prune_local=prune_local)
    session.commit()
    session.refresh(record)
    return result


def verify_storage_object(
    session: Session,
    storage_object_id: int,
    *,
    actor: str = "api_storage",
) -> dict[str, object]:
    record = require_storage_object(session, storage_object_id)
    result = verify_storage_object_record(session, record, actor=actor)
    session.commit()
    session.refresh(record)
    return result


def request_storage_object_rehydration(
    session: Session,
    storage_object_id: int,
    *,
    actor: str = "api_storage",
) -> dict[str, object]:
    record = require_storage_object(session, storage_object_id)
    manifest = get_storage_manifest(record)
    now = utcnow()
    manifest["transfer_status"] = "rehydration_requested"
    manifest["rehydration_requested_at"] = now.isoformat()
    manifest["failure_reason"] = None
    save_storage_manifest(record, manifest)
    session.add(
        CustodyLogORM(
            object_type="storage_object",
            object_id=str(record.storage_object_id),
            action="storage_rehydration_requested",
            actor=actor,
            details_json={
                "object_key": record.object_key,
                "canonical_uri": manifest["canonical_uri"],
            },
        )
    )
    session.commit()
    session.refresh(record)
    return build_storage_action_result(
        action="request_rehydrate",
        message="Storage object queued for rehydration.",
        verified=True,
        storage_object=record,
    )


def rehydrate_storage_object(
    session: Session,
    storage_object_id: int,
    *,
    target_path: str | Path | None = None,
    replace_existing: bool = False,
    actor: str = "api_storage",
) -> dict[str, object]:
    record = require_storage_object(session, storage_object_id)
    result = rehydrate_storage_object_record(
        session,
        record,
        target_path=target_path,
        replace_existing=replace_existing,
        actor=actor,
    )
    session.commit()
    session.refresh(record)
    return result


def prune_storage_object(
    session: Session,
    storage_object_id: int,
    *,
    actor: str = "api_storage",
    reference: datetime | None = None,
) -> dict[str, object]:
    record = require_storage_object(session, storage_object_id)
    result = prune_storage_object_record(session, record, actor=actor, reference=reference)
    session.commit()
    session.refresh(record)
    return result


def quarantine_storage_object(
    session: Session,
    storage_object_id: int,
    *,
    reason: str,
    actor: str = "api_storage",
) -> dict[str, object]:
    record = require_storage_object(session, storage_object_id)
    manifest = get_storage_manifest(record)
    now = utcnow()
    manifest["transfer_status"] = "quarantined"
    manifest["failure_reason"] = reason
    manifest["quarantined_at"] = now.isoformat()
    save_storage_manifest(record, manifest)
    record.lifecycle_status = "quarantined"
    session.add(
        CustodyLogORM(
            object_type="storage_object",
            object_id=str(record.storage_object_id),
            action="storage_quarantined",
            actor=actor,
            details_json={
                "object_key": record.object_key,
                "reason": reason,
            },
        )
    )
    session.commit()
    session.refresh(record)
    return build_storage_action_result(
        action="quarantine",
        message="Storage object quarantined.",
        verified=True,
        storage_object=record,
    )


def unquarantine_storage_object(
    session: Session,
    storage_object_id: int,
    *,
    note: str | None = None,
    actor: str = "api_storage",
) -> dict[str, object]:
    record = require_storage_object(session, storage_object_id)
    manifest = get_storage_manifest(record)
    manifest["transfer_status"] = "ready" if manifest["archive_eligible"] else "verified"
    manifest["failure_reason"] = None
    manifest["quarantined_at"] = None
    save_storage_manifest(record, manifest)
    if has_failed_replica(manifest):
        record.lifecycle_status = "degraded"
    elif has_verified_archive_replica(manifest):
        record.lifecycle_status = "archived"
    else:
        record.lifecycle_status = "active"
    session.add(
        CustodyLogORM(
            object_type="storage_object",
            object_id=str(record.storage_object_id),
            action="storage_unquarantined",
            actor=actor,
            details_json={
                "object_key": record.object_key,
                "note": note,
            },
        )
    )
    session.commit()
    session.refresh(record)
    return build_storage_action_result(
        action="unquarantine",
        message="Storage object removed from quarantine.",
        verified=True,
        storage_object=record,
    )


def get_storage_manifest_for_object(
    session: Session,
    storage_object_id: int,
) -> dict[str, object]:
    record = require_storage_object(session, storage_object_id)
    return StorageManifestRead.model_validate(get_storage_manifest(record)).model_dump(mode="python")


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

    ensure_storage_manifest(record)
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
            "storage_managed": False,
            "archive_eligible": False,
            "prune_eligible": False,
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
                    "storage_managed": False,
                    "archive_eligible": False,
                    "prune_eligible": False,
                },
                actor=actor,
            )
        )
    return registered


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
        "storage_managed": object_kind == "source_cached_payload",
        "archive_eligible": object_kind == "source_cached_payload",
        "prune_eligible": object_kind == "source_cached_payload",
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
    prune_eligible = is_safe_managed_local_path(resolved_path)
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
            "storage_managed": True,
            "archive_eligible": True,
            "prune_eligible": prune_eligible,
            **(metadata_json or {}),
        },
        actor=actor,
    )


def query_archive_candidates(
    session: Session,
    *,
    retention_class: str | None = None,
    limit: int = 100,
) -> list[StorageObjectORM]:
    rows = list_storage_objects(session, retention_class=retention_class, limit=2000)
    return [
        row
        for row in rows
        if is_archive_candidate(row)
    ][:limit]


def query_verification_candidates(
    session: Session,
    *,
    retention_class: str | None = None,
    limit: int = 100,
) -> list[StorageObjectORM]:
    rows = list_storage_objects(session, retention_class=retention_class, limit=2000)
    return [
        row
        for row in rows
        if has_archive_replica(get_storage_manifest(row))
    ][:limit]


def query_rehydration_request_candidates(
    session: Session,
    *,
    retention_class: str | None = None,
    limit: int = 100,
) -> list[StorageObjectORM]:
    rows = list_storage_objects(session, retention_class=retention_class, limit=2000)
    return [
        row
        for row in rows
        if str(get_storage_manifest(row)["transfer_status"]) == "rehydration_requested"
    ][:limit]


def query_prune_candidates(
    session: Session,
    *,
    retention_class: str | None = None,
    limit: int = 100,
    reference: datetime | None = None,
) -> list[StorageObjectORM]:
    now = normalize_timestamp(reference) or utcnow()
    rows = list_storage_objects(session, retention_class=retention_class, limit=2000)
    return [
        row
        for row in rows
        if is_prune_candidate(row, reference=now)
    ][:limit]


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
    ensure_storage_manifest(record)
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
    ensure_storage_manifest(record)
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


def hash_file(path: Path) -> str:
    return backend_hash_file(path)


def hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def media_type_for_export_path(path: Path) -> str:
    return {
        ".json": "application/json",
        ".jsonl": "application/jsonl",
        ".txt": "text/plain",
        ".md": "text/markdown",
        ".csv": "text/csv",
        ".html": "text/html",
    }.get(path.suffix.lower(), "application/octet-stream")


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


def build_manifest_status_buckets(
    rows: list[StorageObjectORM],
    *,
    manifests: dict[int, dict[str, Any]],
    reference: datetime,
) -> list[dict[str, int | str]]:
    grouped: dict[str, dict[str, int | str]] = {}
    for row in rows:
        manifest = manifests[row.storage_object_id]
        key = str(manifest.get("transfer_status") or "unknown")
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


def require_storage_object(session: Session, storage_object_id: int) -> StorageObjectORM:
    record = session.get(StorageObjectORM, storage_object_id)
    if record is None:
        raise ValueError(f"Storage object {storage_object_id} does not exist.")
    return record


def ensure_storage_manifest(record: StorageObjectORM) -> dict[str, Any]:
    metadata = dict(record.metadata_json or {})
    manifest = metadata.get("lifecycle_manifest")
    if not isinstance(manifest, dict):
        manifest = {}
    policy = resolve_storage_policy(record, metadata)
    canonical_uri = str(manifest.get("canonical_uri") or record.object_uri)
    replicas = manifest.get("replicas")
    if not isinstance(replicas, list):
        replicas = []
    replicas = [normalize_replica_entry(item) for item in replicas if isinstance(item, dict)]

    primary_replica = build_primary_replica_entry(record, policy)
    replicas = upsert_replica(replicas, primary_replica)

    normalized_manifest = {
        "canonical_uri": canonical_uri,
        "transfer_status": str(manifest.get("transfer_status") or "ready"),
        "failure_reason": manifest.get("failure_reason"),
        "managed": policy["managed"],
        "archive_eligible": policy["archive_eligible"],
        "prune_eligible": policy["prune_eligible"],
        "archived_at": manifest.get("archived_at"),
        "rehydrated_at": manifest.get("rehydrated_at"),
        "last_verified_at": manifest.get("last_verified_at"),
        "quarantined_at": manifest.get("quarantined_at"),
        "rehydration_requested_at": manifest.get("rehydration_requested_at"),
        "replicas": replicas,
    }
    metadata["lifecycle_manifest"] = normalized_manifest
    record.metadata_json = metadata
    return normalized_manifest


def get_storage_manifest(record: StorageObjectORM) -> dict[str, Any]:
    return ensure_storage_manifest(record)


def save_storage_manifest(record: StorageObjectORM, manifest: dict[str, Any]) -> None:
    metadata = dict(record.metadata_json or {})
    metadata["lifecycle_manifest"] = manifest
    record.metadata_json = metadata


def resolve_storage_policy(record: StorageObjectORM, metadata_json: dict[str, Any] | None = None) -> dict[str, Any]:
    settings = get_settings()
    metadata = metadata_json if isinstance(metadata_json, dict) else dict(record.metadata_json or {})
    managed = bool(metadata.get("storage_managed", infer_managed_storage(record)))
    archive_eligible = bool(
        metadata.get(
            "archive_eligible",
            managed
            and record.object_kind not in NON_ARCHIVEABLE_OBJECT_KINDS
            and is_local_artifact_uri(record.object_uri)
            and storage_policy_name(record) in {"warm", "archive", "permanent"},
        )
    )
    prune_eligible = bool(
        metadata.get(
            "prune_eligible",
            managed
            and archive_eligible
            and is_safe_managed_local_path(local_path_from_uri(record.object_uri))
            and record.object_kind != "local_import_source",
        )
    )
    return {
        "managed": managed,
        "archive_eligible": archive_eligible,
        "prune_eligible": prune_eligible,
        "archive_backend": settings.storage_archive_backend,
        "storage_policy_name": storage_policy_name(record),
    }


def storage_policy_name(record: StorageObjectORM) -> str:
    if record.storage_tier == "permanent" or record.retention_class == "permanent":
        return "permanent"
    return str(record.storage_tier or "hot")


def infer_managed_storage(record: StorageObjectORM) -> bool:
    return record.object_kind in MANAGED_STORAGE_OBJECT_KINDS


def build_primary_replica_entry(record: StorageObjectORM, policy: dict[str, Any]) -> dict[str, Any]:
    return normalize_replica_entry(
        {
            "backend": uri_backend_name(record.object_uri),
            "uri": record.object_uri,
            "role": "primary",
            "status": "present" if is_local_artifact_uri(record.object_uri) or record.object_uri.startswith("s3://") else "reference",
            "content_hash": record.content_hash,
            "byte_size": record.byte_size,
            "created_at": isoformat_nullable(record.observed_at or record.created_at),
            "metadata_json": {
                "managed": policy["managed"],
            },
        }
    )


def normalize_replica_entry(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "backend": str(entry.get("backend") or "unknown"),
        "uri": str(entry.get("uri") or ""),
        "role": str(entry.get("role") or "replica"),
        "status": str(entry.get("status") or "present"),
        "content_hash": as_optional_string(entry.get("content_hash")),
        "byte_size": int(entry["byte_size"]) if isinstance(entry.get("byte_size"), (int, float)) else None,
        "created_at": entry.get("created_at"),
        "archived_at": entry.get("archived_at"),
        "verified_at": entry.get("verified_at"),
        "rehydrated_at": entry.get("rehydrated_at"),
        "pruned_at": entry.get("pruned_at"),
        "last_error": as_optional_string(entry.get("last_error")),
        "metadata_json": entry.get("metadata_json") if isinstance(entry.get("metadata_json"), dict) else {},
    }


def upsert_replica(replicas: list[dict[str, Any]], replica: dict[str, Any]) -> list[dict[str, Any]]:
    filtered = [
        item
        for item in replicas
        if not (item.get("uri") == replica.get("uri") and item.get("role") == replica.get("role"))
    ]
    filtered.append(replica)
    return filtered


def has_archive_replica(manifest: dict[str, Any]) -> bool:
    return any(item["role"] == "archive" for item in manifest["replicas"])


def has_verified_archive_replica(manifest: dict[str, Any]) -> bool:
    return any(
        item["role"] == "archive" and item["status"] in {"archived", "verified"}
        for item in manifest["replicas"]
    )


def has_failed_replica(manifest: dict[str, Any]) -> bool:
    return any(item["status"] in {"failed", "missing", "quarantined"} for item in manifest["replicas"])


def is_archive_candidate(record: StorageObjectORM) -> bool:
    manifest = get_storage_manifest(record)
    return bool(manifest["archive_eligible"]) and not has_verified_archive_replica(manifest)


def is_prune_candidate(record: StorageObjectORM, *, reference: datetime | None = None) -> bool:
    manifest = get_storage_manifest(record)
    if not bool(manifest["prune_eligible"]):
        return False
    if not has_verified_archive_replica(manifest):
        return False
    local_replica = select_local_replica(manifest, role="primary")
    if local_replica is None or local_replica["status"] == "pruned":
        return False
    if record.lifecycle_status == "quarantined":
        return False
    now = normalize_timestamp(reference) or utcnow()
    expires_at = normalize_timestamp(record.expires_at)
    return expires_at is not None and expires_at <= now


def select_local_replica(
    manifest: dict[str, Any],
    *,
    role: str | None = None,
) -> dict[str, Any] | None:
    for item in manifest["replicas"]:
        if item["backend"] != "local":
            continue
        if role is not None and item["role"] != role:
            continue
        if item["status"] == "pruned":
            continue
        return item
    return None


def select_archive_replica(manifest: dict[str, Any]) -> dict[str, Any] | None:
    for item in manifest["replicas"]:
        if item["role"] == "archive" and item["status"] in {"archived", "verified"}:
            return item
    return None


def archive_storage_object_record(
    session: Session,
    record: StorageObjectORM,
    *,
    actor: str,
    prune_local: bool | None = None,
) -> dict[str, object]:
    manifest = get_storage_manifest(record)
    if not manifest["archive_eligible"]:
        raise ValueError(f"Storage object {record.storage_object_id} is not archive-eligible.")
    source_replica = select_local_replica(manifest, role="primary")
    if source_replica is None:
        raise ValueError(f"Storage object {record.storage_object_id} has no local primary replica to archive.")
    source_path = local_path_from_uri(source_replica["uri"])
    if not source_path.exists() or not source_path.is_file():
        raise ValueError(f"Archive source path '{source_path}' does not exist.")

    backend = build_archive_backend()
    destination_uri = build_archive_destination_uri(record, source_path)
    now = utcnow()
    manifest["transfer_status"] = "archiving"
    manifest["failure_reason"] = None
    save_storage_manifest(record, manifest)
    session.add(
        CustodyLogORM(
            object_type="storage_object",
            object_id=str(record.storage_object_id),
            action="storage_archive_started",
            actor=actor,
            details_json={
                "object_key": record.object_key,
                "source_uri": source_replica["uri"],
                "destination_uri": destination_uri,
            },
        )
    )

    expected_hash = record.content_hash or hash_file(source_path)
    transfer = backend.copy_from_local(
        source_path,
        destination_uri,
        media_type=record.media_type,
        content_hash=expected_hash,
    )
    record.content_hash = transfer.content_hash
    record.byte_size = transfer.byte_size
    manifest["canonical_uri"] = destination_uri
    manifest["transfer_status"] = "verified" if transfer.verified else "archived"
    manifest["archived_at"] = now.isoformat()
    manifest["last_verified_at"] = now.isoformat()
    manifest["failure_reason"] = None
    manifest["replicas"] = upsert_replica(
        manifest["replicas"],
        {
            "backend": transfer.backend,
            "uri": transfer.uri,
            "role": "archive",
            "status": "verified" if transfer.verified else "archived",
            "content_hash": transfer.content_hash,
            "byte_size": transfer.byte_size,
            "created_at": now.isoformat(),
            "archived_at": now.isoformat(),
            "verified_at": now.isoformat() if transfer.verified else None,
            "metadata_json": transfer.metadata,
        },
    )
    save_storage_manifest(record, manifest)
    record.object_uri = manifest["canonical_uri"]
    record.storage_tier = "archive" if record.storage_tier != "permanent" else "permanent"
    record.lifecycle_status = "archived"
    session.add(
        CustodyLogORM(
            object_type="storage_object",
            object_id=str(record.storage_object_id),
            action="storage_archived",
            actor=actor,
            details_json={
                "object_key": record.object_key,
                "archive_uri": transfer.uri,
                "content_hash": transfer.content_hash,
                "byte_size": transfer.byte_size,
            },
        )
    )

    if prune_local or (prune_local is None and bool(manifest["prune_eligible"]) and is_prune_candidate(record)):
        prune_storage_object_record(session, record, actor=actor, reference=now)

    return build_storage_action_result(
        action="archive",
        message="Storage object archived and verified.",
        verified=transfer.verified,
        storage_object=record,
    )


def verify_storage_object_record(
    session: Session,
    record: StorageObjectORM,
    *,
    actor: str,
) -> dict[str, object]:
    manifest = get_storage_manifest(record)
    target_replica = select_archive_replica(manifest) or select_local_replica(manifest)
    if target_replica is None:
        raise ValueError(f"Storage object {record.storage_object_id} has no verifiable replica.")
    transfer = verify_replica(target_replica, record)
    now = utcnow()
    target_replica["status"] = "verified"
    target_replica["content_hash"] = transfer.content_hash
    target_replica["byte_size"] = transfer.byte_size
    target_replica["verified_at"] = now.isoformat()
    target_replica["last_error"] = None
    manifest["last_verified_at"] = now.isoformat()
    manifest["transfer_status"] = "verified"
    manifest["failure_reason"] = None
    manifest["replicas"] = upsert_replica(manifest["replicas"], target_replica)
    save_storage_manifest(record, manifest)
    if record.lifecycle_status == "quarantined":
        record.lifecycle_status = "degraded"
    session.add(
        CustodyLogORM(
            object_type="storage_object",
            object_id=str(record.storage_object_id),
            action="storage_verified",
            actor=actor,
            details_json={
                "object_key": record.object_key,
                "verified_uri": target_replica["uri"],
                "content_hash": transfer.content_hash,
                "byte_size": transfer.byte_size,
            },
        )
    )
    return build_storage_action_result(
        action="verify",
        message="Storage object verified.",
        verified=True,
        storage_object=record,
    )


def rehydrate_storage_object_record(
    session: Session,
    record: StorageObjectORM,
    *,
    target_path: str | Path | None = None,
    replace_existing: bool = False,
    actor: str,
) -> dict[str, object]:
    manifest = get_storage_manifest(record)
    source_replica = select_archive_replica(manifest) or select_local_replica(manifest)
    if source_replica is None:
        raise ValueError(f"Storage object {record.storage_object_id} has no replica available for rehydration.")
    destination_path = resolve_rehydrate_destination(record, target_path)
    if destination_path.exists() and not replace_existing:
        raise ValueError(f"Rehydration target '{destination_path}' already exists.")
    now = utcnow()
    manifest["transfer_status"] = "rehydrating"
    manifest["failure_reason"] = None
    save_storage_manifest(record, manifest)
    session.add(
        CustodyLogORM(
            object_type="storage_object",
            object_id=str(record.storage_object_id),
            action="storage_rehydration_started",
            actor=actor,
            details_json={
                "object_key": record.object_key,
                "source_uri": source_replica["uri"],
                "destination_path": str(destination_path),
            },
        )
    )
    backend = backend_for_uri(source_replica["uri"])
    transfer = backend.download_to_local(
        source_replica["uri"],
        destination_path,
        expected_hash=record.content_hash,
    )
    rehydrated_uri = local_path_to_uri(destination_path)
    manifest["transfer_status"] = "rehydrated"
    manifest["rehydrated_at"] = now.isoformat()
    manifest["last_verified_at"] = now.isoformat()
    manifest["rehydration_requested_at"] = None
    manifest["failure_reason"] = None
    manifest["replicas"] = upsert_replica(
        manifest["replicas"],
        {
            "backend": "local",
            "uri": rehydrated_uri,
            "role": "rehydrated",
            "status": "verified",
            "content_hash": transfer.content_hash,
            "byte_size": transfer.byte_size,
            "created_at": now.isoformat(),
            "rehydrated_at": now.isoformat(),
            "verified_at": now.isoformat(),
            "metadata_json": {"target_path": str(destination_path)},
        },
    )
    save_storage_manifest(record, manifest)
    session.add(
        CustodyLogORM(
            object_type="storage_object",
            object_id=str(record.storage_object_id),
            action="storage_rehydrated",
            actor=actor,
            details_json={
                "object_key": record.object_key,
                "source_uri": source_replica["uri"],
                "rehydrated_uri": rehydrated_uri,
                "content_hash": transfer.content_hash,
                "byte_size": transfer.byte_size,
            },
        )
    )
    return build_storage_action_result(
        action="rehydrate",
        message="Storage object rehydrated and verified.",
        verified=True,
        storage_object=record,
    )


def prune_storage_object_record(
    session: Session,
    record: StorageObjectORM,
    *,
    actor: str,
    reference: datetime | None = None,
) -> dict[str, object]:
    manifest = get_storage_manifest(record)
    if not is_prune_candidate(record, reference=reference):
        raise ValueError(f"Storage object {record.storage_object_id} is not eligible for safe local pruning.")
    local_replica = select_local_replica(manifest, role="primary")
    if local_replica is None:
        raise ValueError(f"Storage object {record.storage_object_id} has no primary local replica to prune.")
    local_path = local_path_from_uri(local_replica["uri"])
    if not is_safe_managed_local_path(local_path):
        raise ValueError(f"Refusing to prune unmanaged local path '{local_path}'.")
    if local_path.exists():
        local_path.unlink()
    now = utcnow()
    local_replica["status"] = "pruned"
    local_replica["pruned_at"] = now.isoformat()
    manifest["transfer_status"] = "pruned"
    archive_replica = select_archive_replica(manifest)
    if archive_replica is not None:
        manifest["canonical_uri"] = archive_replica["uri"]
        record.object_uri = archive_replica["uri"]
    manifest["replicas"] = upsert_replica(manifest["replicas"], local_replica)
    save_storage_manifest(record, manifest)
    session.add(
        CustodyLogORM(
            object_type="storage_object",
            object_id=str(record.storage_object_id),
            action="storage_pruned",
            actor=actor,
            details_json={
                "object_key": record.object_key,
                "pruned_uri": local_replica["uri"],
                "canonical_uri": manifest["canonical_uri"],
            },
        )
    )
    return build_storage_action_result(
        action="prune",
        message="Local replica pruned after verified archival.",
        verified=True,
        storage_object=record,
    )


def verify_replica(replica: dict[str, Any], record: StorageObjectORM) -> StorageTransferResult:
    backend = backend_for_uri(replica["uri"])
    return backend.verify(
        replica["uri"],
        expected_hash=record.content_hash,
        expected_size=record.byte_size,
    )


def backend_for_uri(uri: str):
    if uri.startswith("s3://"):
        return build_archive_backend()
    return build_local_backend()


def build_archive_destination_uri(record: StorageObjectORM, source_path: Path) -> str:
    settings = get_settings()
    safe_name = sanitize_file_name(source_path.name or f"storage-{record.storage_object_id}")
    relative_name = f"{record.storage_object_id}-{safe_name}"
    if settings.storage_archive_backend == "r2":
        archive_backend = build_archive_backend()
        object_key = (
            f"{settings.storage_s3_prefix.strip('/')}/"
            f"{record.owner_type}/{record.owner_id}/{relative_name}"
        )
        if hasattr(archive_backend, "build_object_uri"):
            return archive_backend.build_object_uri(object_key)  # type: ignore[attr-defined]
        raise ValueError("Configured archive backend does not support object URI generation.")
    archive_path = settings.storage_archive_dir_effective / record.owner_type / record.owner_id / relative_name
    return local_path_to_uri(archive_path)


def resolve_rehydrate_destination(record: StorageObjectORM, target_path: str | Path | None) -> Path:
    settings = get_settings()
    if target_path is not None:
        return Path(target_path).expanduser().resolve()
    file_name = infer_rehydrate_file_name(record)
    return (settings.storage_rehydrate_dir_effective / file_name).resolve()


def infer_rehydrate_file_name(record: StorageObjectORM) -> str:
    path_like = record.object_uri
    if path_like.startswith("s3://"):
        return sanitize_file_name(path_like.rsplit("/", 1)[-1] or f"storage-{record.storage_object_id}.bin")
    return sanitize_file_name(Path(path_like).name or f"storage-{record.storage_object_id}.bin")


def sanitize_file_name(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._")
    return sanitized or "artifact.bin"


def record_transfer_failure(
    session: Session,
    record: StorageObjectORM,
    *,
    action: str,
    actor: str,
    reason: str,
    quarantine: bool,
) -> None:
    manifest = get_storage_manifest(record)
    manifest["failure_reason"] = reason
    manifest["transfer_status"] = "quarantined" if quarantine else "failed"
    if quarantine:
        manifest["quarantined_at"] = utcnow().isoformat()
        record.lifecycle_status = "quarantined"
    save_storage_manifest(record, manifest)
    session.add(
        CustodyLogORM(
            object_type="storage_object",
            object_id=str(record.storage_object_id),
            action=action,
            actor=actor,
            details_json={
                "object_key": record.object_key,
                "reason": reason,
                "quarantined": quarantine,
            },
        )
    )


def build_storage_action_result(
    *,
    action: str,
    message: str,
    verified: bool,
    storage_object: StorageObjectORM,
) -> dict[str, object]:
    return StorageActionResultRead.model_validate(
        {
            "action": action,
            "message": message,
            "verified": verified,
            "storage_object": storage_object,
            "manifest": get_storage_manifest(storage_object),
        }
    ).model_dump(mode="python")


def normalize_lifecycle_operations(operations: list[str] | None) -> tuple[str, ...]:
    if not operations:
        return DEFAULT_LIFECYCLE_OPERATIONS
    normalized = []
    allowed = set(DEFAULT_LIFECYCLE_OPERATIONS)
    for operation in operations:
        item = operation.strip().lower()
        if item not in allowed:
            raise ValueError(f"Unsupported storage lifecycle operation '{operation}'.")
        if item not in normalized:
            normalized.append(item)
    return tuple(normalized)


def is_local_artifact_uri(uri: str) -> bool:
    return not uri.startswith(("http://", "https://", "s3://"))


def uri_backend_name(uri: str) -> str:
    if uri.startswith("s3://"):
        return "r2"
    if uri.startswith(("http://", "https://")):
        return "http"
    return "local"


def isoformat_nullable(value: datetime | None) -> str | None:
    normalized = normalize_timestamp(value)
    return normalized.isoformat() if normalized is not None else None


def is_safe_managed_local_path(path: Path) -> bool:
    settings = get_settings()
    resolved = path.resolve()
    safe_roots = [
        settings.data_dir.resolve(),
        settings.storage_archive_dir_effective.resolve(),
        settings.storage_rehydrate_dir_effective.resolve(),
    ]
    return any(resolved.is_relative_to(root) for root in safe_roots)
