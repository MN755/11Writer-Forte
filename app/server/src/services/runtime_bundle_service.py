from __future__ import annotations

import hashlib
import json
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

from pydantic import TypeAdapter, ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.config import get_settings
from src.models import CustodyLogORM, StorageObjectORM
from src.schemas import RuntimeBundleManifestRead, RuntimeSnapshotRead
from src.services.runtime_snapshot_service import (
    build_runtime_snapshot,
    build_runtime_snapshot_section_counts,
    restore_runtime_snapshot,
)
from src.services.storage_backends import local_path_from_uri
from src.services.storage_service import hash_file, register_export_storage_object

BUNDLE_FORMAT_VERSION = "1"
SNAPSHOT_ARCHIVE_PATH = "snapshot.json"
MANIFEST_ARCHIVE_PATH = "manifest.json"
WATCH_EVIDENCE_KIND = "watch_image_evidence"
SAFE_FILE_NAME = re.compile(r"[^A-Za-z0-9._-]+")


def bundle_now() -> datetime:
    return datetime.now(timezone.utc)


def default_runtime_bundle_export_dir() -> Path:
    return (get_settings().data_dir_effective / "backups" / "runtime-bundles").resolve()


def build_runtime_bundle_export_path(
    *,
    root_dir: Path | None = None,
    file_prefix: str = "runtime-bundle",
    exported_at: datetime | None = None,
) -> Path:
    normalized_prefix = SAFE_FILE_NAME.sub("-", file_prefix.strip()).strip("-.")
    if not normalized_prefix:
        normalized_prefix = "runtime-bundle"
    timestamp = (exported_at or bundle_now()).strftime("%Y%m%dT%H%M%SZ")
    export_root = (root_dir or default_runtime_bundle_export_dir()).expanduser().resolve()
    return export_root / f"{normalized_prefix}-{timestamp}.zip"


def export_runtime_bundle(
    session: Session,
    *,
    output_path: Path,
    actor: str = "runtime_bundle_export",
) -> dict[str, object]:
    resolved_output_path = output_path.expanduser().resolve()
    snapshot = build_runtime_snapshot(session)
    snapshot_json = TypeAdapter(RuntimeSnapshotRead).validate_python(snapshot).model_dump(mode="json")
    snapshot_bytes = json.dumps(snapshot_json, indent=2, sort_keys=True).encode("utf-8")
    evidence_entries, evidence_payloads = collect_watch_evidence_payloads(session)
    generated_at = bundle_now()
    manifest: dict[str, object] = {
        "format_version": BUNDLE_FORMAT_VERSION,
        "generated_at": generated_at.isoformat(),
        "snapshot_archive_path": SNAPSHOT_ARCHIVE_PATH,
        "snapshot_sha256": sha256_bytes(snapshot_bytes),
        "snapshot_byte_size": len(snapshot_bytes),
        "snapshot_section_counts": build_runtime_snapshot_section_counts(snapshot_json),
        "evidence": evidence_entries,
    }
    manifest_bytes = json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8")

    resolved_output_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(
        resolved_output_path,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=9,
    ) as archive:
        archive.writestr(SNAPSHOT_ARCHIVE_PATH, snapshot_bytes)
        archive.writestr(MANIFEST_ARCHIVE_PATH, manifest_bytes)
        for archive_path, payload in evidence_payloads:
            archive.writestr(archive_path, payload)

    owner_id = generated_at.isoformat()
    storage_object = register_export_storage_object(
        session,
        object_kind="runtime_bundle_export",
        owner_type="runtime_bundle",
        owner_id=owner_id,
        output_path=resolved_output_path,
        source_uri="local://runtime-bundle/export",
        retention_class="permanent",
        observed_at=generated_at,
        metadata_json={
            "format_version": BUNDLE_FORMAT_VERSION,
            "evidence_count": len(evidence_entries),
            "snapshot_section_counts": manifest["snapshot_section_counts"],
        },
        actor=actor,
    )
    session.add(
        CustodyLogORM(
            object_type="runtime_bundle",
            object_id=owner_id,
            action="runtime_bundle_exported",
            actor=actor,
            details_json={
                "output_path": str(resolved_output_path),
                "bundle_sha256": hash_file(resolved_output_path),
                "storage_object_id": storage_object.storage_object_id,
                "evidence_storage_object_ids": [
                    int(entry["storage_object_id"]) for entry in evidence_entries
                ],
            },
        )
    )
    session.commit()
    session.refresh(storage_object)
    return {
        "output_path": str(resolved_output_path),
        "bundle_sha256": hash_file(resolved_output_path),
        "manifest": manifest,
        "storage_object": storage_object,
    }


def restore_runtime_bundle(
    session: Session,
    *,
    bundle_path: Path,
    replace_existing: bool = False,
    actor: str = "runtime_bundle_restore",
) -> dict[str, object]:
    resolved_bundle_path = bundle_path.expanduser().resolve()
    if not resolved_bundle_path.exists() or not resolved_bundle_path.is_file():
        raise ValueError(f"Runtime bundle '{resolved_bundle_path}' does not exist.")
    bundle_sha256 = hash_file(resolved_bundle_path)
    snapshot, manifest, evidence_payloads = read_and_verify_runtime_bundle(resolved_bundle_path)
    snapshot_storage_ids = {
        int(row["storage_object_id"])
        for row in snapshot.get("storage_objects", [])
        if isinstance(row, dict) and row.get("storage_object_id") is not None
    }
    manifest_storage_ids = {
        int(entry["storage_object_id"])
        for entry in manifest.get("evidence", [])
        if isinstance(entry, dict) and entry.get("storage_object_id") is not None
    }
    missing_storage_ids = sorted(manifest_storage_ids - snapshot_storage_ids)
    if missing_storage_ids:
        raise ValueError(
            "Runtime bundle evidence is missing storage ledger rows in snapshot.json: "
            + ", ".join(str(item) for item in missing_storage_ids)
        )
    restore_result = restore_runtime_snapshot(
        session,
        snapshot,
        replace_existing=replace_existing,
    )

    restore_root = (
        get_settings().data_dir_effective
        / "artifacts"
        / "watch-evidence"
        / "restored"
        / bundle_sha256[:16]
    ).resolve()
    restore_root.mkdir(parents=True, exist_ok=True)
    restored_storage_object_ids: list[int] = []
    evidence_by_path = {
        str(entry["archive_path"]): entry for entry in manifest.get("evidence", [])
    }
    for archive_path, payload in evidence_payloads:
        entry = evidence_by_path[archive_path]
        storage_object_id = int(entry["storage_object_id"])
        file_name = f"{storage_object_id}-{safe_evidence_file_name(archive_path, storage_object_id)}"
        destination = (restore_root / file_name).resolve()
        if destination.parent != restore_root:
            raise ValueError(f"Unsafe evidence destination for '{archive_path}'.")
        destination.write_bytes(payload)
        if hash_file(destination) != str(entry["sha256"]):
            raise ValueError(f"Restored evidence checksum mismatch for '{archive_path}'.")
        storage_object = session.get(StorageObjectORM, storage_object_id)
        if storage_object is None:
            raise ValueError(
                f"Runtime bundle references missing storage object {storage_object_id}."
            )
        if storage_object.object_kind != WATCH_EVIDENCE_KIND:
            raise ValueError(
                f"Storage object {storage_object_id} is not {WATCH_EVIDENCE_KIND}."
            )
        storage_object.object_uri = str(destination)
        storage_object.content_hash = str(entry["sha256"])
        storage_object.byte_size = len(payload)
        storage_object.lifecycle_status = "active"
        session.add(
            CustodyLogORM(
                object_type="storage_object",
                object_id=str(storage_object_id),
                action="watch_evidence_restored",
                actor=actor,
                details_json={
                    "bundle_path": str(resolved_bundle_path),
                    "bundle_sha256": bundle_sha256,
                    "archive_path": archive_path,
                    "object_uri": str(destination),
                },
            )
        )
        restored_storage_object_ids.append(storage_object_id)

    bundle_record = register_export_storage_object(
        session,
        object_kind="runtime_bundle_import",
        owner_type="runtime_bundle",
        owner_id=bundle_sha256,
        output_path=resolved_bundle_path,
        source_uri=str(resolved_bundle_path),
        retention_class="permanent",
        observed_at=bundle_now(),
        metadata_json={
            "format_version": manifest["format_version"],
            "restored_evidence_count": len(restored_storage_object_ids),
        },
        actor=actor,
    )
    session.add(
        CustodyLogORM(
            object_type="runtime_bundle",
            object_id=bundle_sha256,
            action="runtime_bundle_restored",
            actor=actor,
            details_json={
                "bundle_path": str(resolved_bundle_path),
                "bundle_storage_object_id": bundle_record.storage_object_id,
                "replaced_existing": replace_existing,
                "restored_evidence_storage_object_ids": restored_storage_object_ids,
            },
        )
    )
    session.commit()
    return {
        "bundle_path": str(resolved_bundle_path),
        "bundle_sha256": bundle_sha256,
        "manifest": manifest,
        "restore_result": restore_result,
        "restored_evidence_count": len(restored_storage_object_ids),
        "restored_evidence_storage_object_ids": restored_storage_object_ids,
        "bundle_storage_object": bundle_record,
    }


def collect_watch_evidence_payloads(
    session: Session,
) -> tuple[list[dict[str, object]], list[tuple[str, bytes]]]:
    records = list(
        session.scalars(
            select(StorageObjectORM)
            .where(StorageObjectORM.object_kind == WATCH_EVIDENCE_KIND)
            .order_by(StorageObjectORM.storage_object_id.asc())
        )
    )
    entries: list[dict[str, object]] = []
    payloads: list[tuple[str, bytes]] = []
    for record in records:
        path = local_path_from_uri(record.object_uri).expanduser().resolve()
        if not path.exists() or not path.is_file():
            raise ValueError(
                f"Watch evidence storage object {record.storage_object_id} has no local file at '{path}'."
            )
        payload = path.read_bytes()
        content_hash = sha256_bytes(payload)
        if record.content_hash and record.content_hash != content_hash:
            raise ValueError(
                f"Watch evidence storage object {record.storage_object_id} failed checksum verification."
            )
        archive_path = (
            f"evidence/{record.storage_object_id}/"
            f"{safe_evidence_file_name(path.name, record.storage_object_id)}"
        )
        entry = {
            "storage_object_id": record.storage_object_id,
            "archive_path": archive_path,
            "sha256": content_hash,
            "byte_size": len(payload),
            "media_type": record.media_type,
            "source_uri": record.source_uri,
            "original_object_uri": record.object_uri,
        }
        entries.append(entry)
        payloads.append((archive_path, payload))
    return entries, payloads


def read_and_verify_runtime_bundle(
    bundle_path: Path,
) -> tuple[dict[str, object], dict[str, Any], list[tuple[str, bytes]]]:
    try:
        with zipfile.ZipFile(bundle_path, mode="r") as archive:
            names = set(archive.namelist())
            if SNAPSHOT_ARCHIVE_PATH not in names or MANIFEST_ARCHIVE_PATH not in names:
                raise ValueError("Runtime bundle is missing snapshot.json or manifest.json.")
            manifest_payload = json.loads(archive.read(MANIFEST_ARCHIVE_PATH).decode("utf-8"))
            try:
                manifest = TypeAdapter(RuntimeBundleManifestRead).validate_python(
                    manifest_payload
                ).model_dump(mode="python")
            except ValidationError as exc:
                raise ValueError(f"Runtime bundle manifest failed schema validation: {exc}") from exc
            snapshot_bytes = archive.read(SNAPSHOT_ARCHIVE_PATH)
            if sha256_bytes(snapshot_bytes) != manifest.get("snapshot_sha256"):
                raise ValueError("Runtime bundle snapshot checksum mismatch.")
            if len(snapshot_bytes) != int(manifest.get("snapshot_byte_size", -1)):
                raise ValueError("Runtime bundle snapshot byte-size mismatch.")
            snapshot = json.loads(snapshot_bytes.decode("utf-8"))
            TypeAdapter(RuntimeSnapshotRead).validate_python(snapshot)
            raw_evidence = manifest["evidence"]
            storage_rows = {
                int(row["storage_object_id"]): row
                for row in snapshot.get("storage_objects", [])
                if isinstance(row, dict) and row.get("storage_object_id") is not None
            }
            evidence_payloads: list[tuple[str, bytes]] = []
            seen_paths: set[str] = set()
            for entry in raw_evidence:
                archive_path = str(entry["archive_path"])
                validate_evidence_archive_path(archive_path)
                if archive_path in seen_paths or archive_path not in names:
                    raise ValueError(f"Runtime bundle evidence entry '{archive_path}' is missing or duplicated.")
                seen_paths.add(archive_path)
                storage_row = storage_rows.get(int(entry["storage_object_id"]))
                if storage_row is None:
                    raise ValueError(
                        "Runtime bundle evidence references a storage object absent from snapshot.json."
                    )
                if storage_row.get("object_kind") != WATCH_EVIDENCE_KIND:
                    raise ValueError(
                        "Runtime bundle evidence must reference a watch_image_evidence storage object."
                    )
                payload = archive.read(archive_path)
                if sha256_bytes(payload) != str(entry["sha256"]):
                    raise ValueError(f"Runtime bundle evidence checksum mismatch for '{archive_path}'.")
                if len(payload) != int(entry["byte_size"]):
                    raise ValueError(f"Runtime bundle evidence byte-size mismatch for '{archive_path}'.")
                evidence_payloads.append((archive_path, payload))
    except (zipfile.BadZipFile, KeyError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"Invalid runtime bundle '{bundle_path}': {exc}") from exc
    return snapshot, manifest, evidence_payloads


def validate_evidence_archive_path(archive_path: str) -> None:
    path = PurePosixPath(archive_path)
    if (
        not archive_path
        or path.is_absolute()
        or ".." in path.parts
        or len(path.parts) != 3
        or path.parts[0] != "evidence"
    ):
        raise ValueError(f"Unsafe runtime bundle evidence path '{archive_path}'.")


def safe_evidence_file_name(value: str, storage_object_id: int) -> str:
    name = Path(value).name
    sanitized = SAFE_FILE_NAME.sub("-", name).strip("-.")
    return sanitized or f"watch-evidence-{storage_object_id}.bin"


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()
