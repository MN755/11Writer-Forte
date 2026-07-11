from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile

from pydantic import TypeAdapter
from sqlalchemy.orm import Session

from src.config import get_settings
from src.models import CustodyLogORM
from src.schemas import RuntimeSnapshotRead
from src.services.runtime_snapshot_service import build_runtime_snapshot, restore_runtime_snapshot
from src.services.storage_service import register_export_storage_object

BUNDLE_FORMAT_VERSION = 1
SNAPSHOT_MEMBER = "snapshot.json"
MANIFEST_MEMBER = "manifest.json"
DATA_DIR_PREFIX = "data_dir/"


def runtime_bundle_now() -> datetime:
    return datetime.now(timezone.utc)


def default_runtime_bundle_output_path() -> Path:
    settings = get_settings()
    timestamp = runtime_bundle_now().strftime("%Y%m%dT%H%M%S%fZ")
    return settings.data_dir / "exports" / "runtime-bundles" / f"runtime-bundle-{timestamp}.zip"


def export_runtime_bundle(
    session: Session,
    output_path: Path,
    *,
    actor: str = "cli_export",
) -> dict[str, Any]:
    settings = get_settings()
    settings.ensure_runtime_dirs()
    resolved_output = output_path.expanduser().resolve()
    snapshot = build_runtime_snapshot(session)
    serializable_snapshot = (
        TypeAdapter(RuntimeSnapshotRead).validate_python(snapshot).model_dump(mode="json")
    )
    bundle_files = collect_bundle_files(
        settings.data_dir,
        exclude_paths={resolved_output, *runtime_bundle_excluded_paths()},
    )
    exported_at = runtime_bundle_now()
    manifest = build_runtime_bundle_manifest(
        serializable_snapshot,
        bundle_files,
        bundle_exported_at=exported_at,
        bundle_actor=actor,
    )

    resolved_output.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(resolved_output, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr(SNAPSHOT_MEMBER, json.dumps(serializable_snapshot, indent=2))
        archive.writestr(MANIFEST_MEMBER, json.dumps(manifest, indent=2))
        for entry in bundle_files:
            archive.write(entry["path"], arcname=f"{DATA_DIR_PREFIX}{entry['relative_path']}")

    record = register_export_storage_object(
        session,
        output_path=resolved_output,
        object_kind="runtime_bundle_export",
        owner_type="runtime_bundle",
        owner_id=str(serializable_snapshot["exported_at"]),
        source_uri="/cli/export-runtime-bundle",
        observed_at=snapshot["exported_at"],
        metadata_json={
            "bundle_format_version": BUNDLE_FORMAT_VERSION,
            "database_backend": serializable_snapshot["database_backend"],
            "spatial_backend": serializable_snapshot["spatial_backend"],
            "data_file_count": len(bundle_files),
            "data_dir": str(settings.data_dir.resolve()),
        },
        actor=actor,
    )
    bundle_sha256 = hash_file(resolved_output)
    session.add(
        CustodyLogORM(
            object_type="runtime_bundle",
            object_id=exported_at.isoformat(),
            action="runtime_bundle_exported",
            actor=actor,
            details_json={
                "output_path": str(resolved_output),
                "bundle_sha256": bundle_sha256,
                "bundle_format_version": BUNDLE_FORMAT_VERSION,
                "snapshot_exported_at": serializable_snapshot["exported_at"],
                "storage_object_id": record.storage_object_id,
                "data_file_count": len(bundle_files),
            },
        )
    )
    session.commit()
    return {
        "exported_at": exported_at,
        "output_path": str(resolved_output),
        "bundle_sha256": bundle_sha256,
        "bundle_format_version": BUNDLE_FORMAT_VERSION,
        "data_file_count": len(bundle_files),
        "storage_object_id": record.storage_object_id,
        "snapshot_exported_at": serializable_snapshot["exported_at"],
    }


def restore_runtime_bundle(
    session: Session,
    input_path: Path,
    *,
    replace_existing: bool = False,
    actor: str = "runtime_restore",
) -> dict[str, Any]:
    settings = get_settings()
    settings.ensure_runtime_dirs()
    resolved_input = input_path.expanduser().resolve()
    bundle_sha256 = hash_file(resolved_input)
    with ZipFile(resolved_input, "r") as archive:
        manifest = parse_runtime_bundle_manifest(archive.read(MANIFEST_MEMBER))
        snapshot_payload = json.loads(archive.read(SNAPSHOT_MEMBER).decode("utf-8"))
        staged_root, staged_files = stage_runtime_bundle_files(archive, manifest["data_dir_files"])

    try:
        restore_result = restore_runtime_snapshot(
            session,
            snapshot_payload,
            replace_existing=replace_existing,
        )
        excluded_paths = {resolved_input, *runtime_bundle_excluded_paths()}
        if replace_existing:
            clear_directory_contents(settings.data_dir, exclude_paths=excluded_paths)
        restored_file_count = materialize_staged_bundle_files(staged_files, settings.data_dir)
        restored_at = runtime_bundle_now()
        session.add(
            CustodyLogORM(
                object_type="runtime_bundle",
                object_id=str(manifest["bundle_exported_at"]),
                action="runtime_bundle_exported",
                actor=str(manifest["bundle_actor"]),
                details_json={
                    "bundle_sha256": bundle_sha256,
                    "bundle_format_version": manifest["bundle_format_version"],
                    "snapshot_exported_at": manifest["snapshot_exported_at"],
                    "restored_from_bundle": True,
                },
            )
        )
        session.add(
            CustodyLogORM(
                object_type="runtime_bundle",
                object_id=restored_at.isoformat(),
                action="runtime_bundle_restored",
                actor=actor,
                details_json={
                    "input_path": str(resolved_input),
                    "bundle_sha256": bundle_sha256,
                    "bundle_format_version": manifest["bundle_format_version"],
                    "snapshot_exported_at": manifest["snapshot_exported_at"],
                    "restored_file_count": restored_file_count,
                    "replace_existing": replace_existing,
                },
            )
        )
        session.commit()
        restore_result.update(
            {
                "restored_file_count": restored_file_count,
                "bundle_sha256": bundle_sha256,
                "bundle_format_version": manifest["bundle_format_version"],
                "data_dir": str(settings.data_dir.resolve()),
            }
        )
        return restore_result
    finally:
        shutil.rmtree(staged_root, ignore_errors=True)


def collect_bundle_files(
    data_dir: Path,
    *,
    exclude_paths: set[Path],
) -> list[dict[str, Any]]:
    if not data_dir.exists():
        return []
    resolved_root = data_dir.resolve()
    normalized_excludes = {path.resolve() for path in exclude_paths}
    files: list[dict[str, Any]] = []
    for path in sorted(resolved_root.rglob("*")):
        if not path.is_file():
            continue
        resolved_path = path.resolve()
        if resolved_path in normalized_excludes:
            continue
        relative_path = resolved_path.relative_to(resolved_root).as_posix()
        files.append(
            {
                "path": resolved_path,
                "relative_path": relative_path,
                "byte_size": resolved_path.stat().st_size,
                "sha256": hash_file(resolved_path),
            }
        )
    return files


def build_runtime_bundle_manifest(
    snapshot: dict[str, Any],
    bundle_files: list[dict[str, Any]],
    *,
    bundle_exported_at: datetime,
    bundle_actor: str,
) -> dict[str, Any]:
    return {
        "bundle_format_version": BUNDLE_FORMAT_VERSION,
        "bundle_exported_at": bundle_exported_at.isoformat(),
        "bundle_actor": str(bundle_actor),
        "snapshot_exported_at": snapshot["exported_at"],
        "app_name": snapshot["app_name"],
        "app_version": snapshot["app_version"],
        "database_backend": snapshot["database_backend"],
        "spatial_backend": snapshot["spatial_backend"],
        "data_dir_files": [
            {
                "relative_path": entry["relative_path"],
                "byte_size": entry["byte_size"],
                "sha256": entry["sha256"],
            }
            for entry in bundle_files
        ],
    }


def parse_runtime_bundle_manifest(payload: bytes) -> dict[str, Any]:
    manifest = json.loads(payload.decode("utf-8"))
    if not isinstance(manifest, dict):
        raise ValueError("Runtime bundle manifest must be an object.")
    if int(manifest.get("bundle_format_version", 0)) != BUNDLE_FORMAT_VERSION:
        raise ValueError("Unsupported runtime bundle format version.")
    bundle_exported_at = str(manifest.get("bundle_exported_at", "")).strip()
    if not bundle_exported_at:
        raise ValueError("Runtime bundle manifest is missing bundle_exported_at.")
    bundle_actor = str(manifest.get("bundle_actor", "")).strip()
    if not bundle_actor:
        raise ValueError("Runtime bundle manifest is missing bundle_actor.")
    data_dir_files = manifest.get("data_dir_files", [])
    if not isinstance(data_dir_files, list):
        raise ValueError("Runtime bundle manifest data_dir_files must be a list.")
    normalized_files = []
    for entry in data_dir_files:
        if not isinstance(entry, dict):
            raise ValueError("Runtime bundle manifest entries must be objects.")
        relative_path = normalize_bundle_relative_path(entry.get("relative_path"))
        byte_size = int(entry.get("byte_size", 0))
        sha256 = str(entry.get("sha256", "")).strip().lower()
        if len(sha256) != 64:
            raise ValueError(
                f"Runtime bundle manifest entry '{relative_path}' has an invalid sha256."
            )
        normalized_files.append(
            {
                "relative_path": relative_path,
                "byte_size": byte_size,
                "sha256": sha256,
            }
        )
    manifest["data_dir_files"] = normalized_files
    manifest["bundle_exported_at"] = bundle_exported_at
    manifest["bundle_actor"] = bundle_actor
    return manifest


def stage_runtime_bundle_files(
    archive: ZipFile,
    manifest_entries: list[dict[str, Any]],
) -> tuple[Path, list[dict[str, Any]]]:
    staged: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="11writer-runtime-bundle-") as temp_dir:
        temp_root = Path(temp_dir)
        for entry in manifest_entries:
            archive_name = f"{DATA_DIR_PREFIX}{entry['relative_path']}"
            payload = archive.read(archive_name)
            if len(payload) != int(entry["byte_size"]):
                raise ValueError(
                    f"Runtime bundle member '{entry['relative_path']}' byte size does not match the manifest."
                )
            digest = hashlib.sha256(payload).hexdigest()
            if digest != entry["sha256"]:
                raise ValueError(
                    f"Runtime bundle member '{entry['relative_path']}' failed sha256 verification."
                )
            staged_path = temp_root / Path(entry["relative_path"])
            staged_path.parent.mkdir(parents=True, exist_ok=True)
            staged_path.write_bytes(payload)
            staged.append(
                {
                    "relative_path": entry["relative_path"],
                    "path": staged_path,
                }
            )
        persisted_root = Path(tempfile.mkdtemp(prefix="11writer-runtime-bundle-staged-"))
        shutil.copytree(temp_root, persisted_root, dirs_exist_ok=True)
    return persisted_root, [
        {
            "relative_path": entry["relative_path"],
            "path": (persisted_root / Path(entry["relative_path"])).resolve(),
        }
        for entry in staged
    ]


def materialize_staged_bundle_files(
    staged_files: list[dict[str, Any]],
    destination_root: Path,
) -> int:
    destination_root.mkdir(parents=True, exist_ok=True)
    for entry in staged_files:
        target_path = safe_runtime_target_path(destination_root, entry["relative_path"])
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(Path(entry["path"]).read_bytes())
    return len(staged_files)


def clear_directory_contents(directory: Path, *, exclude_paths: set[Path]) -> None:
    if not directory.exists():
        return
    resolved_directory = directory.resolve()
    normalized_excludes = {path.resolve() for path in exclude_paths if path.exists()}
    for child in resolved_directory.iterdir():
        resolved_child = child.resolve()
        if any(
            resolved_child == path or path in resolved_child.parents for path in normalized_excludes
        ):
            continue
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


def runtime_bundle_excluded_paths() -> set[Path]:
    settings = get_settings()
    excluded: set[Path] = set()
    database_path = sqlite_database_file_path(settings.database_url)
    if database_path is not None:
        excluded.add(database_path.resolve())
    return excluded


def sqlite_database_file_path(database_url: str) -> Path | None:
    if not database_url.startswith("sqlite:///"):
        return None
    raw_path = database_url.removeprefix("sqlite:///")
    return Path(raw_path).expanduser()


def normalize_bundle_relative_path(value: Any) -> str:
    candidate = str(value or "").replace("\\", "/").strip("/")
    path = Path(candidate)
    if not candidate or path.is_absolute() or ".." in path.parts:
        raise ValueError(f"Invalid runtime bundle relative path: {value!r}")
    return candidate


def safe_runtime_target_path(root: Path, relative_path: str) -> Path:
    resolved_root = root.resolve()
    target = (resolved_root / Path(relative_path)).resolve()
    if resolved_root != target and resolved_root not in target.parents:
        raise ValueError(f"Runtime bundle target escapes data_dir: {relative_path}")
    return target


def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()
