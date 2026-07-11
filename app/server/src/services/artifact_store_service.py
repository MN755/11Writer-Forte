"""Local, content-addressed storage for media artifacts.

This module deliberately keeps bytes on disk and keeps a small JSON ledger alongside
them.  It is independent of the SQL storage-object ledger: callers can safely create
an artifact before their database transaction is committed, then persist the returned
manifest as a reference.  The store has no network behaviour and never downloads
models or dependencies.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import threading
import uuid
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

ArtifactRole = Literal["raw", "normalized", "derived", "evidence", "cache"]
VALID_ARTIFACT_ROLES = frozenset({"raw", "normalized", "derived", "evidence", "cache"})
STORE_VERSION = 1
_CHUNK_SIZE = 1024 * 1024


class ArtifactStoreError(RuntimeError):
    """Base error for a local artifact store operation."""


class ArtifactNotFoundError(ArtifactStoreError):
    """Raised when an artifact UID has no ledger record."""


class ArtifactIntegrityError(ArtifactStoreError):
    """Raised when a blob, manifest, or snapshot fails integrity validation."""


class ArtifactImmutableError(ArtifactStoreError):
    """Raised when a protected evidence artifact would be modified or deleted."""


@dataclass(frozen=True)
class ArtifactOwner:
    """A logical owner reference; multiple owners may share one exact-byte blob."""

    owner_type: str
    owner_id: str
    relationship: str = "owns"

    def __post_init__(self) -> None:
        if not self.owner_type.strip() or not self.owner_id.strip():
            raise ValueError("Artifact owners require a non-empty owner_type and owner_id.")

    @property
    def key(self) -> str:
        return f"{self.owner_type}:{self.owner_id}:{self.relationship}"


@dataclass(frozen=True)
class ArtifactRecord:
    artifact_uid: str
    role: ArtifactRole
    sha256: str
    blake3: str
    blake3_algorithm: str
    byte_size: int
    blob_path: str
    media_type: str | None
    original_uri: str | None
    source_at: str | None
    captured_at: str | None
    retention_class: str
    transform_chain: tuple[str, ...]
    provenance: dict[str, Any]
    custody: tuple[dict[str, Any], ...]
    lifecycle_state: str
    immutable: bool
    parent_artifact_uid: str | None
    owner_references: tuple[ArtifactOwner, ...]
    created_at: str
    updated_at: str
    tombstoned_at: str | None = None

    @property
    def reference_count(self) -> int:
        return len(self.owner_references)


@dataclass(frozen=True)
class IntegrityCheck:
    artifact_uid: str
    ok: bool
    error: str | None = None


@dataclass(frozen=True)
class SnapshotResult:
    snapshot_path: Path
    artifact_count: int
    blob_count: int
    manifest_sha256: str


class ArtifactStore:
    """A local append-only artifact ledger backed by SHA-256-addressed blobs.

    The SHA-256 is the physical address and therefore guarantees stable deduplication
    even when the optional :mod:`blake3` package is unavailable.  In that case a
    BLAKE2b-256 digest is recorded with an explicit algorithm marker; callers must
    never mistake it for a real BLAKE3 digest.
    """

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).expanduser().resolve()
        self._lock = threading.RLock()
        self._ensure_layout()

    @property
    def manifest_path(self) -> Path:
        return self.root / "ledger.json"

    @property
    def blobs_dir(self) -> Path:
        return self.root / "hot" / "blobs"

    def ingest_bytes(
        self,
        payload: bytes,
        *,
        role: ArtifactRole,
        owner: ArtifactOwner,
        media_type: str | None = None,
        original_uri: str | None = None,
        source_at: datetime | str | None = None,
        captured_at: datetime | str | None = None,
        retention_class: str = "operational",
        transform_chain: Iterable[str] = (),
        provenance: dict[str, Any] | None = None,
        custody_actor: str = "system",
        parent_artifact_uid: str | None = None,
    ) -> ArtifactRecord:
        """Store bytes once and add a distinct logical artifact ledger record."""
        if not isinstance(payload, bytes):
            raise TypeError("payload must be bytes")
        temp_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(dir=self.root / "spool", delete=False) as handle:
                handle.write(payload)
                temp_path = Path(handle.name)
            return self.ingest_file(
                temp_path,
                role=role,
                owner=owner,
                media_type=media_type,
                original_uri=original_uri,
                source_at=source_at,
                captured_at=captured_at,
                retention_class=retention_class,
                transform_chain=transform_chain,
                provenance=provenance,
                custody_actor=custody_actor,
                parent_artifact_uid=parent_artifact_uid,
            )
        finally:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)

    def ingest_file(
        self,
        source: str | Path,
        *,
        role: ArtifactRole,
        owner: ArtifactOwner,
        media_type: str | None = None,
        original_uri: str | None = None,
        source_at: datetime | str | None = None,
        captured_at: datetime | str | None = None,
        retention_class: str = "operational",
        transform_chain: Iterable[str] = (),
        provenance: dict[str, Any] | None = None,
        custody_actor: str = "system",
        parent_artifact_uid: str | None = None,
    ) -> ArtifactRecord:
        """Hash and atomically add a file without ever putting its bytes in the ledger."""
        self._validate_role(role)
        source_path = Path(source).expanduser().resolve()
        if not source_path.is_file():
            raise ValueError(f"Artifact input is not a file: {source_path}")
        if parent_artifact_uid is not None:
            self.get_artifact(parent_artifact_uid)  # parent links must never dangle.
        sha256, blake3_digest, algorithm, byte_size = _hash_file(source_path)
        destination = self._blob_path(sha256)
        now = _now()
        with self._lock:
            if destination.exists():
                self._assert_blob_integrity(destination, sha256, byte_size)
            else:
                destination.parent.mkdir(parents=True, exist_ok=True)
                temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.part")
                try:
                    shutil.copyfile(source_path, temporary)
                    _fsync_file(temporary)
                    self._assert_blob_integrity(temporary, sha256, byte_size)
                    os.replace(temporary, destination)
                finally:
                    temporary.unlink(missing_ok=True)

            ledger = self._load_ledger()
            artifact_uid = str(uuid.uuid4())
            record = {
                "artifact_uid": artifact_uid,
                "role": role,
                "sha256": sha256,
                "blake3": blake3_digest,
                "blake3_algorithm": algorithm,
                "byte_size": byte_size,
                "blob_path": str(destination.relative_to(self.root).as_posix()),
                "media_type": media_type,
                "original_uri": original_uri,
                "source_at": _timestamp(source_at),
                "captured_at": _timestamp(captured_at),
                "retention_class": retention_class,
                "transform_chain": list(transform_chain),
                "provenance": dict(provenance or {}),
                "custody": [_custody_event("ingested", custody_actor, now)],
                "lifecycle_state": "active",
                "immutable": role == "evidence",
                "parent_artifact_uid": parent_artifact_uid,
                "owner_references": [asdict(owner)],
                "created_at": now,
                "updated_at": now,
                "tombstoned_at": None,
            }
            ledger["artifacts"][artifact_uid] = record
            self._write_ledger(ledger)
            return _record(record)

    def get_artifact(self, artifact_uid: str) -> ArtifactRecord:
        with self._lock:
            ledger = self._load_ledger()
            raw = ledger["artifacts"].get(artifact_uid)
            if raw is None:
                raise ArtifactNotFoundError(f"Unknown artifact UID: {artifact_uid}")
            return _record(raw)

    def blob_reference_count(self, artifact_uid: str) -> int:
        """Return active logical references to the artifact's exact-byte object."""
        with self._lock:
            ledger, raw = self._mutable_record(artifact_uid)
            return sum(
                len(candidate["owner_references"])
                for candidate in ledger["artifacts"].values()
                if candidate["sha256"] == raw["sha256"]
                and candidate["lifecycle_state"] != "tombstoned"
            )

    def blob_owners(self, artifact_uid: str) -> tuple[ArtifactOwner, ...]:
        """Return de-duplicated owners across all active logical references to a blob."""
        with self._lock:
            ledger, raw = self._mutable_record(artifact_uid)
            owners = {
                ArtifactOwner(**owner)
                for candidate in ledger["artifacts"].values()
                if candidate["sha256"] == raw["sha256"]
                and candidate["lifecycle_state"] != "tombstoned"
                for owner in candidate["owner_references"]
            }
            return tuple(sorted(owners, key=lambda owner: owner.key))

    def add_owner(self, artifact_uid: str, owner: ArtifactOwner, *, actor: str = "system") -> ArtifactRecord:
        with self._lock:
            ledger, raw = self._mutable_record(artifact_uid)
            owners = raw["owner_references"]
            owner_data = asdict(owner)
            if owner_data not in owners:
                owners.append(owner_data)
                self._append_custody(raw, "owner_reference_added", actor, {"owner": owner_data})
                self._write_ledger(ledger)
            return _record(raw)

    def remove_owner(
        self,
        artifact_uid: str,
        owner: ArtifactOwner,
        *,
        actor: str = "system",
        auto_tombstone: bool = False,
    ) -> ArtifactRecord:
        """Drop an owner reference; evidence keeps its protected bytes even at zero refs."""
        with self._lock:
            ledger, raw = self._mutable_record(artifact_uid)
            wanted = asdict(owner)
            raw["owner_references"] = [item for item in raw["owner_references"] if item != wanted]
            self._append_custody(raw, "owner_reference_removed", actor, {"owner": wanted})
            if auto_tombstone and not raw["owner_references"] and raw["role"] != "evidence":
                self._tombstone_raw(raw, actor, "unreferenced")
            self._write_ledger(ledger)
            return _record(raw)

    def promote_to_evidence(self, artifact_uid: str, *, actor: str = "system") -> ArtifactRecord:
        """Make an original immutable and permanent; promotion never changes its blob."""
        with self._lock:
            ledger, raw = self._mutable_record(artifact_uid)
            if raw["lifecycle_state"] == "tombstoned":
                raise ArtifactImmutableError("A tombstoned artifact cannot be promoted to evidence.")
            raw["role"] = "evidence"
            raw["retention_class"] = "permanent"
            raw["immutable"] = True
            self._append_custody(raw, "promoted_to_evidence", actor)
            self._write_ledger(ledger)
            return _record(raw)

    def tombstone(self, artifact_uid: str, *, actor: str = "lifecycle", reason: str = "retention") -> ArtifactRecord:
        """Record deletion intent without deleting a protected evidence original."""
        with self._lock:
            ledger, raw = self._mutable_record(artifact_uid)
            if raw["immutable"] or raw["role"] == "evidence":
                raise ArtifactImmutableError("Evidence artifacts cannot be tombstoned automatically or overwritten.")
            self._tombstone_raw(raw, actor, reason)
            self._write_ledger(ledger)
            return _record(raw)

    def sweep_tombstoned_blobs(self, *, actor: str = "lifecycle") -> int:
        """Remove bytes no active artifact references, leaving permanent ledger tombstones."""
        with self._lock:
            ledger = self._load_ledger()
            active_hashes = {
                raw["sha256"]
                for raw in ledger["artifacts"].values()
                if raw["lifecycle_state"] != "tombstoned"
            }
            removed = 0
            for raw in ledger["artifacts"].values():
                if raw["lifecycle_state"] != "tombstoned" or raw["sha256"] in active_hashes:
                    continue
                blob = self.root / raw["blob_path"]
                if blob.exists():
                    self._assert_within_root(blob)
                    blob.unlink()
                    removed += 1
                self._append_custody(raw, "blob_removed", actor)
            if removed:
                self._write_ledger(ledger)
            return removed

    def verify(self, artifact_uid: str | None = None, *, include_tombstones: bool = False) -> list[IntegrityCheck]:
        """Verify ledger paths, sizes, and SHA-256 / BLAKE digest data."""
        with self._lock:
            ledger = self._load_ledger()
            selected = (
                {artifact_uid: ledger["artifacts"].get(artifact_uid)} if artifact_uid else ledger["artifacts"]
            )
            checks: list[IntegrityCheck] = []
            for uid, raw in selected.items():
                if raw is None:
                    raise ArtifactNotFoundError(f"Unknown artifact UID: {uid}")
                if raw["lifecycle_state"] == "tombstoned" and not include_tombstones:
                    continue
                try:
                    blob = self.root / raw["blob_path"]
                    self._assert_within_root(blob)
                    if not blob.is_file():
                        raise ArtifactIntegrityError(f"Missing artifact blob: {blob}")
                    sha256, blake3_digest, _algorithm, size = _hash_file(blob)
                    if size != raw["byte_size"] or sha256 != raw["sha256"]:
                        raise ArtifactIntegrityError(f"SHA-256 or size mismatch for artifact {uid}")
                    # A fallback digest remains verifiable even if blake3 later becomes installed.
                    expected_algorithm = raw["blake3_algorithm"]
                    if expected_algorithm == "blake3" and _algorithm != "blake3":
                        raise ArtifactIntegrityError("BLAKE3 package is required to verify this artifact.")
                    if expected_algorithm == _algorithm and blake3_digest != raw["blake3"]:
                        raise ArtifactIntegrityError(f"Secondary digest mismatch for artifact {uid}")
                    checks.append(IntegrityCheck(uid, True))
                except ArtifactIntegrityError as exc:
                    checks.append(IntegrityCheck(uid, False, str(exc)))
            return checks

    def snapshot(self, destination: str | Path) -> SnapshotResult:
        """Create a byte-faithful, self-validating local snapshot of this store."""
        snapshot_dir = Path(destination).expanduser().resolve()
        if snapshot_dir == self.root or self.root in snapshot_dir.parents:
            raise ValueError("Snapshot destination must not be inside the artifact store.")
        with self._lock:
            failed = [check for check in self.verify(include_tombstones=False) if not check.ok]
            if failed:
                raise ArtifactIntegrityError(f"Refusing snapshot with corrupt store: {failed[0].error}")
            if snapshot_dir.exists() and any(snapshot_dir.iterdir()):
                raise ValueError(f"Snapshot destination is not empty: {snapshot_dir}")
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            ledger_bytes = self.manifest_path.read_bytes()
            (snapshot_dir / "ledger.json").write_bytes(ledger_bytes)
            ledger = self._load_ledger()
            hashes = sorted({raw["sha256"] for raw in ledger["artifacts"].values() if raw["lifecycle_state"] != "tombstoned"})
            for digest in hashes:
                source = self._blob_path(digest)
                target = snapshot_dir / source.relative_to(self.root)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target)
            manifest = {
                "snapshot_version": STORE_VERSION,
                "created_at": _now(),
                "ledger_sha256": hashlib.sha256(ledger_bytes).hexdigest(),
                "blob_sha256s": hashes,
            }
            _atomic_json_write(snapshot_dir / "snapshot.json", manifest)
            return SnapshotResult(snapshot_dir, len(ledger["artifacts"]), len(hashes), manifest["ledger_sha256"])

    def restore(self, snapshot: str | Path, *, replace: bool = False) -> SnapshotResult:
        """Validate then restore a complete snapshot.  Validation happens before writes."""
        snapshot_dir = Path(snapshot).expanduser().resolve()
        manifest_path = snapshot_dir / "snapshot.json"
        ledger_path = snapshot_dir / "ledger.json"
        if not manifest_path.is_file() or not ledger_path.is_file():
            raise ArtifactIntegrityError("Snapshot is missing snapshot.json or ledger.json.")
        manifest = _read_json(manifest_path)
        ledger_bytes = ledger_path.read_bytes()
        if hashlib.sha256(ledger_bytes).hexdigest() != manifest.get("ledger_sha256"):
            raise ArtifactIntegrityError("Snapshot ledger checksum does not match snapshot manifest.")
        candidate = ArtifactStore(snapshot_dir)
        failed = [check for check in candidate.verify(include_tombstones=False) if not check.ok]
        if failed:
            raise ArtifactIntegrityError(f"Snapshot corruption detected: {failed[0].error}")
        with self._lock:
            existing = self._load_ledger()["artifacts"]
            if existing and not replace:
                raise ArtifactStoreError("Refusing to restore into a non-empty store without replace=True.")
            if replace:
                backup = self.root.with_name(f"{self.root.name}.pre-restore-{uuid.uuid4().hex}")
                os.replace(self.root, backup)
                try:
                    shutil.copytree(snapshot_dir, self.root, ignore=shutil.ignore_patterns("snapshot.json"))
                except Exception:
                    os.replace(backup, self.root)
                    raise
                shutil.rmtree(backup)
            else:
                shutil.copytree(snapshot_dir, self.root, dirs_exist_ok=True, ignore=shutil.ignore_patterns("snapshot.json"))
            self._ensure_layout()
            restored = self._load_ledger()
            return SnapshotResult(self.root, len(restored["artifacts"]), len(manifest["blob_sha256s"]), manifest["ledger_sha256"])

    def _ensure_layout(self) -> None:
        for path in (
            self.root / "spool",
            self.root / "transforms",
            self.root / "cache",
            self.root / "quarantine",
            self.root / "hot" / "blobs",
            self.root / "archive",
            self.root / "tombstones",
        ):
            path.mkdir(parents=True, exist_ok=True)
        if not self.manifest_path.exists():
            _atomic_json_write(self.manifest_path, {"store_version": STORE_VERSION, "artifacts": {}})

    def _load_ledger(self) -> dict[str, Any]:
        ledger = _read_json(self.manifest_path)
        if ledger.get("store_version") != STORE_VERSION or not isinstance(ledger.get("artifacts"), dict):
            raise ArtifactIntegrityError("Unsupported or malformed artifact ledger.")
        return ledger

    def _write_ledger(self, ledger: dict[str, Any]) -> None:
        _atomic_json_write(self.manifest_path, ledger)

    def _blob_path(self, sha256: str) -> Path:
        if len(sha256) != 64 or any(char not in "0123456789abcdef" for char in sha256):
            raise ArtifactIntegrityError("Invalid SHA-256 object address.")
        return self.blobs_dir / sha256[:2] / sha256[2:4] / sha256

    def _mutable_record(self, artifact_uid: str) -> tuple[dict[str, Any], dict[str, Any]]:
        ledger = self._load_ledger()
        raw = ledger["artifacts"].get(artifact_uid)
        if raw is None:
            raise ArtifactNotFoundError(f"Unknown artifact UID: {artifact_uid}")
        return ledger, raw

    def _tombstone_raw(self, raw: dict[str, Any], actor: str, reason: str) -> None:
        if raw["lifecycle_state"] != "tombstoned":
            raw["lifecycle_state"] = "tombstoned"
            raw["tombstoned_at"] = _now()
            self._append_custody(raw, "tombstoned", actor, {"reason": reason})

    def _append_custody(self, raw: dict[str, Any], action: str, actor: str, details: dict[str, Any] | None = None) -> None:
        raw["custody"].append(_custody_event(action, actor, _now(), details))
        raw["updated_at"] = _now()

    def _assert_blob_integrity(self, path: Path, sha256: str, byte_size: int) -> None:
        actual_sha, _digest, _algorithm, actual_size = _hash_file(path)
        if actual_sha != sha256 or actual_size != byte_size:
            raise ArtifactIntegrityError(f"Existing blob at {path} does not match its object address.")

    def _assert_within_root(self, path: Path) -> None:
        try:
            path.resolve().relative_to(self.root)
        except ValueError as exc:
            raise ArtifactIntegrityError("Artifact blob path escapes the local store root.") from exc

    @staticmethod
    def _validate_role(role: str) -> None:
        if role not in VALID_ARTIFACT_ROLES:
            raise ValueError(f"Unsupported artifact role '{role}'. Expected one of {sorted(VALID_ARTIFACT_ROLES)}.")


def _hash_file(path: Path) -> tuple[str, str, str, int]:
    sha256 = hashlib.sha256()
    try:
        import blake3  # type: ignore[import-not-found]

        secondary: Any = blake3.blake3()
        algorithm = "blake3"
    except ImportError:
        secondary = hashlib.blake2b(digest_size=32)
        algorithm = "blake2b-256-fallback"
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(_CHUNK_SIZE):
            size += len(chunk)
            sha256.update(chunk)
            secondary.update(chunk)
    return sha256.hexdigest(), secondary.hexdigest(), algorithm, size


def _record(raw: dict[str, Any]) -> ArtifactRecord:
    return ArtifactRecord(
        artifact_uid=raw["artifact_uid"], role=raw["role"], sha256=raw["sha256"],
        blake3=raw["blake3"], blake3_algorithm=raw["blake3_algorithm"], byte_size=raw["byte_size"],
        blob_path=raw["blob_path"], media_type=raw.get("media_type"), original_uri=raw.get("original_uri"),
        source_at=raw.get("source_at"), captured_at=raw.get("captured_at"),
        retention_class=raw["retention_class"], transform_chain=tuple(raw.get("transform_chain", [])),
        provenance=dict(raw.get("provenance", {})), custody=tuple(raw.get("custody", [])),
        lifecycle_state=raw["lifecycle_state"], immutable=bool(raw["immutable"]),
        parent_artifact_uid=raw.get("parent_artifact_uid"),
        owner_references=tuple(ArtifactOwner(**owner) for owner in raw.get("owner_references", [])),
        created_at=raw["created_at"], updated_at=raw["updated_at"], tombstoned_at=raw.get("tombstoned_at"),
    )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _timestamp(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    return value.isoformat() if isinstance(value, datetime) else str(value)


def _custody_event(action: str, actor: str, at: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"action": action, "actor": actor, "at": at, "details": details or {}}


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ArtifactIntegrityError(f"Unreadable JSON manifest {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ArtifactIntegrityError(f"JSON manifest {path} must be an object.")
    return value


def _atomic_json_write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, sort_keys=True, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _fsync_file(path: Path) -> None:
    # Windows refuses FlushFileBuffers for a read-only file descriptor.
    with path.open("rb+") as handle:
        os.fsync(handle.fileno())
