from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from src.services.artifact_store_service import (
    ArtifactImmutableError,
    ArtifactIntegrityError,
    ArtifactOwner,
    ArtifactStore,
)


def owner(identifier: str) -> ArtifactOwner:
    return ArtifactOwner("source_run", identifier)


def test_exact_duplicate_bytes_share_blob_and_keep_multiple_owner_references(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path / "store")
    payload = b"an exact camera still"

    first = store.ingest_bytes(
        payload,
        role="raw",
        owner=owner("one"),
        media_type="image/jpeg",
        original_uri="https://cams.example/one.jpg",
        provenance={"source": "camera-one"},
    )
    second = store.ingest_bytes(
        payload,
        role="raw",
        owner=owner("two"),
        media_type="image/jpeg",
        original_uri="https://cams.example/two.jpg",
    )

    assert first.artifact_uid != second.artifact_uid
    assert first.sha256 == second.sha256 == hashlib.sha256(payload).hexdigest()
    assert first.blob_path == second.blob_path
    assert (store.root / first.blob_path).read_bytes() == payload
    assert len(list(store.blobs_dir.rglob("*"))) == 3  # two shard dirs + exactly one object.
    assert store.add_owner(first.artifact_uid, owner("three")).reference_count == 2
    assert store.blob_reference_count(first.artifact_uid) == 3
    assert {item.owner_id for item in store.blob_owners(first.artifact_uid)} == {"one", "two", "three"}
    assert all(check.ok for check in store.verify())


def test_evidence_promotion_locks_original_and_preserves_derivative_lineage(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path / "store")
    original = store.ingest_bytes(b"original evidence bytes", role="raw", owner=owner("original"))
    derivative = store.ingest_bytes(
        b"thumbnail bytes",
        role="derived",
        owner=owner("thumb"),
        parent_artifact_uid=original.artifact_uid,
        transform_chain=["image.decode:v1", "thumbnail:320px"],
    )

    evidence = store.promote_to_evidence(original.artifact_uid, actor="investigator")

    assert evidence.role == "evidence"
    assert evidence.immutable is True
    assert evidence.retention_class == "permanent"
    assert derivative.parent_artifact_uid == original.artifact_uid
    with pytest.raises(ArtifactImmutableError):
        store.tombstone(original.artifact_uid)
    assert (store.root / evidence.blob_path).read_bytes() == b"original evidence bytes"
    assert any(item["action"] == "promoted_to_evidence" for item in evidence.custody)


def test_snapshot_restore_round_trip_and_missing_or_corrupt_blob_detection(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path / "store")
    kept = store.ingest_bytes(b"keep me", role="raw", owner=owner("keep"), provenance={"source": "fixture"})
    store.ingest_bytes(b"another object", role="normalized", owner=owner("normal"))
    snapshot = store.snapshot(tmp_path / "snapshot")

    assert snapshot.artifact_count == 2
    assert snapshot.blob_count == 2
    assert (snapshot.snapshot_path / "ledger.json").is_file()
    copied = ArtifactStore(tmp_path / "restored")
    restored = copied.restore(snapshot.snapshot_path)
    assert restored.artifact_count == 2
    assert copied.get_artifact(kept.artifact_uid).provenance == {"source": "fixture"}
    assert all(check.ok for check in copied.verify())

    missing_blob = snapshot.snapshot_path / kept.blob_path
    missing_blob.unlink()
    with pytest.raises(ArtifactIntegrityError, match="corruption|Missing"):
        ArtifactStore(tmp_path / "restore-missing").restore(snapshot.snapshot_path)

    # Rebuild a fresh snapshot then flip bytes while keeping both manifests intact.
    fresh = store.snapshot(tmp_path / "snapshot-corrupt")
    corrupt_blob = fresh.snapshot_path / kept.blob_path
    corrupt_blob.write_bytes(b"not the originally addressed bytes")
    with pytest.raises(ArtifactIntegrityError, match="corruption|mismatch"):
        ArtifactStore(tmp_path / "restore-corrupt").restore(fresh.snapshot_path)


def test_tombstones_remain_after_unprotected_blob_collection(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path / "store")
    record = store.ingest_bytes(b"short lived", role="cache", owner=owner("cache"))

    tombstoned = store.tombstone(record.artifact_uid, reason="cache_expired")
    assert tombstoned.lifecycle_state == "tombstoned"
    assert tombstoned.tombstoned_at is not None
    assert store.sweep_tombstoned_blobs() == 1
    assert not (store.root / record.blob_path).exists()
    retained_tombstone = store.get_artifact(record.artifact_uid)
    assert retained_tombstone.lifecycle_state == "tombstoned"
    ledger = json.loads(store.manifest_path.read_text(encoding="utf-8"))
    assert ledger["artifacts"][record.artifact_uid]["custody"][-1]["action"] == "blob_removed"
