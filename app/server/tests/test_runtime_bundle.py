from __future__ import annotations

import hashlib
import json
from pathlib import Path
import zipfile

from fastapi.testclient import TestClient

from src.config import get_settings
from src.db import get_session_factory
from src.models import StorageObjectORM, WatchORM, WatchRunORM
from src.services.runtime_bundle_service import export_runtime_bundle, restore_runtime_bundle
from src.services.storage_service import register_storage_object


def test_runtime_bundle_round_trip_restores_watch_image_bytes(
    client: TestClient,
    tmp_path: Path,
) -> None:
    del client
    session = get_session_factory()()
    try:
        watch = WatchORM(
            name="Piston Peak construction images",
            slug="piston-peak-construction-images",
            objective="Retain materially new construction images.",
            description="Controlled bundle round-trip fixture.",
            watch_type="image_change",
            state="enabled",
            rule_json={
                "mode": "image_change",
                "comparison": "sha256",
                "alert_on_initial": False,
                "accepted_media_types": ["image/jpeg"],
                "retention_class": "permanent",
            },
            interval_seconds=300,
            severity="warning",
            notification_policy_json={
                "api_enabled": True,
                "rss_enabled": True,
                "analysis_on_change": False,
            },
            baseline_json={"sha256": "pending"},
            dedupe_json={},
            metadata_json={"fixture": True},
            provenance_json={"created_by": "runtime_bundle_test"},
        )
        session.add(watch)
        session.flush()
        watch_run = WatchRunORM(
            watch_id=watch.watch_id,
            status="completed",
            outcome="change",
            change_detected=True,
            baseline_initialized=False,
            dedupe_key="watch:bundle:test",
            evidence_json={"kind": "image", "sha256": "pending"},
            checkpoint_before_json={"sha256": "old"},
            checkpoint_after_json={"sha256": "pending"},
            output_summary="A changed image was retained.",
            metadata_json={"fixture": True},
        )
        session.add(watch_run)
        session.flush()

        payload = b"\xff\xd8\xff\xe0forte-watch-image\xff\xd9"
        evidence_path = (
            get_settings().data_dir_effective
            / "artifacts"
            / "watch-evidence"
            / str(watch.watch_id)
            / "fixture.jpg"
        )
        evidence_path.parent.mkdir(parents=True, exist_ok=True)
        evidence_path.write_bytes(payload)
        storage_object = register_storage_object(
            session,
            object_key=f"watch:{watch.watch_id}:image:fixture",
            object_kind="watch_image_evidence",
            owner_type="watch_run",
            owner_id=str(watch_run.watch_run_id),
            object_uri=str(evidence_path),
            source_uri="http://127.0.0.1:9999/fixture.jpg",
            content_hash=None,
            media_type="image/jpeg",
            storage_tier="hot",
            retention_class="permanent",
            byte_size=len(payload),
            metadata_json={"watch_id": watch.watch_id},
            actor="runtime_bundle_test",
        )
        watch_run.storage_object_id = storage_object.storage_object_id
        session.commit()
        watch_id = watch.watch_id
        watch_run_id = watch_run.watch_run_id
        storage_object_id = storage_object.storage_object_id

        bundle_path = tmp_path / "exports" / "runtime-bundle.zip"
        exported = export_runtime_bundle(
            session,
            output_path=bundle_path,
            actor="runtime_bundle_test",
        )
        assert bundle_path.exists()
        assert exported["manifest"]["format_version"] == "1"
        assert len(exported["manifest"]["evidence"]) == 1

        evidence_path.write_bytes(b"corrupted after export")
        session.expunge_all()
        restored = restore_runtime_bundle(
            session,
            bundle_path=bundle_path,
            replace_existing=True,
            actor="runtime_bundle_test",
        )
        assert restored["restored_evidence_count"] == 1
        restored_storage = session.get(StorageObjectORM, storage_object_id)
        assert restored_storage is not None
        restored_path = Path(restored_storage.object_uri)
        assert restored_path.exists()
        assert restored_path.read_bytes() == payload
        assert session.get(WatchORM, watch_id) is not None
        restored_run = session.get(WatchRunORM, watch_run_id)
        assert restored_run is not None
        assert restored_run.storage_object_id == storage_object_id
    finally:
        session.close()


def test_runtime_bundle_operations_routes_export_and_restore_empty_evidence(
    client: TestClient,
    tmp_path: Path,
) -> None:
    bundle_path = tmp_path / "api-exports" / "runtime-bundle.zip"
    exported = client.post(
        "/api/operations/runtime/bundle/export",
        json={"output_path": str(bundle_path)},
    )
    assert exported.status_code == 200
    export_payload = exported.json()
    assert export_payload["output_path"] == str(bundle_path.resolve())
    assert export_payload["manifest"]["format_version"] == "1"
    assert export_payload["manifest"]["evidence"] == []
    assert export_payload["storage_object"]["object_kind"] == "runtime_bundle_export"

    restored = client.post(
        "/api/operations/runtime/bundle/restore",
        json={"input_path": str(bundle_path), "replace_existing": True},
    )
    assert restored.status_code == 200
    restore_payload = restored.json()
    assert restore_payload["bundle_path"] == str(bundle_path.resolve())
    assert restore_payload["restored_evidence_count"] == 0
    assert restore_payload["restore_result"]["replaced_existing"] is True


def test_invalid_bundle_evidence_is_rejected_before_replacing_runtime(
    client: TestClient,
    tmp_path: Path,
) -> None:
    bundle_path = tmp_path / "invalid-evidence-bundle.zip"
    exported = client.post(
        "/api/operations/runtime/bundle/export",
        json={"output_path": str(bundle_path)},
    )
    assert exported.status_code == 200
    before_snapshot = client.get("/api/operations/runtime/export").json()
    before_storage_count = len(before_snapshot["storage_objects"])

    fake_payload = b"not-a-real-ledger-object"
    fake_archive_path = "evidence/999999/fake.jpg"
    with zipfile.ZipFile(bundle_path, mode="r") as archive:
        snapshot_bytes = archive.read("snapshot.json")
        manifest = json.loads(archive.read("manifest.json"))
    manifest["evidence"] = [
        {
            "storage_object_id": 999999,
            "archive_path": fake_archive_path,
            "sha256": hashlib.sha256(fake_payload).hexdigest(),
            "byte_size": len(fake_payload),
            "media_type": "image/jpeg",
            "source_uri": "https://example.test/fake.jpg",
            "original_object_uri": "/not/a/real/object.jpg",
        }
    ]
    with zipfile.ZipFile(bundle_path, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("snapshot.json", snapshot_bytes)
        archive.writestr("manifest.json", json.dumps(manifest))
        archive.writestr(fake_archive_path, fake_payload)

    rejected = client.post(
        "/api/operations/runtime/bundle/restore",
        json={"input_path": str(bundle_path), "replace_existing": True},
    )
    assert rejected.status_code == 409
    assert "absent from snapshot" in rejected.json()["detail"]["message"]
    after_snapshot = client.get("/api/operations/runtime/export").json()
    assert len(after_snapshot["storage_objects"]) >= before_storage_count
