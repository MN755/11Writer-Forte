from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta, timezone

from src.config import reset_settings_cache
from src.services import storage_backends
from fastapi.testclient import TestClient


def test_storage_object_lifecycle_api(client: TestClient) -> None:
    create_response = client.post(
        "/api/storage/objects",
        json={
            "object_key": "manual:test:1",
            "object_kind": "report_export",
            "owner_type": "event",
            "owner_id": "77",
            "object_uri": "file:///tmp/report-77.json",
            "storage_tier": "hot",
            "retention_class": "operational",
            "lifecycle_status": "active",
            "metadata_json": {"scope": "test"},
        },
    )
    assert create_response.status_code == 200
    created = create_response.json()
    assert created["storage_object_id"] >= 1
    assert created["retention_class"] == "operational"
    assert created["lifecycle_status"] == "active"
    assert created["expires_at"] is not None

    list_response = client.get(
        "/api/storage/objects",
        params={"owner_type": "event", "owner_id": "77"},
    )
    assert list_response.status_code == 200
    listed = list_response.json()
    assert len(listed) == 1
    assert listed[0]["object_key"] == "manual:test:1"

    promote_response = client.patch(
        f"/api/storage/objects/{created['storage_object_id']}/promote",
        json={
            "storage_tier": "archive",
            "retention_class": "permanent",
            "promoted_by_type": "event",
            "promoted_by_id": "77",
            "metadata_json": {"reason": "preserve"},
        },
    )
    assert promote_response.status_code == 200
    promoted = promote_response.json()
    assert promoted["storage_tier"] == "archive"
    assert promoted["retention_class"] == "permanent"
    assert promoted["lifecycle_status"] == "promoted"
    assert promoted["promoted_by_type"] == "event"
    assert promoted["expires_at"] is None

    transition_response = client.patch(
        f"/api/storage/objects/{created['storage_object_id']}/transition",
        json={
            "lifecycle_status": "archived",
            "storage_tier": "archive",
            "metadata_json": {"archived_by": "test"},
        },
    )
    assert transition_response.status_code == 200
    transitioned = transition_response.json()
    assert transitioned["lifecycle_status"] == "archived"
    assert transitioned["storage_tier"] == "archive"

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    custody_rows = custody_response.json()
    assert any(row["action"] == "storage_registered" for row in custody_rows)
    assert any(row["action"] == "storage_promoted" for row in custody_rows)
    assert any(row["action"] == "storage_transitioned" for row in custody_rows)


def test_storage_report_and_lifecycle_sweep_api(client: TestClient) -> None:
    now = datetime.now(timezone.utc)
    expired_response = client.post(
        "/api/storage/objects",
        json={
            "object_key": "manual:expired:1",
            "object_kind": "raw_payload",
            "owner_type": "source_run",
            "owner_id": "501",
            "object_uri": "file:///tmp/source-run-501.json",
            "storage_tier": "warm",
            "retention_class": "operational",
            "lifecycle_status": "active",
            "expires_at": (now - timedelta(hours=2)).isoformat(),
        },
    )
    assert expired_response.status_code == 200
    expired_object_id = expired_response.json()["storage_object_id"]

    future_response = client.post(
        "/api/storage/objects",
        json={
            "object_key": "manual:future:1",
            "object_kind": "raw_payload",
            "owner_type": "source_run",
            "owner_id": "502",
            "object_uri": "file:///tmp/source-run-502.json",
            "storage_tier": "warm",
            "retention_class": "investigative",
            "lifecycle_status": "active",
            "expires_at": (now + timedelta(hours=6)).isoformat(),
        },
    )
    assert future_response.status_code == 200

    report_response = client.get("/api/storage/report", params={"limit": 10})
    assert report_response.status_code == 200
    report = report_response.json()
    assert report["total_count"] >= 2
    assert report["expired_count"] >= 1
    assert report["active_count"] >= 1
    assert report["next_expiration_at"] is not None
    assert any(bucket["key"] == "operational" for bucket in report["retention_class_counts"])

    dry_run_response = client.post(
        "/api/storage/sweep",
        params={"retention_class": "operational", "limit": 10, "dry_run": True, "operations": "expire"},
    )
    assert dry_run_response.status_code == 200
    dry_run_payload = dry_run_response.json()
    assert dry_run_payload["dry_run"] is True
    assert dry_run_payload["expired_candidate_count"] >= 1
    assert dry_run_payload["transitioned_count"] == 0
    assert dry_run_payload["operation_results"][0]["operation"] == "expire"
    assert any(
        row["storage_object_id"] == expired_object_id and row["lifecycle_status"] == "active"
        for row in dry_run_payload["candidates"]
    )

    live_response = client.post(
        "/api/storage/sweep",
        params={"retention_class": "operational", "limit": 10, "operations": "expire"},
    )
    assert live_response.status_code == 200
    live_payload = live_response.json()
    assert live_payload["dry_run"] is False
    assert live_payload["transitioned_count"] >= 1
    assert any(
        row["storage_object_id"] == expired_object_id and row["lifecycle_status"] == "expired"
        for row in live_payload["candidates"]
    )

    expired_list_response = client.get(
        "/api/storage/objects",
        params={"owner_type": "source_run", "owner_id": "501", "lifecycle_status": "expired"},
    )
    assert expired_list_response.status_code == 200
    expired_rows = expired_list_response.json()
    assert len(expired_rows) == 1
    assert expired_rows[0]["storage_object_id"] == expired_object_id

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    assert any(row["action"] == "storage_expired" for row in custody_response.json())


def test_storage_archive_verify_rehydrate_prune_and_quarantine_api(
    client: TestClient,
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "var" / "exports" / "evidence.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text('{"case":"alpha"}', encoding="utf-8")
    expired_at = datetime.now(timezone.utc) - timedelta(hours=2)

    create_response = client.post(
        "/api/storage/objects",
        json={
            "object_key": "managed:evidence:1",
            "object_kind": "operations_report_export",
            "owner_type": "operations_report",
            "owner_id": "alpha",
            "object_uri": artifact_path.as_uri(),
            "storage_tier": "warm",
            "retention_class": "operational",
            "lifecycle_status": "active",
            "expires_at": expired_at.isoformat(),
            "metadata_json": {
                "storage_managed": True,
                "archive_eligible": True,
                "prune_eligible": True,
            },
        },
    )
    assert create_response.status_code == 200
    storage_object_id = create_response.json()["storage_object_id"]

    manifest_response = client.get(f"/api/storage/objects/{storage_object_id}/manifest")
    assert manifest_response.status_code == 200
    manifest = manifest_response.json()
    assert manifest["canonical_uri"] == artifact_path.as_uri()
    assert manifest["transfer_status"] == "ready"
    assert manifest["replicas"][0]["backend"] == "local"

    archive_response = client.post(
        f"/api/storage/objects/{storage_object_id}/archive",
        json={"prune_local": False},
    )
    assert archive_response.status_code == 200
    archive_payload = archive_response.json()
    archive_uri = archive_payload["manifest"]["canonical_uri"]
    assert archive_payload["verified"] is True
    assert archive_payload["storage_object"]["lifecycle_status"] == "archived"
    assert archive_payload["manifest"]["transfer_status"] == "verified"
    assert archive_uri.startswith("file://")
    assert archive_uri != artifact_path.as_uri()

    verify_response = client.post(f"/api/storage/objects/{storage_object_id}/verify")
    assert verify_response.status_code == 200
    assert verify_response.json()["manifest"]["transfer_status"] == "verified"

    request_rehydrate = client.post(f"/api/storage/objects/{storage_object_id}/request-rehydrate")
    assert request_rehydrate.status_code == 200
    assert request_rehydrate.json()["manifest"]["transfer_status"] == "rehydration_requested"

    rehydrated_path = tmp_path / "rehydrated" / "evidence.json"
    rehydrate_response = client.post(
        f"/api/storage/objects/{storage_object_id}/rehydrate",
        json={"target_path": str(rehydrated_path), "replace_existing": True},
    )
    assert rehydrate_response.status_code == 200
    assert rehydrate_response.json()["manifest"]["transfer_status"] == "rehydrated"
    assert rehydrated_path.exists()
    assert rehydrated_path.read_text(encoding="utf-8") == '{"case":"alpha"}'

    quarantine_response = client.post(
        f"/api/storage/objects/{storage_object_id}/quarantine",
        json={"reason": "manual_hold"},
    )
    assert quarantine_response.status_code == 200
    assert quarantine_response.json()["storage_object"]["lifecycle_status"] == "quarantined"

    unquarantine_response = client.post(
        f"/api/storage/objects/{storage_object_id}/unquarantine",
        json={"note": "released"},
    )
    assert unquarantine_response.status_code == 200
    assert unquarantine_response.json()["manifest"]["failure_reason"] is None

    prune_response = client.post(f"/api/storage/objects/{storage_object_id}/prune")
    assert prune_response.status_code == 200
    prune_payload = prune_response.json()
    assert prune_payload["manifest"]["transfer_status"] == "pruned"
    assert artifact_path.exists() is False
    assert prune_payload["manifest"]["canonical_uri"] == archive_uri

    final_manifest_response = client.get(f"/api/storage/objects/{storage_object_id}/manifest")
    assert final_manifest_response.status_code == 200
    final_manifest = final_manifest_response.json()
    assert any(replica["role"] == "archive" and replica["status"] == "verified" for replica in final_manifest["replicas"])
    assert any(replica["role"] == "rehydrated" and replica["status"] == "verified" for replica in final_manifest["replicas"])
    assert any(replica["backend"] == "local" and replica["status"] == "pruned" for replica in final_manifest["replicas"])

    report_response = client.get("/api/storage/report")
    assert report_response.status_code == 200
    report_payload = report_response.json()
    assert report_payload["archived_count"] >= 1
    assert report_payload["archive_pending_count"] == 0
    assert report_payload["quarantined_count"] == 0

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    actions = [row["action"] for row in custody_response.json()]
    assert "storage_archive_started" in actions
    assert "storage_archived" in actions
    assert "storage_verified" in actions
    assert "storage_rehydration_requested" in actions
    assert "storage_rehydrated" in actions
    assert "storage_quarantined" in actions
    assert "storage_unquarantined" in actions
    assert "storage_pruned" in actions


class FakeStorageResponse:
    def __init__(self, payload: bytes = b"", headers: dict[str, str] | None = None) -> None:
        self._payload = payload
        self.headers = headers or {}

    def read(self) -> bytes:
        return self._payload

    def __enter__(self) -> "FakeStorageResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def test_storage_archive_and_rehydrate_support_r2_backend(
    client: TestClient,
    tmp_path: Path,
    monkeypatch,
) -> None:
    artifact_path = tmp_path / "var" / "exports" / "cloud-artifact.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_payload = b'{"cloud":"archive"}'
    artifact_path.write_bytes(artifact_payload)

    monkeypatch.setenv("ELEVENWRITER_STORAGE_ARCHIVE_BACKEND", "r2")
    monkeypatch.setenv("ELEVENWRITER_STORAGE_S3_ENDPOINT", "https://acct.r2.cloudflarestorage.com")
    monkeypatch.setenv("ELEVENWRITER_STORAGE_S3_BUCKET", "forte-archive")
    monkeypatch.setenv("ELEVENWRITER_STORAGE_S3_ACCESS_KEY_ID", "r2-key")
    monkeypatch.setenv("ELEVENWRITER_STORAGE_S3_SECRET_ACCESS_KEY", "r2-secret")
    monkeypatch.setenv("ELEVENWRITER_STORAGE_S3_REGION", "auto")
    reset_settings_cache()

    state: dict[str, object] = {"requests": [], "objects": {}}

    def fake_urlopen(request, timeout=0):  # type: ignore[no-untyped-def]
        method = request.get_method()
        url = request.full_url
        headers = {key.lower(): value for key, value in request.header_items()}
        key = url
        requests = state["requests"]
        assert isinstance(requests, list)
        requests.append({"method": method, "url": url, "headers": headers, "timeout": timeout})
        objects = state["objects"]
        assert isinstance(objects, dict)
        if method == "PUT":
            payload = request.data or b""
            objects[key] = {
                "payload": payload,
                "sha256": headers.get("x-amz-meta-sha256", ""),
                "size": str(len(payload)),
            }
            return FakeStorageResponse()
        if method == "HEAD":
            stored = objects[key]
            assert isinstance(stored, dict)
            return FakeStorageResponse(
                headers={
                    "x-amz-meta-sha256": str(stored["sha256"]),
                    "Content-Length": str(stored["size"]),
                    "ETag": '"etag-value"',
                }
            )
        if method == "GET":
            stored = objects[key]
            assert isinstance(stored, dict)
            return FakeStorageResponse(payload=bytes(stored["payload"]))
        raise AssertionError(f"Unexpected storage request: {method} {url}")

    monkeypatch.setattr(storage_backends, "urlopen", fake_urlopen)

    create_response = client.post(
        "/api/storage/objects",
        json={
            "object_key": "managed:r2:1",
            "object_kind": "runtime_snapshot_export",
            "owner_type": "runtime_snapshot",
            "owner_id": "cloud",
            "object_uri": artifact_path.as_uri(),
            "storage_tier": "warm",
            "retention_class": "operational",
            "lifecycle_status": "active",
            "metadata_json": {
                "storage_managed": True,
                "archive_eligible": True,
                "prune_eligible": False,
            },
        },
    )
    assert create_response.status_code == 200
    storage_object_id = create_response.json()["storage_object_id"]

    archive_response = client.post(f"/api/storage/objects/{storage_object_id}/archive")
    assert archive_response.status_code == 200
    archive_payload = archive_response.json()
    assert archive_payload["manifest"]["canonical_uri"].startswith("s3://forte-archive/")

    rehydrated_path = tmp_path / "rehydrated" / "cloud-artifact.json"
    rehydrate_response = client.post(
        f"/api/storage/objects/{storage_object_id}/rehydrate",
        json={"target_path": str(rehydrated_path), "replace_existing": True},
    )
    assert rehydrate_response.status_code == 200
    assert rehydrated_path.read_bytes() == artifact_payload

    requests = state["requests"]
    assert isinstance(requests, list)
    assert any(item["method"] == "PUT" for item in requests)
    assert any(item["method"] == "HEAD" for item in requests)
    assert any(item["method"] == "GET" for item in requests)

    reset_settings_cache()
