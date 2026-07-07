from __future__ import annotations

from datetime import datetime, timedelta, timezone

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
        params={"retention_class": "operational", "limit": 10, "dry_run": True},
    )
    assert dry_run_response.status_code == 200
    dry_run_payload = dry_run_response.json()
    assert dry_run_payload["dry_run"] is True
    assert dry_run_payload["expired_candidate_count"] >= 1
    assert dry_run_payload["transitioned_count"] == 0
    assert any(
        row["storage_object_id"] == expired_object_id and row["lifecycle_status"] == "active"
        for row in dry_run_payload["candidates"]
    )

    live_response = client.post(
        "/api/storage/sweep",
        params={"retention_class": "operational", "limit": 10},
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
