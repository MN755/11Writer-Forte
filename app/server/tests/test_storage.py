from __future__ import annotations

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
