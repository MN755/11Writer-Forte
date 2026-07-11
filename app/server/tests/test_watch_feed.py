from fastapi.testclient import TestClient

from src.db import get_session_factory
from src.models import StorageObjectORM, WatchRunORM


def create_watch(client: TestClient) -> int:
    response = client.post("/api/watches", json={
        "name": "Feed watch", "slug": "feed-watch", "objective": "Track verified updates.",
        "watch_type": "observation_rule", "rule_json": {"mode": "observation_rule"},
    })
    assert response.status_code == 200, response.text
    return response.json()["watch_id"]


def seed_material_run(
    watch_id: int,
    *,
    ordinal: int,
    redaction: str = "public",
    cited: bool = True,
    summary: str | None = None,
) -> int:
    session = get_session_factory()()
    try:
        evidence = {"materiality": {"is_material": True, "update_type": "corroboration", "score": 88}}
        if cited:
            evidence["citations"] = [{"uri": f"https://example.test/source/{ordinal}", "label": "Independent source"}]
        run = WatchRunORM(
            watch_id=watch_id,
            status="completed",
            outcome="change",
            change_detected=True,
            output_summary=summary or f"Verified update {ordinal}",
            evidence_json=evidence,
            metadata_json={"redaction_level": redaction},
        )
        session.add(run)
        session.commit()
        return run.watch_run_id
    finally:
        session.close()


def test_v1_feed_cursor_is_stable_and_citation_gated(client: TestClient) -> None:
    watch_id = create_watch(client)
    first_id = seed_material_run(watch_id, ordinal=1)
    second_id = seed_material_run(watch_id, ordinal=2)
    seed_material_run(watch_id, ordinal=3, cited=False)

    first = client.get("/api/v1/watch-feed/updates", params={"watch_id": watch_id, "limit": 1})
    assert first.status_code == 200, first.text
    page = first.json()
    assert page["items"][0]["event_id"] == f"watch-update-{second_id}"
    assert page["next_cursor"]
    second = client.get("/api/v1/watch-feed/updates", params={"watch_id": watch_id, "limit": 1, "cursor": page["next_cursor"]})
    assert second.status_code == 200
    assert [item["event_id"] for item in second.json()["items"]] == [f"watch-update-{first_id}"]
    assert client.get("/api/v1/watch-feed/updates", params={"cursor": "not-a-cursor"}).status_code == 422


def test_public_feed_redacts_restricted_and_never_exposes_local_artifact_paths(client: TestClient) -> None:
    watch_id = create_watch(client)
    public_id = seed_material_run(watch_id, ordinal=1, summary="See file:///tmp/private-evidence.txt")
    seed_material_run(watch_id, ordinal=2, redaction="restricted")
    response = client.get("/api/v1/watch-feed/updates", params={"view": "public", "watch_id": watch_id})
    assert response.status_code == 200
    assert [item["event_id"] for item in response.json()["items"]] == [f"watch-update-{public_id}"]
    assert "file:" not in response.text
    assert "private-evidence" not in response.text
    assert "restricted" not in response.text
    rss = client.get("/api/v1/watch-feed/rss", params={"watch_id": watch_id})
    assert rss.status_code == 200
    assert "restricted" not in rss.text


def test_report_and_archive_preserve_evidence_without_auto_deletion(client: TestClient) -> None:
    watch_id = create_watch(client)
    run_id = seed_material_run(watch_id, ordinal=1)
    session = get_session_factory()()
    try:
        artifact = StorageObjectORM(object_key=f"watch:{watch_id}:evidence", object_kind="watch_image_artifact", owner_type="watch", owner_id=str(watch_id), object_uri="https://example.test/artifact", content_hash="f" * 64, retention_class="permanent", lifecycle_status="active", metadata_json={"legal_hold": True})
        session.add(artifact)
        session.flush()
        session.get(WatchRunORM, run_id).storage_object_id = artifact.storage_object_id
        session.commit()
        artifact_id = artifact.storage_object_id
    finally:
        session.close()
    archived = client.post(f"/api/v1/watch-feed/watches/{watch_id}/archive")
    assert archived.status_code == 200, archived.text
    assert archived.json()["state"] == "archived"
    reports = client.get(f"/api/v1/watch-feed/watches/{watch_id}/reports")
    assert reports.status_code == 200 and len(reports.json()) == 1
    session = get_session_factory()()
    try:
        assert session.get(StorageObjectORM, artifact_id).lifecycle_status == "archived"
        assert session.get(StorageObjectORM, artifact_id).metadata_json["legal_hold"] is True
    finally:
        session.close()
