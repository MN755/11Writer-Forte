from fastapi.testclient import TestClient

from src.db import get_session_factory
from src.models import ObservationORM


def test_observation_watch_baselines_then_creates_a_deduplicated_alert(client: TestClient) -> None:
    created = client.post(
        "/api/watches",
        json={
            "name": "Local observation watch",
            "slug": "local-observation-watch",
            "objective": "Detect new local observations.",
            "watch_type": "observation_rule",
            "rule_json": {"mode": "observation_rule"},
            "severity": "warning",
        },
    )
    assert created.status_code == 200, created.text
    watch_id = created.json()["watch_id"]

    first = client.post(f"/api/watches/{watch_id}/run")
    assert first.status_code == 200, first.text
    assert first.json()["outcome"] == "baseline"

    session = get_session_factory()()
    try:
        session.add(
            ObservationORM(
                layer_key="watch-test",
                source_type="test",
                content_text="new observation",
                content_json={},
                raw_hash="watch-test-1",
            )
        )
        session.commit()
    finally:
        session.close()

    changed = client.post(f"/api/watches/{watch_id}/run")
    assert changed.status_code == 200, changed.text
    assert changed.json()["outcome"] == "change"
    assert changed.json()["alert_id"] is not None

    repeated = client.post(f"/api/watches/{watch_id}/run")
    assert repeated.status_code == 200
    assert repeated.json()["outcome"] == "no_change"
    alerts = client.get(f"/api/watches/{watch_id}/alerts")
    assert alerts.status_code == 200
    assert len(alerts.json()) == 1

    feed = client.get("/api/watches/feed.rss")
    assert feed.status_code == 200
    assert "11Writer Forte Watch Alerts" in feed.text


def test_watch_schedule_executes_through_scheduler(client: TestClient) -> None:
    created = client.post(
        "/api/watches",
        json={
            "name": "Scheduled Watch",
            "slug": "scheduled-watch",
            "objective": "Run through the scheduler.",
            "watch_type": "observation_rule",
            "rule_json": {"mode": "observation_rule"},
        },
    )
    watch_id = created.json()["watch_id"]
    scheduled = client.post(f"/api/watches/{watch_id}/schedule", json={"interval_seconds": 60})
    assert scheduled.status_code == 200, scheduled.text
    task_run = client.post(f"/api/scheduler/tasks/{scheduled.json()['task_id']}/run")
    assert task_run.status_code == 200, task_run.text
    runs = client.get(f"/api/watches/{watch_id}/runs")
    assert len(runs.json()) == 1
    assert runs.json()[0]["outcome"] == "baseline"
