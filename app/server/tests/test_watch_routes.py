from __future__ import annotations

import json
from pathlib import Path
from xml.etree import ElementTree

from fastapi.testclient import TestClient


def test_watch_api_lifecycle_run_history_alerts_evidence_and_rss(
    client: TestClient,
    tmp_path: Path,
) -> None:
    fixture = tmp_path / "watch-source.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Initial harbor record",
                    "url": "https://watch.example/initial",
                }
            ]
        ),
        encoding="utf-8",
    )
    source_response = client.post(
        "/api/sources",
        json={
            "name": "watch-route-source",
            "source_kind": "local_file",
            "layer_key": "watch-route-layer",
            "target_uri": str(fixture),
            "metadata_json": {"skip_unchanged": True},
        },
    )
    assert source_response.status_code == 200
    source_id = source_response.json()["source_id"]

    create_response = client.post(
        "/api/watches",
        json={
            "name": "Harbor <Delta> & Watch",
            "slug": "harbor-delta-watch",
            "objective": "Notify when the local harbor source changes.",
            "description": "Controlled API fixture",
            "watch_type": "source_delta",
            "rule_json": {
                "mode": "source_delta",
                "run_source": True,
                "change_basis": "payload_sha256",
                "alert_on_initial": False,
            },
            "source_id": source_id,
            "severity": "warning",
        },
    )
    assert create_response.status_code == 200
    created = create_response.json()
    watch_id = created["watch_id"]
    assert created["state"] == "enabled"
    assert created["notification_policy_json"] == {
        "api_enabled": True,
        "rss_enabled": True,
        "analysis_on_change": False,
    }

    list_response = client.get(
        "/api/watches",
        params={"state": "enabled", "watch_type": "source_delta"},
    )
    assert list_response.status_code == 200
    assert [row["watch_id"] for row in list_response.json()] == [watch_id]

    get_response = client.get(f"/api/watches/{watch_id}")
    assert get_response.status_code == 200
    assert get_response.json()["slug"] == "harbor-delta-watch"

    update_response = client.patch(
        f"/api/watches/{watch_id}",
        json={
            "description": "Updated through the API",
            "severity": "critical",
        },
    )
    assert update_response.status_code == 200
    assert update_response.json()["description"] == "Updated through the API"
    assert update_response.json()["severity"] == "critical"

    pause_response = client.post(f"/api/watches/{watch_id}/pause")
    assert pause_response.status_code == 200
    assert pause_response.json()["state"] == "paused"

    resume_response = client.post(f"/api/watches/{watch_id}/resume")
    assert resume_response.status_code == 200
    assert resume_response.json()["state"] == "enabled"

    schedule_response = client.post(
        f"/api/watches/{watch_id}/schedule",
        json={
            "name": "watch-route-schedule",
            "interval_seconds": 60,
            "retry_attempts": 2,
            "retry_backoff_seconds": 0,
        },
    )
    assert schedule_response.status_code == 200
    schedule = schedule_response.json()
    assert schedule["task_type"] == "watch_evaluate"
    assert schedule["payload_json"] == {"watch_id": watch_id, "force": False}

    baseline_response = client.post(f"/api/watches/{watch_id}/run")
    assert baseline_response.status_code == 200
    baseline = baseline_response.json()
    assert baseline["status"] == "completed"
    assert baseline["outcome"] == "baseline"
    assert baseline["change_detected"] is False

    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Materially changed harbor record",
                    "url": "https://watch.example/changed",
                }
            ]
        ),
        encoding="utf-8",
    )
    change_response = client.post(f"/api/watches/{watch_id}/run")
    assert change_response.status_code == 200
    changed = change_response.json()
    assert changed["status"] == "completed"
    assert changed["outcome"] == "change"
    assert changed["change_detected"] is True
    assert changed["alert_id"] is not None

    global_runs_response = client.get(
        "/api/watches/runs",
        params={"watch_id": watch_id, "status": "completed", "limit": 10},
    )
    assert global_runs_response.status_code == 200
    global_runs = global_runs_response.json()
    assert len(global_runs) == 2
    assert {row["outcome"] for row in global_runs} == {"baseline", "change"}

    watch_runs_response = client.get(f"/api/watches/{watch_id}/runs")
    assert watch_runs_response.status_code == 200
    assert len(watch_runs_response.json()) == 2

    global_alerts_response = client.get(
        "/api/watches/alerts",
        params={"watch_id": watch_id, "status": "open"},
    )
    assert global_alerts_response.status_code == 200
    alerts = global_alerts_response.json()
    assert len(alerts) == 1
    assert alerts[0]["alert_id"] == changed["alert_id"]
    assert alerts[0]["severity"] == "critical"

    watch_alerts_response = client.get(f"/api/watches/{watch_id}/alerts")
    assert watch_alerts_response.status_code == 200
    assert [row["alert_id"] for row in watch_alerts_response.json()] == [changed["alert_id"]]

    evidence_response = client.get(f"/api/watches/{watch_id}/evidence")
    assert evidence_response.status_code == 200
    evidence = evidence_response.json()
    assert isinstance(evidence, list)
    if changed["storage_object_id"] is not None:
        assert changed["storage_object_id"] in {
            row["storage_object_id"] for row in evidence
        }
        assert all(row["content_hash"] for row in evidence)

    feed_response = client.get(
        "/api/watches/feed.rss",
        params={"watch_id": watch_id, "status": "open", "limit": 10},
    )
    assert feed_response.status_code == 200
    assert feed_response.headers["content-type"].startswith("application/rss+xml")
    root = ElementTree.fromstring(feed_response.content)
    assert root.tag == "rss"
    items = root.findall("./channel/item")
    assert len(items) == 1
    assert "Harbor <Delta> & Watch" in " ".join(items[0].itertext())
    assert "Harbor <Delta> & Watch" not in feed_response.text
    assert "&lt;Delta&gt;" in feed_response.text
    assert "&amp; Watch" in feed_response.text
    assert items[0].findtext("link", "").startswith("http://testserver/api/")

    missing_response = client.get("/api/watches/999999")
    assert missing_response.status_code == 404
    assert missing_response.json()["detail"]["watch_id"] == 999999
