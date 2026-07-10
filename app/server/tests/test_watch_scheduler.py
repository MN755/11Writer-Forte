from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import select

from src.db import get_session_factory
from src.models import AlertORM, CustodyLogORM, ScheduledTaskORM, ScheduledTaskRunORM, WatchRunORM
from src.services.scheduler_service import scheduler_now


def test_due_watch_schedule_writes_scheduler_and_watch_history(
    client: TestClient,
    tmp_path: Path,
) -> None:
    fixture = tmp_path / "scheduled-watch.json"
    fixture.write_text(json.dumps([{"value": "baseline"}]), encoding="utf-8")
    source = client.post(
        "/api/sources",
        json={
            "name": "scheduled-watch-source",
            "source_kind": "local_file",
            "layer_key": "scheduled-watch-layer",
            "target_uri": str(fixture),
            "metadata_json": {"skip_unchanged": True},
        },
    ).json()
    watch = client.post(
        "/api/watches",
        json={
            "name": "Scheduled source delta",
            "slug": "scheduled-source-delta",
            "objective": "Run through the scheduler and retain auditable history.",
            "watch_type": "source_delta",
            "source_id": source["source_id"],
            "rule_json": {"mode": "source_delta"},
        },
    ).json()
    schedule_response = client.post(
        f"/api/watches/{watch['watch_id']}/schedule",
        json={"interval_seconds": 60, "retry_attempts": 2},
    )
    assert schedule_response.status_code == 200
    task_id = schedule_response.json()["task_id"]
    reassignment = client.patch(
        f"/api/scheduler/tasks/{task_id}",
        json={"payload_json": {"watch_id": watch["watch_id"] + 1000, "force": False}},
    )
    assert reassignment.status_code == 409

    mark_task_due(task_id)
    first_kick = client.post("/api/scheduler/run-due")
    assert first_kick.status_code == 200
    assert first_kick.json()["runs_created"] == 1
    first_task_run_id = first_kick.json()["task_run_ids"][0]

    session = get_session_factory()()
    try:
        first_task_run = session.get(ScheduledTaskRunORM, first_task_run_id)
        assert first_task_run is not None
        assert first_task_run.status == "completed"
        assert first_task_run.output_json["watch_outcome"] == "baseline"
        first_watch_run = session.scalar(
            select(WatchRunORM).where(
                WatchRunORM.scheduled_task_run_id == first_task_run_id
            )
        )
        assert first_watch_run is not None
        assert first_watch_run.outcome == "baseline"
    finally:
        session.close()

    fixture.write_text(json.dumps([{"value": "changed"}]), encoding="utf-8")
    mark_task_due(task_id)
    second_kick = client.post("/api/scheduler/run-due")
    assert second_kick.status_code == 200
    second_task_run_id = second_kick.json()["task_run_ids"][0]

    session = get_session_factory()()
    try:
        second_task_run = session.get(ScheduledTaskRunORM, second_task_run_id)
        assert second_task_run is not None
        assert second_task_run.status == "completed"
        assert second_task_run.records_affected == 1
        assert second_task_run.output_json["watch_outcome"] == "change"
        changed_run = session.scalar(
            select(WatchRunORM).where(
                WatchRunORM.scheduled_task_run_id == second_task_run_id
            )
        )
        assert changed_run is not None
        assert changed_run.outcome == "change"
        assert changed_run.alert_id is not None
        assert session.get(AlertORM, changed_run.alert_id) is not None
    finally:
        session.close()


def test_detaching_a_watch_schedule_disables_the_previous_task(
    client: TestClient,
    tmp_path: Path,
) -> None:
    fixture = tmp_path / "detached-watch.json"
    fixture.write_text(json.dumps([{"value": "baseline"}]), encoding="utf-8")
    source = client.post(
        "/api/sources",
        json={
            "name": "detached-watch-source",
            "source_kind": "local_file",
            "layer_key": "detached-watch-layer",
            "target_uri": str(fixture),
        },
    ).json()
    watch = client.post(
        "/api/watches",
        json={
            "name": "Detach scheduled watch",
            "slug": "detach-scheduled-watch",
            "objective": "Avoid zombie scheduler tasks after detachment.",
            "watch_type": "source_delta",
            "source_id": source["source_id"],
            "rule_json": {"mode": "source_delta"},
        },
    ).json()
    task = client.post(
        f"/api/watches/{watch['watch_id']}/schedule",
        json={"interval_seconds": 60},
    ).json()
    detached = client.patch(
        f"/api/watches/{watch['watch_id']}",
        json={"scheduled_task_id": None},
    )
    assert detached.status_code == 200
    assert detached.json()["scheduled_task_id"] is None

    session = get_session_factory()()
    try:
        previous_task = session.get(ScheduledTaskORM, task["task_id"])
        assert previous_task is not None
        assert previous_task.enabled is False
        assert previous_task.next_run_at is None
    finally:
        session.close()

def test_watch_schedule_retries_failures_and_preserves_each_failed_attempt(
    client: TestClient,
    tmp_path: Path,
) -> None:
    missing_path = tmp_path / "not-created.json"
    source = client.post(
        "/api/sources",
        json={
            "name": "failing-scheduled-watch-source",
            "source_kind": "local_file",
            "layer_key": "failing-scheduled-watch-layer",
            "target_uri": str(missing_path),
        },
    ).json()
    watch = client.post(
        "/api/watches",
        json={
            "name": "Failing scheduled source delta",
            "slug": "failing-scheduled-source-delta",
            "objective": "Prove scheduler retries retain failed watch history.",
            "watch_type": "source_delta",
            "source_id": source["source_id"],
            "rule_json": {"mode": "source_delta"},
        },
    ).json()
    schedule = client.post(
        f"/api/watches/{watch['watch_id']}/schedule",
        json={
            "interval_seconds": 60,
            "retry_attempts": 2,
            "retry_backoff_seconds": 0,
        },
    ).json()
    mark_task_due(schedule["task_id"])

    kick = client.post("/api/scheduler/run-due")
    assert kick.status_code == 200
    assert kick.json()["runs_created"] == 1
    task_run_id = kick.json()["task_run_ids"][0]

    session = get_session_factory()()
    try:
        task_run = session.get(ScheduledTaskRunORM, task_run_id)
        assert task_run is not None
        assert task_run.status == "failed"
        assert task_run.output_json["attempt_count"] == 2
        assert len(task_run.output_json["attempt_errors"]) == 2
        failed_watch_runs = list(
            session.scalars(
                select(WatchRunORM)
                .where(WatchRunORM.watch_id == watch["watch_id"])
                .order_by(WatchRunORM.watch_run_id.asc())
            )
        )
        assert len(failed_watch_runs) == 2
        assert all(run.status == "failed" and run.outcome == "failure" for run in failed_watch_runs)
        custody_actions = list(
            session.scalars(
                select(CustodyLogORM.action).where(
                    CustodyLogORM.object_type == "scheduled_task_run",
                    CustodyLogORM.object_id == str(task_run_id),
                )
            )
        )
        assert "task_retry_scheduled" in custody_actions
        assert "task_run_failed" in custody_actions
    finally:
        session.close()


def mark_task_due(task_id: int) -> None:
    session = get_session_factory()()
    try:
        task = session.get(ScheduledTaskORM, task_id)
        assert task is not None
        task.next_run_at = scheduler_now()
        session.commit()
    finally:
        session.close()
