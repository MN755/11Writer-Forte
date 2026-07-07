from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from fastapi.testclient import TestClient


@contextmanager
def flaky_json_server(payload: list[dict[str, object]]):
    state = {"requests": 0, "last_user_agent": None, "last_header": None}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            state["requests"] += 1
            state["last_user_agent"] = self.headers.get("User-Agent")
            state["last_header"] = self.headers.get("X-Test-Token")
            if state["requests"] == 1:
                self.send_response(503)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(b"temporary failure")
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(payload).encode("utf-8"))

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/feed.json", state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_source_definition_run_creates_import_and_history(client: TestClient, tmp_path: Path) -> None:
    fixture = tmp_path / "source-file.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Source catalog record",
                    "url": "https://source.example.com/1",
                    "lat": 29.76,
                    "lon": -95.36,
                }
            ]
        ),
        encoding="utf-8",
    )

    source_response = client.post(
        "/api/sources",
        json={
            "name": "harbor-source",
            "source_kind": "local_file",
            "layer_key": "marine-track",
            "target_uri": str(fixture),
            "integrity_source": True,
        },
    )
    assert source_response.status_code == 200
    source_id = source_response.json()["source_id"]

    run_response = client.post(f"/api/sources/{source_id}/run")
    assert run_response.status_code == 200
    payload = run_response.json()
    assert payload["status"] == "completed"
    assert payload["records_imported"] == 1
    assert payload["import_run_id"] is not None

    runs_response = client.get("/api/sources/runs")
    assert runs_response.status_code == 200
    assert runs_response.json()[0]["source_id"] == source_id

    imports_response = client.get("/api/imports/runs")
    assert imports_response.status_code == 200
    assert imports_response.json()[0]["records_imported"] == 1

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    custody_rows = custody_response.json()
    assert any(
        row["object_type"] == "source_definition"
        and row["object_id"] == str(source_id)
        and row["action"] == "source_created"
        for row in custody_rows
    )
    assert any(
        row["object_type"] == "source_run"
        and row["action"] == "source_run_started"
        for row in custody_rows
    )
    assert any(
        row["object_type"] == "source_run"
        and row["action"] == "source_run_completed"
        for row in custody_rows
    )


def test_source_sync_schedule_runs_source_definition(client: TestClient, tmp_path: Path) -> None:
    fixture = tmp_path / "scheduled-source.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Scheduled source sync",
                    "url": "https://sync.example.com/1",
                    "lat": 30.0,
                    "lon": -95.0,
                }
            ]
        ),
        encoding="utf-8",
    )

    source_response = client.post(
        "/api/sources",
        json={
            "name": "scheduled-source",
            "source_kind": "local_file",
            "layer_key": "ops-feed",
            "target_uri": str(fixture),
        },
    )
    assert source_response.status_code == 200
    source_id = source_response.json()["source_id"]

    schedule_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "source-sync-task",
            "task_type": "source_sync",
            "interval_seconds": 300,
            "source_id": source_id,
        },
    )
    assert schedule_response.status_code == 200
    task_id = schedule_response.json()["task_id"]

    run_response = client.post(f"/api/scheduler/tasks/{task_id}/run")
    assert run_response.status_code == 200
    assert run_response.json()["records_affected"] == 1

    source_runs = client.get("/api/sources/runs")
    assert source_runs.status_code == 200
    assert source_runs.json()[0]["records_imported"] == 1

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    custody_rows = custody_response.json()
    assert any(
        row["object_type"] == "scheduled_task"
        and row["object_id"] == str(task_id)
        and row["action"] == "task_created"
        for row in custody_rows
    )


def test_http_source_retries_and_records_fetch_metadata(client: TestClient) -> None:
    payload = [
        {
            "title": "Remote source record",
            "url": "https://remote.example.com/1",
            "lat": 29.81,
            "lon": -95.41,
        }
    ]
    with flaky_json_server(payload) as (target_uri, state):
        source_response = client.post(
            "/api/sources",
            json={
                "name": "remote-json-source",
                "source_kind": "http_json",
                "layer_key": "remote-feed",
                "target_uri": target_uri,
                "metadata_json": {
                    "retry_attempts": 2,
                    "retry_backoff_seconds": 0,
                    "request_timeout_seconds": 5,
                    "headers": {"X-Test-Token": "forte"},
                },
            },
        )
        assert source_response.status_code == 200
        source_id = source_response.json()["source_id"]

        run_response = client.post(f"/api/sources/{source_id}/run")
        assert run_response.status_code == 200
        run_payload = run_response.json()
        assert run_payload["status"] == "completed"
        assert run_payload["records_imported"] == 1
        assert run_payload["output_json"]["attempt_count"] == 2
        assert run_payload["output_json"]["http_status"] == 200
        assert run_payload["output_json"]["content_type"] == "application/json"
        assert run_payload["output_json"]["headers"]["X-Test-Token"] == "forte"
        assert run_payload["output_json"]["cached_path"].endswith(".json")

        assert state["requests"] == 2
        assert state["last_header"] == "forte"
        assert state["last_user_agent"] == "11Writer-Forte/0.1 (+headless-source-fetch)"

        custody_response = client.get("/api/custody/logs")
        assert custody_response.status_code == 200
        custody_rows = custody_response.json()
        assert any(
            row["object_type"] == "source_run"
            and row["action"] == "source_payload_materialized"
            and row["details_json"]["attempt_count"] == 2
            for row in custody_rows
        )


def test_source_run_skips_unchanged_payloads(client: TestClient, tmp_path: Path) -> None:
    fixture = tmp_path / "unchanged-source.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Stable payload",
                    "url": "https://stable.example.com/1",
                    "lat": 29.7,
                    "lon": -95.3,
                }
            ]
        ),
        encoding="utf-8",
    )

    source_response = client.post(
        "/api/sources",
        json={
            "name": "stable-source",
            "source_kind": "local_file",
            "layer_key": "stable-feed",
            "target_uri": str(fixture),
        },
    )
    assert source_response.status_code == 200
    source_id = source_response.json()["source_id"]

    first_run = client.post(f"/api/sources/{source_id}/run")
    assert first_run.status_code == 200
    assert first_run.json()["status"] == "completed"
    first_import_run_id = first_run.json()["import_run_id"]

    second_run = client.post(f"/api/sources/{source_id}/run")
    assert second_run.status_code == 200
    second_payload = second_run.json()
    assert second_payload["status"] == "skipped"
    assert second_payload["import_run_id"] is None
    assert second_payload["output_json"]["skip_reason"] == "payload_unchanged"
    assert second_payload["output_json"]["payload_sha256"]

    imports_response = client.get("/api/imports/runs")
    assert imports_response.status_code == 200
    import_runs = imports_response.json()
    assert len(import_runs) == 1
    assert import_runs[0]["import_run_id"] == first_import_run_id

    source_runs_response = client.get("/api/sources/runs")
    assert source_runs_response.status_code == 200
    source_runs = source_runs_response.json()
    assert source_runs[0]["status"] == "skipped"
    assert source_runs[1]["status"] == "completed"

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    assert any(
        row["object_type"] == "source_run"
        and row["action"] == "source_run_skipped"
        for row in custody_response.json()
    )
