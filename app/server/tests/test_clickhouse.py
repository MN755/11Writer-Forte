from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from src.config import reset_settings_cache
from src.services import clickhouse_service


class FakeResponse:
    def __init__(self, payload: str) -> None:
        self.payload = payload

    def read(self) -> bytes:
        return self.payload.encode("utf-8")

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def test_clickhouse_diagnostics_defaults_to_disabled(client: TestClient) -> None:
    response = client.get("/api/operations/clickhouse")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "disabled"
    assert payload["enabled"] is False
    assert payload["reachable"] is False


def test_clickhouse_sync_and_r2_archive_flow(
    client: TestClient,
    tmp_path: Path,
    monkeypatch,
) -> None:
    fixture = tmp_path / "clickhouse-source.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "ClickHouse sync record",
                    "url": "https://clickhouse.example.com/1",
                    "lat": 29.76,
                    "lon": -95.36,
                    "observed_at": "2026-07-07T02:30:00Z",
                }
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_ENABLED", "true")
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_URL", "http://clickhouse.test:8123")
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_DATABASE", "elevenwriter")
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_USER", "forte")
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_PASSWORD", "secret")
    monkeypatch.setenv(
        "ELEVENWRITER_CLICKHOUSE_R2_ENDPOINT",
        "https://acct.r2.cloudflarestorage.com",
    )
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_R2_BUCKET", "11writer-archive")
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_R2_ACCESS_KEY_ID", "r2-key")
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_R2_SECRET_ACCESS_KEY", "r2-secret")
    monkeypatch.setenv("ELEVENWRITER_CLICKHOUSE_R2_ARCHIVE_PREFIX", "forte-archive")
    reset_settings_cache()

    requests: list[dict[str, object]] = []

    def fake_urlopen(request, timeout=0):  # type: ignore[no-untyped-def]
        body = request.data.decode("utf-8") if request.data else ""
        requests.append(
            {
                "url": request.full_url,
                "method": request.get_method(),
                "body": body,
            }
        )
        if request.full_url.endswith("/ping"):
            return FakeResponse("Ok.\n")
        if "SELECT version()" in body:
            return FakeResponse('{"version":"26.6.1","current_database":"elevenwriter"}\n')
        if "SELECT count(*) AS row_count" in body:
            return FakeResponse('{"row_count":1}\n')
        return FakeResponse("")

    monkeypatch.setattr(clickhouse_service, "urlopen", fake_urlopen)

    import_response = client.post(
        "/api/imports/local",
        json={"source_path": str(fixture), "layer_key": "marine-track"},
    )
    assert import_response.status_code == 200

    status_response = client.get("/api/operations/clickhouse")
    assert status_response.status_code == 200
    status_payload = status_response.json()
    assert status_payload["status"] == "ok"
    assert status_payload["reachable"] is True
    assert status_payload["r2_configured"] is True
    assert status_payload["r2_archive_root"] == "https://acct.r2.cloudflarestorage.com/11writer-archive/forte-archive"

    provision_response = client.post("/api/operations/clickhouse/provision")
    assert provision_response.status_code == 200

    sync_response = client.post("/api/operations/clickhouse/sync", params={"limit": 10})
    assert sync_response.status_code == 200
    sync_payload = sync_response.json()
    assert sync_payload["observation_count"] == 1
    assert sync_payload["storage_object_count"] == 1

    archive_response = client.post("/api/operations/clickhouse/archive", params={"limit": 1})
    assert archive_response.status_code == 200
    archive_payload = archive_response.json()
    assert archive_payload["exported_row_count"] == 1
    assert archive_payload["archive_root_url"].endswith("/11writer-archive/forte-archive")
    assert "INSERT INTO FUNCTION s3(" in archive_payload["sql"]

    config_response = client.get("/api/operations/clickhouse/r2-config")
    assert config_response.status_code == 200
    config_payload = config_response.json()
    assert "<type>s3</type>" in config_payload["storage_xml"]
    assert "storage_policy = 'r2_main'" in config_payload["create_table_sql"]

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    custody_rows = custody_response.json()
    assert any(row["action"] == "clickhouse_provisioned" for row in custody_rows)
    assert any(row["action"] == "clickhouse_synced" for row in custody_rows)
    assert any(row["action"] == "clickhouse_archived_to_r2" for row in custody_rows)

    assert any("/ping" in str(item["url"]) for item in requests)
    assert any("CREATE TABLE IF NOT EXISTS elevenwriter.observation_facts" in str(item["body"]) for item in requests)
    assert any("INSERT INTO elevenwriter.observation_facts FORMAT JSONEachRow" in str(item["body"]) for item in requests)
    assert any("INSERT INTO FUNCTION s3(" in str(item["body"]) for item in requests)
    reset_settings_cache()
