from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient


def create_investigation(client: TestClient) -> dict[str, object]:
    response = client.post(
        "/api/investigations",
        json={
            "slug": "public-logistics-pattern",
            "question": "What do public sources document about the reported logistics pattern?",
            "operator_scope_json": {"public_information_only": True},
            "time_window_json": {"start": "2026-01-01", "end": "2026-07-01"},
            "geography_json": {"country": "Example"},
            "source_policy_snapshot_json": {"robots": "respect", "paywalls": "do_not_bypass"},
            "research_budget_json": {"max_attempts": 12},
            "evidence_threshold_json": {"minimum_independent_sources": 2},
        },
    )
    assert response.status_code == 200, response.text
    return response.json()


def transition(
    client: TestClient, investigation_id: int, state: str, stop_reason: str | None = None
) -> dict[str, object]:
    response = client.post(
        f"/api/investigations/{investigation_id}/transition",
        json={"state": state, "stop_reason": stop_reason},
    )
    assert response.status_code == 200, response.text
    return response.json()


def test_investigation_lifecycle_records_custody_and_alternate_attempt(client: TestClient) -> None:
    investigation = create_investigation(client)
    investigation_id = int(investigation["investigation_id"])
    for state in ("planning", "collecting"):
        transition(client, investigation_id, state)

    attempt_response = client.post(
        f"/api/investigations/{investigation_id}/attempts",
        json={
            "lead_key": "reported-logistics-network",
            "discovery_path": "established_news",
            "source_uri": "https://example.test/unavailable",
            "reason": "unavailable",
            "alternate_eligible": True,
            "alternate_discovery_path": "official_public_data",
            "alternate_source_uri": "https://example.test/official",
            "error_text": "Source timed out.",
        },
    )
    assert attempt_response.status_code == 200, attempt_response.text
    assert attempt_response.json()["alternate_queued"] is True

    attempts = client.get(f"/api/investigations/{investigation_id}/attempts")
    assert attempts.status_code == 200
    assert [row["status"] for row in attempts.json()] == ["completed", "queued"]
    assert client.get(f"/api/investigations/{investigation_id}").json()["state"] == "collecting"

    custody = client.get(
        "/api/custody/logs", params={"object_type": "investigation", "object_id": investigation_id}
    )
    assert custody.status_code == 200
    actions = {row["action"] for row in custody.json()}
    assert {
        "investigation_created",
        "investigation_state_changed",
        "investigation_discovery_attempt_recorded",
    }.issubset(actions)


def test_investigation_archiving_preserves_reports_and_promoted_evidence(
    client: TestClient, tmp_path: Path
) -> None:
    investigation = create_investigation(client)
    investigation_id = int(investigation["investigation_id"])
    for state in ("planning", "collecting", "normalizing", "corroborating", "reporting", "ready"):
        transition(client, investigation_id, state)

    report = client.post(
        f"/api/investigations/{investigation_id}/reports",
        json={
            "report_text": "A short cited report.",
            "citations_json": [{"url": "https://example.test/source"}],
        },
    )
    assert report.status_code == 200, report.text

    raw_path = tmp_path / "raw.txt"
    raw_path.write_text("raw capture", encoding="utf-8")
    raw = client.post(
        "/api/storage/objects",
        json={
            "object_key": "investigation:raw:1",
            "object_kind": "raw_capture",
            "owner_type": "investigation",
            "owner_id": str(investigation_id),
            "object_uri": raw_path.resolve().as_uri(),
            "metadata_json": {"data_role": "raw"},
        },
    )
    assert raw.status_code == 200, raw.text

    evidence_path = tmp_path / "evidence.txt"
    evidence_path.write_text("promoted evidence", encoding="utf-8")
    evidence = client.post(
        "/api/storage/objects",
        json={
            "object_key": "investigation:evidence:1",
            "object_kind": "source_capture",
            "owner_type": "investigation",
            "owner_id": str(investigation_id),
            "object_uri": evidence_path.resolve().as_uri(),
            "metadata_json": {"data_role": "raw"},
        },
    )
    assert evidence.status_code == 200, evidence.text
    promotion = client.post(
        f"/api/investigations/{investigation_id}/evidence",
        json={
            "storage_object_id": evidence.json()["storage_object_id"],
            "rationale": "Directly supports finding.",
        },
    )
    assert promotion.status_code == 200, promotion.text

    archive = client.post(
        f"/api/investigations/{investigation_id}/archive", params={"stop_reason": "review complete"}
    )
    assert archive.status_code == 200, archive.text
    archived = archive.json()
    assert archived["investigation"]["state"] == "archived"
    assert archived["retained_report_count"] == 1
    assert archived["retained_evidence_count"] == 1
    assert raw.json()["storage_object_id"] in archived["archived_raw_storage_object_ids"]

    reports = client.get(f"/api/investigations/{investigation_id}/reports")
    assert reports.status_code == 200
    assert len(reports.json()) == 1
    storage_rows = client.get(
        "/api/storage/objects", params={"owner_type": "investigation", "owner_id": investigation_id}
    )
    assert storage_rows.status_code == 200
    by_id = {row["storage_object_id"]: row for row in storage_rows.json()}
    assert by_id[raw.json()["storage_object_id"]]["lifecycle_status"] == "archived"
    assert by_id[evidence.json()["storage_object_id"]]["retention_class"] == "permanent"


def test_investigation_snapshot_round_trip(client: TestClient) -> None:
    investigation = create_investigation(client)
    investigation_id = int(investigation["investigation_id"])
    transition(client, investigation_id, "planning")
    report = client.post(
        f"/api/investigations/{investigation_id}/reports",
        json={"report_text": "Snapshot report."},
    )
    assert report.status_code == 200

    snapshot_response = client.get("/api/operations/runtime/export")
    assert snapshot_response.status_code == 200, snapshot_response.text
    snapshot = snapshot_response.json()
    assert snapshot["snapshot_version"] == 3
    assert snapshot["investigations"]
    assert snapshot["investigation_report_versions"]

    restore = client.post(
        "/api/operations/runtime/restore", params={"replace_existing": "true"}, json=snapshot
    )
    assert restore.status_code == 200, restore.text
    restored = client.get(f"/api/investigations/{investigation_id}")
    assert restored.status_code == 200
    assert restored.json()["question"] == investigation["question"]
    restored_reports = client.get(f"/api/investigations/{investigation_id}/reports")
    assert restored_reports.status_code == 200
    assert restored_reports.json()[0]["report_text"] == "Snapshot report."


def test_monitor_contract_is_data_only_and_requires_monitoring_state(client: TestClient) -> None:
    investigation = create_investigation(client)
    investigation_id = int(investigation["investigation_id"])
    unavailable = client.get(f"/api/investigations/{investigation_id}/monitor-contract")
    assert unavailable.status_code == 409

    for state in (
        "planning",
        "collecting",
        "normalizing",
        "corroborating",
        "reporting",
        "ready",
        "monitoring",
    ):
        transition(client, investigation_id, state)
    contract = client.get(f"/api/investigations/{investigation_id}/monitor-contract")
    assert contract.status_code == 200
    payload = contract.json()
    assert payload["contract_version"] == "investigation-monitor/v1"
    assert payload["investigation_id"] == investigation_id
    assert "schedule" not in payload
