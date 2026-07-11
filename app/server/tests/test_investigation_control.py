from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from src.services.investigation_control_service import evaluate_llm_gate, validate_model_assistance


def test_llm_gate_fails_closed_for_stale_telemetry_share_and_reserve() -> None:
    current = datetime.now(timezone.utc)
    assert evaluate_llm_gate(
        telemetry_at=current - timedelta(minutes=16),
        eligible_work_units=100,
        llm_work_units=0,
        requested_llm_work_units=1,
        quota_limit_tokens=10_000,
        used_tokens=0,
        estimated_tokens=1,
        model="gpt-5.3-codex-spark",
        now=current,
    )["reason"] == "stale_quota_telemetry"
    assert evaluate_llm_gate(
        telemetry_at=current,
        eligible_work_units=10,
        llm_work_units=1,
        requested_llm_work_units=1,
        quota_limit_tokens=10_000,
        used_tokens=0,
        estimated_tokens=1,
        model="gpt-5.3-codex-spark",
        now=current,
    )["reason"] == "llm_work_share_limit"
    assert evaluate_llm_gate(
        telemetry_at=current,
        eligible_work_units=100,
        llm_work_units=0,
        requested_llm_work_units=1,
        quota_limit_tokens=10_000,
        used_tokens=3_900,
        estimated_tokens=200,
        model="gpt-5.4-mini",
        now=current,
    )["reason"] == "quota_reserve"


def test_model_assistance_can_only_reference_supplied_evidence() -> None:
    assert validate_model_assistance(
        {"aliases": ["Harbor"], "citations": ["artifact-1"]}, evidence_ids={"artifact-1"}
    )["accepted"] is True
    try:
        validate_model_assistance({"schedule": "hourly", "citations": []}, evidence_ids=set())
    except ValueError as exc:
        assert "control-plane" in str(exc)
    else:
        raise AssertionError("control-plane field was accepted")


def test_investigation_plan_and_readiness_endpoints_are_rule_derived(client: TestClient) -> None:
    created = client.post(
        "/api/investigations",
        json={
            "slug": "orchestrator-control",
            "question": "What do public sources document about Harbor logistics?",
            "operator_scope_json": {"public_information_only": True},
            "source_policy_snapshot_json": {"robots": "respect"},
            "research_budget_json": {"max_attempts": 5},
            "evidence_threshold_json": {"min_promoted_evidence": 1, "min_independent_sources": 2},
        },
    )
    assert created.status_code == 200
    investigation_id = created.json()["investigation_id"]
    plan = client.get(f"/api/investigations/{investigation_id}/plan")
    assert plan.status_code == 200
    assert plan.json()["plan_version"] == "investigation-control/v1"
    readiness = client.get(f"/api/investigations/{investigation_id}/readiness")
    assert readiness.status_code == 200
    assert readiness.json()["next_outcome"] == "continue"
