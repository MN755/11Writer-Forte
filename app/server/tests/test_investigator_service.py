from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from subprocess import CompletedProcess

import pytest

from src.services.codex_agent_service import (
    LUNA_MODEL,
    MINI_MODEL,
    SPARK_MODEL,
    CodexAgentError,
    InvestigatorAttempt,
    InvestigatorBudget,
    InvestigatorGovernor,
    QuotaTelemetry,
    UsageEstimate,
    route_investigator_model,
    run_forte_investigator,
    validate_evidence_packet,
    validate_investigator_recommendation,
)


def _packet() -> dict[str, object]:
    return {
        "investigation_id": "investigation-42",
        "task_type": "classify_ambiguity",
        "normalized_records": [{"record_id": "observation-1", "claim": "A public claim"}],
        "selected_source_excerpts": [
            {
                "artifact_id": "artifact-1",
                "source_id": "source-1",
                "source_uri": "https://example.test/public-report",
                "excerpt": "A selected, bounded public excerpt.",
            }
        ],
        "artifact_ids": ["artifact-1"],
        "research_gaps": ["Need an independent public corroborating source."],
        "policy_constraints": {
            "public_information_only": True,
            "network_access": False,
            "filesystem_access": False,
            "schedule_mutation": False,
            "source_mutation": False,
            "alert_mutation": False,
            "lifecycle_mutation": False,
        },
    }


def _budget() -> InvestigatorBudget:
    return InvestigatorBudget(
        max_calls=2,
        max_tokens=1_000,
        max_seconds=60,
        max_cost_usd=1.0,
        eligible_work_items=20,
        eligible_work_token_capacity=10_000,
        eligible_work_cost_capacity_usd=10.0,
    )


def _governor(*, telemetry: QuotaTelemetry | None = None) -> InvestigatorGovernor:
    return InvestigatorGovernor(
        max_calls=10,
        max_tokens=10_000,
        max_seconds=600,
        max_cost_usd=10.0,
        quota_telemetry=telemetry,
    )


def test_packet_is_bounded_and_rejects_private_or_raw_fields() -> None:
    packet = validate_evidence_packet(_packet())
    assert packet.selected_source_excerpts[0]["artifact_id"] == "artifact-1"

    private_packet = _packet()
    private_packet["normalized_records"] = [{"raw_artifact": "do not pass raw evidence"}]
    with pytest.raises(CodexAgentError, match="Forbidden packet field"):
        validate_evidence_packet(private_packet)

    uncited_packet = _packet()
    uncited_packet["selected_source_excerpts"] = [
        {"artifact_id": "not-in-boundary", "source_id": "s", "excerpt": "Public excerpt"}
    ]
    with pytest.raises(CodexAgentError, match="included artifact ID"):
        validate_evidence_packet(uncited_packet)


def test_fixed_routing_never_quietly_substitutes_models() -> None:
    initial = route_investigator_model([], available_models={SPARK_MODEL, MINI_MODEL, LUNA_MODEL})
    assert initial.model == SPARK_MODEL
    assert initial.reasoning_effort == "low"

    with pytest.raises(CodexAgentError, match="Escalation requires"):
        route_investigator_model(
            [InvestigatorAttempt(model=SPARK_MODEL, uncertainty_id="amb-1")],
            available_models={SPARK_MODEL, MINI_MODEL, LUNA_MODEL},
        )

    mini = route_investigator_model(
        [InvestigatorAttempt(model=SPARK_MODEL, uncertainty_id="amb-1")],
        available_models={SPARK_MODEL, MINI_MODEL, LUNA_MODEL},
        uncertainty_id="amb-1",
        escalation_reason="Ambiguous public entity mention remains unresolved.",
    )
    assert mini.model == MINI_MODEL
    assert mini.reasoning_effort == "medium"

    with pytest.raises(CodexAgentError, match="unavailable; failing closed"):
        route_investigator_model([], available_models={MINI_MODEL, LUNA_MODEL})

    luna = route_investigator_model(
        [InvestigatorAttempt(model=MINI_MODEL, uncertainty_id="amb-1")],
        available_models={SPARK_MODEL, MINI_MODEL, LUNA_MODEL},
        uncertainty_id="amb-1",
        escalation_reason="Mini preserved the same unresolved ambiguity.",
    )
    assert luna.model == LUNA_MODEL


def test_rule_complete_work_never_invokes_codex(tmp_path: Path) -> None:
    called = False

    def runner(*_: object, **__: object) -> CompletedProcess[str]:
        nonlocal called
        called = True
        raise AssertionError("runner must not execute")

    route = route_investigator_model([], available_models={SPARK_MODEL})
    with pytest.raises(CodexAgentError, match="Rule-engine-complete"):
        run_forte_investigator(
            _packet(),
            route,
            workspace=tmp_path / "report-workspace",
            governor=_governor(),
            budget=_budget(),
            estimate=UsageEstimate(tokens=20, seconds=5, cost_usd=0.01),
            runner=runner,
            rule_engine_complete=True,
        )
    assert not called


def test_quota_floor_pauses_llm_without_consuming_deterministic_budget(tmp_path: Path) -> None:
    now = datetime.now(timezone.utc)
    telemetry = QuotaTelemetry(
        remaining_tokens=650,
        window_token_capacity=1_000,
        reset_at=now + timedelta(hours=1),
        observed_at=now,
        reliable=True,
    )
    governor = _governor(telemetry=telemetry)
    budget = _budget()
    route = route_investigator_model([], available_models={SPARK_MODEL})
    called = False

    def runner(*_: object, **__: object) -> CompletedProcess[str]:
        nonlocal called
        called = True
        raise AssertionError("quota rejection must precede the runner")

    with pytest.raises(CodexAgentError, match="60% quota reserve"):
        run_forte_investigator(
            _packet(),
            route,
            workspace=tmp_path / "report-workspace",
            governor=governor,
            budget=budget,
            estimate=UsageEstimate(tokens=100, seconds=5, cost_usd=0.01),
            runner=runner,
        )
    assert not called
    assert budget.calls_used == 0
    assert budget.tokens_used == 0
    assert governor.paused_reason == "quota-floor reserve would be breached"


def test_llm_token_and_cost_share_cannot_exceed_ten_percent() -> None:
    governor = _governor(
        telemetry=QuotaTelemetry(
            remaining_tokens=9_000,
            window_token_capacity=10_000,
            reset_at=datetime.now(timezone.utc) + timedelta(hours=1),
            observed_at=datetime.now(timezone.utc),
            reliable=True,
        )
    )
    budget = _budget()
    budget.eligible_work_token_capacity = 100
    budget.eligible_work_cost_capacity_usd = 1.0

    with pytest.raises(CodexAgentError, match="token usage would exceed 10%"):
        governor.authorize(budget, UsageEstimate(tokens=11, seconds=1, cost_usd=0.01))
    with pytest.raises(CodexAgentError, match="cost usage would exceed 10%"):
        governor.authorize(budget, UsageEstimate(tokens=10, seconds=1, cost_usd=0.11))


def test_recommendation_cannot_create_sources_schedules_alerts_or_lifecycle_changes() -> None:
    packet = validate_evidence_packet(_packet())
    with pytest.raises(CodexAgentError, match="unsupported fields"):
        validate_investigator_recommendation(
            {
                "task_type": packet.task_type,
                "citations": ["artifact-1"],
                "schedule": {"every": "hour"},
            },
            packet=packet,
        )

    with pytest.raises(CodexAgentError, match="outside its packet boundary"):
        validate_investigator_recommendation(
            {
                "task_type": packet.task_type,
                "citations": ["artifact-not-provided"],
            },
            packet=packet,
        )
