from __future__ import annotations

import hashlib
import json
import math
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Collection, Mapping, Sequence
from uuid import uuid4

from sqlalchemy.orm import Session

from src.config import Settings, get_settings
from src.models import CustodyLogORM
from src.services.storage_service import register_storage_object


class CodexAgentError(ValueError):
    pass


# This module has two intentionally separate entry points.  The older
# ``run_codex_research`` adapter remains for the operator-facing legacy command;
# the investigator entry point below accepts only an already-preprocessed packet.
# In particular, it never receives a database session or a runtime controller.
SPARK_MODEL = "gpt-5.3-codex-spark"
MINI_MODEL = "gpt-5.4-mini"
LUNA_MODEL = "gpt-5.6-luna"
INVESTIGATOR_MODEL_ORDER = (SPARK_MODEL, MINI_MODEL, LUNA_MODEL)
INVESTIGATOR_TASK_TYPES = frozenset(
    {
        "classify_ambiguity",
        "propose_aliases",
        "research_gaps",
        "resolve_entity_mentions",
        "assess_image_candidate",
        "draft_cited_narrative",
    }
)
_PACKET_KEYS = frozenset(
    {
        "investigation_id",
        "task_type",
        "normalized_records",
        "selected_source_excerpts",
        "artifact_ids",
        "research_gaps",
        "policy_constraints",
    }
)
_EXCERPT_KEYS = frozenset(
    {
        "artifact_id",
        "source_id",
        "source_uri",
        "publication_time",
        "capture_time",
        "excerpt",
        "trust_tier",
        "provenance",
    }
)
_FORBIDDEN_PACKET_KEY_PARTS = frozenset(
    {
        "password",
        "secret",
        "token",
        "cookie",
        "authorization",
        "credential",
        "private",
        "email",
        "phone",
        "ssn",
        "raw_artifact",
        "raw_bytes",
        "file_path",
        "filesystem",
        "network",
        "schedule",
        "alert",
        "lifecycle",
        "source_create",
    }
)
_FORBIDDEN_RECOMMENDATION_KEYS = frozenset(
    {
        "schedule",
        "schedules",
        "source",
        "sources",
        "alert",
        "alerts",
        "lifecycle",
        "state",
        "archive",
        "monitor",
        "delivery",
        "retention",
        "network",
        "file",
        "filesystem",
    }
)
_FORBIDDEN_POLICY_KEY_PARTS = frozenset(
    {
        "password",
        "secret",
        "token",
        "cookie",
        "authorization",
        "credential",
        "private",
        "email",
        "phone",
        "ssn",
        "raw",
        "file_path",
    }
)


@dataclass(frozen=True)
class EvidencePacket:
    """The only information that may be passed to a Forte investigator call."""

    investigation_id: str
    task_type: str
    normalized_records: tuple[Mapping[str, Any], ...]
    selected_source_excerpts: tuple[Mapping[str, Any], ...]
    artifact_ids: tuple[str, ...]
    research_gaps: tuple[str, ...]
    policy_constraints: Mapping[str, Any]

    def as_prompt_payload(self) -> dict[str, Any]:
        return {
            "investigation_id": self.investigation_id,
            "task_type": self.task_type,
            "normalized_records": [dict(record) for record in self.normalized_records],
            "selected_source_excerpts": [
                dict(excerpt) for excerpt in self.selected_source_excerpts
            ],
            "artifact_ids": list(self.artifact_ids),
            "research_gaps": list(self.research_gaps),
            "policy_constraints": dict(self.policy_constraints),
        }


@dataclass(frozen=True)
class InvestigatorRoute:
    model: str
    reasoning_effort: str
    escalation_reason: str
    uncertainty_id: str | None = None


@dataclass(frozen=True)
class InvestigatorAttempt:
    model: str
    uncertainty_id: str | None
    failed: bool = False


@dataclass
class InvestigatorBudget:
    """Hard, caller-owned limits for one investigation."""

    max_calls: int
    max_tokens: int
    max_seconds: float
    max_cost_usd: float
    eligible_work_items: int
    # Forte must supply denominators from the deterministic research plan before an
    # LLM call.  ``None`` means the usage ratio is not auditable and therefore pauses
    # rather than guessing.
    eligible_work_token_capacity: int | None = None
    eligible_work_cost_capacity_usd: float | None = None
    calls_used: int = 0
    tokens_used: int = 0
    seconds_used: float = 0.0
    cost_used_usd: float = 0.0


@dataclass(frozen=True)
class UsageEstimate:
    tokens: int
    seconds: float
    cost_usd: float


@dataclass(frozen=True)
class QuotaTelemetry:
    """Observed provider capacity for the current reset window."""

    remaining_tokens: int
    window_token_capacity: int
    reset_at: datetime
    observed_at: datetime
    reliable: bool


@dataclass
class CalibrationMode:
    """Explicit, operator-approved capacity calibration.  It does not imply a route."""

    operator_approved: bool
    window_count: int
    started_at: datetime
    observations: list[QuotaTelemetry]

    @classmethod
    def begin(
        cls,
        *,
        operator_approved: bool,
        window_count: int = 1,
        started_at: datetime | None = None,
    ) -> "CalibrationMode":
        if not operator_approved:
            raise CodexAgentError("Quota calibration requires explicit operator approval.")
        if window_count not in {1, 2}:
            raise CodexAgentError("Calibration may use exactly one or two five-hour windows.")
        return cls(
            operator_approved=True,
            window_count=window_count,
            started_at=started_at or datetime.now(timezone.utc),
            observations=[],
        )

    @property
    def ends_at(self) -> datetime:
        return self.started_at + timedelta(hours=5 * self.window_count)

    def record_observation(self, telemetry: QuotaTelemetry) -> None:
        if not self.operator_approved:
            raise CodexAgentError("Unapproved calibration cannot record quota telemetry.")
        if telemetry.observed_at < self.started_at or telemetry.observed_at > self.ends_at:
            raise CodexAgentError("Calibration observation falls outside its approved window.")
        self.observations.append(telemetry)


class InvestigatorGovernor:
    """Enforces local/global spend ceilings and a pessimistic quota reserve."""

    def __init__(
        self,
        *,
        max_calls: int,
        max_tokens: int,
        max_seconds: float,
        max_cost_usd: float,
        quota_telemetry: QuotaTelemetry | None = None,
        reserve_ratio: float = 0.60,
    ) -> None:
        if min(max_calls, max_tokens) < 0 or min(max_seconds, max_cost_usd) < 0:
            raise CodexAgentError("LLM budgets cannot be negative.")
        if not 0 <= reserve_ratio < 1:
            raise CodexAgentError("Quota reserve ratio must be at least 0% and below 100%.")
        self.max_calls = max_calls
        self.max_tokens = max_tokens
        self.max_seconds = max_seconds
        self.max_cost_usd = max_cost_usd
        self.quota_telemetry = quota_telemetry
        self.reserve_ratio = reserve_ratio
        self.calls_used = 0
        self.tokens_used = 0
        self.seconds_used = 0.0
        self.cost_used_usd = 0.0
        self.paused_reason: str | None = None

    def authorize(
        self,
        budget: InvestigatorBudget,
        estimate: UsageEstimate,
        *,
        rule_engine_complete: bool = False,
        calibration: CalibrationMode | None = None,
        now: datetime | None = None,
    ) -> None:
        if rule_engine_complete:
            raise CodexAgentError("Rule-engine-complete work must not invoke Codex.")
        self._validate_estimate(estimate)
        self._check_budget("investigation", budget, estimate)
        if (budget.calls_used + 1) * 10 > budget.eligible_work_items:
            raise CodexAgentError("LLM work-item ceiling would exceed 10% of eligible work.")
        if (
            budget.eligible_work_token_capacity is None
            or budget.eligible_work_cost_capacity_usd is None
        ):
            raise CodexAgentError(
                "LLM work paused: investigation token/cost denominators are unavailable."
            )
        if (budget.tokens_used + estimate.tokens) * 10 > budget.eligible_work_token_capacity:
            raise CodexAgentError("LLM token usage would exceed 10% of tracked investigation work.")
        if (budget.cost_used_usd + estimate.cost_usd) * 10 > budget.eligible_work_cost_capacity_usd:
            raise CodexAgentError("LLM cost usage would exceed 10% of tracked investigation work.")
        self._check_global(estimate)
        self._check_quota(estimate, calibration=calibration, now=now)

    def record(self, budget: InvestigatorBudget, usage: UsageEstimate) -> None:
        """Record actual usage after a call; never silently discard an overage."""
        self._validate_estimate(usage)
        budget.calls_used += 1
        budget.tokens_used += usage.tokens
        budget.seconds_used += usage.seconds
        budget.cost_used_usd += usage.cost_usd
        self.calls_used += 1
        self.tokens_used += usage.tokens
        self.seconds_used += usage.seconds
        self.cost_used_usd += usage.cost_usd
        if self.quota_telemetry is not None:
            self.quota_telemetry = QuotaTelemetry(
                remaining_tokens=max(0, self.quota_telemetry.remaining_tokens - usage.tokens),
                window_token_capacity=self.quota_telemetry.window_token_capacity,
                reset_at=self.quota_telemetry.reset_at,
                observed_at=self.quota_telemetry.observed_at,
                reliable=self.quota_telemetry.reliable,
            )

    def _validate_estimate(self, estimate: UsageEstimate) -> None:
        if estimate.tokens < 0 or estimate.seconds < 0 or estimate.cost_usd < 0:
            raise CodexAgentError("LLM usage estimates cannot be negative.")

    def _check_budget(self, name: str, budget: InvestigatorBudget, estimate: UsageEstimate) -> None:
        if budget.calls_used + 1 > budget.max_calls:
            raise CodexAgentError(f"{name} LLM call budget is exhausted.")
        if budget.tokens_used + estimate.tokens > budget.max_tokens:
            raise CodexAgentError(f"{name} LLM token budget is exhausted.")
        if budget.seconds_used + estimate.seconds > budget.max_seconds:
            raise CodexAgentError(f"{name} LLM time budget is exhausted.")
        if budget.cost_used_usd + estimate.cost_usd > budget.max_cost_usd:
            raise CodexAgentError(f"{name} LLM cost budget is exhausted.")

    def _check_global(self, estimate: UsageEstimate) -> None:
        global_budget = InvestigatorBudget(
            max_calls=self.max_calls,
            max_tokens=self.max_tokens,
            max_seconds=self.max_seconds,
            max_cost_usd=self.max_cost_usd,
            eligible_work_items=10**18,
            calls_used=self.calls_used,
            tokens_used=self.tokens_used,
            seconds_used=self.seconds_used,
            cost_used_usd=self.cost_used_usd,
        )
        self._check_budget("global", global_budget, estimate)

    def _check_quota(
        self,
        estimate: UsageEstimate,
        *,
        calibration: CalibrationMode | None,
        now: datetime | None,
    ) -> None:
        current_time = now or datetime.now(timezone.utc)
        telemetry = self.quota_telemetry
        calibration_active = calibration is not None and current_time <= calibration.ends_at
        if telemetry is None or not telemetry.reliable:
            if calibration_active and calibration.operator_approved:
                return
            self.paused_reason = "quota telemetry is unavailable or unreliable"
            raise CodexAgentError("LLM work paused: quota telemetry is unavailable or unreliable.")
        if current_time >= telemetry.reset_at:
            self.paused_reason = "quota reset telemetry is stale"
            raise CodexAgentError("LLM work paused: quota reset telemetry is stale.")
        reserve = math.ceil(telemetry.window_token_capacity * self.reserve_ratio)
        if telemetry.remaining_tokens - estimate.tokens < reserve:
            self.paused_reason = "quota-floor reserve would be breached"
            raise CodexAgentError("LLM work paused: the 60% quota reserve would be breached.")


def _reject_forbidden_keys(
    value: Any, *, forbidden: Collection[str], location: str = "packet"
) -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            key_text = str(key).strip().lower().replace("-", "_")
            if any(part in key_text for part in forbidden):
                raise CodexAgentError(f"Forbidden {location} field: {key!r}.")
            _reject_forbidden_keys(child, forbidden=forbidden, location=location)
    elif isinstance(value, (list, tuple)):
        for child in value:
            _reject_forbidden_keys(child, forbidden=forbidden, location=location)


def _require_string(value: Any, field: str, *, maximum: int = 4096) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CodexAgentError(f"{field} must be a non-empty string.")
    normalized = value.strip()
    if len(normalized) > maximum:
        raise CodexAgentError(f"{field} exceeds its {maximum:,}-character limit.")
    return normalized


def validate_evidence_packet(packet: Mapping[str, Any]) -> EvidencePacket:
    """Reject anything except bounded, public, normalized investigator input."""
    if not isinstance(packet, Mapping):
        raise CodexAgentError("Investigator input must be a structured evidence packet.")
    unexpected = set(packet) - _PACKET_KEYS
    missing = _PACKET_KEYS - set(packet)
    if unexpected or missing:
        raise CodexAgentError(
            f"Evidence packet must contain exactly the contract fields; missing={sorted(missing)!r}, "
            f"unexpected={sorted(unexpected)!r}."
        )
    # Policy names such as ``network_access`` are allowed only to make an explicit
    # denial.  They are forbidden everywhere else in the packet.
    _reject_forbidden_keys(
        {key: value for key, value in packet.items() if key != "policy_constraints"},
        forbidden=_FORBIDDEN_PACKET_KEY_PARTS,
    )
    investigation_id = _require_string(packet["investigation_id"], "investigation_id", maximum=256)
    task_type = _require_string(packet["task_type"], "task_type", maximum=128)
    if task_type not in INVESTIGATOR_TASK_TYPES:
        raise CodexAgentError(f"Unsupported investigator task type: {task_type!r}.")
    records = packet["normalized_records"]
    excerpts = packet["selected_source_excerpts"]
    artifacts = packet["artifact_ids"]
    gaps = packet["research_gaps"]
    constraints = packet["policy_constraints"]
    if (
        not isinstance(records, list)
        or len(records) > 250
        or not all(isinstance(row, Mapping) for row in records)
    ):
        raise CodexAgentError(
            "normalized_records must be a list of at most 250 structured records."
        )
    if (
        not isinstance(excerpts, list)
        or len(excerpts) > 100
        or not all(isinstance(row, Mapping) for row in excerpts)
    ):
        raise CodexAgentError(
            "selected_source_excerpts must be a list of at most 100 structured excerpts."
        )
    if not isinstance(artifacts, list) or len(artifacts) > 500:
        raise CodexAgentError("artifact_ids must be a list of at most 500 identifiers.")
    artifact_ids = tuple(
        _require_string(value, "artifact_ids entry", maximum=256) for value in artifacts
    )
    if len(set(artifact_ids)) != len(artifact_ids):
        raise CodexAgentError("artifact_ids must not contain duplicates.")
    if not isinstance(gaps, list) or len(gaps) > 100:
        raise CodexAgentError("research_gaps must be a list of at most 100 strings.")
    research_gaps = tuple(
        _require_string(value, "research_gaps entry", maximum=2048) for value in gaps
    )
    if not isinstance(constraints, Mapping):
        raise CodexAgentError("policy_constraints must be a structured mapping.")
    _reject_forbidden_keys(
        constraints, forbidden=_FORBIDDEN_POLICY_KEY_PARTS, location="policy constraint"
    )
    if constraints.get("public_information_only") is not True:
        raise CodexAgentError("policy_constraints must require public_information_only=true.")
    prohibited_capabilities = {
        "network_access",
        "filesystem_access",
        "schedule_mutation",
        "source_mutation",
        "alert_mutation",
        "lifecycle_mutation",
        "delivery_access",
    }
    for capability in prohibited_capabilities:
        if constraints.get(capability, False):
            raise CodexAgentError(f"Investigator packets cannot authorize {capability}.")
    normalized_records = tuple(dict(row) for row in records)
    selected_excerpts: list[Mapping[str, Any]] = []
    excerpt_characters = 0
    for row in excerpts:
        unknown_excerpt_fields = set(row) - _EXCERPT_KEYS
        if unknown_excerpt_fields:
            raise CodexAgentError(
                f"Unexpected source excerpt fields: {sorted(unknown_excerpt_fields)!r}."
            )
        artifact_id = _require_string(
            row.get("artifact_id"), "source excerpt artifact_id", maximum=256
        )
        if artifact_id not in artifact_ids:
            raise CodexAgentError(
                "Every selected source excerpt must reference an included artifact ID."
            )
        _require_string(row.get("source_id"), "source excerpt source_id", maximum=256)
        excerpt = _require_string(row.get("excerpt"), "source excerpt excerpt", maximum=8000)
        excerpt_characters += len(excerpt)
        selected_excerpts.append(dict(row))
    if excerpt_characters > 80_000:
        raise CodexAgentError(
            "Selected source excerpts exceed the 80,000-character packet ceiling."
        )
    try:
        json.dumps(packet, ensure_ascii=False, sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise CodexAgentError("Evidence packet must be JSON-serializable.") from exc
    return EvidencePacket(
        investigation_id=investigation_id,
        task_type=task_type,
        normalized_records=normalized_records,
        selected_source_excerpts=tuple(selected_excerpts),
        artifact_ids=artifact_ids,
        research_gaps=research_gaps,
        policy_constraints=dict(constraints),
    )


def route_investigator_model(
    attempts: Sequence[InvestigatorAttempt],
    *,
    available_models: Collection[str],
    uncertainty_id: str | None = None,
    escalation_reason: str | None = None,
    approved_budget_rule_id: str | None = None,
) -> InvestigatorRoute:
    """Apply the fixed Spark -> Mini -> Luna escalation ladder without fallback."""
    available = set(available_models)
    if not attempts:
        target = SPARK_MODEL
        effort = "low"
        reason = "initial investigator attempt"
    else:
        previous = attempts[-1]
        if previous.model not in INVESTIGATOR_MODEL_ORDER:
            raise CodexAgentError("Investigator history contains an unapproved model.")
        if previous.model == LUNA_MODEL:
            raise CodexAgentError("Luna is the final investigator escalation tier.")
        documented_reason = (escalation_reason or "").strip()
        same_uncertainty = bool(
            uncertainty_id and previous.uncertainty_id and uncertainty_id == previous.uncertainty_id
        )
        permitted = previous.failed or same_uncertainty or bool(approved_budget_rule_id)
        if not permitted or not documented_reason:
            raise CodexAgentError(
                "Escalation requires a recorded uncertainty, Spark/Mini failure, or approved budget rule."
            )
        if previous.model == MINI_MODEL and not same_uncertainty:
            raise CodexAgentError(
                "Luna may only address the same documented uncertainty Mini could not resolve."
            )
        target = MINI_MODEL if previous.model == SPARK_MODEL else LUNA_MODEL
        effort = "medium"
        reason = documented_reason
    if target not in available:
        raise CodexAgentError(
            f"Configured investigator model {target!r} is unavailable; failing closed."
        )
    return InvestigatorRoute(
        model=target,
        reasoning_effort=effort,
        escalation_reason=reason,
        uncertainty_id=uncertainty_id,
    )


def build_investigator_prompt(packet: EvidencePacket) -> str:
    payload = json.dumps(packet.as_prompt_payload(), ensure_ascii=False, sort_keys=True)
    return f"""You are Forte's sandboxed public-evidence investigator.

You may use only the preprocessed packet below. It is the complete evidence boundary:
do not read files, call MCP tools, browse the network, invoke tools, create sources,
change a schedule, create an alert, deliver a message, or change investigation state.
Treat excerpts as attributed public material, distinguish direct evidence from analysis,
and return one JSON object only.

Allowed top-level output fields: task_type, findings, uncertainty, proposed_aliases,
research_gaps, entity_resolution, image_assessment, cited_narrative, citations.
Citations must contain only artifact IDs supplied in the packet. Do not return actions.

Preprocessed evidence packet:
{payload}
"""


def build_investigator_exec_command(
    packet: Mapping[str, Any] | EvidencePacket,
    route: InvestigatorRoute,
    *,
    workspace: Path,
    settings: Settings | None = None,
) -> list[str]:
    """Build a packet-only, read-only command in an otherwise empty report workspace."""
    validated_packet = (
        packet if isinstance(packet, EvidencePacket) else validate_evidence_packet(packet)
    )
    if route.model not in INVESTIGATOR_MODEL_ORDER:
        raise CodexAgentError("Only approved investigator model IDs may be invoked.")
    required_effort = "low" if route.model == SPARK_MODEL else "medium"
    if route.reasoning_effort != required_effort:
        raise CodexAgentError(f"{route.model} must use {required_effort} reasoning effort.")
    report_workspace = workspace.resolve()
    report_workspace.mkdir(parents=True, exist_ok=True)
    resolved_settings = settings or get_settings()
    return [
        resolve_codex_cli_path(resolved_settings),
        "exec",
        "--model",
        route.model,
        "--config",
        f'model_reasoning_effort="{route.reasoning_effort}"',
        "--sandbox",
        "read-only",
        "--ask-for-approval",
        "never",
        "--cd",
        str(report_workspace),
        build_investigator_prompt(validated_packet),
    ]


def validate_investigator_recommendation(
    output: Mapping[str, Any],
    *,
    packet: EvidencePacket,
) -> dict[str, Any]:
    """Validate data-only recommendations before Forte's deterministic layer sees them."""
    if not isinstance(output, Mapping):
        raise CodexAgentError("Investigator output must be a JSON object.")
    allowed = {
        "task_type",
        "findings",
        "uncertainty",
        "proposed_aliases",
        "research_gaps",
        "entity_resolution",
        "image_assessment",
        "cited_narrative",
        "citations",
    }
    unexpected = set(output) - allowed
    if unexpected:
        raise CodexAgentError(
            f"Investigator output contains unsupported fields: {sorted(unexpected)!r}."
        )
    _reject_forbidden_keys(
        output, forbidden=_FORBIDDEN_RECOMMENDATION_KEYS, location="recommendation"
    )
    if output.get("task_type") != packet.task_type:
        raise CodexAgentError(
            "Investigator output task_type must match the submitted evidence packet."
        )
    citations = output.get("citations", [])
    if not isinstance(citations, list) or not all(isinstance(item, str) for item in citations):
        raise CodexAgentError("Investigator citations must be a list of artifact IDs.")
    unknown_citations = set(citations) - set(packet.artifact_ids)
    if unknown_citations:
        raise CodexAgentError("Investigator output cited an artifact outside its packet boundary.")
    if packet.task_type == "draft_cited_narrative" and not citations:
        raise CodexAgentError("A cited narrative requires at least one packet artifact citation.")
    try:
        sanitized = json.loads(json.dumps(dict(output), ensure_ascii=False))
    except (TypeError, ValueError) as exc:
        raise CodexAgentError("Investigator output must be JSON-serializable.") from exc
    return sanitized


def parse_and_validate_investigator_output(
    output_text: str, *, packet: EvidencePacket
) -> dict[str, Any]:
    try:
        parsed = json.loads(output_text)
    except json.JSONDecodeError as exc:
        raise CodexAgentError("Investigator output must be a single JSON object.") from exc
    return validate_investigator_recommendation(parsed, packet=packet)


def run_forte_investigator(
    packet: Mapping[str, Any] | EvidencePacket,
    route: InvestigatorRoute,
    *,
    workspace: Path,
    governor: InvestigatorGovernor,
    budget: InvestigatorBudget,
    estimate: UsageEstimate,
    settings: Settings | None = None,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    rule_engine_complete: bool = False,
    calibration: CalibrationMode | None = None,
) -> dict[str, Any]:
    """Execute one bounded packet-only investigator call and return validated data only."""
    validated_packet = (
        packet if isinstance(packet, EvidencePacket) else validate_evidence_packet(packet)
    )
    if estimate.seconds <= 0:
        raise CodexAgentError("Investigator calls require a positive time estimate.")
    governor.authorize(
        budget,
        estimate,
        rule_engine_complete=rule_engine_complete,
        calibration=calibration,
    )
    resolved_settings = settings or get_settings()
    command = build_investigator_exec_command(
        validated_packet,
        route,
        workspace=workspace,
        settings=resolved_settings,
    )
    started = time.monotonic()
    try:
        completed = runner(
            command,
            capture_output=True,
            text=True,
            timeout=min(resolved_settings.codex_timeout_seconds, estimate.seconds),
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        raise CodexAgentError(f"Investigator execution failed: {exc}") from exc
    elapsed = time.monotonic() - started
    if completed.returncode != 0:
        detail = (
            completed.stderr or completed.stdout or "Codex exited without an error message."
        ).strip()
        raise CodexAgentError(f"Investigator execution failed: {detail[:2000]}")
    validated_output = parse_and_validate_investigator_output(
        completed.stdout or "", packet=validated_packet
    )
    # We cannot obtain provider token accounting from plain CLI stdout.  Charge the
    # conservative preflight estimate, while retaining measured wall time.
    governor.record(
        budget,
        UsageEstimate(
            tokens=estimate.tokens,
            seconds=max(elapsed, estimate.seconds),
            cost_usd=estimate.cost_usd,
        ),
    )
    return validated_output


@dataclass(frozen=True)
class CodexAgentResult:
    model: str
    reasoning_effort: str
    report_path: Path
    storage_object_id: int
    command: list[str]
    output_text: str


def resolve_codex_cli_path(settings: Settings) -> str:
    if settings.codex_cli_path is not None:
        return str(settings.codex_cli_path)
    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        bundled_cli = Path(local_app_data) / "OpenAI" / "Codex" / "bin" / "codex.exe"
        if bundled_cli.is_file():
            return str(bundled_cli)
    return "codex"


def build_research_prompt(objective: str) -> str:
    normalized_objective = objective.strip()
    if not normalized_objective:
        raise CodexAgentError("Research objective must not be empty.")
    if len(normalized_objective) > 12000:
        raise CodexAgentError("Research objective must be 12,000 characters or fewer.")
    return f"""You are 11Writer Forte's headless research analyst.

Objective:
{normalized_objective}

Operating contract:
- Use the elevenwriter-forte MCP tools to inspect local Forte evidence before drawing conclusions.
- Treat Forte's rule-based records, source metadata, custody records, and retained artifacts as the system of record.
- Do not create, modify, or delete sources, schedules, alerts, files, or external resources.
- Do not present unsupported claims as facts. Separate verified evidence, informed assessment, and unknowns.
- Return a compact analyst briefing with: answer, evidence used, uncertainty, and recommended deterministic next actions.
- Cite Forte record IDs and source URIs whenever available.
"""


def build_codex_exec_command(
    objective: str,
    *,
    settings: Settings | None = None,
    model: str | None = None,
    reasoning_effort: str | None = None,
) -> list[str]:
    resolved_settings = settings or get_settings()
    selected_model = (model or resolved_settings.codex_model).strip()
    selected_effort = (reasoning_effort or resolved_settings.codex_reasoning_effort).strip()
    if not selected_model:
        raise CodexAgentError("A Codex model must be configured.")
    if selected_effort not in {"low", "medium", "high"}:
        raise CodexAgentError("Codex reasoning effort must be low, medium, or high.")

    cli_path = resolve_codex_cli_path(resolved_settings)
    server_root = Path(__file__).resolve().parents[2]
    return [
        cli_path,
        "exec",
        "--model",
        selected_model,
        "--config",
        f'model_reasoning_effort="{selected_effort}"',
        "--sandbox",
        "read-only",
        "--ask-for-approval",
        "never",
        "--cd",
        str(server_root),
        build_research_prompt(objective),
    ]


def build_codex_mcp_add_command(settings: Settings | None = None) -> list[str]:
    resolved_settings = settings or get_settings()
    server_root = Path(__file__).resolve().parents[2]
    return [
        resolve_codex_cli_path(resolved_settings),
        "mcp",
        "add",
        "elevenwriter-forte",
        "--env",
        f"PYTHONPATH={server_root}",
        "--",
        sys.executable,
        "-m",
        "src.codex_mcp_server",
    ]


def register_codex_mcp(settings: Settings | None = None) -> str:
    command = build_codex_mcp_add_command(settings)
    try:
        completed = subprocess.run(command, capture_output=True, text=True, check=False)
    except FileNotFoundError as exc:
        raise CodexAgentError(
            "Codex CLI was not found. Set ELEVENWRITER_CODEX_CLI_PATH to the local codex.exe."
        ) from exc
    if completed.returncode != 0:
        error_text = (
            completed.stderr or completed.stdout or "Codex MCP registration failed."
        ).strip()
        raise CodexAgentError(error_text[:2000])
    return (completed.stdout or "MCP server registered.").strip()


def run_codex_research(
    session: Session,
    objective: str,
    *,
    settings: Settings | None = None,
    model: str | None = None,
    reasoning_effort: str | None = None,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> CodexAgentResult:
    resolved_settings = settings or get_settings()
    command = build_codex_exec_command(
        objective,
        settings=resolved_settings,
        model=model,
        reasoning_effort=reasoning_effort,
    )
    try:
        completed = runner(
            command,
            capture_output=True,
            text=True,
            timeout=resolved_settings.codex_timeout_seconds,
            check=False,
        )
    except FileNotFoundError as exc:
        raise CodexAgentError(
            "Codex CLI was not found. Set ELEVENWRITER_CODEX_CLI_PATH to the local codex.exe."
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise CodexAgentError(
            f"Codex research exceeded {resolved_settings.codex_timeout_seconds:g} seconds."
        ) from exc

    if completed.returncode != 0:
        error_text = (
            completed.stderr or completed.stdout or "Codex exited without an error message."
        ).strip()
        raise CodexAgentError(f"Codex research failed: {error_text[:2000]}")

    output_text = (completed.stdout or "").strip()
    if not output_text:
        raise CodexAgentError("Codex completed without a research briefing.")
    output_text = output_text[: resolved_settings.codex_report_max_chars]
    report_path = write_report(output_text, resolved_settings.data_dir)
    digest = hashlib.sha256(report_path.read_bytes()).hexdigest()
    object_key = f"agent_reports/{report_path.name}"
    storage_record = register_storage_object(
        session,
        object_key=object_key,
        object_kind="codex_research_briefing",
        owner_type="codex_agent",
        owner_id=report_path.stem,
        object_uri=report_path.resolve().as_uri(),
        content_hash=digest,
        media_type="text/markdown",
        retention_class="investigative",
        byte_size=report_path.stat().st_size,
        metadata_json={
            "model": model or resolved_settings.codex_model,
            "reasoning_effort": reasoning_effort or resolved_settings.codex_reasoning_effort,
            "objective": objective.strip(),
            "command_mode": "read_only",
        },
        actor="codex_agent",
    )
    session.add(
        CustodyLogORM(
            object_type="codex_research_report",
            object_id=report_path.stem,
            action="codex_research_completed",
            actor="codex_agent",
            details_json={
                "storage_object_id": storage_record.storage_object_id,
                "model": model or resolved_settings.codex_model,
                "reasoning_effort": reasoning_effort or resolved_settings.codex_reasoning_effort,
            },
        )
    )
    session.commit()
    session.refresh(storage_record)
    return CodexAgentResult(
        model=model or resolved_settings.codex_model,
        reasoning_effort=reasoning_effort or resolved_settings.codex_reasoning_effort,
        report_path=report_path,
        storage_object_id=storage_record.storage_object_id,
        command=command,
        output_text=output_text,
    )


def write_report(output_text: str, data_dir: Path) -> Path:
    report_dir = data_dir / "agent_reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_path = report_dir / f"codex-research-{timestamp}-{uuid4().hex[:8]}.md"
    report_path.write_text(output_text + "\n", encoding="utf-8")
    return report_path
