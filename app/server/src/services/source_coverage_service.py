"""Deterministic, serializable source-coverage planning for investigation watches.

This module intentionally has no database or network dependency.  A watch runner owns
the persistence boundary: it stores ``CoverageLedger.to_json()`` alongside its watch
and records the source-run outcome after every attempt.  Keeping this policy pure
makes the public-web limits inspectable and prevents a failed source from quietly
ending a watch.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterable


RETRIABLE_FAILURES = frozenset({"network_error", "timeout", "rate_limited", "server_error"})


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class SourceCandidate:
    """A policy-reduced source candidate; it never carries credentials or secrets."""

    source_id: str
    source_kind: str
    domain: str
    domain_class: str
    alternate_rank: int = 0
    enabled: bool = True


@dataclass(frozen=True)
class RetryPolicy:
    """Bounded retry policy.  Domain budgets are keyed by declared class only."""

    allowed_source_kinds: frozenset[str]
    domain_class_budgets: dict[str, int]
    max_retry_attempts: int = 2
    retry_base_seconds: float = 1.0
    retry_max_seconds: float = 60.0
    max_alternates_per_gap: int = 2

    def __post_init__(self) -> None:
        if not self.allowed_source_kinds:
            raise ValueError("At least one allowed source kind is required.")
        if self.max_retry_attempts < 0:
            raise ValueError("max_retry_attempts cannot be negative.")
        if self.max_alternates_per_gap < 1:
            raise ValueError("max_alternates_per_gap must be at least one.")
        if self.retry_base_seconds < 0 or self.retry_max_seconds < 0:
            raise ValueError("Retry delays cannot be negative.")
        if any(limit < 1 for limit in self.domain_class_budgets.values()):
            raise ValueError("Each domain-class budget must be at least one.")

    def budget_for(self, domain_class: str) -> int:
        """Return a declared class budget; domain names never affect this result."""

        try:
            return self.domain_class_budgets[domain_class]
        except KeyError as exc:
            raise ValueError(f"No auditable budget declared for domain class {domain_class!r}.") from exc


@dataclass(frozen=True)
class CollectionAction:
    action: str
    source_id: str | None = None
    retry_at: datetime | None = None
    reason: str = ""
    gap_key: str | None = None

    def to_json(self) -> dict[str, object]:
        payload = asdict(self)
        payload["retry_at"] = self.retry_at.isoformat() if self.retry_at else None
        return payload


@dataclass
class CoverageLedger:
    """Append-only-ish coverage state that can be saved in one JSON column.

    The runner may compact old entries according to retention policy, but the current
    snapshot always captures searched scopes, attempts, changes, and unresolved gaps.
    """

    searches: list[dict[str, object]] = field(default_factory=list)
    attempts: list[dict[str, object]] = field(default_factory=list)
    changes: list[dict[str, object]] = field(default_factory=list)
    gaps: dict[str, dict[str, object]] = field(default_factory=dict)

    @classmethod
    def from_json(cls, payload: dict[str, object] | None) -> "CoverageLedger":
        payload = payload or {}
        gaps = payload.get("gaps")
        return cls(
            searches=[dict(item) for item in payload.get("searches", []) if isinstance(item, dict)],
            attempts=[dict(item) for item in payload.get("attempts", []) if isinstance(item, dict)],
            changes=[dict(item) for item in payload.get("changes", []) if isinstance(item, dict)],
            gaps={str(key): dict(value) for key, value in gaps.items() if isinstance(value, dict)}
            if isinstance(gaps, dict)
            else {},
        )

    def to_json(self) -> dict[str, object]:
        return {
            "searches": list(self.searches),
            "attempts": list(self.attempts),
            "changes": list(self.changes),
            "gaps": {key: self.gaps[key] for key in sorted(self.gaps)},
        }

    def record_search(self, *, scope: str, source_ids: Iterable[str], occurred_at: datetime | None = None) -> None:
        self.searches.append(
            {
                "scope": scope,
                "source_ids": sorted(set(source_ids)),
                "occurred_at": (occurred_at or utcnow()).isoformat(),
            }
        )

    def record_attempt(
        self,
        candidate: SourceCandidate,
        *,
        status: str,
        changed: bool = False,
        failure_kind: str | None = None,
        occurred_at: datetime | None = None,
    ) -> None:
        timestamp = (occurred_at or utcnow()).isoformat()
        self.attempts.append(
            {
                "source_id": candidate.source_id,
                "source_kind": candidate.source_kind,
                "domain": candidate.domain,
                "domain_class": candidate.domain_class,
                "status": status,
                "failure_kind": failure_kind,
                "occurred_at": timestamp,
            }
        )
        if changed:
            self.changes.append({"source_id": candidate.source_id, "occurred_at": timestamp})

    def record_gap(self, *, gap_key: str, reason: str, source_id: str, occurred_at: datetime | None = None) -> None:
        prior = self.gaps.get(gap_key, {})
        self.gaps[gap_key] = {
            "reason": reason,
            "source_id": source_id,
            "opened_at": prior.get("opened_at", (occurred_at or utcnow()).isoformat()),
            "last_observed_at": (occurred_at or utcnow()).isoformat(),
            "status": "open",
        }

    def resolve_gap(self, gap_key: str, *, occurred_at: datetime | None = None) -> None:
        if gap_key in self.gaps:
            self.gaps[gap_key]["status"] = "resolved"
            self.gaps[gap_key]["resolved_at"] = (occurred_at or utcnow()).isoformat()

    def attempts_for(self, source_id: str) -> int:
        return sum(1 for attempt in self.attempts if attempt.get("source_id") == source_id)

    def attempts_for_domain_class(self, domain_class: str) -> int:
        return sum(1 for attempt in self.attempts if attempt.get("domain_class") == domain_class)

    def attempted_source_ids(self) -> set[str]:
        return {str(attempt["source_id"]) for attempt in self.attempts if attempt.get("source_id")}


def plan_collection_action(
    candidate: SourceCandidate,
    *,
    outcome: str,
    failure_kind: str | None,
    changed: bool = False,
    ledger: CoverageLedger,
    policy: RetryPolicy,
    alternate_candidates: Iterable[SourceCandidate] = (),
    now: datetime | None = None,
) -> list[CollectionAction]:
    """Return bounded retry/diversification actions after recording the outcome.

    ``outcome`` is expected to be ``success``, ``stale``, or ``failed``.  A failed or
    stale primary first gets a finite retry when appropriate, then opens a documented
    research gap and proposes policy-eligible alternates.  The caller executes these
    actions; this function makes no network request.
    """

    timestamp = now or utcnow()
    status = "success" if outcome == "success" else outcome
    ledger.record_attempt(
        candidate,
        status=status,
        changed=changed,
        failure_kind=failure_kind,
        occurred_at=timestamp,
    )
    if outcome == "success":
        ledger.resolve_gap(f"source:{candidate.source_id}", occurred_at=timestamp)
        return []

    source_attempts = ledger.attempts_for(candidate.source_id)
    domain_attempts = ledger.attempts_for_domain_class(candidate.domain_class)
    if (
        outcome == "failed"
        and failure_kind in RETRIABLE_FAILURES
        and source_attempts <= policy.max_retry_attempts
        and domain_attempts < policy.budget_for(candidate.domain_class)
    ):
        delay = min(policy.retry_max_seconds, policy.retry_base_seconds * (2 ** (source_attempts - 1)))
        return [
            CollectionAction(
                action="retry_source",
                source_id=candidate.source_id,
                retry_at=timestamp + timedelta(seconds=delay),
                reason=failure_kind or "retriable_failure",
            )
        ]

    gap_key = f"source:{candidate.source_id}"
    ledger.record_gap(
        gap_key=gap_key,
        reason=failure_kind or outcome,
        source_id=candidate.source_id,
        occurred_at=timestamp,
    )
    actions: list[CollectionAction] = [
        CollectionAction(action="record_research_gap", source_id=candidate.source_id, reason=failure_kind or outcome, gap_key=gap_key)
    ]
    actions.extend(
        CollectionAction(
            action="try_alternate_source",
            source_id=alternate.source_id,
            reason=f"diversify_after:{candidate.source_id}",
            gap_key=gap_key,
        )
        for alternate in select_alternates(candidate, alternate_candidates, ledger=ledger, policy=policy)
    )
    return actions


def select_alternates(
    failed_candidate: SourceCandidate,
    candidates: Iterable[SourceCandidate],
    *,
    ledger: CoverageLedger,
    policy: RetryPolicy,
) -> list[SourceCandidate]:
    """Select permitted, untried alternates without evading declared class budgets."""

    attempted = ledger.attempted_source_ids()
    selected: list[SourceCandidate] = []
    class_counts = {
        domain_class: ledger.attempts_for_domain_class(domain_class)
        for domain_class in policy.domain_class_budgets
    }
    for candidate in sorted(candidates, key=lambda item: (item.alternate_rank, item.source_id)):
        if candidate.source_id == failed_candidate.source_id or candidate.source_id in attempted:
            continue
        if not candidate.enabled or candidate.source_kind not in policy.allowed_source_kinds:
            continue
        if candidate.domain_class not in policy.domain_class_budgets:
            continue
        if class_counts[candidate.domain_class] >= policy.budget_for(candidate.domain_class):
            continue
        selected.append(candidate)
        class_counts[candidate.domain_class] += 1
        if len(selected) >= policy.max_alternates_per_gap:
            break
    return selected
