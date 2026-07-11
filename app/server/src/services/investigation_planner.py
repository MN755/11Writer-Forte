"""Deterministic, policy-bound research planning.

This module deliberately has no database or network dependency.  The investigation
lifecycle can persist its immutable inputs and the returned plan, while a worker can
record attempts and request the next eligible item without changing planning policy.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
import re
from typing import Iterable, Mapping, Protocol
from urllib.parse import urlparse


class SourceClass(str, Enum):
    OFFICIAL_PUBLIC_DATA = "official_public_data"
    ESTABLISHED_NEWS = "established_news"
    HIGH_VALUE_PUBLIC = "high_value_public"
    UNKNOWN_PERSONAL = "unknown_personal"
    OTHER_PUBLIC = "other_public"


class AttemptReason(str, Enum):
    CONFIRMED = "confirmed"
    CORROBORATING = "corroborating"
    CONFLICTING = "conflicting"
    LOW_QUALITY = "low_quality"
    UNAVAILABLE = "unavailable"
    BLOCKED_BY_POLICY = "blocked_by_policy"
    DEAD_END = "dead_end"


class SourceLike(Protocol):
    """The intentionally small source catalog contract used by the planner."""

    source_id: str | int
    name: str
    target_uri: str


@dataclass(frozen=True)
class SourceBudgetPolicy:
    """Auditable caps by source class, plus a hard investigation-wide cap."""

    caps: Mapping[SourceClass, int] = field(
        default_factory=lambda: {
            SourceClass.OFFICIAL_PUBLIC_DATA: 6,
            SourceClass.ESTABLISHED_NEWS: 5,
            SourceClass.HIGH_VALUE_PUBLIC: 4,
            SourceClass.OTHER_PUBLIC: 2,
            SourceClass.UNKNOWN_PERSONAL: 1,
        }
    )
    global_cap: int = 20
    per_domain_cap: int = 3
    vetted_unknown_personal_cap: int = 2

    def cap_for(self, source_class: SourceClass) -> int:
        return max(0, int(self.caps.get(source_class, 0)))

    def as_dict(self) -> dict[str, object]:
        return {
            "caps": {kind.value: self.cap_for(kind) for kind in SourceClass},
            "global_cap": self.global_cap,
            "per_domain_cap": self.per_domain_cap,
            "vetted_unknown_personal_cap": self.vetted_unknown_personal_cap,
        }


@dataclass(frozen=True)
class SourceCatalogEntry:
    source_id: str
    name: str
    target_uri: str
    source_class: SourceClass = SourceClass.OTHER_PUBLIC
    enabled: bool = True
    trust_score: float = 0.5
    reliability_checked: bool = False
    relevance_checked: bool = False
    access_policy: str = "public"
    robots_allowed: bool = True
    rate_limit_ok: bool = True
    byte_limit_ok: bool = True
    concurrency_ok: bool = True
    tags: tuple[str, ...] = ()
    aliases: tuple[str, ...] = ()

    @property
    def domain(self) -> str:
        return (urlparse(self.target_uri).hostname or "").lower()

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "SourceCatalogEntry":
        raw_class = str(value.get("source_class") or value.get("domain_class") or "other_public")
        try:
            source_class = SourceClass(raw_class)
        except ValueError:
            source_class = SourceClass.OTHER_PUBLIC
        return cls(
            source_id=str(value.get("source_id", value.get("id", value.get("name", "")))),
            name=str(value.get("name", value.get("source_id", "source"))),
            target_uri=str(value.get("target_uri", value.get("url", ""))),
            source_class=source_class,
            enabled=bool(value.get("enabled", True)),
            trust_score=float(value.get("trust_score", 0.5)),
            reliability_checked=bool(value.get("reliability_checked", False)),
            relevance_checked=bool(value.get("relevance_checked", False)),
            access_policy=str(value.get("access_policy", "public")),
            robots_allowed=bool(value.get("robots_allowed", True)),
            rate_limit_ok=bool(value.get("rate_limit_ok", True)),
            byte_limit_ok=bool(value.get("byte_limit_ok", True)),
            concurrency_ok=bool(value.get("concurrency_ok", True)),
            tags=tuple(sorted(str(item).lower() for item in value.get("tags", ()))),
            aliases=tuple(sorted(str(item).lower() for item in value.get("aliases", ()))),
        )


@dataclass(frozen=True)
class ResearchLead:
    kind: str
    value: str
    normalized: str


@dataclass(frozen=True)
class ResearchWorkItem:
    item_id: str
    source_id: str
    source_name: str
    target_uri: str
    source_class: SourceClass
    domain: str
    query: str
    leads: tuple[ResearchLead, ...]
    priority: int
    alternate_for: str | None = None


@dataclass(frozen=True)
class SourceAttempt:
    source_id: str
    reason: AttemptReason
    item_id: str | None = None
    detail: str = ""
    recorded_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class ResearchPlan:
    question: str
    leads: tuple[ResearchLead, ...]
    work_items: tuple[ResearchWorkItem, ...]
    blocked_attempts: tuple[SourceAttempt, ...]
    policy: SourceBudgetPolicy

    def audit(self) -> dict[str, object]:
        by_class = {kind.value: 0 for kind in SourceClass}
        for item in self.work_items:
            by_class[item.source_class.value] += 1
        return {
            "policy": self.policy.as_dict(),
            "scheduled_by_source_class": by_class,
            "blocked": [
                {
                    "source_id": attempt.source_id,
                    "reason": attempt.reason.value,
                    "detail": attempt.detail,
                }
                for attempt in self.blocked_attempts
            ],
        }


_ACCESS_BOUNDARY = {"requires_login", "login", "paywalled", "access_controlled", "blocked"}
_TOKEN = re.compile(r"[A-Za-z][A-Za-z0-9'/-]{2,}")
_DATE = re.compile(r"\b(?:19|20)\d{2}(?:[-/]\d{1,2}(?:[-/]\d{1,2})?)?\b")
_LOCATION = re.compile(r"\b(?:in|near|at|from)\s+([A-Z][\w.-]*(?:\s+[A-Z][\w.-]*){0,3})")


def extract_research_leads(question: str) -> tuple[ResearchLead, ...]:
    """Extract conservative, stable leads without pretending this is NER magic."""
    normalized_question = " ".join(question.split())
    leads: dict[tuple[str, str], ResearchLead] = {}
    for value in _DATE.findall(normalized_question):
        leads[("time_range", value)] = ResearchLead("time_range", value, value)
    for value in _LOCATION.findall(normalized_question):
        clean = " ".join(value.split())
        leads[("location", clean.lower())] = ResearchLead("location", clean, clean.lower())
    # Capitalized phrases are useful operator hints, not facts or resolved entities.
    for value in re.findall(r"\b[A-Z][\w.-]*(?:\s+[A-Z][\w.-]*){0,3}\b", normalized_question):
        if value.lower() not in {"what", "when", "where", "which", "who", "why", "how"}:
            leads[("entity", value.lower())] = ResearchLead("entity", value, value.lower())
    for value in _TOKEN.findall(normalized_question):
        lower = value.lower()
        if len(lower) >= 5 and lower not in {"about", "would", "could", "there", "their", "which"}:
            leads.setdefault(("keyword", lower), ResearchLead("keyword", value, lower))
    return tuple(leads[key] for key in sorted(leads))


def _policy_block(source: SourceCatalogEntry) -> str | None:
    parsed = urlparse(source.target_uri)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return "source URI is not a public HTTP(S) endpoint"
    if source.access_policy.strip().lower() in _ACCESS_BOUNDARY:
        return "source requires a prohibited login, paywall, or access-control bypass"
    if not source.robots_allowed:
        return "robots policy does not permit collection"
    if not source.rate_limit_ok:
        return "rate-limit budget is unavailable"
    if not source.byte_limit_ok:
        return "byte-limit budget is unavailable"
    if not source.concurrency_ok:
        return "concurrency budget is unavailable"
    return None


def _source_relevance(source: SourceCatalogEntry, leads: tuple[ResearchLead, ...]) -> int:
    terms = {lead.normalized for lead in leads}
    haystack = " ".join((source.name, source.target_uri, *source.tags, *source.aliases)).lower()
    return sum(term in haystack for term in terms)


class InvestigationPlanner:
    """Rule-first initial planner and deterministic alternate-work selector."""

    def __init__(self, policy: SourceBudgetPolicy | None = None) -> None:
        self.policy = policy or SourceBudgetPolicy()

    def build_initial_plan(
        self,
        question: str,
        sources: Iterable[SourceCatalogEntry | Mapping[str, object]],
    ) -> ResearchPlan:
        question = " ".join(question.split())
        if not question:
            raise ValueError("An investigation question is required.")
        leads = extract_research_leads(question)
        entries = [
            item if isinstance(item, SourceCatalogEntry) else SourceCatalogEntry.from_mapping(item)
            for item in sources
        ]
        # Stable sort is the reproducibility guarantee: no input order or random rank leakage.
        entries.sort(key=lambda item: (item.source_id, item.name.lower(), item.target_uri))
        candidates: list[tuple[SourceCatalogEntry, int]] = []
        blocked: list[SourceAttempt] = []
        for source in entries:
            block = _policy_block(source)
            if not source.enabled:
                continue
            if block:
                blocked.append(
                    SourceAttempt(source.source_id, AttemptReason.BLOCKED_BY_POLICY, detail=block)
                )
                continue
            candidates.append((source, _source_relevance(source, leads)))
        candidates.sort(
            key=lambda pair: (
                -pair[1],
                -pair[0].trust_score,
                pair[0].source_class.value,
                pair[0].domain,
                pair[0].source_id,
            )
        )
        class_counts = {kind: 0 for kind in SourceClass}
        domain_counts: dict[str, int] = {}
        work_items: list[ResearchWorkItem] = []
        for source, relevance in candidates:
            if len(work_items) >= self.policy.global_cap:
                break
            cap = self.policy.cap_for(source.source_class)
            if source.source_class is SourceClass.UNKNOWN_PERSONAL:
                cap = (
                    min(cap, self.policy.vetted_unknown_personal_cap)
                    if source.reliability_checked and source.relevance_checked
                    else min(cap, 1)
                )
            if class_counts[source.source_class] >= cap:
                continue
            if domain_counts.get(source.domain, 0) >= self.policy.per_domain_cap:
                continue
            class_counts[source.source_class] += 1
            domain_counts[source.domain] = domain_counts.get(source.domain, 0) + 1
            item_id = f"source:{source.source_id}"
            work_items.append(
                ResearchWorkItem(
                    item_id=item_id,
                    source_id=source.source_id,
                    source_name=source.name,
                    target_uri=source.target_uri,
                    source_class=source.source_class,
                    domain=source.domain,
                    query=question,
                    leads=leads,
                    priority=1000 + relevance * 10 + int(source.trust_score * 10),
                )
            )
        return ResearchPlan(question, leads, tuple(work_items), tuple(blocked), self.policy)

    def next_work(
        self,
        plan: ResearchPlan,
        attempts: Iterable[SourceAttempt],
    ) -> ResearchWorkItem | None:
        """Return the first eligible work item, naturally moving past a dead source."""
        recorded_attempts = tuple(attempts)
        attempted = {attempt.source_id for attempt in recorded_attempts}
        alternate_for = next(
            (
                attempt.source_id
                for attempt in reversed(recorded_attempts)
                if attempt.reason in {AttemptReason.UNAVAILABLE, AttemptReason.DEAD_END}
            ),
            None,
        )
        for item in sorted(plan.work_items, key=lambda value: (-value.priority, value.item_id)):
            if item.source_id not in attempted:
                return replace(item, alternate_for=alternate_for)
        return None

    def record_attempt(
        self,
        attempt_log: Iterable[SourceAttempt],
        *,
        source_id: str,
        reason: AttemptReason,
        item_id: str | None = None,
        detail: str = "",
    ) -> tuple[SourceAttempt, ...]:
        """Return an append-only attempt log; callers persist it with their lifecycle record."""
        return (*attempt_log, SourceAttempt(source_id, reason, item_id, detail))
