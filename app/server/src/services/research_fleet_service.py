"""Deterministic, policy-bound planning for the public research fleet.

This is deliberately a control-plane module: it plans configured providers and
validates static collection targets, but never performs a network request.  A
durable worker persists the returned task/ledger records and is responsible for
calling the existing bounded fetch implementation.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import re
from typing import Iterable, Mapping
from urllib.parse import urlsplit

from src.services.discovery_analysis import canonicalize_url
from src.services.discovery_fetch import Resolver, UnsafeTargetError, validate_fetch_url
from src.services.source_coverage_service import CoverageLedger
from src.services.trust_service import normalize_domain


STATIC_COLLECTION_CAPABILITIES = frozenset(
    {"structured_api", "rss_atom", "sitemap", "static_html", "document", "dataset"}
)
PUBLIC_ACCESS_REQUIREMENTS = frozenset({"none", "operator_supplied"})
_SPACE = re.compile(r"\s+")


@dataclass(frozen=True)
class ProviderDefinition:
    """A reviewed provider contract, without credentials or executable URLs."""

    provider_id: str
    name: str
    source_kind: str
    capabilities: tuple[str, ...]
    license_notes: str
    jurisdiction: str
    languages: tuple[str, ...]
    freshness_hours: int
    cost: str
    access_requirement: str
    robots_supported: bool
    evidence_capture_method: str
    default_request_budget: int = 10
    default_byte_budget: int = 5 * 1024 * 1024
    default_concurrency: int = 1
    default_retry_budget: int = 2
    retention_days: int = 30
    enabled: bool = True
    schema_version: str = "1"

    def __post_init__(self) -> None:
        if not self.provider_id or not self.name:
            raise ValueError("Provider id and name are required.")
        if self.cost not in {"free", "operator-supplied", "disabled"}:
            raise ValueError("Provider cost must be free, operator-supplied, or disabled.")
        if self.access_requirement not in PUBLIC_ACCESS_REQUIREMENTS:
            raise ValueError("Providers may not require login or access-control bypass.")
        if not set(self.capabilities):
            raise ValueError("A provider must declare at least one capability.")
        if min(
            self.default_request_budget,
            self.default_byte_budget,
            self.default_concurrency,
            self.default_retry_budget,
            self.retention_days,
        ) < 0:
            raise ValueError("Provider budgets cannot be negative.")

    def to_public_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["capabilities"] = list(self.capabilities)
        payload["languages"] = list(self.languages)
        payload["configured"] = self.enabled and self.cost != "disabled"
        return payload


# These are contracts, rather than a stealth collection list.  Deployments add actual
# endpoint configuration through their source/onboarding controls and keep secrets out
# of the plan and API responses.
DEFAULT_PUBLIC_PROVIDER_REGISTRY: tuple[ProviderDefinition, ...] = (
    ProviderDefinition(
        provider_id="federal_register_api",
        name="Federal Register API",
        source_kind="official_public_record",
        capabilities=("search", "structured_api", "document"),
        license_notes="Public federal register data; confirm downstream reuse for each artifact.",
        jurisdiction="US-federal",
        languages=("en",),
        freshness_hours=24,
        cost="free",
        access_requirement="none",
        robots_supported=True,
        evidence_capture_method="response_manifest",
    ),
    ProviderDefinition(
        provider_id="govinfo_api",
        name="GovInfo API",
        source_kind="official_public_record",
        capabilities=("search", "structured_api", "document", "dataset"),
        license_notes="Official U.S. government publications; preserve publication metadata.",
        jurisdiction="US-federal",
        languages=("en",),
        freshness_hours=24,
        cost="operator-supplied",
        access_requirement="operator_supplied",
        robots_supported=True,
        evidence_capture_method="response_manifest",
    ),
    ProviderDefinition(
        provider_id="public_newsroom_rss",
        name="Approved public newsroom RSS/Atom feeds",
        source_kind="established_news",
        capabilities=("rss_atom", "static_html", "document"),
        license_notes="Only feeds explicitly approved by the operator; retain publisher attribution.",
        jurisdiction="operator-configured",
        languages=("en",),
        freshness_hours=6,
        cost="free",
        access_requirement="none",
        robots_supported=True,
        evidence_capture_method="feed_item_and_response_manifest",
    ),
    ProviderDefinition(
        provider_id="operator_public_dataset",
        name="Operator-approved public datasets",
        source_kind="public_dataset",
        capabilities=("structured_api", "dataset", "document"),
        license_notes="Operator must record dataset license, jurisdiction, and capture terms.",
        jurisdiction="operator-configured",
        languages=("en",),
        freshness_hours=168,
        cost="operator-supplied",
        access_requirement="operator_supplied",
        robots_supported=True,
        evidence_capture_method="response_manifest",
    ),
)


def configured_public_provider_registry(
    providers: Iterable[ProviderDefinition] = DEFAULT_PUBLIC_PROVIDER_REGISTRY,
) -> tuple[ProviderDefinition, ...]:
    """Return an ordered, duplicate-free public registry suitable for planning."""

    registry = tuple(sorted(providers, key=lambda provider: provider.provider_id))
    ids = [provider.provider_id for provider in registry]
    if len(ids) != len(set(ids)):
        raise ValueError("Provider ids must be unique.")
    return registry


@dataclass(frozen=True)
class ResearchFleetTask:
    task_id: str
    provider_id: str
    capability: str
    normalized_query: str
    languages: tuple[str, ...]
    place: str | None
    date_range: str | None
    evidence_requirement: str
    priority: int
    ranking_reason: str

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["languages"] = list(self.languages)
        return payload


@dataclass(frozen=True)
class ResearchFleetPlan:
    question: str
    normalized_query: str
    tasks: tuple[ResearchFleetTask, ...]
    blocked_providers: tuple[dict[str, str], ...]
    coverage_ledger: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "question": self.question,
            "normalized_query": self.normalized_query,
            "tasks": [task.to_dict() for task in self.tasks],
            "blocked_providers": [dict(item) for item in self.blocked_providers],
            "coverage_ledger": self.coverage_ledger,
        }


def normalize_research_query(
    question: str,
    *,
    aliases: Iterable[str] = (),
    place: str | None = None,
    date_range: str | None = None,
) -> str:
    """Normalize operator input into a stable, non-semantic query string."""

    chunks = [_SPACE.sub(" ", str(question).strip())]
    chunks.extend(_SPACE.sub(" ", str(alias).strip()) for alias in aliases if str(alias).strip())
    if place and place.strip():
        chunks.append(_SPACE.sub(" ", place.strip()))
    if date_range and date_range.strip():
        chunks.append(_SPACE.sub(" ", date_range.strip()))
    normalized = " ".join(dict.fromkeys(chunk.casefold() for chunk in chunks if chunk))
    if not normalized:
        raise ValueError("An investigation question is required.")
    return normalized


def plan_research_fleet(
    question: str,
    *,
    aliases: Iterable[str] = (),
    languages: Iterable[str] = ("en",),
    place: str | None = None,
    date_range: str | None = None,
    source_kinds: Iterable[str] = (),
    evidence_requirement: str = "citation_required",
    providers: Iterable[ProviderDefinition] = DEFAULT_PUBLIC_PROVIDER_REGISTRY,
) -> ResearchFleetPlan:
    """Create deterministic provider tasks and an initial coverage ledger."""

    normalized = normalize_research_query(question, aliases=aliases, place=place, date_range=date_range)
    requested_languages = tuple(sorted({item.strip().lower() for item in languages if item.strip()})) or ("en",)
    requested_kinds = {item.strip() for item in source_kinds if item.strip()}
    ledger = CoverageLedger()
    tasks: list[ResearchFleetTask] = []
    blocked: list[dict[str, str]] = []
    for provider in configured_public_provider_registry(providers):
        if requested_kinds and provider.source_kind not in requested_kinds:
            continue
        if not provider.enabled or provider.cost == "disabled":
            blocked.append({"provider_id": provider.provider_id, "reason": "provider_disabled"})
            continue
        if not provider.robots_supported:
            blocked.append({"provider_id": provider.provider_id, "reason": "robots_not_supported"})
            continue
        language_overlap = tuple(language for language in requested_languages if language in provider.languages)
        if not language_overlap:
            blocked.append({"provider_id": provider.provider_id, "reason": "language_not_supported"})
            continue
        capability = next((item for item in provider.capabilities if item in STATIC_COLLECTION_CAPABILITIES), None)
        if capability is None:
            blocked.append({"provider_id": provider.provider_id, "reason": "no_static_collection_capability"})
            continue
        task_key = f"{provider.provider_id}\0{capability}\0{normalized}\0{','.join(language_overlap)}"
        task_id = "fleet-" + hashlib.sha256(task_key.encode("utf-8")).hexdigest()[:20]
        tasks.append(
            ResearchFleetTask(
                task_id=task_id,
                provider_id=provider.provider_id,
                capability=capability,
                normalized_query=normalized,
                languages=language_overlap,
                place=place.strip() if place else None,
                date_range=date_range.strip() if date_range else None,
                evidence_requirement=evidence_requirement,
                priority=1000 + (100 if provider.source_kind == "official_public_record" else 0),
                ranking_reason=f"configured_{provider.source_kind}; capability={capability}",
            )
        )
    tasks.sort(key=lambda task: (-task.priority, task.provider_id, task.task_id))
    ledger.record_search(scope=normalized, source_ids=(task.provider_id for task in tasks))
    for item in blocked:
        ledger.record_gap(
            gap_key=f"provider:{item['provider_id']}",
            source_id=item["provider_id"],
            reason=item["reason"],
        )
    return ResearchFleetPlan(
        question=" ".join(question.split()),
        normalized_query=normalized,
        tasks=tuple(tasks),
        blocked_providers=tuple(sorted(blocked, key=lambda item: item["provider_id"])),
        coverage_ledger=ledger.to_json(),
    )


def deduplicate_candidate_urls(urls: Iterable[str]) -> list[dict[str, str]]:
    """Globally dedupe canonical URLs while retaining stable domain accounting."""

    unique: dict[str, dict[str, str]] = {}
    for raw_url in urls:
        try:
            canonical_url = canonicalize_url(str(raw_url))
        except ValueError:
            continue
        if canonical_url not in unique:
            unique[canonical_url] = {
                "canonical_url": canonical_url,
                "domain": normalize_domain(canonical_url) or "unknown",
            }
    return [unique[key] for key in sorted(unique)]


def admit_static_collection_url(
    url: str,
    *,
    provider: ProviderDefinition,
    resolver: Resolver | None = None,
) -> dict[str, object]:
    """Validate static collection targets before a worker is allowed to fetch them.

    The existing discovery validator re-resolves each host and rejects loopback,
    private, link-local and other non-public addresses.  Browser-rendered collection
    is intentionally never admitted by this Phase 1 static worker.
    """

    if not provider.enabled or provider.cost == "disabled":
        raise ValueError("Provider is disabled.")
    if not provider.robots_supported:
        raise ValueError("Provider does not support robots-aware collection.")
    if provider.access_requirement not in PUBLIC_ACCESS_REQUIREMENTS:
        raise ValueError("Provider requires prohibited access.")
    static_capability = next((item for item in provider.capabilities if item in STATIC_COLLECTION_CAPABILITIES), None)
    if static_capability is None:
        raise ValueError("Provider has no approved static collection capability.")
    candidate = canonicalize_url(url)
    if urlsplit(candidate).scheme != "https":
        raise UnsafeTargetError("Static collection requires HTTPS.")
    kwargs = {"allow_private_networks": False}
    if resolver is not None:
        kwargs["resolver"] = resolver
    validated_url = validate_fetch_url(candidate, **kwargs)  # type: ignore[arg-type]
    return {
        "provider_id": provider.provider_id,
        "capability": static_capability,
        "canonical_url": canonicalize_url(validated_url),
        "domain": normalize_domain(validated_url) or "unknown",
        "max_response_bytes": provider.default_byte_budget,
        "max_requests": provider.default_request_budget,
        "max_concurrency": provider.default_concurrency,
        "max_retries": provider.default_retry_budget,
    }


def source_health_and_gaps(
    *,
    ledger_payload: Mapping[str, object] | None = None,
    providers: Iterable[ProviderDefinition] = DEFAULT_PUBLIC_PROVIDER_REGISTRY,
    now: datetime | None = None,
) -> dict[str, object]:
    """Return a stable, scheduler-ready health and coverage-gap snapshot."""

    ledger = CoverageLedger.from_json(dict(ledger_payload or {}))
    registry = configured_public_provider_registry(providers)
    attempted = ledger.attempted_source_ids()
    open_gaps = [
        {"gap_key": key, **value}
        for key, value in sorted(ledger.gaps.items())
        if value.get("status") == "open"
    ]
    health: list[dict[str, object]] = []
    for provider in registry:
        provider_attempts = [item for item in ledger.attempts if item.get("source_id") == provider.provider_id]
        failures = sum(1 for item in provider_attempts if item.get("status") == "failed")
        state = "disabled" if not provider.enabled or provider.cost == "disabled" else "unknown"
        if provider.provider_id in attempted:
            state = "degraded" if failures else "healthy"
        health.append(
            {
                "provider_id": provider.provider_id,
                "state": state,
                "schema_version": provider.schema_version,
                "attempt_count": len(provider_attempts),
                "failure_count": failures,
                "freshness_hours": provider.freshness_hours,
            }
        )
    covered_kinds = {provider.source_kind for provider in registry if provider.enabled and provider.cost != "disabled"}
    expected_kinds = {"official_public_record", "established_news", "public_dataset"}
    for source_kind in sorted(expected_kinds - covered_kinds):
        open_gaps.append({"gap_key": f"source_kind:{source_kind}", "reason": "no_configured_provider", "status": "open"})
    return {
        "generated_at": (now or datetime.now(timezone.utc)).isoformat(),
        "providers": health,
        "open_gaps": open_gaps,
        "coverage": {
            "search_count": len(ledger.searches),
            "attempt_count": len(ledger.attempts),
            "changed_count": len(ledger.changes),
            "provider_count": len(registry),
        },
    }
