from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from email.utils import format_datetime
from urllib.parse import urlparse
from xml.etree import ElementTree

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import (
    AlertORM,
    CustodyLogORM,
    ObservationORM,
    ScheduledTaskORM,
    SourceDefinitionORM,
    SourceRunORM,
    StorageObjectORM,
    WatchORM,
    WatchReportORM,
    WatchRuleVersionORM,
    WatchRunORM,
)
from src.schemas import (
    InvestigationWatchCompilation,
    InvestigationWatchCompileRequest,
    InvestigationWatchRule,
    WatchCreate,
    WatchScheduleCreate,
    WatchUpdate,
)
from src.config import get_settings
from src.services.discovery_fetch import FetchPolicy, fetch_url
from src.services.storage_service import register_storage_object
from src.services.scheduler_service import create_scheduled_task
from src.schemas import ScheduledTaskCreate
from src.services.source_service import run_source_definition
from src.services.materiality_service import (
    MaterialityCandidate,
    MaterialityPolicy,
    UpdateLedger,
    evaluate_materiality,
)
from src.services.source_coverage_service import (
    CoverageLedger,
    RetryPolicy,
    SourceCandidate,
    plan_collection_action,
)


def now() -> datetime:
    return datetime.now(timezone.utc)


def require_watch(session: Session, watch_id: int) -> WatchORM:
    watch = session.get(WatchORM, watch_id)
    if watch is None:
        raise ValueError(f"Watch {watch_id} does not exist.")
    return watch


def canonical_rule_json(rule: object) -> dict[str, object]:
    """Produce a stable representation used for version lineage and content hashing."""
    if hasattr(rule, "model_dump"):
        value = rule.model_dump(mode="json")  # type: ignore[union-attr]
    elif isinstance(rule, dict):
        value = rule
    else:
        raise ValueError("watch rule must be an object")
    return json.loads(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False))


def rule_hash(rule: object) -> str:
    return hashlib.sha256(
        json.dumps(canonical_rule_json(rule), sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _scope_preview(rule: dict[str, object]) -> dict[str, object]:
    if rule.get("mode") != "investigation_watch":
        return {"mode": rule.get("mode"), "source_id": rule.get("source_id")}
    return {
        "parent_investigation_id": rule["parent_investigation_id"],
        "query_version": rule["query_version"],
        "query": rule["query"],
        "targets": {
            "concepts": rule.get("target_concepts", []),
            "entities": rule.get("target_entities", []),
            "locations": rule.get("target_locations", []),
            "time_context": rule.get("time_context", {}),
        },
        "allowed_source_types": rule.get("allowed_source_types", []),
        "domain_policy_snapshot": rule.get("domain_policy_snapshot", {}),
        "thresholds": {
            "relevance": rule.get("relevance_threshold"),
            "materiality": rule.get("materiality_threshold"),
        },
        "fetch_budget": rule.get("fetch_budget", {}),
        "schedule_policy": rule.get("schedule_policy", {}),
    }


def _record_rule_version(
    session: Session,
    watch: WatchORM,
    *,
    rule: object,
    status: str,
    compiler_name: str = "structured-watch-v1",
    original_instruction: str | None = None,
) -> WatchRuleVersionORM:
    canonical = canonical_rule_json(rule)
    previous = session.scalar(
        select(WatchRuleVersionORM)
        .where(WatchRuleVersionORM.watch_id == watch.watch_id)
        .order_by(WatchRuleVersionORM.version_number.desc())
    )
    version = WatchRuleVersionORM(
        watch_id=watch.watch_id,
        version_number=(previous.version_number + 1) if previous else 1,
        query_version=str(canonical.get("query_version", "1")),
        status=status,
        compiler_name=compiler_name,
        original_instruction=original_instruction,
        rule_hash=rule_hash(canonical),
        rule_json=canonical,
        scope_preview_json=_scope_preview(canonical),
        activated_at=now() if status == "active" else None,
    )
    session.add(version)
    return version


def list_watch_rule_versions(session: Session, watch_id: int) -> list[WatchRuleVersionORM]:
    require_watch(session, watch_id)
    return list(
        session.scalars(
            select(WatchRuleVersionORM)
            .where(WatchRuleVersionORM.watch_id == watch_id)
            .order_by(WatchRuleVersionORM.version_number.asc())
        )
    )


def compile_investigation_watch_instruction(
    payload: InvestigationWatchCompileRequest,
) -> InvestigationWatchCompilation:
    """Compile without a model call so identical input always has identical scope."""
    instruction = " ".join(payload.instruction.split())
    lowered = instruction.casefold()
    quoted = _extract_quoted_phrases(instruction)
    target_concepts = list(payload.target_concepts) + quoted
    target_locations = list(payload.target_locations)
    target_entities = list(payload.target_entities)

    # Keep extraction intentionally modest. Ambiguous wording is surfaced for review,
    # never delegated to an opaque model before the rule is enforced.
    subject = _extract_subject(instruction)
    if subject:
        value = subject.strip(" ,.;:")
        if value:
            target_concepts.append(value)
    location = _extract_location(instruction)
    if location:
        target_locations.append(location)
    # A quoted phrase is a deliberate entity reference when the caller did not provide one.
    if quoted and not target_entities:
        target_entities.extend(quoted)

    ambiguities: list[str] = []
    if not (target_concepts or target_entities or target_locations):
        # A non-empty instruction is still a valid scope; the normalized instruction is
        # the explicit concept and the preview asks the operator to verify it.
        target_concepts.append(instruction)
        ambiguities.append(
            "No explicit target phrase was detected; confirm the normalized instruction."
        )
    if not payload.target_locations and " in " in f" {lowered} ":
        ambiguities.append(
            "A location phrase was inferred; confirm its geographic boundary before enabling."
        )

    rule = InvestigationWatchRule(
        parent_investigation_id=payload.parent_investigation_id,
        query_version=payload.query_version,
        query=instruction,
        target_concepts=target_concepts,
        target_entities=target_entities,
        target_locations=target_locations,
        allowed_source_types=payload.allowed_source_types
        or ["public_api", "rss", "website", "news", "dataset", "government_record"],
        domain_policy_snapshot=payload.domain_policy_snapshot,
        relevance_threshold=payload.relevance_threshold,
        materiality_threshold=payload.materiality_threshold,
        fetch_budget=payload.fetch_budget,
        schedule_policy=payload.schedule_policy,
    )
    canonical = canonical_rule_json(rule)
    return InvestigationWatchCompilation(
        rule=rule,
        canonical_rule_json=canonical,
        rule_hash=rule_hash(canonical),
        scope_preview_json=_scope_preview(canonical),
        unresolved_ambiguities=ambiguities,
    )


def _extract_quoted_phrases(value: str) -> list[str]:
    phrases: list[str] = []
    open_quote: str | None = None
    start = 0
    closing_quotes = {'"': '"', '“': '”'}
    for index, character in enumerate(value):
        if open_quote is None and character in closing_quotes:
            open_quote, start = character, index + 1
        elif open_quote is not None and character == closing_quotes[open_quote]:
            phrase = value[start:index].strip()
            if phrase:
                phrases.append(phrase)
            open_quote = None
    return phrases


def _extract_subject(value: str) -> str | None:
    lowered = value.casefold()
    for verb in ("watch", "monitor", "track", "follow", "investigate"):
        marker = f"{verb} "
        position = lowered.find(marker)
        if position < 0:
            continue
        start = position + len(marker)
        if lowered[start:].startswith("for "):
            start += len("for ")
        subject = _trim_watch_clause(
            value[start:],
            ("in", "near", "around", "at", "from", "within", "during", "since", "after", "before"),
        )
        normalized = subject.casefold()
        for prefix in ("about ", "related to "):
            if normalized.startswith(prefix):
                return subject[len(prefix) :]
        return subject
    return None


def _extract_location(value: str) -> str | None:
    lowered = value.casefold()
    matches = [
        (lowered.find(f" {marker} "), marker)
        for marker in ("in", "near", "around", "at", "within")
    ]
    positions = [(position, marker) for position, marker in matches if position >= 0]
    if not positions:
        return None
    position, marker = min(positions)
    start = position + len(marker) + 2
    location = _trim_watch_clause(value[start:], ("during", "since", "after", "before", "from", "for"))
    return location.strip() or None


def _trim_watch_clause(value: str, stop_words: tuple[str, ...]) -> str:
    lowered = value.casefold()
    boundaries = [len(value)]
    for punctuation in (",", ".", ";"):
        position = value.find(punctuation)
        if position >= 0:
            boundaries.append(position)
    for word in stop_words:
        position = lowered.find(f" {word} ")
        if position >= 0:
            boundaries.append(position)
    return value[: min(boundaries)].strip()


def create_investigation_watch_candidate(
    session: Session,
    payload: InvestigationWatchCompileRequest,
    *,
    name: str,
    slug: str,
    description: str = "",
    severity: str = "info",
    actor: str = "api",
) -> tuple[WatchORM, InvestigationWatchCompilation]:
    """Persist a paused candidate; an explicit resume is the enable acknowledgement."""
    compilation = compile_investigation_watch_instruction(payload)
    watch = create_watch(
        session,
        WatchCreate(
            name=name,
            slug=slug,
            objective=compilation.rule.query,
            description=description,
            watch_type="investigation_watch",
            state="paused",
            rule_json=compilation.rule,
            severity=severity,
            provenance_json={
                "original_instruction": payload.instruction,
                "compiler_name": compilation.compiler_name,
                "rule_hash": compilation.rule_hash,
            },
        ),
        actor=actor,
    )
    return watch, compilation


def create_watch(session: Session, payload: WatchCreate, *, actor: str = "api") -> WatchORM:
    if session.scalar(select(WatchORM).where(WatchORM.name == payload.name)) is not None:
        raise ValueError(f"Watch name {payload.name!r} already exists.")
    if session.scalar(select(WatchORM).where(WatchORM.slug == payload.slug)) is not None:
        raise ValueError(f"Watch slug {payload.slug!r} already exists.")
    values = payload.model_dump(mode="json")
    # Pydantic serializes discriminated unions through the declared union in a few
    # paths; serialize the concrete rule itself so its required `mode` is preserved.
    values["rule_json"] = canonical_rule_json(payload.rule_json)
    watch = WatchORM(**values)
    session.add(watch)
    session.flush()
    initial_status = (
        "archived"
        if watch.state == "archived"
        else ("active" if watch.state == "enabled" else "candidate")
    )
    compiler_name = (
        "deterministic-investigation-watch-v1"
        if watch.watch_type == "investigation_watch"
        else "structured-watch-v1"
    )
    _record_rule_version(
        session,
        watch,
        rule=watch.rule_json,
        status=initial_status,
        compiler_name=compiler_name,
        original_instruction=(watch.provenance_json or {}).get("original_instruction"),
    )
    session.add(
        CustodyLogORM(
            object_type="watch",
            object_id=str(watch.watch_id),
            action="watch_created",
            actor=actor,
            details_json={"watch_type": watch.watch_type, "rule_json": watch.rule_json},
        )
    )
    session.commit()
    session.refresh(watch)
    return watch


def list_watches(
    session: Session, *, state: str | None = None, watch_type: str | None = None, limit: int = 200
) -> list[WatchORM]:
    statement = select(WatchORM).order_by(WatchORM.watch_id.desc())
    if state:
        statement = statement.where(WatchORM.state == state)
    if watch_type:
        statement = statement.where(WatchORM.watch_type == watch_type)
    return list(session.scalars(statement.limit(limit)))


def get_watch(session: Session, watch_id: int) -> WatchORM:
    return require_watch(session, watch_id)


def update_watch(
    session: Session, watch_id: int, payload: WatchUpdate, *, actor: str = "api"
) -> WatchORM:
    watch = require_watch(session, watch_id)
    if watch.state == "archived":
        raise ValueError(f"Watch {watch_id} is archived and its scope is immutable.")
    changes = payload.model_dump(mode="json", exclude_unset=True)
    if payload.rule_json is not None:
        changes["rule_json"] = canonical_rule_json(payload.rule_json)
    if "name" in changes and changes["name"] != watch.name:
        if session.scalar(select(WatchORM).where(WatchORM.name == changes["name"])) is not None:
            raise ValueError(f"Watch name {changes['name']!r} already exists.")
    if "slug" in changes and changes["slug"] != watch.slug:
        if session.scalar(select(WatchORM).where(WatchORM.slug == changes["slug"])) is not None:
            raise ValueError(f"Watch slug {changes['slug']!r} already exists.")
    if "rule_json" in changes:
        requested_type = changes.get("watch_type", watch.watch_type)
        if changes["rule_json"].get("mode") != requested_type:
            raise ValueError(
                "rule_json mode must match the effective watch_type "
                f"({changes['rule_json'].get('mode')!r} != {requested_type!r})"
            )
        previous_versions = list_watch_rule_versions(session, watch_id)
        for version in previous_versions:
            if version.status in {"active", "candidate"}:
                version.status = "superseded"
                version.superseded_at = now()
    for key, value in changes.items():
        setattr(watch, key, value)
    if any(key in changes for key in {"watch_type", "rule_json", "source_id", "layer_key"}):
        watch.baseline_json, watch.dedupe_json = {}, {}
    if "rule_json" in changes:
        _record_rule_version(
            session,
            watch,
            rule=watch.rule_json,
            status="active" if watch.state == "enabled" else "candidate",
            compiler_name=(
                "deterministic-investigation-watch-v1"
                if watch.watch_type == "investigation_watch"
                else "structured-watch-v1"
            ),
            original_instruction=(watch.provenance_json or {}).get("original_instruction"),
        )
    session.add(
        CustodyLogORM(
            object_type="watch",
            object_id=str(watch_id),
            action="watch_updated",
            actor=actor,
            details_json={"changes": list(changes)},
        )
    )
    session.commit()
    session.refresh(watch)
    return watch


def pause_watch(session: Session, watch_id: int, *, actor: str = "api") -> WatchORM:
    return set_watch_state(session, watch_id, "paused", actor=actor)


def resume_watch(session: Session, watch_id: int, *, actor: str = "api") -> WatchORM:
    return set_watch_state(session, watch_id, "enabled", actor=actor)


def set_watch_state(session: Session, watch_id: int, state: str, *, actor: str) -> WatchORM:
    watch = require_watch(session, watch_id)
    if watch.state == "archived" and state != "archived":
        raise ValueError(f"Watch {watch_id} is archived and cannot be resumed.")
    if state not in {"enabled", "paused", "archived"}:
        raise ValueError(f"Unsupported watch state {state!r}.")
    watch.state = state
    task = (
        session.get(ScheduledTaskORM, watch.scheduled_task_id) if watch.scheduled_task_id else None
    )
    if task is not None:
        task.enabled = state == "enabled"
    if state == "enabled":
        for version in list_watch_rule_versions(session, watch_id):
            if version.status == "candidate":
                version.status, version.activated_at = "active", now()
    elif state == "archived":
        for version in list_watch_rule_versions(session, watch_id):
            if version.status in {"active", "candidate"}:
                version.status, version.archived_at = "archived", now()
    session.add(
        CustodyLogORM(
            object_type="watch",
            object_id=str(watch_id),
            action=f"watch_{state}",
            actor=actor,
            details_json={},
        )
    )
    session.commit()
    session.refresh(watch)
    return watch


def archive_watch(session: Session, watch_id: int, *, actor: str = "api") -> WatchORM:
    """Stop collection permanently while preserving every rule, run, and report."""
    return set_watch_state(session, watch_id, "archived", actor=actor)


def _watch_expired(watch: WatchORM) -> bool:
    if watch.watch_type != "investigation_watch":
        return False
    expires_at = ((watch.rule_json or {}).get("schedule_policy") or {}).get("expires_at")
    if not expires_at:
        return False
    try:
        expiry = datetime.fromisoformat(str(expires_at).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("investigation watch expiry is not a valid ISO-8601 timestamp") from exc
    return expiry.astimezone(timezone.utc) <= now()


def _source_candidate(source: SourceDefinitionORM) -> SourceCandidate:
    metadata = source.metadata_json or {}
    domain = (urlparse(source.target_uri).hostname or "local").casefold()
    return SourceCandidate(
        source_id=str(source.source_id),
        source_kind=source.source_kind,
        domain=domain,
        domain_class=str(metadata.get("domain_class") or "default"),
        enabled=source.enabled,
    )


def _source_type_for_kind(source_kind: str) -> str:
    if source_kind == "rss":
        return "rss"
    if source_kind in {"web_search", "web_crawl", "web_discovery"}:
        return "website"
    if source_kind == "local_file":
        return "local_feed"
    return "public_api"


def _eligible_coverage_candidates(
    watch: WatchORM,
    sources: list[SourceDefinitionORM],
) -> list[SourceCandidate]:
    rule = watch.rule_json or {}
    allowed_types = set(rule.get("allowed_source_types", []))
    snapshot = (
        rule.get("domain_policy_snapshot")
        if isinstance(rule.get("domain_policy_snapshot"), dict)
        else {}
    )
    allow_domains = set(snapshot.get("allowed_domains", []))
    blocked_domains = set(snapshot.get("blocked_domains", []))
    candidates: list[SourceCandidate] = []
    for source in sources:
        candidate = _source_candidate(source)
        if candidate.domain in blocked_domains:
            continue
        if allow_domains and candidate.domain not in allow_domains:
            continue
        if allowed_types and _source_type_for_kind(source.source_kind) not in allowed_types:
            continue
        candidates.append(candidate)
    return candidates


def _coverage_policy(
    watch: WatchORM,
    source: SourceDefinitionORM,
    candidates: list[SourceCandidate],
) -> RetryPolicy:
    rule = watch.rule_json or {}
    metadata = source.metadata_json or {}
    declared_budgets = metadata.get("domain_class_budgets")
    budgets = (
        {str(key): int(value) for key, value in declared_budgets.items()}
        if isinstance(declared_budgets, dict)
        else {
            candidate.domain_class: int(rule.get("fetch_budget", {}).get("max_sources_per_run", 25))
            for candidate in candidates
        }
    )
    budgets.setdefault(
        str(metadata.get("domain_class") or "default"),
        int(rule.get("fetch_budget", {}).get("max_sources_per_run", 25)),
    )
    allowed = metadata.get("allowed_source_kinds")
    allowed_kinds = (
        frozenset(str(item) for item in allowed)
        if isinstance(allowed, list)
        else frozenset(candidate.source_kind for candidate in candidates)
    )
    schedule = rule.get("schedule_policy") if isinstance(rule.get("schedule_policy"), dict) else {}
    return RetryPolicy(
        allowed_source_kinds=allowed_kinds,
        domain_class_budgets=budgets,
        max_retry_attempts=int(schedule.get("retry_attempts", 2)),
        retry_base_seconds=float(schedule.get("retry_backoff_seconds", 1.0)),
        retry_max_seconds=float(schedule.get("retry_backoff_seconds", 1.0)) * 32 or 60.0,
    )


def _record_coverage(
    session: Session,
    watch: WatchORM,
    *,
    changed: bool,
    failure: Exception | None = None,
) -> None:
    """Persist an auditable source attempt without turning a source failure into silence."""
    if watch.source_id is None:
        return
    source = session.get(SourceDefinitionORM, watch.source_id)
    if source is None:
        return
    ledger = CoverageLedger.from_json(watch.coverage_json or {})
    candidate = _source_candidate(source)
    sources = list(
        session.scalars(
            select(SourceDefinitionORM).where(SourceDefinitionORM.layer_key == source.layer_key)
        )
    )
    candidates = _eligible_coverage_candidates(watch, sources)
    # Non-investigation watches predate source-type policy; their configured source is
    # still eligible, while alternates remain bounded to the same source fleet.
    if candidate not in candidates:
        candidates.append(candidate)
    policy = _coverage_policy(watch, source, candidates)
    ledger.record_search(scope=watch.objective, source_ids=[candidate.source_id])
    if failure is None:
        plan_collection_action(
            candidate,
            outcome="success",
            changed=changed,
            failure_kind=None,
            ledger=ledger,
            policy=policy,
        )
    else:
        text = str(failure).casefold()
        failure_kind = (
            "timeout"
            if "timeout" in text
            else "network_error"
            if "network" in text
            else "server_error"
        )
        actions = plan_collection_action(
            candidate,
            outcome="failed",
            changed=False,
            failure_kind=failure_kind,
            ledger=ledger,
            policy=policy,
            alternate_candidates=candidates,
        )
        # Actions are intentionally recorded in the coverage snapshot for the W2
        # worker to execute; this evaluator does not bypass scheduling/politeness.
        ledger.changes.extend(
            {"action": action.to_json(), "occurred_at": now().isoformat()} for action in actions
        )
    watch.coverage_json = ledger.to_json()


def _materiality_candidate(
    watch: WatchORM,
    *,
    fingerprint: str,
    evidence: dict[str, object],
    source_run_id: int | None,
    storage_object_id: int | None,
) -> MaterialityCandidate:
    source_id = str(evidence.get("source_id") or watch.source_id or f"watch:{watch.watch_id}")
    source_key = str(evidence.get("source_uri") or evidence.get("source_domain") or source_id)
    citation_ids = tuple(
        str(item)
        for item in (
            evidence.get("source_uri"),
            source_run_id,
            storage_object_id,
            *evidence.get("observation_ids", []),
        )
        if item is not None
    )
    rule_hash_value = rule_hash(watch.rule_json or {})
    return MaterialityCandidate(
        evidence_id=str(evidence.get("source_run_id") or evidence.get("sha256") or fingerprint),
        evidence_version=str(evidence.get("source_run_id") or fingerprint),
        source_id=source_id,
        source_independence_key=source_key,
        content_hash=fingerprint,
        claim_id=f"watch:{watch.watch_id}:{rule_hash_value}",
        claim_version=fingerprint,
        confidence=float(evidence.get("confidence", 1.0)),
        claim_impact=float(evidence.get("claim_impact", 0.75)),
        temporal_relevance=float(evidence.get("temporal_relevance", 0.75)),
        geographic_relevance=float(evidence.get("geographic_relevance", 0.75)),
        event_id=str(watch.event_id) if watch.event_id is not None else None,
        citation_ids=citation_ids,
    )


def _materiality_policy(watch: WatchORM) -> MaterialityPolicy:
    rule = watch.rule_json or {}
    if watch.watch_type == "investigation_watch":
        return MaterialityPolicy(
            min_materiality_score=float(rule.get("materiality_threshold", 0.75)),
            min_alert_confidence=float(rule.get("relevance_threshold", 0.6)),
        )
    return MaterialityPolicy()


def evaluate_watch(
    session: Session,
    watch_id: int,
    *,
    actor: str = "api",
    force: bool = False,
    scheduled_task_run_id: int | None = None,
) -> WatchRunORM:
    watch = require_watch(session, watch_id)
    # `force` is kept for backwards-compatible request parsing, but it cannot defeat
    # an operator pause/archive: those controls must stop all new source work.
    if watch.state != "enabled":
        raise ValueError(f"Watch {watch_id} is {watch.state}.")
    if _watch_expired(watch):
        archive_watch(session, watch_id, actor="system:expiry")
        raise ValueError(f"Watch {watch_id} expired and was archived.")
    run = WatchRunORM(
        watch_id=watch_id,
        scheduled_task_run_id=scheduled_task_run_id,
        checkpoint_before_json=dict(watch.baseline_json or {}),
    )
    session.add(run)
    session.flush()
    try:
        fingerprint, evidence, message, source_run_id, storage_object_id = evaluate_rule(
            session, watch, actor
        )
        baseline = dict(watch.baseline_json or {})
        initialized = "fingerprint" not in baseline
        changed = not initialized and baseline.get("fingerprint") != fingerprint
        ledger = UpdateLedger.from_json(watch.dedupe_json or {})
        decision = evaluate_materiality(
            _materiality_candidate(
                watch,
                fingerprint=fingerprint,
                evidence=evidence,
                source_run_id=source_run_id,
                storage_object_id=storage_object_id,
            ),
            ledger,
            policy=_materiality_policy(watch),
        )
        watch.dedupe_json = ledger.to_json()
        _record_coverage(session, watch, changed=changed)
        baseline["fingerprint"] = fingerprint
        baseline["evaluated_at"] = now().isoformat()
        watch.baseline_json = baseline
        watch.last_evaluated_at = now()
        evidence = {**evidence, "materiality": decision.to_json()}
        run.baseline_initialized = initialized
        run.change_detected = changed
        run.source_run_id = source_run_id
        run.storage_object_id = storage_object_id
        run.outcome = "baseline" if initialized else ("change" if changed else "no_change")
        run.status = "completed"
        run.evidence_json = evidence
        run.metadata_json = {"materiality": decision.to_json()}
        run.output_summary = message
        run.checkpoint_after_json = baseline
        run.finished_at = now()
        if changed and decision.alert:
            key = f"watch:{watch.watch_id}:{decision.dedupe_key}"
            alert = session.scalar(
                select(AlertORM).where(AlertORM.dedupe_key == key, AlertORM.status == "open")
            )
            if alert is None:
                alert = AlertORM(
                    severity=watch.severity,
                    status="open",
                    dedupe_key=key,
                    message=message,
                    trigger_basis_json={
                        "watch_id": watch.watch_id,
                        "watch_run_id": run.watch_run_id,
                        "evidence": evidence,
                        "materiality": decision.to_json(),
                    },
                )
                session.add(alert)
                session.flush()
                session.add(
                    CustodyLogORM(
                        object_type="alert",
                        object_id=str(alert.alert_id),
                        action="alert_created",
                        actor=actor,
                        details_json={"watch_id": watch.watch_id},
                    )
                )
            run.alert_id = alert.alert_id
            watch.last_changed_at = now()
        session.add(
            CustodyLogORM(
                object_type="watch_run",
                object_id=str(run.watch_run_id),
                action="watch_evaluated",
                actor=actor,
                details_json={"outcome": run.outcome, "change_detected": changed},
            )
        )
        session.commit()
        session.refresh(run)
        return run
    except Exception as exc:
        _record_coverage(session, watch, changed=False, failure=exc)
        run.status, run.outcome, run.error_text, run.finished_at = (
            "failed",
            "failure",
            str(exc),
            now(),
        )
        session.commit()
        raise


def evaluate_rule(
    session: Session, watch: WatchORM, actor: str
) -> tuple[str, dict[str, object], str, int | None, int | None]:
    rule = watch.rule_json or {}
    if watch.watch_type == "investigation_watch":
        # An investigation watch may be bound to one managed source (which runs
        # through the normal bounded/robots-aware source path) or to the local
        # normalized observation ledger.  The latter is intentionally read-only:
        # it gives an operator a continuing watch over already-approved inbound
        # feeds without silently inventing new public-web collection work.
        if watch.source_id is not None:
            source = session.get(SourceDefinitionORM, watch.source_id)
            if source is None or not source.enabled:
                raise ValueError("investigation watch source is missing or disabled.")
            source_run = run_source_definition(session, watch.source_id, actor=actor)
            fingerprint = str(
                (source_run.output_json or {}).get("payload_sha256") or source_run.source_run_id
            )
            evidence = {
                "source_id": source.source_id,
                "source_run_id": source_run.source_run_id,
                "source_uri": source.target_uri,
                "citations": [{"uri": source.target_uri, "label": source.name}],
                "confidence": 1.0,
                "claim_impact": rule.get("materiality_threshold", 0.75),
                "temporal_relevance": 1.0,
                "geographic_relevance": 1.0 if rule.get("target_locations") else 0.75,
            }
            return (
                fingerprint,
                evidence,
                f"Investigation watch {watch.name} processed managed source {source.name}.",
                source_run.source_run_id,
                None,
            )

        max_results = int((rule.get("fetch_budget") or {}).get("max_sources_per_run", 25))
        target_terms = {
            str(term).casefold()
            for key in ("target_concepts", "target_entities", "target_locations")
            for term in rule.get(key, [])
            if str(term).strip()
        }
        rows = list(
            session.scalars(
                select(ObservationORM)
                .order_by(ObservationORM.observation_id.desc())
                .limit(max_results * 10)
            )
        )
        matched = [
            row
            for row in rows
            if not target_terms
            or any(
                term in f"{row.content_text} {row.content_json}".casefold() for term in target_terms
            )
        ][:max_results]
        ids = [row.observation_id for row in matched]
        fingerprint = hashlib.sha256(",".join(str(item) for item in ids).encode()).hexdigest()
        return (
            fingerprint,
            {
                "observation_ids": ids,
                "confidence": min((row.confidence_score for row in matched), default=0.0),
                "claim_impact": rule.get("materiality_threshold", 0.75),
                "temporal_relevance": 0.75,
                "geographic_relevance": 1.0 if rule.get("target_locations") else 0.75,
            },
            f"Investigation watch {watch.name} matched {len(ids)} normalized observations.",
            None,
            None,
        )
    if watch.watch_type == "image_change":
        if watch.source_id is None:
            raise ValueError("image_change watches require source_id.")
        source = session.get(SourceDefinitionORM, watch.source_id)
        if source is None or not source.enabled:
            raise ValueError("image_change watch source is missing or disabled.")
        result = fetch_url(source.target_uri, policy=FetchPolicy(max_response_bytes=20_000_000))
        media_type = str(result.headers.get("Content-Type", "")).split(";", 1)[0].lower()
        accepted = set(
            rule.get("accepted_media_types", ["image/jpeg", "image/png", "image/webp", "image/gif"])
        )
        if media_type not in accepted:
            raise ValueError(f"Image watch received unsupported media type {media_type!r}.")
        digest = hashlib.sha256(result.payload).hexdigest()
        artifact_dir = get_settings().data_dir / "watch_artifacts"
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = artifact_dir / f"watch-{watch.watch_id}-{digest[:16]}"
        if not artifact_path.exists():
            artifact_path.write_bytes(result.payload)
        storage = register_storage_object(
            session,
            object_key=f"watch:{watch.watch_id}:image:{digest}",
            object_kind="watch_image_artifact",
            owner_type="watch",
            owner_id=str(watch.watch_id),
            object_uri=artifact_path.resolve().as_uri(),
            content_hash=digest,
            media_type=media_type,
            retention_class=rule.get("retention_class", "permanent"),
            source_uri=result.final_url,
            byte_size=len(result.payload),
            metadata_json={"watch_id": watch.watch_id, "status_code": result.status_code},
            actor=actor,
        )
        return (
            digest,
            {"source_id": source.source_id, "source_uri": result.final_url, "sha256": digest},
            f"Watch {watch.name} detected an image update.",
            None,
            storage.storage_object_id,
        )
    if watch.watch_type == "source_delta":
        if watch.source_id is None:
            raise ValueError("source_delta watches require source_id.")
        source_run = (
            run_source_definition(session, watch.source_id, actor=actor)
            if rule.get("run_source", True)
            else session.scalar(
                select(SourceRunORM)
                .where(SourceRunORM.source_id == watch.source_id)
                .order_by(SourceRunORM.source_run_id.desc())
            )
        )
        if source_run is None:
            raise ValueError("Source has no run to evaluate.")
        fingerprint = str(
            (source_run.output_json or {}).get("payload_sha256") or source_run.source_run_id
        )
        source = session.get(SourceDefinitionORM, watch.source_id)
        return (
            fingerprint,
            {
                "source_id": watch.source_id,
                "source_run_id": source_run.source_run_id,
                "source_uri": source.target_uri if source else None,
            },
            f"Watch {watch.name} detected a source update.",
            source_run.source_run_id,
            None,
        )
    if watch.watch_type == "observation_rule":
        statement = (
            select(ObservationORM)
            .order_by(ObservationORM.observation_id.desc())
            .limit(int(rule.get("max_results", 200)))
        )
        if watch.layer_key:
            statement = statement.where(ObservationORM.layer_key == watch.layer_key)
        if rule.get("source_domain"):
            statement = statement.where(ObservationORM.source_domain == rule["source_domain"])
        rows = list(session.scalars(statement))
        ids = [
            row.observation_id
            for row in rows
            if row.confidence_score >= float(rule.get("min_confidence", 0.0))
        ]
        fingerprint = hashlib.sha256(",".join(map(str, ids)).encode()).hexdigest()
        return (
            fingerprint,
            {"observation_ids": ids},
            f"Watch {watch.name} detected {len(ids)} matching observations.",
            None,
            None,
        )
    if watch.watch_type == "source_health":
        if watch.source_id is None:
            raise ValueError("source_health watches require source_id.")
        source = session.get(SourceDefinitionORM, watch.source_id)
        latest = session.scalar(
            select(SourceRunORM)
            .where(SourceRunORM.source_id == watch.source_id)
            .order_by(SourceRunORM.source_run_id.desc())
        )
        state = (
            "disabled"
            if source is None or not source.enabled
            else ("never_run" if latest is None else latest.status)
        )
        return (
            state,
            {
                "source_id": watch.source_id,
                "source_uri": source.target_uri if source else None,
                "state": state,
            },
            f"Watch {watch.name} source health is {state}.",
            latest.source_run_id if latest else None,
            None,
        )
    raise ValueError(f"Watch type {watch.watch_type!r} is not yet wired to a canonical evaluator.")


def attach_watch_schedule(
    session: Session, watch_id: int, payload: WatchScheduleCreate, *, actor: str = "api"
) -> ScheduledTaskORM:
    watch = require_watch(session, watch_id)
    if watch.state == "archived":
        raise ValueError(f"Watch {watch_id} is archived and cannot receive a schedule.")
    task = create_scheduled_task(
        session,
        ScheduledTaskCreate(
            name=payload.name or f"watch-{watch.slug}",
            task_type="watch_evaluate",
            interval_seconds=payload.interval_seconds,
            enabled=payload.enabled and watch.state == "enabled",
            retry_attempts=payload.retry_attempts,
            retry_backoff_seconds=payload.retry_backoff_seconds,
            notes=payload.notes,
            payload_json={"watch_id": watch_id},
        ),
    )
    watch.scheduled_task_id, watch.interval_seconds, watch.next_run_at = (
        task.task_id,
        task.interval_seconds,
        task.next_run_at,
    )
    session.commit()
    return task


def list_watch_runs(
    session: Session,
    *,
    watch_id: int | None = None,
    status: str | None = None,
    outcome: str | None = None,
    limit: int = 200,
) -> list[WatchRunORM]:
    statement = select(WatchRunORM).order_by(WatchRunORM.watch_run_id.desc())
    if watch_id:
        statement = statement.where(WatchRunORM.watch_id == watch_id)
    if status:
        statement = statement.where(WatchRunORM.status == status)
    if outcome:
        statement = statement.where(WatchRunORM.outcome == outcome)
    return list(session.scalars(statement.limit(limit)))


def list_watch_alerts(
    session: Session, *, watch_id: int | None = None, status: str | None = None, limit: int = 200
) -> list[AlertORM]:
    statement = select(AlertORM).order_by(AlertORM.alert_id.desc())
    if status:
        statement = statement.where(AlertORM.status == status)
    if watch_id:
        statement = statement.where(
            AlertORM.trigger_basis_json["watch_id"].as_integer() == watch_id
        )
    return list(session.scalars(statement.limit(limit)))


def list_watch_evidence(
    session: Session, watch_id: int, *, limit: int = 200
) -> list[StorageObjectORM]:
    require_watch(session, watch_id)
    ids = [
        str(run.storage_object_id)
        for run in list_watch_runs(session, watch_id=watch_id, limit=limit)
        if run.storage_object_id
    ]
    return (
        list(
            session.scalars(
                select(StorageObjectORM).where(StorageObjectORM.storage_object_id.in_(ids))
            )
        )
        if ids
        else []
    )


def generate_watch_report(session: Session, watch_id: int, *, actor: str = "api") -> WatchReportORM:
    """Create a retained, on-demand report from the immutable run and rule ledgers."""
    watch = require_watch(session, watch_id)
    if watch.watch_type == "investigation_watch" and not (watch.rule_json or {}).get(
        "report_on_demand", True
    ):
        raise ValueError(f"Watch {watch_id} does not permit on-demand reports.")
    versions = list_watch_rule_versions(session, watch_id)
    active_version = versions[-1].version_number if versions else None
    runs = list_watch_runs(session, watch_id=watch_id, limit=2000)
    material_runs = [run for run in runs if run.change_detected]
    citations = [
        {
            "watch_run_id": run.watch_run_id,
            "source_run_id": run.source_run_id,
            "storage_object_id": run.storage_object_id,
            "evidence": run.evidence_json,
        }
        for run in material_runs
    ]
    report = {
        "watch_id": watch.watch_id,
        "watch_name": watch.name,
        "state": watch.state,
        "generated_from_rule_version": active_version,
        "rule_versions": [
            {"version": version.version_number, "hash": version.rule_hash, "status": version.status}
            for version in versions
        ],
        "run_counts": {
            "total": len(runs),
            "material": len(material_runs),
            "failed": sum(run.status == "failed" for run in runs),
        },
        "citations": citations,
    }
    digest = hashlib.sha256(
        json.dumps(report, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()
    stored = WatchReportORM(
        watch_id=watch_id,
        rule_version=active_version,
        report_hash=digest,
        report_json=report,
        requested_by=actor,
    )
    session.add(stored)
    session.flush()
    session.add(
        CustodyLogORM(
            object_type="watch_report",
            object_id=str(stored.watch_report_id),
            action="watch_report_generated",
            actor=actor,
            details_json={"watch_id": watch_id, "report_hash": digest},
        )
    )
    session.commit()
    session.refresh(stored)
    return stored


def list_watch_reports(
    session: Session, watch_id: int, *, limit: int = 200
) -> list[WatchReportORM]:
    require_watch(session, watch_id)
    return list(
        session.scalars(
            select(WatchReportORM)
            .where(WatchReportORM.watch_id == watch_id)
            .order_by(WatchReportORM.watch_report_id.desc())
            .limit(limit)
        )
    )


def render_watch_alert_rss(
    session: Session, *, base_url: str, watch_id: int | None, status: str | None, limit: int
) -> str:
    root = ElementTree.Element("rss", version="2.0")
    channel = ElementTree.SubElement(root, "channel")
    ElementTree.SubElement(channel, "title").text = "11Writer Forte Watch Alerts"
    ElementTree.SubElement(channel, "link").text = f"{base_url}/api/watches/feed.rss"
    ElementTree.SubElement(channel, "description").text = "Local rule-generated watch alerts"
    for alert in list_watch_alerts(session, watch_id=watch_id, status=status, limit=limit):
        item = ElementTree.SubElement(channel, "item")
        ElementTree.SubElement(item, "title").text = f"[{alert.severity}] {alert.message}"
        ElementTree.SubElement(item, "guid").text = f"alert:{alert.alert_id}"
        ElementTree.SubElement(item, "pubDate").text = format_datetime(alert.created_at)
    return ElementTree.tostring(root, encoding="unicode", xml_declaration=True)
