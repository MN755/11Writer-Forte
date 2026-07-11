"""Pure deterministic materiality evaluation and update deduplication for watches."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Any


UPDATE_TYPES = frozenset(
    {
        "new_source",
        "new_fact",
        "corroboration",
        "contradiction",
        "status_change",
        "visual_change_candidate",
        "no_material_change",
    }
)


def clamp(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


@dataclass(frozen=True)
class MaterialityPolicy:
    min_materiality_score: float = 0.55
    min_alert_confidence: float = 0.60
    require_citation: bool = True
    alert_visual_candidates: bool = False

    def __post_init__(self) -> None:
        if not 0 <= self.min_materiality_score <= 1:
            raise ValueError("min_materiality_score must be between zero and one.")
        if not 0 <= self.min_alert_confidence <= 1:
            raise ValueError("min_alert_confidence must be between zero and one.")


@dataclass(frozen=True)
class MaterialityCandidate:
    evidence_id: str
    evidence_version: str
    source_id: str
    source_independence_key: str
    content_hash: str
    claim_id: str | None = None
    claim_version: str | None = None
    claim_relation: str = "supports"  # supports | contradicts
    confidence: float = 0.5
    previous_confidence: float | None = None
    claim_impact: float = 0.5
    temporal_relevance: float = 0.5
    geographic_relevance: float = 0.5
    event_id: str | None = None
    previous_status: str | None = None
    status: str | None = None
    visual_change: bool = False
    citation_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for field_name in (
            "confidence",
            "claim_impact",
            "temporal_relevance",
            "geographic_relevance",
        ):
            if not 0 <= float(getattr(self, field_name)) <= 1:
                raise ValueError(f"{field_name} must be between zero and one.")
        if self.previous_confidence is not None and not 0 <= float(self.previous_confidence) <= 1:
            raise ValueError("previous_confidence must be between zero and one.")
        if not self.evidence_id or not self.evidence_version or not self.content_hash:
            raise ValueError("evidence_id, evidence_version, and content_hash are required.")


@dataclass(frozen=True)
class MaterialityDecision:
    update_type: str
    score: float
    material: bool
    alert: bool
    reviewable: bool
    dedupe_key: str
    reason: str
    citation_valid: bool

    def to_json(self) -> dict[str, object]:
        return asdict(self)


@dataclass
class UpdateLedger:
    """Version-aware dedupe ledger stored by a watch as JSON.

    Evidence/claim versions and content hashes are tracked separately.  That makes a
    repost or byte-identical payload deterministic no-op while preserving independent
    corroboration for the same claim.
    """

    evidence_versions: dict[str, dict[str, object]] = field(default_factory=dict)
    claim_versions: dict[str, list[str]] = field(default_factory=dict)
    content_hashes: dict[str, str] = field(default_factory=dict)
    source_ids: set[str] = field(default_factory=set)

    @classmethod
    def from_json(cls, payload: dict[str, object] | None) -> "UpdateLedger":
        payload = payload or {}
        evidence = payload.get("evidence_versions")
        claim = payload.get("claim_versions")
        hashes = payload.get("content_hashes")
        return cls(
            evidence_versions={str(key): dict(value) for key, value in evidence.items() if isinstance(value, dict)}
            if isinstance(evidence, dict)
            else {},
            claim_versions={str(key): sorted(str(item) for item in value) for key, value in claim.items() if isinstance(value, list)}
            if isinstance(claim, dict)
            else {},
            content_hashes={str(key): str(value) for key, value in hashes.items()} if isinstance(hashes, dict) else {},
            source_ids={str(item) for item in payload.get("source_ids", [])} if isinstance(payload.get("source_ids"), list) else set(),
        )

    def to_json(self) -> dict[str, object]:
        return {
            "evidence_versions": {key: self.evidence_versions[key] for key in sorted(self.evidence_versions)},
            "claim_versions": {key: sorted(self.claim_versions[key]) for key in sorted(self.claim_versions)},
            "content_hashes": {key: self.content_hashes[key] for key in sorted(self.content_hashes)},
            "source_ids": sorted(self.source_ids),
        }

    @staticmethod
    def evidence_key(candidate: MaterialityCandidate) -> str:
        return f"{candidate.evidence_id}:{candidate.evidence_version}"

    @staticmethod
    def claim_key(candidate: MaterialityCandidate) -> str | None:
        if candidate.claim_id is None:
            return None
        return f"{candidate.claim_id}:{candidate.claim_version or 'current'}"

    def has_duplicate(self, candidate: MaterialityCandidate) -> bool:
        return self.evidence_key(candidate) in self.evidence_versions or candidate.content_hash in self.content_hashes

    def source_is_independent_for_claim(self, candidate: MaterialityCandidate) -> bool:
        claim_key = self.claim_key(candidate)
        if claim_key is None:
            return candidate.source_independence_key not in self.source_ids
        return candidate.source_independence_key not in self.claim_versions.get(claim_key, [])

    def record(self, candidate: MaterialityCandidate, decision: MaterialityDecision) -> None:
        evidence_key = self.evidence_key(candidate)
        self.evidence_versions[evidence_key] = {
            "content_hash": candidate.content_hash,
            "source_id": candidate.source_id,
            "source_independence_key": candidate.source_independence_key,
            "update_type": decision.update_type,
            "score": decision.score,
        }
        self.content_hashes[candidate.content_hash] = evidence_key
        self.source_ids.add(candidate.source_independence_key)
        claim_key = self.claim_key(candidate)
        if claim_key is not None:
            source_keys = self.claim_versions.setdefault(claim_key, [])
            if candidate.source_independence_key not in source_keys:
                source_keys.append(candidate.source_independence_key)
                source_keys.sort()


def evaluate_materiality(
    candidate: MaterialityCandidate,
    ledger: UpdateLedger,
    *,
    policy: MaterialityPolicy | None = None,
) -> MaterialityDecision:
    """Classify one cited evidence/claim version and atomically update its ledger.

    The caller should persist the ledger only after its surrounding watch transaction
    succeeds.  A no-material decision is also recorded so repeated low-confidence
    candidates do not churn the review queue.
    """

    policy = policy or MaterialityPolicy()
    duplicate = ledger.has_duplicate(candidate)
    independent = ledger.source_is_independent_for_claim(candidate)
    update_type = classify_update(candidate, ledger, duplicate=duplicate, independent=independent)
    score = score_materiality(candidate, novelty=not duplicate, independent=independent)
    citation_valid = bool(candidate.citation_ids) or not policy.require_citation
    material = update_type not in {"no_material_change", "visual_change_candidate"} and score >= policy.min_materiality_score
    if update_type == "contradiction" and candidate.confidence >= policy.min_alert_confidence:
        material = True
    alert = material and citation_valid and candidate.confidence >= policy.min_alert_confidence
    if update_type == "visual_change_candidate" and not policy.alert_visual_candidates:
        alert = False
    reviewable = update_type != "no_material_change" and (not alert or update_type == "visual_change_candidate")
    decision = MaterialityDecision(
        update_type=update_type,
        score=score,
        material=material,
        alert=alert,
        reviewable=reviewable,
        dedupe_key=stable_dedupe_key(candidate, update_type),
        reason=decision_reason(candidate, update_type, duplicate, independent),
        citation_valid=citation_valid,
    )
    ledger.record(candidate, decision)
    return decision


def classify_update(
    candidate: MaterialityCandidate,
    ledger: UpdateLedger,
    *,
    duplicate: bool,
    independent: bool,
) -> str:
    if duplicate:
        return "no_material_change"
    if candidate.claim_relation == "contradicts":
        return "contradiction"
    if candidate.visual_change:
        return "visual_change_candidate"
    if candidate.previous_status is not None and candidate.status is not None and candidate.previous_status != candidate.status:
        return "status_change"
    claim_key = ledger.claim_key(candidate)
    if claim_key is not None and claim_key not in ledger.claim_versions:
        return "new_fact"
    if claim_key is not None and independent:
        return "corroboration"
    if candidate.source_independence_key not in ledger.source_ids:
        return "new_source"
    return "no_material_change"


def score_materiality(candidate: MaterialityCandidate, *, novelty: bool, independent: bool) -> float:
    confidence_change = (
        abs(candidate.confidence - candidate.previous_confidence)
        if candidate.previous_confidence is not None
        else 0.0
    )
    score = (
        0.20 * float(novelty)
        + 0.20 * float(independent)
        + 0.20 * clamp(candidate.claim_impact)
        + 0.10 * clamp(candidate.temporal_relevance)
        + 0.10 * clamp(candidate.geographic_relevance)
        + 0.10 * clamp(confidence_change)
        + 0.10 * float(candidate.event_id is not None)
    )
    return round(score, 6)


def stable_dedupe_key(candidate: MaterialityCandidate, update_type: str) -> str:
    payload: dict[str, Any] = {
        "evidence_id": candidate.evidence_id,
        "evidence_version": candidate.evidence_version,
        "claim_id": candidate.claim_id,
        "claim_version": candidate.claim_version,
        "content_hash": candidate.content_hash,
        "source_independence_key": candidate.source_independence_key,
        "update_type": update_type,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def decision_reason(candidate: MaterialityCandidate, update_type: str, duplicate: bool, independent: bool) -> str:
    if duplicate:
        return "evidence version or byte-identical content already recorded"
    if update_type == "corroboration":
        return "new independent source supports an existing claim version"
    if update_type == "contradiction":
        return "credible source contradicts the tracked claim"
    if update_type == "new_fact":
        return "new claim version"
    if update_type == "status_change":
        return "tracked status changed"
    if update_type == "visual_change_candidate":
        return "visual change requires the visual-change contract or review"
    if update_type == "new_source":
        return "previously unseen source"
    return "candidate does not add a material deterministic change"
