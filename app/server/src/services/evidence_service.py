"""Claim-level evidence, contradiction, and conservative confidence assessment."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Iterable


class StatementKind(str, Enum):
    DIRECT = "direct"
    ATTRIBUTED = "attributed"
    INFERENTIAL = "inferential"


class EvidenceRelation(str, Enum):
    SUPPORTS = "supports"
    CONTRADICTS = "contradicts"
    CONTEXT = "context"


class ClaimStatus(str, Enum):
    SUPPORTED = "supported"
    CONTESTED = "contested"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"


@dataclass(frozen=True)
class EvidenceRecord:
    evidence_id: str
    source_id: str
    source_artifact_id: str
    quote: str
    collection_time: datetime
    relation: EvidenceRelation
    statement_kind: StatementKind = StatementKind.DIRECT
    source_trust: float = 0.5
    publication_time: datetime | None = None
    normalized_observation_id: str | None = None
    origin_id: str | None = None
    geo_time_fit: float = 1.0
    completeness: float = 1.0
    metadata: dict[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.quote.strip() and not self.normalized_observation_id:
            raise ValueError("Evidence requires a quoted span or normalized observation reference.")
        if not 0 <= self.source_trust <= 1:
            raise ValueError("source_trust must be between zero and one.")
        if not 0 <= self.geo_time_fit <= 1 or not 0 <= self.completeness <= 1:
            raise ValueError("geo_time_fit and completeness must be between zero and one.")

    @property
    def independence_key(self) -> str:
        # Syndications/reposts carry the original ID and deliberately count once.
        return (
            self.origin_id
            or self.normalized_observation_id
            or self.source_artifact_id
            or self.source_id
        )


@dataclass(frozen=True)
class ClaimRecord:
    claim_id: str
    text: str
    statement_kind: StatementKind
    evidence_ids: tuple[str, ...] = ()
    required_evidence_types: tuple[str, ...] = ()


@dataclass(frozen=True)
class ClaimAssessment:
    claim_id: str
    status: ClaimStatus
    confidence: float
    conclusion: str | None
    supporting_evidence_ids: tuple[str, ...]
    conflicting_evidence_ids: tuple[str, ...]
    independent_support_count: int
    independent_conflict_count: int
    limitations: tuple[str, ...]

    @property
    def insufficient_evidence(self) -> bool:
        return self.status is ClaimStatus.INSUFFICIENT_EVIDENCE


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _recency_score(evidence: EvidenceRecord, now: datetime) -> float:
    published = evidence.publication_time or evidence.collection_time
    days = max(0.0, (now - _utc(published)).total_seconds() / 86_400)
    return max(0.25, 1.0 - min(days, 730.0) / 1_460.0)


def _independent_weight(records: Iterable[EvidenceRecord], now: datetime) -> tuple[float, int]:
    unique: dict[str, EvidenceRecord] = {}
    for evidence in records:
        previous = unique.get(evidence.independence_key)
        if previous is None or evidence.source_trust > previous.source_trust:
            unique[evidence.independence_key] = evidence
    if not unique:
        return 0.0, 0
    scores = [
        evidence.source_trust
        * _recency_score(evidence, now)
        * evidence.geo_time_fit
        * evidence.completeness
        for evidence in unique.values()
    ]
    # First independent source does most of the work; later ones corroborate with diminishing returns.
    return min(1.0, sum(scores) / len(scores) + 0.12 * (len(scores) - 1)), len(scores)


class EvidenceService:
    """Assess claims without hiding conflicts or manufacturing a conclusion."""

    def __init__(self, *, minimum_confidence: float = 0.6) -> None:
        if not 0 < minimum_confidence <= 1:
            raise ValueError("minimum_confidence must be in (0, 1].")
        self.minimum_confidence = minimum_confidence

    def assess_claim(
        self,
        claim: ClaimRecord,
        evidence: Iterable[EvidenceRecord],
        *,
        now: datetime | None = None,
    ) -> ClaimAssessment:
        assessment_time = _utc(now or datetime.now(timezone.utc))
        selected_ids = set(claim.evidence_ids)
        records = [
            record for record in evidence if not selected_ids or record.evidence_id in selected_ids
        ]
        supports = [record for record in records if record.relation is EvidenceRelation.SUPPORTS]
        conflicts = [
            record for record in records if record.relation is EvidenceRelation.CONTRADICTS
        ]
        support_weight, support_count = _independent_weight(supports, assessment_time)
        conflict_weight, conflict_count = _independent_weight(conflicts, assessment_time)
        confidence = round(max(0.0, min(1.0, support_weight * (1.0 - 0.55 * conflict_weight))), 3)
        limitations: list[str] = []
        if support_count == 0:
            limitations.append("No supporting evidence was collected.")
        if conflict_count:
            limitations.append("Conflicting evidence remains visible and requires attribution.")
        if any(record.statement_kind is StatementKind.INFERENTIAL for record in supports):
            limitations.append("Some support is inferential rather than directly observed.")
        if support_count == 1:
            limitations.append("Only one independent supporting origin was found.")
        if support_count == 0 or confidence < self.minimum_confidence:
            status = ClaimStatus.INSUFFICIENT_EVIDENCE
            conclusion = None
            limitations.append("Evidence threshold was not met; no conclusion is emitted.")
        elif conflict_count:
            status = ClaimStatus.CONTESTED
            conclusion = claim.text
        else:
            status = ClaimStatus.SUPPORTED
            conclusion = claim.text
        return ClaimAssessment(
            claim_id=claim.claim_id,
            status=status,
            confidence=confidence,
            conclusion=conclusion,
            supporting_evidence_ids=tuple(record.evidence_id for record in supports),
            conflicting_evidence_ids=tuple(record.evidence_id for record in conflicts),
            independent_support_count=support_count,
            independent_conflict_count=conflict_count,
            limitations=tuple(limitations),
        )
