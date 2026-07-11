from datetime import datetime, timezone

from src.services.evidence_service import (
    ClaimRecord,
    ClaimStatus,
    EvidenceRecord,
    EvidenceRelation,
    EvidenceService,
    StatementKind,
)


NOW = datetime(2026, 7, 10, tzinfo=timezone.utc)


def _evidence(evidence_id: str, relation: EvidenceRelation, **kwargs: object) -> EvidenceRecord:
    return EvidenceRecord(
        evidence_id,
        kwargs.pop("source_id", evidence_id),
        kwargs.pop("source_artifact_id", evidence_id),
        kwargs.pop("quote", "quoted public record"),
        kwargs.pop("collection_time", NOW),
        relation,
        **kwargs,
    )


def test_reposts_of_one_original_are_not_independent_corroboration() -> None:
    claim = ClaimRecord("c1", "The shipment arrived.", StatementKind.DIRECT)
    result = EvidenceService(minimum_confidence=0.5).assess_claim(
        claim,
        [
            _evidence("one", EvidenceRelation.SUPPORTS, origin_id="original-1", source_trust=0.7),
            _evidence("two", EvidenceRelation.SUPPORTS, origin_id="original-1", source_trust=0.7),
        ],
        now=NOW,
    )

    assert result.independent_support_count == 1


def test_high_trust_support_and_weak_contradiction_are_both_preserved() -> None:
    claim = ClaimRecord("c1", "The facility closed.", StatementKind.DIRECT)
    result = EvidenceService(minimum_confidence=0.5).assess_claim(
        claim,
        [
            _evidence("official", EvidenceRelation.SUPPORTS, source_trust=0.95),
            _evidence("blog", EvidenceRelation.CONTRADICTS, source_trust=0.2),
        ],
        now=NOW,
    )

    assert result.status is ClaimStatus.CONTESTED
    assert result.supporting_evidence_ids == ("official",)
    assert result.conflicting_evidence_ids == ("blog",)


def test_no_support_emits_insufficient_evidence_instead_of_a_conclusion() -> None:
    result = EvidenceService().assess_claim(
        ClaimRecord("c1", "An unverified assertion", StatementKind.ATTRIBUTED),
        [_evidence("context", EvidenceRelation.CONTEXT)],
        now=NOW,
    )

    assert result.status is ClaimStatus.INSUFFICIENT_EVIDENCE
    assert result.conclusion is None
