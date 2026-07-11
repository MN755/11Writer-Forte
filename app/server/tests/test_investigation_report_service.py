from datetime import datetime, timezone

from src.services.evidence_service import (
    ClaimRecord,
    ClaimStatus,
    EvidenceRecord,
    EvidenceRelation,
    EvidenceService,
    StatementKind,
)
from src.services.investigation_report_service import (
    REPORT_SPEC_VERSION,
    render_investigation_report,
)


NOW = datetime(2026, 7, 10, tzinfo=timezone.utc)


def test_report_preserves_immutable_citations_conflict_and_insufficient_conclusion() -> None:
    claim = ClaimRecord("claim-1", "The port closed.", StatementKind.ATTRIBUTED)
    support = EvidenceRecord(
        "evidence-1",
        "official",
        "artifact-1",
        "The port closed at noon.",
        NOW,
        EvidenceRelation.SUPPORTS,
        source_trust=0.9,
    )
    conflict = EvidenceRecord(
        "evidence-2",
        "local-blog",
        "artifact-2",
        "The port remained open.",
        NOW,
        EvidenceRelation.CONTRADICTS,
        source_trust=0.2,
    )
    assessment = EvidenceService(minimum_confidence=0.5).assess_claim(
        claim, [support, conflict], now=NOW
    )

    report = render_investigation_report(
        question="Did the port close?",
        claims=[claim],
        assessments=[assessment],
        evidence=[support, conflict],
        generated_at=NOW,
    )

    assert report.specification_version == REPORT_SPEC_VERSION
    assert report.citation_count == 2
    assert "artifact://artifact-1" in report.markdown
    assert "Conflicting evidence" in report.markdown
    assert "attributed to local-blog" in report.markdown
    assert assessment.status is ClaimStatus.CONTESTED


def test_report_never_invents_an_answer_for_unassessed_claim() -> None:
    report = render_investigation_report(
        question="Unknown event?",
        claims=[ClaimRecord("claim-1", "An unverified claim", StatementKind.INFERENTIAL)],
        assessments=[],
        evidence=[],
        generated_at=NOW,
    )

    assert report.conclusion_count == 0
    assert "Insufficient evidence" in report.markdown
    assert "**Unknown:** An unverified claim" in report.markdown
