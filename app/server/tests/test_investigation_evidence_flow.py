from datetime import datetime, timezone

from src.services.evidence_service import (
    ClaimRecord,
    EvidenceRecord,
    EvidenceRelation,
    EvidenceService,
    StatementKind,
)
from src.services.investigation_planner import (
    AttemptReason,
    InvestigationPlanner,
    SourceCatalogEntry,
    SourceClass,
)
from src.services.investigation_report_service import render_investigation_report


NOW = datetime(2026, 7, 10, tzinfo=timezone.utc)


def test_public_multi_source_fixture_uses_alternate_preserves_conflict_and_stable_citations() -> (
    None
):
    planner = InvestigationPlanner()
    plan = planner.build_initial_plan(
        "What do public sources document about Harbor logistics in 2026?",
        [
            SourceCatalogEntry(
                "dead",
                "Official archive",
                "https://archive.gov/harbor",
                SourceClass.OFFICIAL_PUBLIC_DATA,
            ),
            SourceCatalogEntry(
                "news",
                "Established report",
                "https://news.example/harbor",
                SourceClass.ESTABLISHED_NEWS,
            ),
            SourceCatalogEntry(
                "blog", "Local blog", "https://blog.example/harbor", SourceClass.UNKNOWN_PERSONAL
            ),
        ],
    )
    first = planner.next_work(plan, [])
    assert first is not None
    attempts = planner.record_attempt(
        [], source_id=first.source_id, reason=AttemptReason.UNAVAILABLE
    )
    alternate = planner.next_work(plan, attempts)
    assert alternate is not None and alternate.source_id != first.source_id

    claim = ClaimRecord("claim-1", "A delay was publicly reported.", StatementKind.ATTRIBUTED)
    support = EvidenceRecord(
        "support",
        "news",
        "artifact-news",
        "The agency reported a delay.",
        NOW,
        EvidenceRelation.SUPPORTS,
        StatementKind.ATTRIBUTED,
        source_trust=0.85,
    )
    conflict = EvidenceRecord(
        "conflict",
        "blog",
        "artifact-blog",
        "No delay occurred.",
        NOW,
        EvidenceRelation.CONTRADICTS,
        StatementKind.ATTRIBUTED,
        source_trust=0.2,
    )
    assessment = EvidenceService(minimum_confidence=0.5).assess_claim(
        claim, [support, conflict], now=NOW
    )
    report = render_investigation_report(
        question=plan.question,
        claims=[claim],
        assessments=[assessment],
        evidence=[support, conflict],
        generated_at=NOW,
    )

    assert assessment.conclusion == claim.text
    assert "artifact://artifact-news" in report.markdown
    assert "artifact://artifact-blog" in report.markdown
    assert "Conflicting evidence" in report.markdown
