from src.services.materiality_service import (
    MaterialityCandidate,
    MaterialityPolicy,
    UpdateLedger,
    evaluate_materiality,
)


def candidate(**changes: object) -> MaterialityCandidate:
    data: dict[str, object] = {
        "evidence_id": "evidence-1",
        "evidence_version": "v1",
        "source_id": "source-1",
        "source_independence_key": "publisher-1",
        "content_hash": "a" * 64,
        "claim_id": "claim-1",
        "claim_version": "v1",
        "confidence": 0.9,
        "claim_impact": 0.9,
        "temporal_relevance": 0.9,
        "geographic_relevance": 0.9,
        "event_id": "event-1",
        "citation_ids": ("citation-1",),
    }
    data.update(changes)
    return MaterialityCandidate(**data)  # type: ignore[arg-type]


def test_reposted_or_byte_identical_content_creates_no_new_update() -> None:
    ledger = UpdateLedger()
    first = evaluate_materiality(candidate(), ledger)
    repost = evaluate_materiality(candidate(evidence_id="evidence-2", evidence_version="v2"), ledger)
    assert first.update_type == "new_fact"
    assert repost.update_type == "no_material_change"
    assert not repost.alert
    repeated_repost = evaluate_materiality(candidate(evidence_id="evidence-2", evidence_version="v2"), ledger)
    assert repost.dedupe_key == repeated_repost.dedupe_key


def test_independent_corroboration_is_material_for_existing_claim() -> None:
    ledger = UpdateLedger()
    evaluate_materiality(candidate(), ledger)
    corroboration = evaluate_materiality(
        candidate(
            evidence_id="evidence-2",
            source_id="source-2",
            source_independence_key="publisher-2",
            content_hash="b" * 64,
        ),
        ledger,
    )
    assert corroboration.update_type == "corroboration"
    assert corroboration.material
    assert corroboration.alert


def test_credible_contradiction_is_attributed_update() -> None:
    ledger = UpdateLedger()
    evaluate_materiality(candidate(), ledger)
    contradiction = evaluate_materiality(
        candidate(
            evidence_id="evidence-2",
            source_id="source-2",
            source_independence_key="publisher-2",
            content_hash="b" * 64,
            claim_relation="contradicts",
            confidence=0.75,
        ),
        ledger,
    )
    assert contradiction.update_type == "contradiction"
    assert contradiction.material and contradiction.alert
    assert "contradicts" in contradiction.reason


def test_low_confidence_candidate_is_reviewable_without_alerting() -> None:
    decision = evaluate_materiality(
        candidate(confidence=0.25, claim_impact=1.0, source_independence_key="publisher-low"),
        UpdateLedger(),
        policy=MaterialityPolicy(min_materiality_score=0.50, min_alert_confidence=0.60),
    )
    assert decision.update_type == "new_fact"
    assert decision.material
    assert decision.reviewable
    assert not decision.alert
