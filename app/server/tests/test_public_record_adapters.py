from __future__ import annotations

import pytest

from src.services.public_record_adapter_service import (
    AdapterSchemaMismatchError,
    EntityCandidate,
    PublicRecordAdapter,
    canonicalize_text,
    decide_merge,
    evaluate_public_source,
    extract_identifiers,
    extract_resolution_signals,
    source_domain_reliability,
    transliterate_text,
    validate_source_and_record,
)


def test_canonical_text_and_transliteration_are_deterministic() -> None:
    assert transliterate_text("\u0410\u043b\u0435\u043a\u0441\u0435\u0439 Jos\u00e9") == "Aleksei Jose"
    assert canonicalize_text("  \u0410\u043b\u0435\u043a\u0441\u0435\u0439--Jos\u00e9! ") == "aleksei jose"
    assert canonicalize_text("M\u00dcLLER\u00a0GmbH") == "muller gmbh"


def test_identifier_extraction_and_signals_keep_labeled_public_identifiers() -> None:
    text = "Case No. 2:24-cv-00123; IMO: 1234567; MMSI 123456789; Registration No: ACME-42"
    identifiers = extract_identifiers(text)
    assert identifiers == {
        "case_id": "224CV00123",
        "imo": "1234567",
        "mmsi": "123456789",
        "organization_registration_id": "ACME42",
    }
    signals = extract_resolution_signals({"full_name": "Jos\u00e9 \u041f\u0435\u0442\u0440\u043e\u0432", "text": text, "jurisdiction": "U.S. - TX"})
    assert {(signal.key, signal.strength) for signal in signals} >= {
        ("name", "weak"), ("case_id", "strong"), ("imo", "strong"), ("jurisdiction", "context"),
    }


def test_source_policy_refuses_login_payment_bypass_and_private_sources() -> None:
    allowed = evaluate_public_source("https://court.example.gov/docket", source_type="court")
    assert allowed.allowed and allowed.reliability == 0.95
    assert not evaluate_public_source("https://news.example/login", source_type="news", requires_login=True).allowed
    assert not evaluate_public_source("https://127.0.0.1/feed", source_type="api").allowed
    assert not evaluate_public_source("https://localhost/feed", source_type="api").allowed
    assert not evaluate_public_source("https://data.example/feed", source_type="dataset", bypasses_access_controls=True).allowed
    assert source_domain_reliability("api.court.example", "api", {"court.example": 0.9}) == 0.9


def test_schema_change_is_quarantined_or_raises_explicitly() -> None:
    adapter = PublicRecordAdapter(
        source_name="court-docket",
        schema_version="2026-01",
        source_type="court",
        required_fields=frozenset({"case_number", "title"}),
        allowed_fields=frozenset({"case_number", "title", "filed_on"}),
    )
    result = validate_source_and_record(
        adapter,
        "https://court.example.gov/api/dockets",
        {"case_number": "2:24-cv-00123", "title": "Public filing"},
        actual_schema_version="2026-02",
    )
    assert result.status == "quarantined"
    assert "version mismatch" in (result.quarantine_reason or "")
    with pytest.raises(AdapterSchemaMismatchError):
        adapter.validate({"case_number": "2:24-cv-00123", "title": "Public filing"}, actual_schema_version="old", strict=True)


def test_same_name_people_with_weak_context_remain_ambiguous() -> None:
    left = EntityCandidate(entity_type="person", canonical_name="Alex Kim", source_citations=("https://a.example/1",))
    right = EntityCandidate(entity_type="person", canonical_name="Alex Kim", source_citations=("https://b.example/1",))
    decision = decide_merge(left, right)
    assert decision.action == "ambiguous"


def test_cited_strong_public_identifier_merges_and_conflicts_refuse() -> None:
    left = EntityCandidate(
        entity_type="vessel", canonical_name="Northern Star", identifiers={"imo": "IMO 1234567"},
        source_citations=("https://registry.example/one",), source_reliability=0.95,
    )
    right = EntityCandidate(
        entity_type="vessel", canonical_name="N. Star", identifiers={"imo": "1234567"},
        source_citations=("https://court.example/two",), source_reliability=0.95,
    )
    assert decide_merge(left, right).action == "merge"
    conflict = EntityCandidate(
        entity_type="vessel", canonical_name="Northern Star", identifiers={"imo": "7654321"},
        source_citations=("https://registry.example/three",), source_reliability=0.95,
    )
    assert decide_merge(left, conflict).action == "do_not_merge"


def test_independent_context_can_merge_but_one_context_cannot() -> None:
    left = EntityCandidate(
        entity_type="person", canonical_name="Maria Silva", context={"jurisdiction": "BR-SP", "event_date": "2024-05-01"},
        source_citations=("https://court.example/a",), source_domains=("court.example",), source_reliability=0.95,
    )
    right = EntityCandidate(
        entity_type="person", canonical_name="Mar\u00eda Silva", context={"jurisdiction": "br sp", "event_date": "2024-05-01"},
        source_citations=("https://news.example/b",), source_domains=("news.example",), source_reliability=0.70,
    )
    assert decide_merge(left, right).action == "merge"
