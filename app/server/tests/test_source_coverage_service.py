from datetime import datetime, timezone

from src.services.source_coverage_service import (
    CoverageLedger,
    RetryPolicy,
    SourceCandidate,
    plan_collection_action,
)


def policy() -> RetryPolicy:
    return RetryPolicy(
        allowed_source_kinds=frozenset({"rss", "http_json", "web_search"}),
        domain_class_budgets={"government": 2, "news": 3},
        max_retry_attempts=1,
        retry_base_seconds=2,
        retry_max_seconds=10,
        max_alternates_per_gap=2,
    )


def test_retriable_source_failure_is_bounded_then_records_gap_and_alternates() -> None:
    ledger = CoverageLedger()
    primary = SourceCandidate("primary", "http_json", "city.gov", "government")
    alternate = SourceCandidate("alternate", "rss", "localnews.example", "news", alternate_rank=1)
    now = datetime(2026, 7, 10, tzinfo=timezone.utc)

    retry = plan_collection_action(
        primary,
        outcome="failed",
        failure_kind="timeout",
        ledger=ledger,
        policy=policy(),
        alternate_candidates=[alternate],
        now=now,
    )
    assert [action.action for action in retry] == ["retry_source"]
    assert retry[0].retry_at == datetime(2026, 7, 10, 0, 0, 2, tzinfo=timezone.utc)

    diversified = plan_collection_action(
        primary,
        outcome="failed",
        failure_kind="timeout",
        ledger=ledger,
        policy=policy(),
        alternate_candidates=[alternate],
        now=now,
    )
    assert [action.action for action in diversified] == ["record_research_gap", "try_alternate_source"]
    assert diversified[1].source_id == "alternate"
    assert ledger.gaps["source:primary"]["status"] == "open"


def test_domain_budget_depends_only_on_declared_domain_class() -> None:
    ledger = CoverageLedger()
    primary = SourceCandidate("one", "http_json", "a.gov", "government")
    alternate_same_class = SourceCandidate("two", "rss", "b.gov", "government")
    alternate_news = SourceCandidate("three", "rss", "news.example", "news")
    ledger.record_attempt(primary, status="failed")
    ledger.record_attempt(SourceCandidate("spent", "rss", "elsewhere.gov", "government"), status="success")

    actions = plan_collection_action(
        primary,
        outcome="stale",
        failure_kind=None,
        ledger=ledger,
        policy=policy(),
        alternate_candidates=[alternate_same_class, alternate_news],
    )
    assert [action.source_id for action in actions if action.action == "try_alternate_source"] == ["three"]


def test_ledger_round_trip_preserves_search_attempt_change_and_gap() -> None:
    ledger = CoverageLedger()
    source = SourceCandidate("primary", "http_json", "city.gov", "government")
    ledger.record_search(scope="event:42", source_ids=["primary"])
    ledger.record_attempt(source, status="success", changed=True)
    ledger.record_gap(gap_key="source:primary", reason="stale", source_id="primary")
    restored = CoverageLedger.from_json(ledger.to_json())
    assert restored.to_json() == ledger.to_json()
