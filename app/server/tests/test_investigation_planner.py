from src.services.investigation_planner import (
    AttemptReason,
    InvestigationPlanner,
    SourceBudgetPolicy,
    SourceCatalogEntry,
    SourceClass,
)


def _source(
    source_id: str, source_class: SourceClass, url: str, **kwargs: object
) -> SourceCatalogEntry:
    return SourceCatalogEntry(source_id, source_id, url, source_class=source_class, **kwargs)


def test_equivalent_questions_make_a_stable_plan_regardless_of_catalog_order() -> None:
    sources = [
        _source(
            "news", SourceClass.ESTABLISHED_NEWS, "https://news.example/report", trust_score=0.8
        ),
        _source(
            "official", SourceClass.OFFICIAL_PUBLIC_DATA, "https://agency.gov/data", trust_score=0.9
        ),
    ]
    planner = InvestigationPlanner()

    one = planner.build_initial_plan("What happened in North Harbor during 2025?", sources)
    two = planner.build_initial_plan(
        " What happened in North Harbor during 2025? ", reversed(sources)
    )

    assert [(item.source_id, item.priority) for item in one.work_items] == [
        (item.source_id, item.priority) for item in two.work_items
    ]
    assert one.leads == two.leads


def test_unavailable_source_records_attempt_and_next_work_is_an_alternate() -> None:
    plan = InvestigationPlanner().build_initial_plan(
        "Harbor logistics",
        [
            _source("first", SourceClass.OFFICIAL_PUBLIC_DATA, "https://a.gov/data", trust_score=1),
            _source(
                "alternate", SourceClass.ESTABLISHED_NEWS, "https://b.news/report", trust_score=0.8
            ),
        ],
    )
    planner = InvestigationPlanner()
    first = planner.next_work(plan, [])
    assert first is not None
    attempts = planner.record_attempt(
        [], source_id=first.source_id, reason=AttemptReason.UNAVAILABLE
    )
    alternate = planner.next_work(plan, attempts)
    assert alternate is not None
    assert alternate.source_id != first.source_id
    assert alternate.alternate_for == first.source_id


def test_source_class_budgets_are_enforced_and_auditable() -> None:
    policy = SourceBudgetPolicy(
        caps={kind: 1 for kind in SourceClass}, global_cap=3, per_domain_cap=3
    )
    plan = InvestigationPlanner(policy).build_initial_plan(
        "public records",
        [
            _source("gov-one", SourceClass.OFFICIAL_PUBLIC_DATA, "https://one.gov/a"),
            _source("gov-two", SourceClass.OFFICIAL_PUBLIC_DATA, "https://two.gov/a"),
            _source("news", SourceClass.ESTABLISHED_NEWS, "https://news.example/a"),
            _source("high", SourceClass.HIGH_VALUE_PUBLIC, "https://high.example/a"),
        ],
    )

    assert len(plan.work_items) == 3
    assert (
        sum(item.source_class is SourceClass.OFFICIAL_PUBLIC_DATA for item in plan.work_items) == 1
    )
    assert plan.audit()["policy"] == policy.as_dict()


def test_access_boundaries_are_recorded_not_silently_crossed() -> None:
    plan = InvestigationPlanner().build_initial_plan(
        "restricted report",
        [
            _source(
                "paid",
                SourceClass.ESTABLISHED_NEWS,
                "https://paid.example/article",
                access_policy="paywalled",
            )
        ],
    )

    assert not plan.work_items
    assert plan.blocked_attempts[0].reason is AttemptReason.BLOCKED_BY_POLICY


def test_unknown_personal_sources_only_receive_extra_budget_after_vetting() -> None:
    policy = SourceBudgetPolicy(
        caps={**SourceBudgetPolicy().caps, SourceClass.UNKNOWN_PERSONAL: 3},
        vetted_unknown_personal_cap=2,
        global_cap=10,
    )
    unvetted = [
        _source(f"u{index}", SourceClass.UNKNOWN_PERSONAL, f"https://u{index}.example/a")
        for index in range(3)
    ]
    vetted = [
        _source(
            f"v{index}",
            SourceClass.UNKNOWN_PERSONAL,
            f"https://v{index}.example/a",
            reliability_checked=True,
            relevance_checked=True,
        )
        for index in range(3)
    ]

    assert len(InvestigationPlanner(policy).build_initial_plan("records", unvetted).work_items) == 1
    assert len(InvestigationPlanner(policy).build_initial_plan("records", vetted).work_items) == 2
