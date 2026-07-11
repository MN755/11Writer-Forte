from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session

from src.auth import OperatorPrincipal, require_scope
from src.db import get_db
from src.schemas import (
    CandidateHealthCheckRead,
    CandidatePromotionRequest,
    CandidatePromotionResultRead,
    CandidateScoreExplanationRead,
    CandidateSuppressionRead,
    CandidateSuppressionRequest,
    DiscoveryCampaignCreate,
    DiscoveryCampaignRead,
    DiscoveryCampaignUpdate,
    DiscoveryDomainPolicyCreate,
    DiscoveryDomainPolicyRead,
    DiscoveryDomainPolicyUpdate,
    DiscoveryExportSummaryRead,
    DiscoveryHealthSummaryRead,
    DiscoveryInventoryDiffRead,
    DiscoveryLineageRead,
    DiscoveryOpsSummaryRead,
    DiscoveryRevisitRequest,
    DiscoveryRevisitResultRead,
    DiscoveryRunRead,
    DiscoveryRunRequest,
    DiscoveryRunResultRead,
    CandidateHealthScanRequest,
    CandidateHealthScanResultRead,
    SourceCandidateDetailRead,
    SourceCandidateRead,
    ResearchProviderCreate,
    ResearchProviderRead,
    ResearchProviderUpdate,
)
from src.services.discovery_service import (
    build_candidate_lineage,
    build_discovery_campaign_detail,
    build_discovery_export_summary,
    build_discovery_health_summary,
    build_discovery_ops_summary,
    build_discovery_run_detail,
    build_source_candidate_detail,
    check_candidate_health,
    create_discovery_campaign,
    diff_discovery_inventories,
    explain_candidate_score,
    list_discovery_campaigns,
    list_discovery_runs,
    list_domain_policies,
    list_source_candidates,
    promote_source_candidate,
    revisit_discovery,
    run_discovery_campaign,
    scan_candidate_health,
    suppress_source_candidate,
    update_discovery_campaign,
    update_domain_policy,
    upsert_domain_policy,
)
from src.services.provider_registry_service import (
    ProviderPauseRequest,
    create_provider,
    enable_provider,
    get_provider,
    list_providers,
    pause_provider,
    provider_coverage_summary,
    update_provider,
)

router = APIRouter(prefix="/discovery", tags=["discovery"])


@router.get("/providers/coverage")
def discovery_provider_coverage(
    session: Session = Depends(get_db),
    _: OperatorPrincipal = Depends(require_scope("read")),
) -> dict[str, object]:
    return provider_coverage_summary(session)


@router.get("/providers", response_model=list[ResearchProviderRead])
def discovery_providers(
    kind: str | None = None,
    enabled: bool | None = None,
    health: str | None = None,
    capability: str | None = None,
    jurisdiction: str | None = None,
    limit: int = Query(default=200, ge=1, le=5000),
    session: Session = Depends(get_db),
    _: OperatorPrincipal = Depends(require_scope("read")),
) -> list[object]:
    return list_providers(
        session,
        provider_kind=kind,
        enabled=enabled,
        health=health,
        capability=capability,
        jurisdiction=jurisdiction,
        limit=limit,
    )


@router.post("/providers", response_model=ResearchProviderRead)
async def create_discovery_provider(
    payload: ResearchProviderCreate,
    request: Request,
    session: Session = Depends(get_db),
    principal: OperatorPrincipal = Depends(require_scope("operate")),
) -> object:
    try:
        raw_payload = await request.json()
        if not isinstance(raw_payload, dict):
            raise HTTPException(status_code=422, detail="Provider configuration must be an object.")
        unexpected_fields = sorted(set(raw_payload) - set(ResearchProviderCreate.model_fields))
        if unexpected_fields:
            raise HTTPException(
                status_code=422,
                detail=f"Unsupported provider configuration fields: {', '.join(unexpected_fields)}.",
            )
        return create_provider(session, payload, actor=principal.actor)
    except ValueError as exc:
        raise translate_discovery_error(exc) from exc


@router.get("/providers/{provider_id}", response_model=ResearchProviderRead)
def discovery_provider(
    provider_id: int,
    session: Session = Depends(get_db),
    _: OperatorPrincipal = Depends(require_scope("read")),
) -> object:
    try:
        return get_provider(session, provider_id)
    except ValueError as exc:
        raise translate_discovery_error(exc) from exc


@router.patch("/providers/{provider_id}", response_model=ResearchProviderRead)
def patch_discovery_provider(
    provider_id: int,
    payload: ResearchProviderUpdate,
    session: Session = Depends(get_db),
    principal: OperatorPrincipal = Depends(require_scope("operate")),
) -> object:
    try:
        return update_provider(session, provider_id, payload, actor=principal.actor)
    except ValueError as exc:
        raise translate_discovery_error(exc) from exc


@router.post("/providers/{provider_id}/enable", response_model=ResearchProviderRead)
def enable_discovery_provider(
    provider_id: int,
    session: Session = Depends(get_db),
    principal: OperatorPrincipal = Depends(require_scope("admin")),
) -> object:
    try:
        return enable_provider(session, provider_id, actor=principal.actor)
    except ValueError as exc:
        raise translate_discovery_error(exc) from exc


@router.post("/providers/{provider_id}/pause", response_model=ResearchProviderRead)
def pause_discovery_provider(
    provider_id: int,
    payload: ProviderPauseRequest,
    session: Session = Depends(get_db),
    principal: OperatorPrincipal = Depends(require_scope("operate")),
) -> object:
    try:
        return pause_provider(session, provider_id, reason=payload.reason, actor=principal.actor)
    except ValueError as exc:
        raise translate_discovery_error(exc) from exc


@router.get("/health", response_model=DiscoveryHealthSummaryRead)
def discovery_health(session: Session = Depends(get_db)) -> dict[str, object]:
    return build_discovery_health_summary(session)


@router.get("/ops", response_model=DiscoveryOpsSummaryRead)
def discovery_ops(
    stale_after_hours: float = Query(default=24.0, ge=0.0),
    limit: int = Query(default=25, ge=1, le=500),
    session: Session = Depends(get_db),
) -> dict[str, object]:
    return build_discovery_ops_summary(
        session,
        stale_after_hours=stale_after_hours,
        limit=limit,
    )


@router.get("/export/summary", response_model=DiscoveryExportSummaryRead)
def discovery_export_summary(
    stale_after_hours: float = Query(default=24.0, ge=0.0),
    candidate_limit: int = Query(default=500, ge=1, le=5000),
    report_limit: int = Query(default=25, ge=1, le=500),
    session: Session = Depends(get_db),
) -> dict[str, object]:
    return build_discovery_export_summary(
        session,
        stale_after_hours=stale_after_hours,
        candidate_limit=candidate_limit,
        report_limit=report_limit,
    )


@router.get("/inventory-diff", response_model=DiscoveryInventoryDiffRead)
def discovery_inventory_diff(
    from_run_id: int,
    to_run_id: int,
    campaign_id: int | None = None,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        return diff_discovery_inventories(
            session,
            from_run_id=from_run_id,
            to_run_id=to_run_id,
            campaign_id=campaign_id,
        )
    except ValueError as exc:
        raise translate_discovery_error(exc) from exc


@router.get("/domain-policies", response_model=list[DiscoveryDomainPolicyRead])
def discovery_domain_policies(
    domain: str | None = None,
    limit: int = Query(default=200, ge=1, le=5000),
    session: Session = Depends(get_db),
) -> list[object]:
    return list_domain_policies(session, domain=domain, limit=limit)


@router.put("/domain-policies", response_model=DiscoveryDomainPolicyRead)
def put_discovery_domain_policy(
    payload: DiscoveryDomainPolicyCreate,
    session: Session = Depends(get_db),
) -> object:
    try:
        return upsert_domain_policy(session, payload, actor="api_discovery")
    except ValueError as exc:
        raise translate_discovery_error(exc) from exc


@router.patch("/domain-policies/{normalized_domain:path}", response_model=DiscoveryDomainPolicyRead)
def patch_discovery_domain_policy(
    normalized_domain: str,
    payload: DiscoveryDomainPolicyUpdate,
    session: Session = Depends(get_db),
) -> object:
    try:
        return update_domain_policy(
            session,
            normalized_domain,
            payload,
            actor="api_discovery",
        )
    except ValueError as exc:
        raise translate_discovery_error(exc) from exc


@router.post("/health-scan", response_model=CandidateHealthScanResultRead)
def run_discovery_health_scan(
    payload: CandidateHealthScanRequest,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        return scan_candidate_health(session, payload, actor="api_discovery")
    except ValueError as exc:
        raise translate_discovery_error(exc) from exc


@router.post("/revisit", response_model=DiscoveryRevisitResultRead)
def revisit_discovery_inventory(
    payload: DiscoveryRevisitRequest,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        return revisit_discovery(session, payload, actor="api_discovery")
    except ValueError as exc:
        raise translate_discovery_error(exc) from exc


@router.get("/campaigns", response_model=list[DiscoveryCampaignRead])
def discovery_campaigns(
    status: str | None = None,
    enabled: bool | None = None,
    limit: int = Query(default=200, ge=1, le=5000),
    session: Session = Depends(get_db),
) -> list[object]:
    return list_discovery_campaigns(
        session,
        status=status,
        enabled=enabled,
        limit=limit,
    )


@router.post("/campaigns", response_model=DiscoveryCampaignRead)
def create_campaign(
    payload: DiscoveryCampaignCreate,
    session: Session = Depends(get_db),
) -> object:
    try:
        return create_discovery_campaign(session, payload, actor="api_discovery")
    except ValueError as exc:
        raise translate_discovery_error(exc) from exc


@router.get("/runs", response_model=list[DiscoveryRunRead])
def discovery_runs(
    campaign_id: int | None = None,
    status: str | None = None,
    limit: int = Query(default=200, ge=1, le=5000),
    session: Session = Depends(get_db),
) -> list[object]:
    return list_discovery_runs(
        session,
        campaign_id=campaign_id,
        status=status,
        limit=limit,
    )


@router.get("/candidates", response_model=list[SourceCandidateRead])
def discovery_candidates(
    campaign_id: int | None = None,
    run_id: int | None = None,
    status: str | None = None,
    outcome: str | None = None,
    candidate_type: str | None = None,
    domain: str | None = None,
    min_score: float | None = Query(default=None, ge=0.0, le=100.0),
    limit: int = Query(default=200, ge=1, le=5000),
    session: Session = Depends(get_db),
) -> list[object]:
    return list_source_candidates(
        session,
        campaign_id=campaign_id,
        run_id=run_id,
        status=status,
        score_bucket=outcome,
        candidate_type=candidate_type,
        domain=domain,
        min_score=min_score,
        limit=limit,
    )


@router.get("/campaigns/{campaign_id}", response_model=dict[str, object])
def discovery_campaign_detail(
    campaign_id: int,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        detail = build_discovery_campaign_detail(session, campaign_id)
        campaign = DiscoveryCampaignRead.model_validate(detail["campaign"]).model_dump(
            mode="python"
        )
        recent_runs = [
            DiscoveryRunRead.model_validate(run).model_dump(mode="python")
            for run in detail.get("recent_runs", [])
        ]
        return {**detail, "campaign": campaign, "recent_runs": recent_runs}
    except ValueError as exc:
        raise translate_discovery_error(exc) from exc


@router.patch("/campaigns/{campaign_id}", response_model=DiscoveryCampaignRead)
def patch_discovery_campaign(
    campaign_id: int,
    payload: DiscoveryCampaignUpdate,
    session: Session = Depends(get_db),
) -> object:
    try:
        return update_discovery_campaign(
            session,
            campaign_id,
            payload,
            actor="api_discovery",
        )
    except ValueError as exc:
        raise translate_discovery_error(exc) from exc


@router.post("/campaigns/{campaign_id}/run", response_model=DiscoveryRunResultRead)
def run_campaign(
    campaign_id: int,
    payload: DiscoveryRunRequest,
    session: Session = Depends(get_db),
) -> object:
    try:
        if payload.campaign_id not in {None, campaign_id}:
            raise ValueError("Discovery run campaign_id does not match the route campaign_id.")
        payload = payload.model_copy(update={"campaign_id": campaign_id})
        return run_discovery_campaign(
            session,
            campaign_id,
            payload,
            actor="api_discovery",
        )
    except ValueError as exc:
        raise translate_discovery_error(exc) from exc


@router.get("/runs/{run_id}", response_model=dict[str, object])
def discovery_run_detail(
    run_id: int,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        return build_discovery_run_detail(session, run_id)
    except ValueError as exc:
        raise translate_discovery_error(exc) from exc


@router.get("/candidates/{candidate_id}", response_model=SourceCandidateDetailRead)
def discovery_candidate_detail(
    candidate_id: int,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        return build_source_candidate_detail(session, candidate_id)
    except ValueError as exc:
        raise translate_discovery_error(exc) from exc


@router.get(
    "/candidates/{candidate_id}/score",
    response_model=CandidateScoreExplanationRead,
)
def discovery_candidate_score(
    candidate_id: int,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        return explain_candidate_score(session, candidate_id)
    except ValueError as exc:
        raise translate_discovery_error(exc) from exc


@router.get("/candidates/{candidate_id}/lineage", response_model=DiscoveryLineageRead)
def discovery_candidate_lineage(
    candidate_id: int,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        return build_candidate_lineage(session, candidate_id)
    except ValueError as exc:
        raise translate_discovery_error(exc) from exc


@router.post(
    "/candidates/{candidate_id}/promote",
    response_model=CandidatePromotionResultRead,
)
def promote_discovery_candidate(
    candidate_id: int,
    payload: CandidatePromotionRequest,
    session: Session = Depends(get_db),
) -> object:
    try:
        if payload.candidate_id not in {None, candidate_id}:
            raise ValueError("Promotion candidate_id does not match the route candidate_id.")
        payload = payload.model_copy(update={"candidate_id": candidate_id})
        return promote_source_candidate(
            session,
            candidate_id,
            payload,
            actor="api_discovery",
        )
    except ValueError as exc:
        raise translate_discovery_error(exc) from exc


@router.post(
    "/candidates/{candidate_id}/suppress",
    response_model=CandidateSuppressionRead,
)
def suppress_discovery_candidate(
    candidate_id: int,
    payload: CandidateSuppressionRequest,
    session: Session = Depends(get_db),
) -> object:
    try:
        if payload.candidate_id not in {None, candidate_id}:
            raise ValueError("Suppression candidate_id does not match the route candidate_id.")
        payload = payload.model_copy(update={"candidate_id": candidate_id})
        return suppress_source_candidate(
            session,
            candidate_id,
            payload,
            actor="api_discovery",
        )
    except ValueError as exc:
        raise translate_discovery_error(exc) from exc


@router.post(
    "/candidates/{candidate_id}/revisit",
    response_model=DiscoveryRevisitResultRead,
)
def revisit_discovery_candidate(
    candidate_id: int,
    payload: DiscoveryRevisitRequest,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        if payload.candidate_id not in {None, candidate_id}:
            raise ValueError("Revisit candidate_id does not match the route candidate_id.")
        payload = payload.model_copy(update={"candidate_id": candidate_id})
        return revisit_discovery(
            session,
            payload,
            candidate_id=candidate_id,
            actor="api_discovery",
        )
    except ValueError as exc:
        raise translate_discovery_error(exc) from exc


@router.post(
    "/candidates/{candidate_id}/health",
    response_model=CandidateHealthCheckRead,
)
def check_discovery_candidate_health(
    candidate_id: int,
    session: Session = Depends(get_db),
) -> object:
    try:
        return check_candidate_health(
            session,
            candidate_id,
            actor="api_discovery",
        )
    except ValueError as exc:
        raise translate_discovery_error(exc) from exc


def translate_discovery_error(exc: ValueError) -> HTTPException:
    detail = str(exc)
    status_code = 404 if "does not exist" in detail or "not found" in detail.lower() else 409
    return HTTPException(status_code=status_code, detail=detail)
