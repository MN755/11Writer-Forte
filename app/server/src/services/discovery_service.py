from __future__ import annotations

import hashlib
import re
import time
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote_plus, urlsplit, urlunsplit
from urllib.robotparser import RobotFileParser

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session

from src.config import get_settings
from src.models import (
    AlertORM,
    CameraSourceInventoryORM,
    CandidateHealthCheckORM,
    CandidatePromotionDecisionORM,
    CandidateSuppressionORM,
    CustodyLogORM,
    DiscoveryArtifactORM,
    DiscoveryCampaignORM,
    DiscoveryDomainPolicyORM,
    DiscoveryFrontierEntryORM,
    DiscoveryGraphEdgeORM,
    DiscoveryRunORM,
    RobotsObservationORM,
    ScheduledTaskORM,
    SourceCandidateORM,
    SourceCandidateRevisionORM,
    SourceDefinitionORM,
    StorageObjectORM,
)
from src.schemas import DiscoveryCampaignCreate, DiscoveryCampaignUpdate
from src.services.discovery_analysis import (
    DocumentAnalysis,
    analyze_document,
    canonical_url_hash,
    canonicalize_url,
    compute_candidate_score,
    normalize_path_pattern,
    recommend_source_kind,
)
from src.services.discovery_fetch import (
    DiscoveryFetchError,
    FetchPolicy,
    FetchResult,
    HTTPStatusFetchError,
    ResponseTooLargeError,
    UnsafeTargetError,
    apply_domain_pacing,
    fetch_url,
    validate_fetch_url,
)
from src.services.layer_service import ensure_data_layer
from src.services.trust_service import normalize_domain, resolve_trust


DISCOVERY_MODES = {
    "query_seeded",
    "seed_url",
    "sitemap",
    "neighborhood",
    "format_targeted",
    "geospatial",
    "entity_led",
    "historical_backfill",
}

DEFAULT_CRAWL_POLICY: dict[str, Any] = {
    "robots_aware": True,
    "allow_private_networks": False,
    "request_timeout_seconds": 15.0,
    "retry_attempts": 2,
    "retry_backoff_seconds": 1.0,
    "max_response_bytes": 5_000_000,
    "crawl_delay_seconds": 1.0,
    "max_pages_per_domain": 25,
    "allow_cross_domain_links": False,
    "max_seconds": 300.0,
    "store_artifacts": True,
    "robots_ttl_seconds": 86_400,
    "user_agent": "11Writer-Forte/0.1 (+bounded-source-discovery)",
}

RUNNABLE_SOURCE_KINDS = {
    "local_file",
    "http_json",
    "http_jsonl",
    "http_text",
    "http_xml",
    "rss",
    "web_search",
    "web_crawl",
    "web_discovery",
}

REFERENCE_ONLY_SOURCE_KINDS = {
    "websocket_stream",
    "sse_stream",
    "webhook_ingest",
    "camera_image",
    "camera_stream",
}

FORMAT_EXTENSIONS = {
    ".atom",
    ".csv",
    ".geojson",
    ".json",
    ".jsonl",
    ".kml",
    ".m3u8",
    ".pdf",
    ".rss",
    ".txt",
    ".xml",
    ".yaml",
    ".yml",
}


@dataclass(frozen=True)
class FrontierFetchTask:
    entry_id: int
    canonical_url: str
    fetch_policy: FetchPolicy
    allowed_content_types: tuple[str, ...]


@dataclass(frozen=True)
class FrontierFetchOutcome:
    entry_id: int
    fetch_result: FetchResult
    analysis: DocumentAnalysis


def discovery_now() -> datetime:
    return datetime.now(timezone.utc)


def normalize_timestamp(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def payload_value(payload: object | None, key: str, default: Any = None) -> Any:
    if payload is None:
        return default
    if isinstance(payload, dict):
        return payload.get(key, default)
    return getattr(payload, key, default)


def payload_changes(payload: object) -> dict[str, Any]:
    if isinstance(payload, dict):
        return dict(payload)
    model_dump = getattr(payload, "model_dump", None)
    if callable(model_dump):
        return dict(model_dump(exclude_unset=True))
    raise TypeError("Discovery payload must be a mapping or Pydantic model.")


def to_json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): to_json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_json_safe(item) for item in value]
    return value


def campaign_modes(campaign: DiscoveryCampaignORM) -> list[str]:
    explicit = getattr(campaign, "modes_json", None)
    if isinstance(explicit, list) and explicit:
        raw_modes = explicit
    else:
        request_modes = (campaign.request_json or {}).get("modes", [])
        raw_modes = request_modes if isinstance(request_modes, list) else []
        raw_modes.extend(part.strip() for part in (campaign.mode or "").split(",") if part.strip())
    modes = [str(mode).strip().lower() for mode in raw_modes if str(mode).strip()]
    if campaign.historical_backfill:
        modes.append("historical_backfill")
    return list(dict.fromkeys(mode for mode in modes if mode in DISCOVERY_MODES)) or ["seed_url"]


def campaign_queries(campaign: DiscoveryCampaignORM) -> list[str]:
    explicit = getattr(campaign, "query_strings_json", None)
    values: list[str] = list(explicit) if isinstance(explicit, list) else []
    if campaign.query_text.strip():
        values.extend(
            part.strip()
            for part in re.split(r"[\r\n]+", campaign.query_text)
            if part.strip()
        )
    values.extend(
        str(value).strip()
        for value in (campaign.request_json or {}).get("queries", [])
        if str(value).strip()
    )
    return list(dict.fromkeys(values))


def campaign_search_templates(campaign: DiscoveryCampaignORM) -> list[str]:
    explicit = getattr(campaign, "search_templates_json", None)
    values = list(explicit) if isinstance(explicit, list) else []
    values.extend(
        str(value).strip()
        for value in (campaign.request_json or {}).get("search_templates", [])
        if str(value).strip()
    )
    return list(dict.fromkeys(str(value).strip() for value in values if str(value).strip()))


def campaign_format_targets(campaign: DiscoveryCampaignORM) -> list[str]:
    explicit = getattr(campaign, "format_targets_json", None)
    values = list(explicit) if isinstance(explicit, list) else []
    values.extend(
        str(value).strip().lower()
        for value in (campaign.request_json or {}).get("format_targets", [])
        if str(value).strip()
    )
    return list(dict.fromkeys(values))


def normalize_domain_list(values: Iterable[str]) -> list[str]:
    normalized = [normalize_domain(value) for value in values]
    return list(dict.fromkeys(value for value in normalized if value))


def validate_campaign_configuration(data: dict[str, Any]) -> None:
    request_json = data.get("request_json") if isinstance(data.get("request_json"), dict) else {}
    modes = data.get("modes_json") or request_json.get("modes") or [data.get("mode")]
    invalid = sorted(
        str(mode)
        for mode in modes
        if mode and str(mode).strip().lower() not in DISCOVERY_MODES and str(mode).strip().lower() != "multi"
    )
    if invalid:
        raise ValueError(f"Unsupported discovery mode(s): {', '.join(invalid)}")
    allowlist = normalize_domain_list(data.get("domain_allowlist_json") or [])
    denylist = normalize_domain_list(data.get("domain_denylist_json") or [])
    overlap = sorted(set(allowlist).intersection(denylist))
    if overlap:
        raise ValueError(f"Domains cannot be both allowed and denied: {', '.join(overlap)}")
    for seed in data.get("seed_urls_json") or []:
        canonicalize_url(str(seed))


def create_discovery_campaign(
    session: Session,
    payload: DiscoveryCampaignCreate,
    *,
    actor: str = "system",
) -> DiscoveryCampaignORM:
    existing = session.scalar(
        select(DiscoveryCampaignORM).where(DiscoveryCampaignORM.name == payload.name)
    )
    if existing is not None:
        raise ValueError(f"Discovery campaign name '{payload.name}' already exists.")

    data = payload.model_dump()
    validate_campaign_configuration(data)
    data["domain_allowlist_json"] = normalize_domain_list(data.get("domain_allowlist_json", []))
    data["domain_denylist_json"] = normalize_domain_list(data.get("domain_denylist_json", []))
    if data.get("layer_key"):
        ensure_data_layer(session, str(data["layer_key"]), actor=actor)
    allowed_columns = {column.name for column in DiscoveryCampaignORM.__table__.columns}
    record = DiscoveryCampaignORM(
        **{
            key: value
            for key, value in data.items()
            if key in allowed_columns and key not in {"campaign_id", "created_at", "updated_at"}
        }
    )
    session.add(record)
    session.flush()
    session.add(
        CustodyLogORM(
            object_type="discovery_campaign",
            object_id=str(record.campaign_id),
            action="discovery_campaign_created",
            actor=actor,
            details_json={
                "campaign_id": record.campaign_id,
                "name": record.name,
                "modes": campaign_modes(record),
                "seed_count": len(record.seed_urls_json),
                "query_count": len(campaign_queries(record)),
            },
        )
    )
    session.commit()
    session.refresh(record)
    return record


def update_discovery_campaign(
    session: Session,
    campaign_id: int,
    payload: DiscoveryCampaignUpdate,
    *,
    actor: str = "system",
) -> DiscoveryCampaignORM:
    record = session.get(DiscoveryCampaignORM, campaign_id)
    if record is None:
        raise ValueError(f"Discovery campaign {campaign_id} does not exist.")
    changes = payload_changes(payload)
    if not changes:
        return record
    proposed = {
        column.name: getattr(record, column.name)
        for column in DiscoveryCampaignORM.__table__.columns
        if hasattr(record, column.name)
    }
    proposed.update(changes)
    validate_campaign_configuration(proposed)
    if "name" in changes and changes["name"] != record.name:
        duplicate = session.scalar(
            select(DiscoveryCampaignORM).where(DiscoveryCampaignORM.name == changes["name"])
        )
        if duplicate is not None:
            raise ValueError(f"Discovery campaign name '{changes['name']}' already exists.")
    for field_name in ("domain_allowlist_json", "domain_denylist_json"):
        if field_name in changes and changes[field_name] is not None:
            changes[field_name] = normalize_domain_list(changes[field_name])
    list_fields = {
        "modes_json",
        "query_strings_json",
        "search_templates_json",
        "format_targets_json",
        "seed_urls_json",
        "locale_variants_json",
        "language_variants_json",
        "domain_allowlist_json",
        "domain_denylist_json",
        "entity_seeds_json",
    }
    dict_fields = {
        "target_geography_json",
        "request_json",
        "crawl_policy_json",
        "scoring_weights_json",
        "schedule_json",
        "metadata_json",
    }
    for field_name in list_fields:
        if field_name in changes and changes[field_name] is None:
            changes[field_name] = []
    for field_name in dict_fields:
        if field_name in changes and changes[field_name] is None:
            changes[field_name] = {}
    if "crawl_policy_json" in changes:
        existing_policy = (
            dict(record.crawl_policy_json)
            if isinstance(record.crawl_policy_json, dict)
            else {}
        )
        incoming_policy = (
            dict(changes["crawl_policy_json"])
            if isinstance(changes["crawl_policy_json"], dict)
            else {}
        )
        changes["crawl_policy_json"] = {**existing_policy, **incoming_policy}
    if changes.get("layer_key"):
        ensure_data_layer(session, str(changes["layer_key"]), actor=actor)
    change_log: dict[str, dict[str, Any]] = {}
    for field_name, new_value in changes.items():
        if not hasattr(record, field_name):
            continue
        old_value = getattr(record, field_name)
        if old_value == new_value:
            continue
        setattr(record, field_name, new_value)
        change_log[field_name] = {"old": to_json_safe(old_value), "new": to_json_safe(new_value)}
    if change_log:
        session.add(
            CustodyLogORM(
                object_type="discovery_campaign",
                object_id=str(record.campaign_id),
                action="discovery_campaign_updated",
                actor=actor,
                details_json={"changes": change_log},
            )
        )
        session.commit()
        session.refresh(record)
    return record


def list_discovery_campaigns(
    session: Session,
    *,
    status: str | None = None,
    enabled: bool | None = None,
    limit: int = 200,
) -> list[DiscoveryCampaignORM]:
    statement = select(DiscoveryCampaignORM).order_by(
        DiscoveryCampaignORM.updated_at.desc(),
        DiscoveryCampaignORM.campaign_id.desc(),
    )
    if status:
        statement = statement.where(DiscoveryCampaignORM.status == status)
    if enabled is not None:
        statement = statement.where(DiscoveryCampaignORM.enabled == enabled)
    return list(session.scalars(statement.limit(max(1, min(limit, 5000)))))


def list_discovery_runs(
    session: Session,
    *,
    campaign_id: int | None = None,
    status: str | None = None,
    limit: int = 200,
) -> list[DiscoveryRunORM]:
    statement = select(DiscoveryRunORM).order_by(DiscoveryRunORM.discovery_run_id.desc())
    if campaign_id is not None:
        statement = statement.where(DiscoveryRunORM.campaign_id == campaign_id)
    if status:
        statement = statement.where(DiscoveryRunORM.status == status)
    return list(session.scalars(statement.limit(max(1, min(limit, 5000)))))


def build_discovery_campaign_detail(session: Session, campaign_id: int) -> dict[str, object]:
    from src.schemas import DiscoveryCampaignRead, DiscoveryRunRead

    campaign = session.get(DiscoveryCampaignORM, campaign_id)
    if campaign is None:
        raise ValueError(f"Discovery campaign {campaign_id} does not exist.")
    runs = list_discovery_runs(session, campaign_id=campaign_id, limit=25)
    candidate_count = int(
        session.scalar(
            select(func.count(func.distinct(SourceCandidateRevisionORM.candidate_id))).where(
                SourceCandidateRevisionORM.campaign_id == campaign_id
            )
        )
        or 0
    )
    frontier_counts = dict(
        session.execute(
            select(DiscoveryFrontierEntryORM.state, func.count())
            .where(DiscoveryFrontierEntryORM.campaign_id == campaign_id)
            .group_by(DiscoveryFrontierEntryORM.state)
        ).all()
    )
    promotion_count = int(
        session.scalar(
            select(func.count())
            .select_from(CandidatePromotionDecisionORM)
            .join(
                DiscoveryRunORM,
                DiscoveryRunORM.discovery_run_id
                == CandidatePromotionDecisionORM.discovery_run_id,
            )
            .where(DiscoveryRunORM.campaign_id == campaign_id)
        )
        or 0
    )
    return {
        "campaign": DiscoveryCampaignRead.model_validate(campaign).model_dump(mode="json"),
        "recent_runs": [
            DiscoveryRunRead.model_validate(run).model_dump(mode="json") for run in runs
        ],
        "run_count": len(
            list(
                session.scalars(
                    select(DiscoveryRunORM.discovery_run_id).where(
                        DiscoveryRunORM.campaign_id == campaign_id
                    )
                )
            )
        ),
        "candidate_count": candidate_count,
        "frontier_count": sum(int(value) for value in frontier_counts.values()),
        "promotion_count": promotion_count,
        "frontier_state_counts": {str(key): int(value) for key, value in frontier_counts.items()},
        "modes": campaign_modes(campaign),
        "query_count": len(campaign_queries(campaign)),
        "seed_count": len(campaign.seed_urls_json),
    }


def list_domain_policies(
    session: Session,
    *,
    domain: str | None = None,
    limit: int = 200,
) -> list[DiscoveryDomainPolicyORM]:
    statement = select(DiscoveryDomainPolicyORM).order_by(
        DiscoveryDomainPolicyORM.normalized_domain.asc()
    )
    if domain:
        normalized = normalize_domain(domain)
        if normalized:
            statement = statement.where(DiscoveryDomainPolicyORM.normalized_domain == normalized)
    return list(session.scalars(statement.limit(max(1, min(limit, 5000)))))


def upsert_domain_policy(
    session: Session,
    payload: object,
    *,
    actor: str = "system",
) -> DiscoveryDomainPolicyORM:
    data = payload_changes(payload) if not isinstance(payload, dict) else dict(payload)
    normalized = normalize_domain(str(data.get("normalized_domain", "")))
    if not normalized:
        raise ValueError("Discovery domain policy requires a valid normalized_domain.")
    data["normalized_domain"] = normalized
    record = session.scalar(
        select(DiscoveryDomainPolicyORM).where(
            DiscoveryDomainPolicyORM.normalized_domain == normalized
        )
    )
    action = "discovery_domain_policy_created"
    if record is None:
        allowed_columns = {column.name for column in DiscoveryDomainPolicyORM.__table__.columns}
        record = DiscoveryDomainPolicyORM(
            **{
                key: value
                for key, value in data.items()
                if key in allowed_columns
                and key not in {"domain_policy_id", "created_at", "updated_at"}
            }
        )
        session.add(record)
        session.flush()
    else:
        action = "discovery_domain_policy_updated"
        for key, value in data.items():
            if key != "normalized_domain" and hasattr(record, key):
                setattr(record, key, value)
    session.add(
        CustodyLogORM(
            object_type="discovery_domain_policy",
            object_id=str(record.domain_policy_id),
            action=action,
            actor=actor,
            details_json={
                "normalized_domain": normalized,
                "policy": record.policy,
                "robots_mode": record.robots_mode,
            },
        )
    )
    session.commit()
    session.refresh(record)
    return record


def update_domain_policy(
    session: Session,
    normalized_domain: str,
    payload: object,
    *,
    actor: str = "system",
) -> DiscoveryDomainPolicyORM:
    normalized = normalize_domain(normalized_domain)
    if not normalized:
        raise ValueError("Discovery domain policy requires a valid normalized_domain.")
    record = session.scalar(
        select(DiscoveryDomainPolicyORM).where(
            DiscoveryDomainPolicyORM.normalized_domain == normalized
        )
    )
    if record is None:
        raise ValueError(f"Discovery domain policy for domain '{normalized}' does not exist.")
    if isinstance(payload, dict):
        changes = {key: value for key, value in payload.items() if value is not None}
    else:
        model_dump = getattr(payload, "model_dump", None)
        if callable(model_dump):
            changes = dict(model_dump(exclude_unset=True, exclude_none=True))
        else:
            changes = payload_changes(payload)
    change_log: dict[str, dict[str, Any]] = {}
    for key, value in changes.items():
        if key == "normalized_domain" or not hasattr(record, key):
            continue
        old_value = getattr(record, key)
        if old_value == value:
            continue
        setattr(record, key, value)
        change_log[key] = {
            "old": to_json_safe(old_value),
            "new": to_json_safe(value),
        }
    if not change_log:
        return record
    session.add(
        CustodyLogORM(
            object_type="discovery_domain_policy",
            object_id=str(record.domain_policy_id),
            action="discovery_domain_policy_updated",
            actor=actor,
            details_json={
                "normalized_domain": normalized,
                "changes": change_log,
            },
        )
    )
    session.commit()
    session.refresh(record)
    return record


def domain_matches(candidate: str, configured: str) -> bool:
    return candidate == configured or candidate.endswith(f".{configured}")


def find_domain_policy(
    session: Session,
    domain: str,
) -> DiscoveryDomainPolicyORM | None:
    policies = list(
        session.scalars(
            select(DiscoveryDomainPolicyORM).where(
                DiscoveryDomainPolicyORM.enabled.is_(True)
            )
        )
    )
    matches = [
        policy
        for policy in policies
        if policy.normalized_domain == domain
        or (policy.allow_subdomains and domain.endswith(f".{policy.normalized_domain}"))
    ]
    if not matches:
        return None
    return max(matches, key=lambda policy: len(policy.normalized_domain))


def ensure_domain_policy(
    session: Session,
    domain: str,
    campaign_policy: dict[str, Any],
) -> DiscoveryDomainPolicyORM:
    existing = find_domain_policy(session, domain)
    if existing is not None:
        return existing
    record = DiscoveryDomainPolicyORM(
        normalized_domain=domain,
        policy="allow",
        robots_mode="respect" if campaign_policy.get("robots_aware", True) else "ignore",
        crawl_delay_seconds=float(campaign_policy.get("crawl_delay_seconds", 1.0)),
        max_concurrency=int(campaign_policy.get("max_concurrency", 1)),
        max_depth=int(campaign_policy.get("max_depth", 2)),
        max_pages_per_run=int(campaign_policy.get("max_pages_per_domain", 25)),
        max_response_bytes=int(campaign_policy.get("max_response_bytes", 5_000_000)),
        request_timeout_seconds=float(campaign_policy.get("request_timeout_seconds", 15.0)),
        retry_attempts=int(campaign_policy.get("retry_attempts", 2)),
        retry_backoff_seconds=float(campaign_policy.get("retry_backoff_seconds", 1.0)),
        metadata_json={"auto_created": True},
    )
    session.add(record)
    session.flush()
    return record


def effective_campaign_policy(campaign: DiscoveryCampaignORM) -> dict[str, Any]:
    policy = dict(DEFAULT_CRAWL_POLICY)
    if isinstance(campaign.crawl_policy_json, dict):
        policy.update(campaign.crawl_policy_json)
    policy["robots_aware"] = bool(policy.get("robots_aware", True))
    private_networks_requested = bool(policy.get("allow_private_networks", False))
    private_networks_enabled = bool(get_settings().discovery_allow_private_networks)
    policy["allow_private_networks_requested"] = private_networks_requested
    policy["private_network_override_enabled"] = private_networks_enabled
    policy["allow_private_networks"] = private_networks_requested and private_networks_enabled
    policy["store_artifacts"] = bool(policy.get("store_artifacts", True))
    policy["allow_cross_domain_links"] = bool(
        policy.get("allow_cross_domain_links", False)
    )
    policy["request_timeout_seconds"] = clamp_float(
        policy.get("request_timeout_seconds"), 15.0, 0.1, 300.0
    )
    policy["retry_attempts"] = clamp_int(policy.get("retry_attempts"), 2, 1, 10)
    policy["retry_backoff_seconds"] = clamp_float(
        policy.get("retry_backoff_seconds"), 1.0, 0.0, 300.0
    )
    policy["max_response_bytes"] = clamp_int(
        policy.get("max_response_bytes"), 5_000_000, 1024, 100_000_000
    )
    policy["crawl_delay_seconds"] = clamp_float(
        policy.get("crawl_delay_seconds"), 1.0, 0.0, 3600.0
    )
    policy["max_concurrency"] = clamp_int(policy.get("max_concurrency"), 1, 1, 20)
    policy["max_pages_per_domain"] = clamp_int(
        policy.get("max_pages_per_domain"), 25, 1, 10_000
    )
    policy["max_seconds"] = clamp_float(policy.get("max_seconds"), 300.0, 1.0, 86_400.0)
    policy["robots_ttl_seconds"] = clamp_int(
        policy.get("robots_ttl_seconds"), 86_400, 300, 2_592_000
    )
    policy["max_depth"] = max(0, min(int(campaign.max_depth), 10))
    policy["max_pages"] = max(1, min(int(campaign.max_pages), 10_000))
    policy["max_candidates"] = max(1, min(int(campaign.max_candidates), 100_000))
    policy["user_agent"] = str(
        policy.get("user_agent") or DEFAULT_CRAWL_POLICY["user_agent"]
    )[:500]
    return policy


def discovery_fetch_claim_timeout_seconds(policy: dict[str, Any]) -> float:
    return max(
        60.0,
        float(policy.get("request_timeout_seconds", 15.0))
        * min(5, int(policy.get("retry_attempts", 2)))
        + max(
            60.0,
            float(policy.get("retry_backoff_seconds", 1.0)),
            float(policy.get("crawl_delay_seconds", 1.0)),
        )
        * max(0, min(5, int(policy.get("retry_attempts", 2))) - 1)
        + 30.0,
    )


def discovery_run_lease_timeout_seconds(policy: dict[str, Any]) -> float:
    return max(
        300.0,
        min(float(policy.get("max_seconds", 300.0)) + 120.0, 7_200.0),
        discovery_fetch_claim_timeout_seconds(policy) + 120.0,
    )


def active_discovery_run_lease(campaign: DiscoveryCampaignORM) -> dict[str, Any] | None:
    metadata = campaign.metadata_json if isinstance(campaign.metadata_json, dict) else {}
    lease = metadata.get("active_run_lease")
    return dict(lease) if isinstance(lease, dict) else None


def discovery_lease_expires_at(lease: dict[str, Any]) -> datetime | None:
    explicit = lease.get("expires_at")
    if isinstance(explicit, str):
        try:
            return normalize_timestamp(datetime.fromisoformat(explicit.replace("Z", "+00:00")))
        except ValueError:
            return None
    heartbeat = lease.get("heartbeat_at")
    timeout_seconds = clamp_float(lease.get("lease_timeout_seconds"), 300.0, 60.0, 86_400.0)
    if isinstance(heartbeat, str):
        try:
            heartbeat_at = normalize_timestamp(datetime.fromisoformat(heartbeat.replace("Z", "+00:00")))
        except ValueError:
            return None
        if heartbeat_at is not None:
            return heartbeat_at + timedelta(seconds=timeout_seconds)
    return None


def discovery_lease_is_active(lease: dict[str, Any], *, now: datetime | None = None) -> bool:
    expires_at = discovery_lease_expires_at(lease)
    reference = now or discovery_now()
    return expires_at is not None and expires_at > reference


def build_discovery_run_lease(
    campaign: DiscoveryCampaignORM,
    run: DiscoveryRunORM,
    *,
    actor: str,
    policy: dict[str, Any],
    previous_takeover_count: int = 0,
) -> dict[str, Any]:
    now = discovery_now()
    timeout_seconds = discovery_run_lease_timeout_seconds(policy)
    token_seed = (
        f"{campaign.campaign_id}:{run.discovery_run_id}:{actor}:"
        f"{now.isoformat()}:{time.monotonic_ns()}"
    )
    return {
        "campaign_id": campaign.campaign_id,
        "run_id": run.discovery_run_id,
        "actor": actor,
        "lease_token": hashlib.sha256(token_seed.encode("utf-8")).hexdigest(),
        "acquired_at": now.isoformat(),
        "heartbeat_at": now.isoformat(),
        "expires_at": (now + timedelta(seconds=timeout_seconds)).isoformat(),
        "lease_timeout_seconds": timeout_seconds,
        "stale_takeover_count": max(0, int(previous_takeover_count)),
    }


def set_discovery_run_lease(
    campaign: DiscoveryCampaignORM,
    run: DiscoveryRunORM,
    lease: dict[str, Any],
) -> None:
    campaign_metadata = dict(campaign.metadata_json or {})
    campaign_metadata["active_run_lease"] = lease
    campaign.metadata_json = campaign_metadata
    run_metadata = dict(run.metadata_json or {})
    run_metadata["active_lease"] = lease
    run.metadata_json = run_metadata


def release_discovery_run_lease(
    campaign: DiscoveryCampaignORM,
    run: DiscoveryRunORM,
    *,
    actor: str,
    reason: str,
) -> None:
    metadata = dict(campaign.metadata_json or {})
    active_lease = metadata.get("active_run_lease")
    if isinstance(active_lease, dict) and int(active_lease.get("run_id") or 0) == run.discovery_run_id:
        metadata.pop("active_run_lease", None)
        campaign.metadata_json = metadata
    run_metadata = dict(run.metadata_json or {})
    active_lease = run_metadata.pop("active_lease", None)
    history = dict(run_metadata.get("last_released_lease") or {})
    if isinstance(active_lease, dict):
        history = {
            **active_lease,
            "released_at": discovery_now().isoformat(),
            "released_by": actor,
            "release_reason": reason,
        }
    run_metadata["last_released_lease"] = history
    run.metadata_json = run_metadata


def heartbeat_discovery_run_lease(
    campaign: DiscoveryCampaignORM,
    run: DiscoveryRunORM,
    *,
    actor: str,
) -> None:
    metadata = dict(campaign.metadata_json or {})
    lease = metadata.get("active_run_lease")
    if not isinstance(lease, dict):
        return
    if int(lease.get("run_id") or 0) != run.discovery_run_id:
        return
    now = discovery_now()
    timeout_seconds = clamp_float(lease.get("lease_timeout_seconds"), 300.0, 60.0, 86_400.0)
    refreshed = {
        **lease,
        "actor": actor,
        "heartbeat_at": now.isoformat(),
        "expires_at": (now + timedelta(seconds=timeout_seconds)).isoformat(),
    }
    set_discovery_run_lease(campaign, run, refreshed)


def acquire_discovery_run_lease(
    session: Session,
    campaign: DiscoveryCampaignORM,
    run: DiscoveryRunORM,
    *,
    actor: str,
    policy: dict[str, Any],
) -> dict[str, Any]:
    existing = active_discovery_run_lease(campaign)
    now = discovery_now()
    if existing is not None and discovery_lease_is_active(existing, now=now):
        active_run_id = int(existing.get("run_id") or 0)
        raise ValueError(
            f"Discovery campaign {campaign.campaign_id} is already leased by run "
            f"{active_run_id or 'unknown'} ({existing.get('actor') or 'unknown'}); "
            "wait for that lease to expire or resume the active run."
        )
    previous_takeover_count = 0
    stale_takeover = existing is not None and not discovery_lease_is_active(existing, now=now)
    if stale_takeover:
        previous_takeover_count = int(existing.get("stale_takeover_count") or 0) + 1
    lease = build_discovery_run_lease(
        campaign,
        run,
        actor=actor,
        policy=policy,
        previous_takeover_count=previous_takeover_count,
    )
    set_discovery_run_lease(campaign, run, lease)
    if stale_takeover:
        session.add(
            CustodyLogORM(
                object_type="discovery_campaign",
                object_id=str(campaign.campaign_id),
                action="discovery_run_lease_recovered",
                actor=actor,
                details_json={
                    "recovered_run_id": existing.get("run_id"),
                    "recovered_actor": existing.get("actor"),
                    "recovered_heartbeat_at": existing.get("heartbeat_at"),
                    "new_run_id": run.discovery_run_id,
                },
            )
        )
    return lease


def clamp_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(parsed, maximum))


def clamp_float(value: Any, default: float, minimum: float, maximum: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(parsed, maximum))


def effective_fetch_policy(
    campaign_policy: dict[str, Any],
    domain_policy: DiscoveryDomainPolicyORM,
) -> FetchPolicy:
    metadata = domain_policy.metadata_json if isinstance(domain_policy.metadata_json, dict) else {}
    private_networks_requested = bool(
        metadata.get(
            "allow_private_networks",
            campaign_policy.get("allow_private_networks_requested", False),
        )
    )
    return FetchPolicy(
        timeout_seconds=domain_policy.request_timeout_seconds,
        retry_attempts=domain_policy.retry_attempts,
        # Retries are requests too.  Never let their backoff undercut the domain's
        # persisted crawl delay.
        retry_backoff_seconds=max(
            domain_policy.retry_backoff_seconds,
            domain_policy.crawl_delay_seconds,
        ),
        max_response_bytes=domain_policy.max_response_bytes,
        allow_private_networks=(
            private_networks_requested
            and bool(get_settings().discovery_allow_private_networks)
        ),
        user_agent=str(campaign_policy.get("user_agent", DEFAULT_CRAWL_POLICY["user_agent"])),
    )


def url_allowed_for_campaign(
    campaign: DiscoveryCampaignORM,
    url: str,
    *,
    domain_policy: DiscoveryDomainPolicyORM | None = None,
) -> tuple[bool, str | None]:
    domain = normalize_domain(url)
    if not domain:
        return False, "invalid_domain"
    allowlist = normalize_domain_list(campaign.domain_allowlist_json or [])
    denylist = normalize_domain_list(campaign.domain_denylist_json or [])
    if allowlist and not any(domain_matches(domain, allowed) for allowed in allowlist):
        return False, "not_in_domain_allowlist"
    if any(domain_matches(domain, denied) for denied in denylist):
        return False, "domain_denylist"
    if domain_policy is not None and domain_policy.policy in {"deny", "block", "quarantine"}:
        return False, f"domain_policy_{domain_policy.policy}"
    path = urlsplit(url).path or "/"
    if domain_policy is not None:
        denied_patterns = domain_policy.denied_path_patterns_json or []
        if any(path_pattern_matches(path, str(pattern)) for pattern in denied_patterns):
            return False, "path_denied"
        allowed_patterns = domain_policy.allowed_path_patterns_json or []
        if allowed_patterns and not any(
            path_pattern_matches(path, str(pattern)) for pattern in allowed_patterns
        ):
            return False, "path_not_allowed"
    return True, None


def path_pattern_matches(path: str, pattern: str) -> bool:
    escaped = re.escape(pattern).replace(r"\*", ".*").replace(r"\?", ".")
    return re.fullmatch(escaped, path) is not None


def active_suppression_for_url(
    session: Session,
    canonical_url: str,
    domain: str,
) -> CandidateSuppressionORM | None:
    now = discovery_now()
    reconcile_expired_suppressions(session, now=now)
    suppressions = list(
        session.scalars(
            select(CandidateSuppressionORM).where(
                CandidateSuppressionORM.status == "active",
                or_(
                    CandidateSuppressionORM.expires_at.is_(None),
                    CandidateSuppressionORM.expires_at > now,
                ),
            )
        )
    )
    for suppression in suppressions:
        if suppression.scope == "domain" and suppression.normalized_domain:
            if domain_matches(domain, suppression.normalized_domain):
                return suppression
        if suppression.candidate_id is not None:
            candidate = session.get(SourceCandidateORM, suppression.candidate_id)
            if candidate is not None and candidate.canonical_url == canonical_url:
                return suppression
    return None


def suppression_applies_to_candidate(
    suppression: CandidateSuppressionORM,
    candidate: SourceCandidateORM,
) -> bool:
    if suppression.scope == "domain" and suppression.normalized_domain:
        return domain_matches(candidate.normalized_domain, suppression.normalized_domain)
    return suppression.candidate_id == candidate.candidate_id


def reconcile_expired_suppressions(
    session: Session,
    *,
    now: datetime | None = None,
    actor: str = "discovery_suppression_reconciler",
) -> list[int]:
    """Expire elapsed suppression rows and release candidates they no longer cover."""

    resolved_now = now or discovery_now()
    expired = list(
        session.scalars(
            select(CandidateSuppressionORM).where(
                CandidateSuppressionORM.status == "active",
                CandidateSuppressionORM.expires_at.is_not(None),
                CandidateSuppressionORM.expires_at <= resolved_now,
            )
        )
    )
    if not expired:
        return []
    candidates = list(session.scalars(select(SourceCandidateORM)))
    affected_ids = {
        candidate.candidate_id
        for suppression in expired
        for candidate in candidates
        if suppression_applies_to_candidate(suppression, candidate)
    }
    for suppression in expired:
        suppression.status = "expired"
        session.add(
            CustodyLogORM(
                object_type="candidate_suppression",
                object_id=str(suppression.suppression_id),
                action="candidate_suppression_expired",
                actor=actor,
                details_json={"expired_at": resolved_now.isoformat()},
            )
        )
    session.flush()
    remaining_active = list(
        session.scalars(
            select(CandidateSuppressionORM).where(
                CandidateSuppressionORM.status == "active",
                or_(
                    CandidateSuppressionORM.expires_at.is_(None),
                    CandidateSuppressionORM.expires_at > resolved_now,
                ),
            )
        )
    )
    released_ids: list[int] = []
    by_id = {candidate.candidate_id: candidate for candidate in candidates}
    for candidate_id in sorted(affected_ids):
        candidate = by_id[candidate_id]
        if candidate.status != "suppressed" or any(
            suppression_applies_to_candidate(suppression, candidate)
            for suppression in remaining_active
        ):
            continue
        trust_level = str((candidate.trust_hints_json or {}).get("trust_level") or "neutral")
        candidate.status = (
            "promoted"
            if candidate.promoted_source_id is not None
            else status_for_score(
                candidate.score_bucket,
                trust_level=trust_level,
                existing_status=None,
            )
        )
        append_candidate_revision(
            session,
            candidate,
            campaign_id=candidate.last_campaign_id,
            run_id=candidate.last_run_id,
            revision_kind="suppression_expired",
            changed=True,
            reason="All applicable candidate suppressions expired.",
        )
        session.add(
            CustodyLogORM(
                object_type="source_candidate",
                object_id=str(candidate.candidate_id),
                action="candidate_suppression_reconciled",
                actor=actor,
                details_json={"status": candidate.status},
            )
        )
        released_ids.append(candidate.candidate_id)
    session.flush()
    return released_ids


def reconcile_expired_suppressions_for_read(session: Session) -> None:
    """Persist time-based suppression transitions before inventory read models."""

    reconcile_expired_suppressions(session)
    if session.new or session.dirty or session.deleted:
        session.commit()


def build_search_seed_urls(campaign: DiscoveryCampaignORM) -> list[tuple[str, dict[str, Any]]]:
    seeds: list[tuple[str, dict[str, Any]]] = []
    queries = campaign_queries(campaign)
    for entity in campaign.entity_seeds_json or []:
        name = entity.get("name") or entity.get("canonical_name")
        if name:
            entity_type = str(entity.get("type") or entity.get("entity_type") or "entity")
            queries.append(f"{entity_type} {name}".strip())
        aliases = entity.get("aliases", [])
        if isinstance(aliases, list):
            queries.extend(str(alias).strip() for alias in aliases if str(alias).strip())
    target_geo = campaign.target_geography_json or {}
    for key in ("place_names", "jurisdictions", "routes", "corridors"):
        values = target_geo.get(key, [])
        if isinstance(values, list):
            queries.extend(str(value).strip() for value in values if str(value).strip())
    queries = list(dict.fromkeys(query for query in queries if query))
    templates = campaign_search_templates(campaign)
    languages = campaign.language_variants_json or [""]
    locales = campaign.locale_variants_json or [""]

    for query in queries:
        parsed_query = urlsplit(query)
        if parsed_query.scheme in {"http", "https"} and parsed_query.netloc:
            seeds.append((query, {"query": query, "query_direct_url": True}))
        for template in templates:
            for language in languages:
                for locale in locales:
                    try:
                        search_url = template.format(
                            query=quote_plus(query),
                            raw_query=query,
                            language=quote_plus(str(language)),
                            locale=quote_plus(str(locale)),
                            recency_days=campaign.recency_days or "",
                        )
                    except (KeyError, ValueError) as exc:
                        raise ValueError(f"Invalid discovery search template '{template}': {exc}") from exc
                    seeds.append(
                        (
                            search_url,
                            {
                                "query": query,
                                "language": language,
                                "locale": locale,
                                "search_template": template,
                                "is_search_page": True,
                            },
                        )
                    )
    return seeds


def seed_run_frontier(
    session: Session,
    campaign: DiscoveryCampaignORM,
    run: DiscoveryRunORM,
) -> int:
    queued = 0
    modes = set(campaign_modes(campaign))
    for seed_url in campaign.seed_urls_json or []:
        if enqueue_frontier(
            session,
            campaign,
            run,
            str(seed_url),
            discovery_method="seed_url",
            depth=0,
            priority=100.0,
            metadata={"seed": True},
        ) is not None:
            queued += 1

    for search_url, metadata in build_search_seed_urls(campaign):
        if enqueue_frontier(
            session,
            campaign,
            run,
            search_url,
            discovery_method="query_seeded",
            depth=0,
            priority=95.0,
            metadata=metadata,
        ) is not None:
            queued += 1

    if "sitemap" in modes:
        origins: set[str] = set()
        for seed_url in campaign.seed_urls_json or []:
            parsed = urlsplit(str(seed_url))
            if parsed.scheme in {"http", "https"} and parsed.netloc:
                origins.add(urlunsplit((parsed.scheme, parsed.netloc, "", "", "")))
        for origin in origins:
            if enqueue_frontier(
                session,
                campaign,
                run,
                f"{origin.rstrip('/')}/sitemap.xml",
                discovery_method="sitemap_probe",
                depth=0,
                priority=90.0,
                metadata={"sitemap_probe": True},
            ) is not None:
                queued += 1
    run.pages_queued = queued
    return queued


def enqueue_frontier(
    session: Session,
    campaign: DiscoveryCampaignORM,
    run: DiscoveryRunORM,
    url: str,
    *,
    discovery_method: str,
    depth: int,
    priority: float,
    parent_url: str | None = None,
    parent_candidate_id: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> DiscoveryFrontierEntryORM | None:
    try:
        canonical = canonicalize_url(url, base_url=parent_url)
    except (TypeError, ValueError):
        return None
    domain = normalize_domain(canonical)
    if not domain:
        return None
    existing = session.scalar(
        select(DiscoveryFrontierEntryORM).where(
            DiscoveryFrontierEntryORM.discovery_run_id == run.discovery_run_id,
            DiscoveryFrontierEntryORM.canonical_url_hash == canonical_url_hash(canonical),
        )
    )
    if existing is not None:
        return None
    campaign_policy = effective_campaign_policy(campaign)
    domain_policy = find_domain_policy(session, domain)
    allowed, reason = url_allowed_for_campaign(
        campaign,
        canonical,
        domain_policy=domain_policy,
    )
    suppression = active_suppression_for_url(session, canonical, domain)
    state = "queued" if allowed and suppression is None else "blocked"
    details = dict(metadata or {})
    if reason:
        details["block_reason"] = reason
    if suppression is not None:
        details["block_reason"] = "active_suppression"
        details["suppression_id"] = suppression.suppression_id
    record = DiscoveryFrontierEntryORM(
        discovery_run_id=run.discovery_run_id,
        campaign_id=campaign.campaign_id,
        canonical_url_hash=canonical_url_hash(canonical),
        canonical_url=canonical,
        discovered_url=url,
        priority=float(priority),
        state=state,
        max_attempts=max(1, min(int(campaign_policy.get("retry_attempts", 2)), 10)),
        depth=depth,
        parent_url=parent_url,
        parent_candidate_id=parent_candidate_id,
        discovery_method=discovery_method,
        metadata_json=details,
    )
    session.add(record)
    session.flush()
    run.pages_queued += 1
    return record


def run_discovery_campaign(
    session: Session,
    campaign_id: int,
    payload: object | None = None,
    *,
    actor: str = "discovery_engine",
) -> dict[str, object]:
    campaign = session.get(DiscoveryCampaignORM, campaign_id)
    if campaign is None:
        raise ValueError(f"Discovery campaign {campaign_id} does not exist.")
    if not campaign.enabled:
        raise ValueError(f"Discovery campaign {campaign_id} is disabled.")

    requested_resume_id = payload_value(payload, "resume_run_id")
    resume_enabled = bool(payload_value(payload, "resume", True))
    run: DiscoveryRunORM | None = None
    if requested_resume_id is not None:
        run = session.get(DiscoveryRunORM, int(requested_resume_id))
        if run is None or run.campaign_id != campaign_id:
            raise ValueError(
                f"Discovery run {requested_resume_id} does not exist for campaign {campaign_id}."
            )
    elif resume_enabled:
        run = session.scalar(
            select(DiscoveryRunORM)
            .where(
                DiscoveryRunORM.campaign_id == campaign_id,
                DiscoveryRunORM.status.in_(("checkpointed", "paused")),
            )
            .order_by(DiscoveryRunORM.discovery_run_id.desc())
            .limit(1)
        )

    persisted_policy = effective_campaign_policy(campaign)
    if run is None:
        active_run = session.scalar(
            select(DiscoveryRunORM)
            .where(
                DiscoveryRunORM.campaign_id == campaign_id,
                DiscoveryRunORM.status == "running",
            )
            .order_by(DiscoveryRunORM.discovery_run_id.desc())
            .limit(1)
        )
        if active_run is not None:
            raise ValueError(
                f"Discovery campaign {campaign_id} already has running run "
                f"{active_run.discovery_run_id}; resume that run explicitly."
            )
        resolved_modes = campaign_modes(campaign)
        run = DiscoveryRunORM(
            campaign_id=campaign_id,
            mode=resolved_modes[0] if len(resolved_modes) == 1 else "multi",
            status="running",
            trigger_kind=str(payload_value(payload, "trigger_kind", "manual")),
            actor=actor,
            request_snapshot_json={
                "queries": campaign_queries(campaign),
                "modes": resolved_modes,
                "seed_urls": list(campaign.seed_urls_json or []),
                "search_templates": campaign_search_templates(campaign),
                "target_geography": campaign.target_geography_json or {},
                "entities": campaign.entity_seeds_json or [],
                "format_targets": campaign_format_targets(campaign),
            },
            policy_snapshot_json=persisted_policy,
        )
        session.add(run)
        session.flush()
        seed_run_frontier(session, campaign, run)
        if run.pages_queued == 0:
            run.error_count += 1
            run.error_text = (
                "Campaign produced no runnable frontier entries. Query-seeded campaigns "
                "require an operator-configured search template or direct seed URL."
            )
            run.stats_json = {"warnings": ["empty_frontier"]}
        session.add(
            CustodyLogORM(
                object_type="discovery_run",
                object_id=str(run.discovery_run_id),
                action="discovery_run_started",
                actor=actor,
                details_json={
                    "campaign_id": campaign_id,
                    "modes": campaign_modes(campaign),
                    "pages_queued": run.pages_queued,
                },
            )
        )
    else:
        if run.status not in {"checkpointed", "paused", "running"}:
            raise ValueError(
                f"Discovery run {run.discovery_run_id} cannot resume from status {run.status}."
            )
        run.status = "running"
        run.actor = actor
        run.metadata_json = {
            **(run.metadata_json or {}),
            "resume_count": int((run.metadata_json or {}).get("resume_count", 0)) + 1,
            "last_resumed_at": discovery_now().isoformat(),
        }
        session.add(
            CustodyLogORM(
                object_type="discovery_run",
                object_id=str(run.discovery_run_id),
                action="discovery_run_resumed",
                actor=actor,
                details_json={"campaign_id": campaign_id},
            )
        )
    campaign.status = "running"
    campaign.last_run_at = discovery_now()
    acquire_discovery_run_lease(
        session,
        campaign,
        run,
        actor=actor,
        policy=(run.policy_snapshot_json or persisted_policy),
    )
    session.commit()

    if bool(payload_value(payload, "dry_run", False)):
        run.status = "dry_run"
        run.finished_at = discovery_now()
        campaign.status = "active"
        release_discovery_run_lease(campaign, run, actor=actor, reason="dry_run")
        session.commit()
        session.refresh(run)
        return build_discovery_run_result(session, run)

    max_pages = int(payload_value(payload, "max_pages", None) or campaign.max_pages)
    max_candidates = int(
        payload_value(payload, "max_candidates", None) or campaign.max_candidates
    )
    max_seconds = float(
        payload_value(payload, "max_seconds", None)
        or (run.policy_snapshot_json or persisted_policy).get("max_seconds", 300.0)
    )
    try:
        process_discovery_frontier(
            session,
            campaign,
            run,
            max_pages=max(1, min(max_pages, campaign.max_pages)),
            max_candidates=max(1, min(max_candidates, campaign.max_candidates)),
            max_seconds=max(1.0, min(max_seconds, 3600.0)),
            actor=actor,
        )
    except Exception as exc:
        session.rollback()
        failed_campaign = session.get(DiscoveryCampaignORM, campaign_id)
        failed_run = session.get(DiscoveryRunORM, run.discovery_run_id)
        if failed_campaign is not None and failed_run is not None:
            failed_run.status = "paused"
            failed_run.error_count += 1
            failed_run.error_text = str(exc)[:4000]
            failed_campaign.status = "paused"
            release_discovery_run_lease(
                failed_campaign,
                failed_run,
                actor=actor,
                reason="unhandled_exception",
            )
            session.add(
                CustodyLogORM(
                    object_type="discovery_run",
                    object_id=str(failed_run.discovery_run_id),
                    action="discovery_run_paused",
                    actor=actor,
                    details_json={
                        "campaign_id": campaign_id,
                        "error_type": type(exc).__name__,
                        "error_text": str(exc)[:1000],
                    },
                )
            )
            session.commit()
        raise
    session.refresh(run)
    session.refresh(campaign)
    release_discovery_run_lease(campaign, run, actor=actor, reason="run_finished")
    session.commit()
    session.refresh(run)
    return build_discovery_run_result(session, run)


def process_discovery_frontier(
    session: Session,
    campaign: DiscoveryCampaignORM,
    run: DiscoveryRunORM,
    *,
    max_pages: int,
    max_candidates: int,
    max_seconds: float,
    actor: str,
) -> None:
    started = time.monotonic()
    processed_this_call = 0
    reclaim_stale_frontier_claims(session, campaign, run, actor=actor)
    touched_candidate_ids = set(
        session.scalars(
            select(SourceCandidateRevisionORM.candidate_id)
            .where(SourceCandidateRevisionORM.discovery_run_id == run.discovery_run_id)
            .distinct()
        )
    )
    touched_this_call: set[int] = set()
    persisted_policy = run.policy_snapshot_json or effective_campaign_policy(campaign)
    max_concurrency = max(1, min(int(persisted_policy.get("max_concurrency", 1)), 20))
    run_page_budget = max(
        1,
        min(int(persisted_policy.get("max_pages", campaign.max_pages)), 10_000),
    )
    run_candidate_budget = max(
        1,
        min(
            int(persisted_policy.get("max_candidates", campaign.max_candidates)),
            100_000,
        ),
    )
    started_entry_count = int(
        session.scalar(
            select(func.count()).select_from(DiscoveryFrontierEntryORM).where(
                DiscoveryFrontierEntryORM.discovery_run_id == run.discovery_run_id,
                DiscoveryFrontierEntryORM.attempt_count > 0,
            )
        )
        or 0
    )
    domain_fetch_counts: Counter[str] = Counter()
    for attempted_entry in session.scalars(
        select(DiscoveryFrontierEntryORM).where(
            DiscoveryFrontierEntryORM.discovery_run_id == run.discovery_run_id,
            DiscoveryFrontierEntryORM.attempt_count > 0,
        )
    ):
        attempted_domain = normalize_domain(attempted_entry.canonical_url)
        if attempted_domain:
            domain_fetch_counts[attempted_domain] += attempted_entry.attempt_count

    while processed_this_call < max_pages and time.monotonic() - started < max_seconds:
        now = discovery_now()
        candidate_budget_exhausted = len(touched_candidate_ids) >= run_candidate_budget
        if candidate_budget_exhausted:
            break
        if len(touched_this_call) >= max_candidates:
            break
        if max_concurrency > 1:
            batch_limit = min(
                max_concurrency,
                max_pages - processed_this_call,
                max_candidates - len(touched_this_call),
            )
            (
                batch_processed,
                batch_candidate_ids,
                started_entry_count,
            ) = process_frontier_batch(
                session,
                campaign,
                run,
                actor=actor,
                batch_limit=max(1, batch_limit),
                run_page_budget=run_page_budget,
                started_entry_count=started_entry_count,
                domain_fetch_counts=domain_fetch_counts,
            )
            if batch_processed == 0:
                break
            processed_this_call += batch_processed
            touched_candidate_ids.update(batch_candidate_ids)
            touched_this_call.update(batch_candidate_ids)
            continue
        page_budget_exhausted = started_entry_count >= run_page_budget
        claim_statement = (
            select(DiscoveryFrontierEntryORM)
            .where(
                DiscoveryFrontierEntryORM.discovery_run_id == run.discovery_run_id,
                DiscoveryFrontierEntryORM.state.in_(("queued", "retry_wait")),
                or_(
                    DiscoveryFrontierEntryORM.next_attempt_at.is_(None),
                    DiscoveryFrontierEntryORM.next_attempt_at <= now,
                ),
                *(
                    (DiscoveryFrontierEntryORM.attempt_count > 0,)
                    if page_budget_exhausted
                    else ()
                ),
            )
            .order_by(
                DiscoveryFrontierEntryORM.priority.desc(),
                DiscoveryFrontierEntryORM.frontier_entry_id.asc(),
            )
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        entry = session.scalar(claim_statement)
        if entry is None:
            break
        domain = normalize_domain(entry.canonical_url) or "unknown"
        campaign_policy = effective_campaign_policy(campaign)
        domain_policy = ensure_domain_policy(session, domain, campaign_policy)
        per_domain_limit = min(
            domain_policy.max_pages_per_run,
            int(campaign_policy.get("max_pages_per_domain", 25)),
        )
        if domain_fetch_counts[domain] >= per_domain_limit:
            entry.state = "deferred"
            entry.completed_at = now
            entry.last_error_text = "per_domain_page_limit"
            session.commit()
            continue
        heartbeat_discovery_run_lease(campaign, run, actor=actor)
        entry.state = "fetching"
        entry.claimed_at = now
        entry.last_attempt_at = now
        entry.attempt_count += 1
        if entry.attempt_count == 1:
            started_entry_count += 1
        session.commit()
        domain_fetch_counts[domain] += 1
        try:
            candidate = process_frontier_entry(
                session,
                campaign,
                run,
                entry,
                domain_policy=domain_policy,
                actor=actor,
            )
            if candidate is not None:
                touched_candidate_ids.add(candidate.candidate_id)
                touched_this_call.add(candidate.candidate_id)
            processed_this_call += 1
        except Exception as exc:
            session.rollback()
            handle_frontier_error(session, campaign, run, entry, exc, actor=actor)
            processed_this_call += 1

    if len(touched_candidate_ids) >= run_candidate_budget:
        defer_frontier_entries(
            session,
            run,
            reason="run_candidate_limit",
            include_attempted=True,
        )
    elif started_entry_count >= run_page_budget:
        defer_frontier_entries(
            session,
            run,
            reason="run_page_limit",
            include_attempted=False,
        )

    remaining = int(
        session.scalar(
            select(func.count()).select_from(DiscoveryFrontierEntryORM).where(
                DiscoveryFrontierEntryORM.discovery_run_id == run.discovery_run_id,
                DiscoveryFrontierEntryORM.state.in_(("queued", "retry_wait", "fetching")),
            )
        )
        or 0
    )
    dead_letters = int(
        session.scalar(
            select(func.count()).select_from(DiscoveryFrontierEntryORM).where(
                DiscoveryFrontierEntryORM.discovery_run_id == run.discovery_run_id,
                DiscoveryFrontierEntryORM.state == "dead_letter",
            )
        )
        or 0
    )
    run.frontier_checkpoint_json = {
        "remaining_entries": remaining,
        "processed_this_call": processed_this_call,
        "candidate_ids": sorted(touched_candidate_ids),
        "domain_fetch_counts": dict(domain_fetch_counts),
        "checkpointed_at": discovery_now().isoformat(),
    }
    run.stats_json = {
        **(run.stats_json or {}),
        "remaining_entries": remaining,
        "dead_letter_count": dead_letters,
        "domain_fetch_counts": dict(domain_fetch_counts),
    }
    run.status = "checkpointed" if remaining else (
        "completed_with_errors" if run.error_count else "completed"
    )
    if not remaining:
        run.finished_at = discovery_now()
        campaign.status = "active"
        campaign.last_completed_at = run.finished_at
    else:
        campaign.status = "checkpointed"
    heartbeat_discovery_run_lease(campaign, run, actor=actor)
    session.add(
        CustodyLogORM(
            object_type="discovery_run",
            object_id=str(run.discovery_run_id),
            action=("discovery_run_checkpointed" if remaining else "discovery_run_completed"),
            actor=actor,
            details_json={
                "campaign_id": campaign.campaign_id,
                "status": run.status,
                "pages_fetched": run.pages_fetched,
                "candidates_discovered": run.candidates_discovered,
                "candidates_updated": run.candidates_updated,
                "errors": run.error_count,
                "remaining_entries": remaining,
            },
        )
    )
    evaluate_run_alerts(session, campaign, run, actor=actor)
    session.commit()


def defer_frontier_entries(
    session: Session,
    run: DiscoveryRunORM,
    *,
    reason: str,
    include_attempted: bool,
) -> int:
    statement = select(DiscoveryFrontierEntryORM).where(
        DiscoveryFrontierEntryORM.discovery_run_id == run.discovery_run_id,
        DiscoveryFrontierEntryORM.state.in_(("queued", "retry_wait")),
    )
    if not include_attempted:
        statement = statement.where(DiscoveryFrontierEntryORM.attempt_count == 0)
    entries = list(session.scalars(statement))
    completed_at = discovery_now()
    for entry in entries:
        entry.state = "deferred"
        entry.completed_at = completed_at
        entry.last_error_text = reason
    if entries:
        session.flush()
    return len(entries)


def reclaim_stale_frontier_claims(
    session: Session,
    campaign: DiscoveryCampaignORM,
    run: DiscoveryRunORM,
    *,
    actor: str,
) -> int:
    """Return abandoned fetch claims to the persisted frontier.

    A worker commits its claim before network I/O.  If that worker exits, the claim
    otherwise remains ``fetching`` forever and a resumed run can incorrectly finish
    with work stranded outside the runnable states.
    """

    policy = effective_campaign_policy(campaign)
    claim_timeout_seconds = discovery_fetch_claim_timeout_seconds(policy)
    now = discovery_now()
    stale_before = now - timedelta(seconds=claim_timeout_seconds)
    stale_entries = list(
        session.scalars(
            select(DiscoveryFrontierEntryORM).where(
                DiscoveryFrontierEntryORM.discovery_run_id == run.discovery_run_id,
                DiscoveryFrontierEntryORM.state == "fetching",
                or_(
                    DiscoveryFrontierEntryORM.claimed_at.is_(None),
                    DiscoveryFrontierEntryORM.claimed_at <= stale_before,
                ),
            )
        )
    )
    for entry in stale_entries:
        exhausted = entry.attempt_count >= entry.max_attempts
        entry.state = "dead_letter" if exhausted else "retry_wait"
        entry.claimed_at = None
        entry.next_attempt_at = None if exhausted else now
        entry.completed_at = now if exhausted else None
        entry.dead_lettered_at = now if exhausted else None
        entry.last_error_text = "stale_fetch_claim_recovered"
        run.error_count += 1
        session.add(
            CustodyLogORM(
                object_type="discovery_frontier_entry",
                object_id=str(entry.frontier_entry_id),
                action=(
                    "discovery_frontier_dead_lettered"
                    if exhausted
                    else "discovery_frontier_claim_recovered"
                ),
                actor=actor,
                details_json={
                    "discovery_run_id": run.discovery_run_id,
                    "attempt_count": entry.attempt_count,
                    "claim_timeout_seconds": claim_timeout_seconds,
                },
            )
        )
    if stale_entries:
        session.commit()
    return len(stale_entries)


def process_frontier_batch(
    session: Session,
    campaign: DiscoveryCampaignORM,
    run: DiscoveryRunORM,
    *,
    actor: str,
    batch_limit: int,
    run_page_budget: int,
    started_entry_count: int,
    domain_fetch_counts: Counter[str],
) -> tuple[int, set[int], int]:
    processed_count = 0
    candidate_ids: set[int] = set()
    in_flight_by_domain: Counter[str] = Counter()
    skipped_entry_ids: set[int] = set()
    tasks: list[FrontierFetchTask] = []
    campaign_policy = effective_campaign_policy(campaign)

    while len(tasks) < batch_limit:
        page_budget_exhausted = started_entry_count >= run_page_budget
        statement = select(DiscoveryFrontierEntryORM).where(
            DiscoveryFrontierEntryORM.discovery_run_id == run.discovery_run_id,
            DiscoveryFrontierEntryORM.state.in_(("queued", "retry_wait")),
            or_(
                DiscoveryFrontierEntryORM.next_attempt_at.is_(None),
                DiscoveryFrontierEntryORM.next_attempt_at <= discovery_now(),
            ),
            *(
                (DiscoveryFrontierEntryORM.attempt_count > 0,)
                if page_budget_exhausted
                else ()
            ),
        )
        if skipped_entry_ids:
            statement = statement.where(
                DiscoveryFrontierEntryORM.frontier_entry_id.not_in(skipped_entry_ids)
            )
        entry = session.scalar(
            statement.order_by(
                DiscoveryFrontierEntryORM.priority.desc(),
                DiscoveryFrontierEntryORM.frontier_entry_id.asc(),
            )
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if entry is None:
            break
        domain = normalize_domain(entry.canonical_url) or "unknown"
        domain_policy = ensure_domain_policy(session, domain, campaign_policy)
        per_domain_limit = min(
            domain_policy.max_pages_per_run,
            int(campaign_policy.get("max_pages_per_domain", 25)),
        )
        if domain_fetch_counts[domain] >= per_domain_limit:
            entry.state = "deferred"
            entry.completed_at = discovery_now()
            entry.last_error_text = "per_domain_page_limit"
            session.commit()
            continue
        if in_flight_by_domain[domain] >= resolve_domain_concurrency_limit(
            domain_policy,
            campaign_policy,
        ):
            skipped_entry_ids.add(entry.frontier_entry_id)
            continue
        heartbeat_discovery_run_lease(campaign, run, actor=actor)
        entry.state = "fetching"
        entry.claimed_at = discovery_now()
        entry.last_attempt_at = entry.claimed_at
        entry.attempt_count += 1
        if entry.attempt_count == 1:
            started_entry_count += 1
        session.commit()
        domain_fetch_counts[domain] += 1
        in_flight_by_domain[domain] += 1
        try:
            task = prepare_frontier_fetch_task(
                session,
                campaign,
                run,
                entry,
                domain_policy=domain_policy,
                actor=actor,
            )
        except Exception as exc:
            session.rollback()
            handle_frontier_error(session, campaign, run, entry, exc, actor=actor)
            processed_count += 1
            in_flight_by_domain[domain] -= 1
            continue
        if task is None:
            processed_count += 1
            in_flight_by_domain[domain] -= 1
            refreshed_entry = session.get(DiscoveryFrontierEntryORM, entry.frontier_entry_id)
            if refreshed_entry is not None and refreshed_entry.candidate_id is not None:
                candidate_ids.add(refreshed_entry.candidate_id)
            continue
        tasks.append(task)

    if tasks:
        with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
            future_map = {
                executor.submit(execute_frontier_fetch_task, task): task
                for task in tasks
            }
            for future, task in future_map.items():
                entry = session.get(DiscoveryFrontierEntryORM, task.entry_id)
                if entry is None:
                    processed_count += 1
                    continue
                try:
                    outcome = future.result()
                    candidate = finalize_frontier_fetch_success(
                        session,
                        campaign,
                        run,
                        entry,
                        fetch_result=outcome.fetch_result,
                        analysis=outcome.analysis,
                        actor=actor,
                    )
                    if candidate is not None:
                        candidate_ids.add(candidate.candidate_id)
                except Exception as exc:
                    session.rollback()
                    handle_frontier_error(session, campaign, run, entry, exc, actor=actor)
                processed_count += 1
    return processed_count, candidate_ids, started_entry_count


def resolve_domain_concurrency_limit(
    domain_policy: DiscoveryDomainPolicyORM,
    campaign_policy: dict[str, Any],
) -> int:
    campaign_limit = max(1, int(campaign_policy.get("max_concurrency", 1)))
    auto_created = bool((domain_policy.metadata_json or {}).get("auto_created"))
    domain_limit = max(1, int(domain_policy.max_concurrency or 1))
    if auto_created and domain_limit == 1 and campaign_limit > 1:
        domain_limit = campaign_limit
    if domain_policy.crawl_delay_seconds > 0:
        return 1
    return max(1, min(domain_limit, campaign_limit))


def prepare_frontier_fetch_task(
    session: Session,
    campaign: DiscoveryCampaignORM,
    run: DiscoveryRunORM,
    entry: DiscoveryFrontierEntryORM,
    *,
    domain_policy: DiscoveryDomainPolicyORM,
    actor: str,
) -> FrontierFetchTask | None:
    policy = effective_campaign_policy(campaign)
    allowed, reason = url_allowed_for_campaign(
        campaign,
        entry.canonical_url,
        domain_policy=domain_policy,
    )
    if not allowed:
        entry.state = "blocked"
        entry.completed_at = discovery_now()
        entry.last_error_text = reason
        session.commit()
        return None
    if entry.depth > min(campaign.max_depth, domain_policy.max_depth):
        entry.state = "blocked"
        entry.completed_at = discovery_now()
        entry.last_error_text = "domain_policy_max_depth"
        session.commit()
        return None
    if urlsplit(entry.canonical_url).scheme not in {"http", "https"}:
        candidate = process_frontier_entry(
            session,
            campaign,
            run,
            entry,
            domain_policy=domain_policy,
            actor=actor,
        )
        return None if candidate is None else None

    fetch_policy = effective_fetch_policy(policy, domain_policy)
    validate_fetch_url(
        entry.canonical_url,
        allow_private_networks=fetch_policy.allow_private_networks,
    )
    robots_allowed, robots_observation = check_robots_permission(
        session,
        campaign,
        run,
        entry.canonical_url,
        domain_policy,
    )
    if not robots_allowed:
        entry.state = "robots_blocked"
        entry.completed_at = discovery_now()
        entry.last_error_text = "robots_disallowed"
        run.error_count += 1
        emit_discovery_alert(
            session,
            alert_type="robots_block",
            dedupe_scope=f"run:{run.discovery_run_id}:domain:{domain_policy.normalized_domain}",
            severity="info",
            message=f"Robots policy blocked discovery fetches on {domain_policy.normalized_domain}.",
            basis={
                "campaign_id": campaign.campaign_id,
                "discovery_run_id": run.discovery_run_id,
                "robots_observation_id": (
                    robots_observation.robots_observation_id if robots_observation else None
                ),
            },
            actor=actor,
        )
        session.commit()
        return None
    apply_domain_pacing(domain_policy.last_fetch_at, domain_policy.crawl_delay_seconds)
    return FrontierFetchTask(
        entry_id=entry.frontier_entry_id,
        canonical_url=entry.canonical_url,
        fetch_policy=fetch_policy,
        allowed_content_types=tuple(
            str(pattern) for pattern in (domain_policy.allowed_content_types_json or [])
        ),
    )


def execute_frontier_fetch_task(task: FrontierFetchTask) -> FrontierFetchOutcome:
    fetch_result = fetch_url(
        task.canonical_url,
        policy=task.fetch_policy,
    )
    if task.allowed_content_types and not any(
        content_type_matches(fetch_result.content_type, pattern)
        for pattern in task.allowed_content_types
    ):
        raise UnsafeTargetError(
            f"Response content type {fetch_result.content_type or 'unknown'} is not allowed by policy."
        )
    analysis = analyze_document(
        fetch_result.final_url,
        fetch_result.payload,
        content_type=fetch_result.content_type,
        headers=fetch_result.headers,
    )
    return FrontierFetchOutcome(
        entry_id=task.entry_id,
        fetch_result=fetch_result,
        analysis=analysis,
    )


def finalize_frontier_fetch_success(
    session: Session,
    campaign: DiscoveryCampaignORM,
    run: DiscoveryRunORM,
    entry: DiscoveryFrontierEntryORM,
    *,
    fetch_result: FetchResult,
    analysis: DocumentAnalysis,
    actor: str,
) -> SourceCandidateORM | None:
    domain = normalize_domain(entry.canonical_url) or "unknown"
    domain_policy = ensure_domain_policy(session, domain, effective_campaign_policy(campaign))
    fetched_at = discovery_now()
    domain_policy.last_fetch_at = fetched_at
    domain_policy.next_allowed_at = fetched_at + timedelta(
        seconds=max(0.0, domain_policy.crawl_delay_seconds)
    )
    candidate, created = upsert_fetched_candidate(
        session,
        campaign,
        run,
        entry,
        fetch_result,
        analysis,
        robots_allowed=True,
    )
    if created:
        run.candidates_discovered += 1
    else:
        run.candidates_updated += 1
    run.pages_fetched += 1
    entry.candidate_id = candidate.candidate_id
    entry.state = "completed"
    entry.fetched_at = fetched_at
    entry.completed_at = fetched_at
    entry.last_error_text = None
    created_artifact_path: Path | None = None
    try:
        _, created_artifact_path = persist_discovery_artifact(
            session,
            campaign,
            run,
            entry,
            candidate,
            fetch_result,
            analysis,
        )
        create_discovery_edge(session, campaign, run, entry, candidate)
        enqueue_analysis_links(session, campaign, run, entry, candidate, analysis)
        maybe_emit_candidate_alert(session, campaign, run, candidate, created=created, actor=actor)
        session.commit()
    except Exception:
        if created_artifact_path is not None:
            created_artifact_path.unlink(missing_ok=True)
        raise
    return candidate


def process_frontier_entry(
    session: Session,
    campaign: DiscoveryCampaignORM,
    run: DiscoveryRunORM,
    entry: DiscoveryFrontierEntryORM,
    *,
    domain_policy: DiscoveryDomainPolicyORM,
    actor: str,
) -> SourceCandidateORM | None:
    policy = effective_campaign_policy(campaign)
    allowed, reason = url_allowed_for_campaign(
        campaign,
        entry.canonical_url,
        domain_policy=domain_policy,
    )
    if not allowed:
        entry.state = "blocked"
        entry.completed_at = discovery_now()
        entry.last_error_text = reason
        session.commit()
        return None
    if entry.depth > min(campaign.max_depth, domain_policy.max_depth):
        entry.state = "blocked"
        entry.completed_at = discovery_now()
        entry.last_error_text = "domain_policy_max_depth"
        session.commit()
        return None

    if urlsplit(entry.canonical_url).scheme in {"ws", "wss"}:
        analysis = analyze_document(
            entry.canonical_url,
            b"",
            content_type="application/octet-stream",
            headers={},
        )
        observed_result = FetchResult(
            requested_url=entry.canonical_url,
            final_url=entry.canonical_url,
            status_code=0,
            headers={},
            payload=b"",
            elapsed_ms=0.0,
            attempt_count=0,
        )
        candidate, created = upsert_fetched_candidate(
            session,
            campaign,
            run,
            entry,
            observed_result,
            analysis,
            robots_allowed=True,
        )
        observed_health = latest_candidate_health(session, candidate.candidate_id)
        if observed_health is not None:
            observed_health.status = "observed_reference"
            observed_health.reachable = False
            observed_health.http_status = None
            observed_health.metadata_json = {"network_fetch_skipped": True}
        candidate.operational_hints_json = {
            **(candidate.operational_hints_json or {}),
            "network_fetch_skipped": True,
            "reference_scheme": urlsplit(entry.canonical_url).scheme,
        }
        run.candidates_discovered += int(created)
        run.candidates_updated += int(not created)
        entry.candidate_id = candidate.candidate_id
        entry.state = "completed"
        entry.completed_at = discovery_now()
        create_discovery_edge(session, campaign, run, entry, candidate)
        session.commit()
        return candidate

    fetch_policy = effective_fetch_policy(policy, domain_policy)
    # Classify an unsafe target as an unsafe candidate before attempting its robots
    # endpoint.  Otherwise a private target is mislabeled as a robots denial and loses
    # the quarantine/dead-letter evidence that explains why it never ran.
    validate_fetch_url(
        entry.canonical_url,
        allow_private_networks=fetch_policy.allow_private_networks,
    )
    robots_allowed, robots_observation = check_robots_permission(
        session,
        campaign,
        run,
        entry.canonical_url,
        domain_policy,
    )
    if not robots_allowed:
        entry.state = "robots_blocked"
        entry.completed_at = discovery_now()
        entry.last_error_text = "robots_disallowed"
        run.error_count += 1
        emit_discovery_alert(
            session,
            alert_type="robots_block",
            dedupe_scope=f"run:{run.discovery_run_id}:domain:{domain_policy.normalized_domain}",
            severity="info",
            message=f"Robots policy blocked discovery fetches on {domain_policy.normalized_domain}.",
            basis={
                "campaign_id": campaign.campaign_id,
                "discovery_run_id": run.discovery_run_id,
                "robots_observation_id": (
                    robots_observation.robots_observation_id if robots_observation else None
                ),
            },
            actor=actor,
        )
        session.commit()
        return None

    apply_domain_pacing(domain_policy.last_fetch_at, domain_policy.crawl_delay_seconds)
    fetch_result = fetch_url(
        entry.canonical_url,
        policy=fetch_policy,
    )
    if domain_policy.allowed_content_types_json and not any(
        content_type_matches(fetch_result.content_type, str(pattern))
        for pattern in domain_policy.allowed_content_types_json
    ):
        raise UnsafeTargetError(
            f"Response content type {fetch_result.content_type or 'unknown'} is not allowed "
            f"by policy for {domain_policy.normalized_domain}."
        )
    fetched_at = discovery_now()
    domain_policy.last_fetch_at = fetched_at
    domain_policy.next_allowed_at = fetched_at + timedelta(
        seconds=max(0.0, domain_policy.crawl_delay_seconds)
    )
    analysis = analyze_document(
        fetch_result.final_url,
        fetch_result.payload,
        content_type=fetch_result.content_type,
        headers=fetch_result.headers,
    )
    candidate, created = upsert_fetched_candidate(
        session,
        campaign,
        run,
        entry,
        fetch_result,
        analysis,
        robots_allowed=robots_allowed,
    )
    if created:
        run.candidates_discovered += 1
    else:
        run.candidates_updated += 1
    run.pages_fetched += 1
    entry.candidate_id = candidate.candidate_id
    entry.state = "completed"
    entry.fetched_at = fetched_at
    entry.completed_at = fetched_at
    entry.last_error_text = None
    created_artifact_path: Path | None = None
    try:
        _, created_artifact_path = persist_discovery_artifact(
            session,
            campaign,
            run,
            entry,
            candidate,
            fetch_result,
            analysis,
        )
        create_discovery_edge(session, campaign, run, entry, candidate)
        enqueue_analysis_links(session, campaign, run, entry, candidate, analysis)
        maybe_emit_candidate_alert(session, campaign, run, candidate, created=created, actor=actor)
        session.commit()
    except Exception:
        # Filesystem writes cannot participate in the SQL transaction.  Remove only a
        # file created by this attempt so a failed flush/commit does not leave an
        # untracked artifact behind.
        if created_artifact_path is not None:
            created_artifact_path.unlink(missing_ok=True)
        raise
    return candidate


def content_type_matches(content_type: str | None, pattern: str) -> bool:
    if content_type is None:
        return False
    normalized = content_type.split(";", 1)[0].strip().lower()
    expected = pattern.split(";", 1)[0].strip().lower()
    if expected.endswith("/*"):
        return normalized.startswith(expected[:-1])
    return normalized == expected


def handle_frontier_error(
    session: Session,
    campaign: DiscoveryCampaignORM,
    run: DiscoveryRunORM,
    entry: DiscoveryFrontierEntryORM,
    exc: Exception,
    *,
    actor: str,
) -> None:
    now = discovery_now()
    run.error_count += 1
    entry.last_error_text = str(exc)[:4000]
    entry.claimed_at = None
    fatal = isinstance(exc, (UnsafeTargetError, ResponseTooLargeError))
    if fatal or entry.attempt_count >= entry.max_attempts:
        entry.state = "dead_letter"
        entry.dead_lettered_at = now
        entry.completed_at = now
    else:
        entry.state = "retry_wait"
        backoff = float(effective_campaign_policy(campaign).get("retry_backoff_seconds", 1.0))
        entry.next_attempt_at = now + timedelta(seconds=max(0.0, backoff * entry.attempt_count))
    if isinstance(exc, UnsafeTargetError):
        existing_candidate = session.scalar(
            select(SourceCandidateORM).where(
                SourceCandidateORM.canonical_url_hash == entry.canonical_url_hash
            )
        )
        record_unfetched_quarantine(session, campaign, run, entry, exc)
        if existing_candidate is None:
            run.candidates_discovered += 1
        else:
            run.candidates_updated += 1
    session.add(
        CustodyLogORM(
            object_type="discovery_frontier_entry",
            object_id=str(entry.frontier_entry_id),
            action=("discovery_frontier_dead_lettered" if entry.state == "dead_letter" else "discovery_frontier_retry_scheduled"),
            actor=actor,
            details_json={
                "discovery_run_id": run.discovery_run_id,
                "url_hash": entry.canonical_url_hash,
                "attempt_count": entry.attempt_count,
                "error_type": type(exc).__name__,
                "error_text": str(exc)[:1000],
            },
        )
    )
    session.commit()


def check_robots_permission(
    session: Session,
    campaign: DiscoveryCampaignORM,
    run: DiscoveryRunORM | None,
    url: str,
    domain_policy: DiscoveryDomainPolicyORM,
) -> tuple[bool, RobotsObservationORM | None]:
    campaign_policy = effective_campaign_policy(campaign)
    if not campaign_policy.get("robots_aware", True) or domain_policy.robots_mode == "ignore":
        return True, None
    now = discovery_now()
    existing = session.scalar(
        select(RobotsObservationORM)
        .where(
            RobotsObservationORM.normalized_domain == domain_policy.normalized_domain,
            or_(
                RobotsObservationORM.expires_at.is_(None),
                RobotsObservationORM.expires_at > now,
            ),
        )
        .order_by(RobotsObservationORM.robots_observation_id.desc())
        .limit(1)
    )
    user_agent = str(campaign_policy.get("user_agent", DEFAULT_CRAWL_POLICY["user_agent"]))
    if existing is not None and isinstance(existing.rules_json, dict):
        lines = existing.rules_json.get("lines")
        if isinstance(lines, list):
            parser = RobotFileParser(existing.robots_url)
            parser.parse([str(line) for line in lines])
            return parser.can_fetch(user_agent, url), existing
        if existing.allowed is not None:
            return bool(existing.allowed), existing

    parsed = urlsplit(url)
    robots_url = urlunsplit((parsed.scheme, parsed.netloc, "/robots.txt", "", ""))
    ttl_seconds = max(300, int(campaign_policy.get("robots_ttl_seconds", 86_400)))
    observation = RobotsObservationORM(
        domain_policy_id=domain_policy.domain_policy_id,
        discovery_run_id=run.discovery_run_id if run is not None else None,
        normalized_domain=domain_policy.normalized_domain,
        robots_url=robots_url,
        fetched_at=now,
        expires_at=now + timedelta(seconds=ttl_seconds),
        status="fetching",
    )
    session.add(observation)
    session.flush()
    try:
        apply_domain_pacing(domain_policy.last_fetch_at, domain_policy.crawl_delay_seconds)
        result = fetch_url(
            robots_url,
            policy=effective_fetch_policy(campaign_policy, domain_policy),
        )
        fetched_at = discovery_now()
        domain_policy.last_fetch_at = fetched_at
        domain_policy.next_allowed_at = fetched_at + timedelta(
            seconds=max(0.0, domain_policy.crawl_delay_seconds)
        )
        text = result.payload.decode("utf-8", errors="replace")
        lines = text.splitlines()[:20_000]
        parser = RobotFileParser(robots_url)
        parser.parse(lines)
        allowed = parser.can_fetch(user_agent, url)
        sitemap_urls = [
            line.split(":", 1)[1].strip()
            for line in lines
            if line.lower().startswith("sitemap:") and ":" in line
        ]
        crawl_delay = parser.crawl_delay(user_agent) or parser.crawl_delay("*")
        observation.status = "available"
        observation.http_status = result.status_code
        observation.allowed = allowed
        normalized_crawl_delay = (
            max(0.0, min(float(crawl_delay), 3600.0))
            if crawl_delay is not None
            else None
        )
        observation.crawl_delay_seconds = normalized_crawl_delay
        observation.sitemap_urls_json = sitemap_urls
        observation.rules_json = {"lines": lines}
        observation.content_hash = hashlib.sha256(result.payload).hexdigest()
        observation.metadata_json = {
            "content_type": result.content_type,
            "byte_count": len(result.payload),
            "attempt_count": result.attempt_count,
        }
        if normalized_crawl_delay is not None:
            domain_policy.crawl_delay_seconds = max(
                domain_policy.crawl_delay_seconds,
                normalized_crawl_delay,
            )
        if run is not None and "sitemap" in campaign_modes(campaign):
            for sitemap_url in sitemap_urls:
                enqueue_frontier(
                    session,
                    campaign,
                    run,
                    sitemap_url,
                    discovery_method="robots_sitemap",
                    depth=0,
                    priority=92.0,
                    metadata={"robots_observation_id": observation.robots_observation_id},
                )
        return allowed, observation
    except DiscoveryFetchError as exc:
        status_code = exc.status_code if isinstance(exc, HTTPStatusFetchError) else None
        # RFC-style unavailable semantics: an ordinary missing-file/client 4xx means
        # there is no usable robots policy.  Authentication denial, throttling,
        # transient server failure, DNS failure, and timeout remain closed until the
        # observation expires.  Unattended discovery must not interpret an outage as
        # permission.
        allowed_on_error = bool(
            status_code is not None
            and 400 <= status_code < 500
            and status_code not in {401, 403, 408, 425, 429}
        )
        observation.status = "missing" if allowed_on_error else "unavailable"
        observation.http_status = status_code
        observation.allowed = allowed_on_error
        observation.error_text = str(exc)[:4000]
        observation.metadata_json = {
            "default_behavior": (
                "allow_on_missing_robots_file"
                if allowed_on_error
                else "deny_on_robots_fetch_error"
            )
        }
        return allowed_on_error, observation


def analysis_value(analysis: DocumentAnalysis, name: str, default: Any) -> Any:
    value = getattr(analysis, name, default)
    return default if value is None else value


def score_candidate_analysis(
    session: Session,
    campaign: DiscoveryCampaignORM,
    analysis: DocumentAnalysis,
    *,
    domain: str,
    trust_level: str,
    is_new: bool,
    canonical_url: str,
    path_pattern: str,
    robots_allowed: bool,
) -> dict[str, Any]:
    same_pattern_count = int(
        session.scalar(
            select(func.count()).select_from(SourceCandidateORM).where(
                SourceCandidateORM.normalized_domain == domain,
                SourceCandidateORM.path_pattern == path_pattern,
            )
        )
        or 0
    )
    if not is_new:
        same_pattern_count = max(0, same_pattern_count - 1)
    managed_source_count = 0
    for source in session.scalars(select(SourceDefinitionORM)):
        try:
            source_canonical = canonicalize_url(source.target_uri)
        except (TypeError, ValueError):
            continue
        if source_canonical == canonical_url:
            managed_source_count += 1
    query_terms = list(campaign_queries(campaign))
    for entity in campaign.entity_seeds_json or []:
        for key in ("name", "canonical_name", "type", "entity_type"):
            value = entity.get(key)
            if value:
                query_terms.append(str(value))
    component_overrides: dict[str, float] = {}
    if campaign.historical_backfill:
        component_overrides["freshness"] = 60.0
        component_overrides["temporal"] = (
            95.0 if analysis.temporal_hints.get("historical") else 70.0
        )
    recency_timestamp = latest_analysis_timestamp(analysis.temporal_hints)
    if campaign.recency_days is not None and recency_timestamp is not None:
        age_days = max(0.0, (discovery_now() - recency_timestamp).total_seconds() / 86_400.0)
        component_overrides["freshness"] = (
            90.0 if age_days <= campaign.recency_days else 20.0
        )
    analysis_for_score: DocumentAnalysis | dict[str, Any] = analysis
    if effective_campaign_policy(campaign).get("allow_private_networks", False):
        analysis_for_score = analysis.as_dict()
        analysis_for_score["operational_hints"] = {
            **analysis.operational_hints,
            "private_network": False,
            "private_network_allowed_by_policy": True,
        }
    trust_value = {"trusted": 90.0, "neutral": 50.0, "blocked": 0.0}.get(
        trust_level, 50.0
    )
    if trust_level != "blocked" and (
        is_official_domain(domain) or analysis.trust_hints.get("official_domain")
    ):
        trust_value = max(trust_value, 82.0)
    result = compute_candidate_score(
        analysis=analysis_for_score,
        target_geo=campaign.target_geography_json or {},
        query_terms=query_terms,
        health={"reachable": True, "robots_allowed": robots_allowed},
        novelty=90.0 if is_new else 55.0,
        redundancy_penalty=min(100.0, same_pattern_count * 12.0 + managed_source_count * 60.0),
        trust=trust_value,
        component_overrides=component_overrides,
        quarantine=trust_level == "blocked",
    )
    return apply_custom_scoring_weights(result, campaign.scoring_weights_json or {})


def latest_analysis_timestamp(temporal_hints: dict[str, Any]) -> datetime | None:
    parsed: list[datetime] = []
    values: list[Any] = [
        temporal_hints.get("updated_at"),
        temporal_hints.get("modified_at"),
        temporal_hints.get("published_at"),
        *(temporal_hints.get("observed_dates") or []),
    ]
    for value in values:
        if not isinstance(value, str) or not value.strip():
            continue
        try:
            timestamp = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            continue
        parsed.append(timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc))
    return max(parsed) if parsed else None


def apply_custom_scoring_weights(
    result: dict[str, Any],
    custom_weights: dict[str, Any],
) -> dict[str, Any]:
    if not custom_weights:
        return result
    components = result.get("components", {})
    base_weights = dict(result.get("weights", {}))
    aliases = {"geospatial_relevance": "geo", "structural_usefulness": "structural"}
    for raw_key, raw_value in custom_weights.items():
        key = aliases.get(str(raw_key), str(raw_key))
        if key not in base_weights:
            continue
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            continue
        base_weights[key] = max(0.0, value)
    weight_sum = sum(base_weights.values())
    if weight_sum <= 0:
        return result
    normalized_weights = {key: value / weight_sum for key, value in base_weights.items()}
    positive = sum(float(components.get(key, 0.0)) * weight for key, weight in normalized_weights.items())
    penalties = result.get("penalties", {})
    total = max(0.0, min(100.0, positive - sum(float(value) for value in penalties.values())))
    bucket = (
        "quarantine"
        if result.get("bucket") == "quarantine"
        else "promote_now"
        if total >= 72.0
        else "keep_candidate"
        if total >= 52.0
        else "revisit_later"
        if total >= 32.0
        else "ignore"
    )
    return {
        **result,
        "total_score": round(total, 2),
        "bucket": bucket,
        "weights": normalized_weights,
        "custom_weights_applied": True,
    }


def status_for_score(
    bucket: str,
    *,
    trust_level: str,
    existing_status: str | None,
) -> str:
    if existing_status == "promoted":
        return "promoted"
    if existing_status == "suppressed":
        return "suppressed"
    if trust_level == "blocked" or bucket == "quarantine":
        return "quarantined"
    if bucket == "ignore":
        return "ignored"
    if bucket == "revisit_later":
        return "deferred"
    return "candidate"


def upsert_fetched_candidate(
    session: Session,
    campaign: DiscoveryCampaignORM,
    run: DiscoveryRunORM,
    entry: DiscoveryFrontierEntryORM,
    fetch_result: FetchResult,
    analysis: DocumentAnalysis,
    *,
    robots_allowed: bool,
) -> tuple[SourceCandidateORM, bool]:
    now = discovery_now()
    canonical = canonicalize_url(fetch_result.final_url)
    url_hash = canonical_url_hash(canonical)
    domain = normalize_domain(canonical) or "unknown"
    path_pattern = normalize_path_pattern(canonical)
    record = session.scalar(
        select(SourceCandidateORM).where(SourceCandidateORM.canonical_url_hash == url_hash)
    )
    if record is not None and record.canonical_url != canonical:
        raise RuntimeError("Canonical URL hash collision detected; candidate was not merged.")
    created = record is None
    trust_level, approval_policy, trust_score = resolve_trust(session, domain)
    content_hash = hashlib.sha256(fetch_result.payload).hexdigest()
    schema_hash = str(analysis_value(analysis, "schema_fingerprint", "")) or None
    score_result = score_candidate_analysis(
        session,
        campaign,
        analysis,
        domain=domain,
        trust_level=trust_level,
        is_new=created,
        canonical_url=canonical,
        path_pattern=path_pattern,
        robots_allowed=robots_allowed,
    )
    observation_score_result = dict(score_result)
    observation_score = float(observation_score_result.get("total_score", 0.0))
    observation_bucket = str(observation_score_result.get("bucket", "keep_candidate"))
    is_search_page = bool((entry.metadata_json or {}).get("is_search_page"))
    candidate_type = (
        "search_results"
        if is_search_page
        else str(analysis_value(analysis, "candidate_type", "unknown"))
    )
    detected_format = (analysis_value(analysis, "format_hints", {}) or {}).get(
        "detected_format", "unknown"
    )
    format_hint = str(analysis_value(analysis, "format_hint", detected_format))
    geo_hints = dict(analysis_value(analysis, "geo_hints", {}) or {})
    temporal_hints = dict(analysis_value(analysis, "temporal_hints", {}) or {})
    format_hints = {
        **dict(analysis_value(analysis, "format_hints", {}) or {}),
        "structural": dict(analysis_value(analysis, "structural_hints", {}) or {}),
    }
    operational_hints = dict(analysis_value(analysis, "operational_hints", {}) or {})
    analysis_trust_hints = dict(analysis_value(analysis, "trust_hints", {}) or {})
    existing_metadata = record.metadata_json if record is not None and isinstance(record.metadata_json, dict) else {}
    score_owner_campaign_id = int(
        existing_metadata.get("best_score_campaign_id")
        or (record.last_campaign_id if record is not None else campaign.campaign_id)
        or campaign.campaign_id
    )
    use_observation_score = (
        record is None
        or score_owner_campaign_id == campaign.campaign_id
        or observation_score >= record.score
    )
    if use_observation_score:
        score = observation_score
        bucket = observation_bucket
        score_owner_campaign_id = campaign.campaign_id
        score_result = {
            **observation_score_result,
            "score_provenance": {
                "campaign_id": campaign.campaign_id,
                "discovery_run_id": run.discovery_run_id,
                "observed_at": now.isoformat(),
            },
        }
    else:
        score = record.score
        bucket = record.score_bucket
        score_result = dict(record.score_breakdown_json or {})
    recommended_kind = "web_search" if is_search_page else recommend_source_kind(analysis)
    promotion = {
        "suitable": recommended_kind is not None and bucket in {"promote_now", "keep_candidate"},
        "recommended_source_kind": recommended_kind,
        "runtime_support": (
            "runnable"
            if recommended_kind in RUNNABLE_SOURCE_KINDS
            else "reference_only"
            if recommended_kind in REFERENCE_ONLY_SOURCE_KINDS
            else "not_recommended"
        ),
        "recommended_schedule_seconds": recommended_schedule_seconds(
            candidate_type,
            format_hint,
            temporal_hints,
            historical_backfill=campaign.historical_backfill,
        ),
        "score_bucket": bucket,
    }
    footprint_kind = str(
        geo_hints.get("footprint_kind")
        or analysis_value(analysis, "footprint_kind", "unknown")
        or "unknown"
    )
    footprint_geojson = geo_hints.get("geometry")
    existing_status = record.status if record is not None else None
    status = status_for_score(bucket, trust_level=trust_level, existing_status=existing_status)
    changed = record is None or record.content_hash != content_hash or record.schema_hash != schema_hash

    if record is None:
        record = SourceCandidateORM(
            canonical_url_hash=url_hash,
            canonical_url=canonical,
            discovered_url=entry.discovered_url,
            normalized_domain=domain,
            path_pattern=path_pattern,
            first_campaign_id=campaign.campaign_id,
            last_campaign_id=campaign.campaign_id,
            first_run_id=run.discovery_run_id,
            last_run_id=run.discovery_run_id,
            parent_url=entry.parent_url,
            discovery_method=entry.discovery_method,
            candidate_type=candidate_type,
            format_hint=format_hint,
            footprint_kind=footprint_kind,
            footprint_geojson=footprint_geojson,
            geo_hints_json=geo_hints,
            temporal_hints_json=temporal_hints,
            format_hints_json=format_hints,
            trust_hints_json={
                **analysis_trust_hints,
                "trust_level": trust_level,
                "approval_policy": approval_policy,
                "trust_score": trust_score,
                "official_domain": is_official_domain(domain),
            },
            operational_hints_json={
                **operational_hints,
                "http_status": fetch_result.status_code,
                "latency_ms": fetch_result.elapsed_ms,
                "robots_allowed": robots_allowed,
            },
            promotion_json=promotion,
            status=status,
            score=score,
            score_bucket=bucket,
            score_breakdown_json=score_result,
            content_hash=content_hash,
            schema_hash=schema_hash,
            first_seen_at=now,
            last_seen_at=now,
            last_changed_at=now,
            last_checked_at=now,
            next_revisit_at=compute_next_revisit_at(promotion["recommended_schedule_seconds"], now),
            metadata_json={
                "title": analysis_value(analysis, "title", ""),
                "text_excerpt": analysis_value(
                    analysis,
                    "text_excerpt",
                    analysis_value(analysis, "text", ""),
                )[:2000],
                "final_url": fetch_result.final_url,
                "best_score_campaign_id": score_owner_campaign_id,
                "best_score_run_id": run.discovery_run_id,
                "best_score_observed_at": now.isoformat(),
            },
        )
        session.add(record)
        session.flush()
    else:
        record.discovered_url = entry.discovered_url
        record.canonical_url = canonical
        record.normalized_domain = domain
        record.path_pattern = path_pattern
        record.last_campaign_id = campaign.campaign_id
        record.last_run_id = run.discovery_run_id
        record.parent_url = entry.parent_url
        record.discovery_method = entry.discovery_method
        record.candidate_type = candidate_type
        record.format_hint = format_hint
        record.footprint_kind = footprint_kind
        record.footprint_geojson = footprint_geojson
        record.geo_hints_json = geo_hints
        record.temporal_hints_json = temporal_hints
        record.format_hints_json = format_hints
        record.trust_hints_json = {
            **analysis_trust_hints,
            "trust_level": trust_level,
            "approval_policy": approval_policy,
            "trust_score": trust_score,
            "official_domain": is_official_domain(domain),
        }
        record.operational_hints_json = {
            **operational_hints,
            "http_status": fetch_result.status_code,
            "latency_ms": fetch_result.elapsed_ms,
            "robots_allowed": robots_allowed,
        }
        record.promotion_json = promotion
        record.status = status
        record.score = score
        record.score_bucket = bucket
        record.score_breakdown_json = score_result
        record.content_hash = content_hash
        record.schema_hash = schema_hash
        record.last_seen_at = now
        record.last_checked_at = now
        record.last_error_text = None
        record.failure_count = 0
        if changed:
            record.last_changed_at = now
        record.next_revisit_at = compute_next_revisit_at(
            promotion["recommended_schedule_seconds"], now
        )
        record.metadata_json = {
            **(record.metadata_json or {}),
            "title": analysis_value(analysis, "title", ""),
            "text_excerpt": analysis_value(
                analysis,
                "text_excerpt",
                analysis_value(analysis, "text", ""),
            )[:2000],
            "final_url": fetch_result.final_url,
            "best_score_campaign_id": score_owner_campaign_id,
            "best_score_run_id": (
                run.discovery_run_id
                if use_observation_score
                else existing_metadata.get("best_score_run_id")
            ),
            "best_score_observed_at": (
                now.isoformat()
                if use_observation_score
                else existing_metadata.get("best_score_observed_at")
            ),
        }
    append_candidate_revision(
        session,
        record,
        campaign_id=campaign.campaign_id,
        run_id=run.discovery_run_id,
        revision_kind="discovered" if created else "rediscovered",
        changed=changed,
        reason=("New canonical candidate." if created else "Candidate observed in another discovery pass."),
        score_override=observation_score,
        score_bucket_override=observation_bucket,
        score_breakdown_override=observation_score_result,
    )
    session.add(
        CandidateHealthCheckORM(
            candidate_id=record.candidate_id,
            discovery_run_id=run.discovery_run_id,
            checked_at=now,
            status="changed" if changed and not created else "healthy",
            reachable=True,
            http_status=fetch_result.status_code,
            latency_ms=fetch_result.elapsed_ms,
            content_type=fetch_result.content_type,
            content_length=len(fetch_result.payload),
            content_hash=content_hash,
            schema_hash=schema_hash,
            changed=changed and not created,
            redirect_url=(
                fetch_result.final_url
                if fetch_result.final_url != fetch_result.requested_url
                else None
            ),
            robots_allowed=robots_allowed,
            metadata_json={
                "attempt_count": fetch_result.attempt_count,
                "response_headers": fetch_result.headers,
            },
        )
    )
    session.flush()
    return record, created


def append_candidate_revision(
    session: Session,
    candidate: SourceCandidateORM,
    *,
    campaign_id: int | None,
    run_id: int | None,
    revision_kind: str,
    changed: bool,
    reason: str,
    score_override: float | None = None,
    score_bucket_override: str | None = None,
    score_breakdown_override: dict[str, Any] | None = None,
) -> SourceCandidateRevisionORM:
    latest_number = int(
        session.scalar(
            select(func.max(SourceCandidateRevisionORM.revision_number)).where(
                SourceCandidateRevisionORM.candidate_id == candidate.candidate_id
            )
        )
        or 0
    )
    snapshot = serialize_candidate_snapshot(candidate)
    revision_score = candidate.score if score_override is None else score_override
    revision_bucket = candidate.score_bucket if score_bucket_override is None else score_bucket_override
    revision_breakdown = (
        candidate.score_breakdown_json or {}
        if score_breakdown_override is None
        else score_breakdown_override
    )
    snapshot.update(
        {
            "score": revision_score,
            "score_bucket": revision_bucket,
            "score_breakdown_json": revision_breakdown,
        }
    )
    revision = SourceCandidateRevisionORM(
        candidate_id=candidate.candidate_id,
        campaign_id=campaign_id,
        discovery_run_id=run_id,
        revision_number=latest_number + 1,
        revision_kind=revision_kind,
        observed_at=discovery_now(),
        status=candidate.status,
        score=revision_score,
        score_bucket=revision_bucket,
        score_breakdown_json=revision_breakdown,
        content_hash=candidate.content_hash,
        schema_hash=candidate.schema_hash,
        changed=changed,
        reason=reason,
        snapshot_json=snapshot,
        metadata_json={
            "trust_snapshot": candidate.trust_hints_json or {},
            "promotion_snapshot": candidate.promotion_json or {},
        },
    )
    session.add(revision)
    session.flush()
    return revision


def serialize_candidate_snapshot(candidate: SourceCandidateORM) -> dict[str, Any]:
    return {
        "candidate_id": candidate.candidate_id,
        "canonical_url": candidate.canonical_url,
        "canonical_url_hash": candidate.canonical_url_hash,
        "normalized_domain": candidate.normalized_domain,
        "path_pattern": candidate.path_pattern,
        "campaign_id": candidate.last_campaign_id,
        "discovery_run_id": candidate.last_run_id,
        "parent_url": candidate.parent_url,
        "discovery_method": candidate.discovery_method,
        "candidate_type": candidate.candidate_type,
        "format_hint": candidate.format_hint,
        "footprint_kind": candidate.footprint_kind,
        "geo_hints_json": candidate.geo_hints_json or {},
        "temporal_hints_json": candidate.temporal_hints_json or {},
        "format_hints_json": candidate.format_hints_json or {},
        "trust_hints_json": candidate.trust_hints_json or {},
        "operational_hints_json": candidate.operational_hints_json or {},
        "promotion_json": candidate.promotion_json or {},
        "status": candidate.status,
        "score": candidate.score,
        "score_bucket": candidate.score_bucket,
        "score_breakdown_json": candidate.score_breakdown_json or {},
        "content_hash": candidate.content_hash,
        "schema_hash": candidate.schema_hash,
        "observed_at": discovery_now().isoformat(),
    }


def recommended_schedule_seconds(
    candidate_type: str,
    format_hint: str,
    temporal_hints: dict[str, Any],
    *,
    historical_backfill: bool,
) -> int:
    if historical_backfill or candidate_type in {"historical_archive", "archive_dataset"}:
        return 7 * 24 * 3600
    cadence = str(temporal_hints.get("cadence") or "unknown")
    if cadence in {"live", "realtime", "continuous"}:
        return 300
    if cadence in {"hourly", "frequent"} or format_hint in {"rss", "atom", "jsonl"}:
        return 1800
    if cadence in {"daily"}:
        return 6 * 3600
    if candidate_type in {"api", "open_data_dataset", "incident_status"}:
        return 3600
    return 24 * 3600


def compute_next_revisit_at(interval_seconds: int, reference: datetime) -> datetime:
    return reference + timedelta(seconds=max(300, int(interval_seconds)))


def is_official_domain(domain: str) -> bool:
    return domain.endswith((".gov", ".gov.uk", ".gc.ca", ".edu")) or any(
        token in domain.split(".") for token in ("state", "county", "city", "agency")
    )


def record_unfetched_quarantine(
    session: Session,
    campaign: DiscoveryCampaignORM,
    run: DiscoveryRunORM,
    entry: DiscoveryFrontierEntryORM,
    exc: Exception,
) -> SourceCandidateORM:
    now = discovery_now()
    record = session.scalar(
        select(SourceCandidateORM).where(
            SourceCandidateORM.canonical_url_hash == entry.canonical_url_hash
        )
    )
    created = record is None
    domain = normalize_domain(entry.canonical_url) or "unknown"
    if record is None:
        record = SourceCandidateORM(
            canonical_url_hash=entry.canonical_url_hash,
            canonical_url=entry.canonical_url,
            discovered_url=entry.discovered_url,
            normalized_domain=domain,
            path_pattern=normalize_path_pattern(entry.canonical_url),
            first_campaign_id=campaign.campaign_id,
            last_campaign_id=campaign.campaign_id,
            first_run_id=run.discovery_run_id,
            last_run_id=run.discovery_run_id,
            parent_url=entry.parent_url,
            discovery_method=entry.discovery_method,
            candidate_type="unsafe_target",
            format_hint="unknown",
            status="quarantined",
            score=0.0,
            score_bucket="quarantine",
            score_breakdown_json={
                "total_score": 0.0,
                "bucket": "quarantine",
                "reasons": [str(exc)],
            },
            first_seen_at=now,
            last_seen_at=now,
            last_checked_at=now,
            failure_count=1,
            last_failure_at=now,
            last_error_text=str(exc)[:4000],
            operational_hints_json={"unsafe_target": True, "error_type": type(exc).__name__},
        )
        session.add(record)
        session.flush()
    else:
        record.last_campaign_id = campaign.campaign_id
        record.last_run_id = run.discovery_run_id
        record.last_seen_at = now
        record.last_checked_at = now
        record.failure_count += 1
        record.last_failure_at = now
        record.last_error_text = str(exc)[:4000]
        if record.status != "promoted":
            record.status = "quarantined"
            record.score_bucket = "quarantine"
    entry.candidate_id = record.candidate_id
    append_candidate_revision(
        session,
        record,
        campaign_id=campaign.campaign_id,
        run_id=run.discovery_run_id,
        revision_kind="security_quarantine",
        changed=created,
        reason=str(exc)[:1000],
    )
    session.add(
        CandidateHealthCheckORM(
            candidate_id=record.candidate_id,
            discovery_run_id=run.discovery_run_id,
            checked_at=now,
            status="quarantined",
            reachable=False,
            robots_allowed=None,
            error_type=type(exc).__name__,
            error_text=str(exc)[:4000],
        )
    )
    return record


def create_discovery_edge(
    session: Session,
    campaign: DiscoveryCampaignORM,
    run: DiscoveryRunORM,
    entry: DiscoveryFrontierEntryORM,
    child: SourceCandidateORM,
) -> DiscoveryGraphEdgeORM:
    raw_key = "|".join(
        [
            str(run.discovery_run_id),
            str(entry.parent_candidate_id or "root"),
            child.canonical_url_hash,
            entry.discovery_method,
        ]
    )
    edge_key = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
    existing = session.scalar(
        select(DiscoveryGraphEdgeORM).where(DiscoveryGraphEdgeORM.edge_key == edge_key)
    )
    if existing is not None:
        return existing
    edge = DiscoveryGraphEdgeORM(
        edge_key=edge_key,
        campaign_id=campaign.campaign_id,
        discovery_run_id=run.discovery_run_id,
        parent_candidate_id=entry.parent_candidate_id,
        child_candidate_id=child.candidate_id,
        parent_url=entry.parent_url,
        child_url=child.canonical_url,
        edge_type="seed" if entry.parent_url is None else "discovered_from",
        discovery_method=entry.discovery_method,
        depth=entry.depth,
        evidence_json={
            "frontier_entry_id": entry.frontier_entry_id,
            "discovered_url": entry.discovered_url,
        },
        metadata_json=entry.metadata_json or {},
    )
    session.add(edge)
    session.flush()
    return edge


def enqueue_analysis_links(
    session: Session,
    campaign: DiscoveryCampaignORM,
    run: DiscoveryRunORM,
    entry: DiscoveryFrontierEntryORM,
    candidate: SourceCandidateORM,
    analysis: DocumentAnalysis,
) -> int:
    if entry.depth >= campaign.max_depth:
        return 0
    modes = set(campaign_modes(campaign))
    policy = effective_campaign_policy(campaign)
    format_targets = set(campaign_format_targets(campaign))
    queued = 0
    for link in analysis_value(analysis, "links", ()):
        link_url = str(getattr(link, "url", "") or "")
        if not link_url:
            continue
        relation = str(getattr(link, "relation", "link") or "link")
        link_source = str(getattr(link, "source", "") or "")
        same_domain = bool(getattr(link, "same_domain", False))
        metadata = dict(getattr(link, "metadata", {}) or {})
        metadata.update(
            {
                "link_source": getattr(link, "source", None),
                "format_hint": getattr(link, "format_hint", None),
                "same_domain": getattr(link, "same_domain", None),
                "nofollow": getattr(link, "nofollow", None),
                "anchor_text": getattr(link, "anchor_text", None),
            }
        )
        parsed = urlsplit(link_url)
        extension = Path(parsed.path).suffix.lower()
        link_format = str(getattr(link, "format_hint", "") or "").lower()
        is_format_target = extension in FORMAT_EXTENSIONS or relation in {
            "feed",
            "api",
            "openapi",
            "sitemap",
            "download",
            "stream",
            "image",
        }
        is_search_result = bool((entry.metadata_json or {}).get("is_search_page"))
        if is_search_result:
            method = "query_result"
            priority = 85.0
            metadata["query_context"] = {
                key: value
                for key, value in (entry.metadata_json or {}).items()
                if key in {"query", "language", "locale", "search_template"}
            }
        elif relation == "sitemap" or (
            candidate.candidate_type == "sitemap"
            and (link_source == "xml_loc" or relation in {"loc", "sitemap"})
        ):
            if "sitemap" not in modes:
                continue
            if not same_domain and not policy.get("allow_cross_domain_links", False):
                continue
            method = "sitemap_expansion"
            priority = 80.0
        elif is_format_target:
            if "format_targeted" not in modes and "neighborhood" not in modes:
                continue
            if format_targets and not {
                link_format,
                extension.lstrip("."),
                relation.lower(),
            }.intersection(format_targets):
                continue
            if not same_domain and not policy.get("allow_cross_domain_links", False):
                continue
            method = "format_targeted"
            priority = 75.0
        elif "neighborhood" in modes or "seed_url" in modes:
            if not same_domain and not policy.get("allow_cross_domain_links", False):
                continue
            method = "neighborhood"
            priority = 50.0
        else:
            continue
        record = enqueue_frontier(
            session,
            campaign,
            run,
            link_url,
            discovery_method=method,
            depth=entry.depth + 1,
            priority=priority,
            parent_url=candidate.canonical_url,
            parent_candidate_id=candidate.candidate_id,
            metadata={"relation": relation, **metadata},
        )
        if record is not None:
            queued += 1
    return queued


def persist_discovery_artifact(
    session: Session,
    campaign: DiscoveryCampaignORM,
    run: DiscoveryRunORM,
    entry: DiscoveryFrontierEntryORM,
    candidate: SourceCandidateORM,
    fetch_result: FetchResult,
    analysis: DocumentAnalysis,
) -> tuple[DiscoveryArtifactORM, Path | None]:
    policy = effective_campaign_policy(campaign)
    object_uri: str | None = None
    storage_object: StorageObjectORM | None = None
    created_artifact_path: Path | None = None
    try:
        if bool(policy.get("store_artifacts", True)):
            artifact_dir = (
                get_settings().data_dir
                / "discovery_artifacts"
                / f"run-{run.discovery_run_id}"
            )
            artifact_dir.mkdir(parents=True, exist_ok=True)
            suffix = artifact_suffix(candidate.format_hint, fetch_result.content_type)
            artifact_path = artifact_dir / (
                f"{candidate.content_hash or entry.canonical_url_hash}{suffix}"
            )
            if not artifact_path.exists():
                created_artifact_path = artifact_path
                artifact_path.write_bytes(fetch_result.payload)
            object_uri = artifact_path.resolve().as_uri()
            object_key = f"discovery:{run.discovery_run_id}:{entry.frontier_entry_id}"
            storage_object = session.scalar(
                select(StorageObjectORM).where(StorageObjectORM.object_key == object_key)
            )
            if storage_object is None:
                storage_object = StorageObjectORM(
                    object_key=object_key,
                    object_kind="discovery_fetched_document",
                    owner_type="discovery_run",
                    owner_id=str(run.discovery_run_id),
                    content_hash=candidate.content_hash,
                    media_type=fetch_result.content_type,
                    storage_tier="hot",
                    retention_class="operational",
                    lifecycle_status="active",
                    source_uri=candidate.canonical_url,
                    object_uri=object_uri,
                    byte_size=len(fetch_result.payload),
                    observed_at=discovery_now(),
                    expires_at=discovery_now() + timedelta(days=30),
                    metadata_json={
                        "campaign_id": campaign.campaign_id,
                        "candidate_id": candidate.candidate_id,
                        "frontier_entry_id": entry.frontier_entry_id,
                    },
                )
                session.add(storage_object)
                session.flush()
        artifact = DiscoveryArtifactORM(
            candidate_id=candidate.candidate_id,
            discovery_run_id=run.discovery_run_id,
            frontier_entry_id=entry.frontier_entry_id,
            storage_object_id=storage_object.storage_object_id if storage_object else None,
            artifact_kind="fetched_document",
            source_url=candidate.canonical_url,
            media_type=fetch_result.content_type,
            content_hash=candidate.content_hash,
            byte_size=len(fetch_result.payload),
            fetched_at=discovery_now(),
            object_uri=object_uri,
            metadata_json={
                "http_status": fetch_result.status_code,
                "response_headers": fetch_result.headers,
                "schema_hash": analysis_value(analysis, "schema_fingerprint", None),
                "attempt_count": fetch_result.attempt_count,
            },
        )
        session.add(artifact)
        session.flush()
        return artifact, created_artifact_path
    except Exception:
        if created_artifact_path is not None:
            created_artifact_path.unlink(missing_ok=True)
        raise


def artifact_suffix(format_hint: str, content_type: str | None) -> str:
    mapping = {
        "json": ".json",
        "jsonl": ".jsonl",
        "geojson": ".geojson",
        "csv": ".csv",
        "rss": ".rss",
        "atom": ".atom",
        "xml": ".xml",
        "sitemap": ".xml",
        "kml": ".kml",
        "pdf": ".pdf",
        "html": ".html",
        "text": ".txt",
    }
    if format_hint in mapping:
        return mapping[format_hint]
    if content_type and "image/" in content_type:
        subtype = content_type.split("/", 1)[1].split(";", 1)[0]
        return f".{re.sub(r'[^a-z0-9]', '', subtype.lower()) or 'img'}"
    return ".bin"


def emit_discovery_alert(
    session: Session,
    *,
    alert_type: str,
    dedupe_scope: str,
    severity: str,
    message: str,
    basis: dict[str, Any],
    actor: str,
) -> AlertORM | None:
    dedupe_key = f"discovery:{alert_type}:{dedupe_scope}"[:160]
    existing = session.scalar(
        select(AlertORM).where(AlertORM.dedupe_key == dedupe_key, AlertORM.status == "open")
    )
    if existing is not None:
        return None
    alert = AlertORM(
        severity=severity,
        status="open",
        dedupe_key=dedupe_key,
        message=message,
        trigger_basis_json={"alert_type": alert_type, **basis},
    )
    session.add(alert)
    session.flush()
    session.add(
        CustodyLogORM(
            object_type="alert",
            object_id=str(alert.alert_id),
            action="alert_created",
            actor=actor,
            details_json={
                "dedupe_key": dedupe_key,
                "alert_type": alert_type,
                **basis,
            },
        )
    )
    return alert


def maybe_emit_candidate_alert(
    session: Session,
    campaign: DiscoveryCampaignORM,
    run: DiscoveryRunORM,
    candidate: SourceCandidateORM,
    *,
    created: bool,
    actor: str,
) -> None:
    if not created or candidate.score_bucket != "promote_now":
        return
    emit_discovery_alert(
        session,
        alert_type="high_value_candidate",
        dedupe_scope=f"candidate:{candidate.candidate_id}",
        severity="info",
        message=(
            f"High-value discovery candidate {candidate.candidate_id} scored "
            f"{candidate.score:.1f}: {candidate.canonical_url}"
        ),
        basis={
            "campaign_id": campaign.campaign_id,
            "discovery_run_id": run.discovery_run_id,
            "candidate_id": candidate.candidate_id,
            "score": candidate.score,
            "score_bucket": candidate.score_bucket,
        },
        actor=actor,
    )


def evaluate_run_alerts(
    session: Session,
    campaign: DiscoveryCampaignORM,
    run: DiscoveryRunORM,
    *,
    actor: str,
) -> None:
    revisions = list(
        session.scalars(
            select(SourceCandidateRevisionORM).where(
                SourceCandidateRevisionORM.discovery_run_id == run.discovery_run_id
            )
        )
    )
    candidates = [
        candidate
        for candidate in (
            session.get(SourceCandidateORM, revision.candidate_id) for revision in revisions
        )
        if candidate is not None
    ]
    if len(candidates) >= 10:
        junk_count = sum(
            1 for candidate in candidates if candidate.score_bucket in {"ignore", "quarantine"}
        )
        if junk_count / len(candidates) >= 0.8:
            emit_discovery_alert(
                session,
                alert_type="campaign_junk_ratio",
                dedupe_scope=f"campaign:{campaign.campaign_id}",
                severity="warning",
                message=f"Discovery campaign {campaign.name} produced mostly ignored or quarantined candidates.",
                basis={
                    "campaign_id": campaign.campaign_id,
                    "discovery_run_id": run.discovery_run_id,
                    "candidate_count": len(candidates),
                    "junk_count": junk_count,
                },
                actor=actor,
            )
        domain_counts = Counter(candidate.normalized_domain for candidate in candidates)
        top_domain, top_count = domain_counts.most_common(1)[0]
        if top_count >= 10 and top_count / len(candidates) >= 0.7:
            emit_discovery_alert(
                session,
                alert_type="single_domain_spike",
                dedupe_scope=f"run:{run.discovery_run_id}:domain:{top_domain}",
                severity="warning",
                message=f"Discovery run {run.discovery_run_id} produced a candidate spike from {top_domain}.",
                basis={
                    "campaign_id": campaign.campaign_id,
                    "discovery_run_id": run.discovery_run_id,
                    "normalized_domain": top_domain,
                    "candidate_count": top_count,
                },
                actor=actor,
            )
    robots_blocks = int(
        session.scalar(
            select(func.count()).select_from(DiscoveryFrontierEntryORM).where(
                DiscoveryFrontierEntryORM.discovery_run_id == run.discovery_run_id,
                DiscoveryFrontierEntryORM.state == "robots_blocked",
            )
        )
        or 0
    )
    if robots_blocks >= 5:
        emit_discovery_alert(
            session,
            alert_type="robots_block_spike",
            dedupe_scope=f"run:{run.discovery_run_id}",
            severity="warning",
            message=f"Discovery run {run.discovery_run_id} hit {robots_blocks} robots blocks.",
            basis={
                "campaign_id": campaign.campaign_id,
                "discovery_run_id": run.discovery_run_id,
                "robots_block_count": robots_blocks,
            },
            actor=actor,
        )


def build_discovery_run_result(session: Session, run: DiscoveryRunORM) -> dict[str, object]:
    state_counts = dict(
        session.execute(
            select(DiscoveryFrontierEntryORM.state, func.count())
            .where(DiscoveryFrontierEntryORM.discovery_run_id == run.discovery_run_id)
            .group_by(DiscoveryFrontierEntryORM.state)
        ).all()
    )
    candidate_ids = list(
        dict.fromkeys(
            session.scalars(
                select(SourceCandidateRevisionORM.candidate_id)
                .where(SourceCandidateRevisionORM.discovery_run_id == run.discovery_run_id)
                .order_by(SourceCandidateRevisionORM.candidate_revision_id.asc())
            )
        )
    )
    return {
        "run": run,
        "frontier_queued_count": int(state_counts.get("queued", 0))
        + int(state_counts.get("retry_wait", 0)),
        "frontier_completed_count": int(state_counts.get("completed", 0)),
        "frontier_dead_letter_count": int(state_counts.get("dead_letter", 0)),
        "candidate_ids": candidate_ids,
    }


def build_discovery_run_detail(session: Session, run_id: int) -> dict[str, object]:
    from src.schemas import (
        DiscoveryCampaignRead,
        DiscoveryFrontierEntryRead,
        DiscoveryRunRead,
        SourceCandidateRead,
        SourceCandidateRevisionRead,
    )

    run = session.get(DiscoveryRunORM, run_id)
    if run is None:
        raise ValueError(f"Discovery run {run_id} does not exist.")
    campaign = session.get(DiscoveryCampaignORM, run.campaign_id)
    frontier = list(
        session.scalars(
            select(DiscoveryFrontierEntryORM)
            .where(DiscoveryFrontierEntryORM.discovery_run_id == run_id)
            .order_by(DiscoveryFrontierEntryORM.frontier_entry_id.asc())
            .limit(1000)
        )
    )
    revisions = list(
        session.scalars(
            select(SourceCandidateRevisionORM)
            .where(SourceCandidateRevisionORM.discovery_run_id == run_id)
            .order_by(SourceCandidateRevisionORM.candidate_revision_id.asc())
            .limit(1000)
        )
    )
    candidate_ids = list(dict.fromkeys(revision.candidate_id for revision in revisions))
    candidates = (
        list(
            session.scalars(
                select(SourceCandidateORM)
                .where(SourceCandidateORM.candidate_id.in_(candidate_ids))
                .order_by(SourceCandidateORM.score.desc())
            )
        )
        if candidate_ids
        else []
    )
    result = build_discovery_run_result(session, run)
    return {
        **result,
        "run": DiscoveryRunRead.model_validate(run).model_dump(mode="json"),
        "campaign": (
            DiscoveryCampaignRead.model_validate(campaign).model_dump(mode="json")
            if campaign is not None
            else None
        ),
        "frontier": [
            DiscoveryFrontierEntryRead.model_validate(row).model_dump(mode="json")
            for row in frontier
        ],
        "revisions": [
            SourceCandidateRevisionRead.model_validate(row).model_dump(mode="json")
            for row in revisions
        ],
        "candidates": [
            SourceCandidateRead.model_validate(row).model_dump(mode="json")
            for row in candidates
        ],
    }


def list_source_candidates(
    session: Session,
    *,
    campaign_id: int | None = None,
    run_id: int | None = None,
    status: str | None = None,
    score_bucket: str | None = None,
    candidate_type: str | None = None,
    domain: str | None = None,
    min_score: float | None = None,
    limit: int = 200,
) -> list[SourceCandidateORM]:
    reconcile_expired_suppressions_for_read(session)
    statement = select(SourceCandidateORM).order_by(
        SourceCandidateORM.score.desc(),
        SourceCandidateORM.updated_at.desc(),
    )
    if campaign_id is not None:
        revision_ids = select(SourceCandidateRevisionORM.candidate_id).where(
            SourceCandidateRevisionORM.campaign_id == campaign_id
        )
        statement = statement.where(SourceCandidateORM.candidate_id.in_(revision_ids))
    if run_id is not None:
        revision_ids = select(SourceCandidateRevisionORM.candidate_id).where(
            SourceCandidateRevisionORM.discovery_run_id == run_id
        )
        statement = statement.where(SourceCandidateORM.candidate_id.in_(revision_ids))
    if status:
        statement = statement.where(SourceCandidateORM.status == status)
    if score_bucket:
        statement = statement.where(SourceCandidateORM.score_bucket == score_bucket)
    if candidate_type:
        statement = statement.where(SourceCandidateORM.candidate_type == candidate_type)
    if domain:
        normalized = normalize_domain(domain)
        if normalized:
            statement = statement.where(SourceCandidateORM.normalized_domain == normalized)
    if min_score is not None:
        statement = statement.where(SourceCandidateORM.score >= min_score)
    return list(session.scalars(statement.limit(max(1, min(limit, 5000)))))


def build_source_candidate_detail(session: Session, candidate_id: int) -> dict[str, object]:
    reconcile_expired_suppressions_for_read(session)
    candidate = session.get(SourceCandidateORM, candidate_id)
    if candidate is None:
        raise ValueError(f"Source candidate {candidate_id} does not exist.")
    domain_labels = candidate.normalized_domain.split(".")
    domain_suffixes = [
        ".".join(domain_labels[index:])
        for index in range(max(0, len(domain_labels) - 1))
    ]
    return {
        "candidate": candidate,
        "revisions": list(
            session.scalars(
                select(SourceCandidateRevisionORM)
                .where(SourceCandidateRevisionORM.candidate_id == candidate_id)
                .order_by(SourceCandidateRevisionORM.candidate_revision_id.desc())
                .limit(100)
            )
        ),
        "incoming_edges": list(
            session.scalars(
                select(DiscoveryGraphEdgeORM)
                .where(DiscoveryGraphEdgeORM.child_candidate_id == candidate_id)
                .order_by(DiscoveryGraphEdgeORM.graph_edge_id.desc())
                .limit(100)
            )
        ),
        "outgoing_edges": list(
            session.scalars(
                select(DiscoveryGraphEdgeORM)
                .where(DiscoveryGraphEdgeORM.parent_candidate_id == candidate_id)
                .order_by(DiscoveryGraphEdgeORM.graph_edge_id.desc())
                .limit(100)
            )
        ),
        "health_checks": list(
            session.scalars(
                select(CandidateHealthCheckORM)
                .where(CandidateHealthCheckORM.candidate_id == candidate_id)
                .order_by(CandidateHealthCheckORM.health_check_id.desc())
                .limit(100)
            )
        ),
        "suppressions": list(
            session.scalars(
                select(CandidateSuppressionORM)
                .where(
                    or_(
                        CandidateSuppressionORM.candidate_id == candidate_id,
                        and_(
                            CandidateSuppressionORM.scope == "domain",
                            CandidateSuppressionORM.normalized_domain.in_(domain_suffixes),
                        ),
                    )
                )
                .order_by(CandidateSuppressionORM.suppression_id.desc())
                .limit(100)
            )
        ),
        "promotion_decisions": list(
            session.scalars(
                select(CandidatePromotionDecisionORM)
                .where(CandidatePromotionDecisionORM.candidate_id == candidate_id)
                .order_by(CandidatePromotionDecisionORM.promotion_decision_id.desc())
                .limit(100)
            )
        ),
        "artifacts": list(
            session.scalars(
                select(DiscoveryArtifactORM)
                .where(DiscoveryArtifactORM.candidate_id == candidate_id)
                .order_by(DiscoveryArtifactORM.discovery_artifact_id.desc())
                .limit(100)
            )
        ),
    }


def explain_candidate_score(session: Session, candidate_id: int) -> dict[str, object]:
    candidate = session.get(SourceCandidateORM, candidate_id)
    if candidate is None:
        raise ValueError(f"Source candidate {candidate_id} does not exist.")
    breakdown = candidate.score_breakdown_json or {}
    reasons = breakdown.get("reasons", [])
    if not isinstance(reasons, list):
        reasons = [str(reasons)]
    evaluated_at = candidate.updated_at
    best_score_observed_at = (candidate.metadata_json or {}).get("best_score_observed_at")
    if isinstance(best_score_observed_at, str):
        try:
            evaluated_at = datetime.fromisoformat(
                best_score_observed_at.replace("Z", "+00:00")
            )
        except ValueError:
            pass
    return {
        "candidate_id": candidate.candidate_id,
        "canonical_url": candidate.canonical_url,
        "score": candidate.score,
        "score_bucket": candidate.score_bucket,
        "score_breakdown_json": breakdown,
        "reasons": [str(reason) for reason in reasons],
        "evaluated_at": evaluated_at,
    }


def build_candidate_lineage(session: Session, candidate_id: int) -> dict[str, object]:
    candidate = session.get(SourceCandidateORM, candidate_id)
    if candidate is None:
        raise ValueError(f"Source candidate {candidate_id} does not exist.")
    incoming = list(
        session.scalars(
            select(DiscoveryGraphEdgeORM)
            .where(DiscoveryGraphEdgeORM.child_candidate_id == candidate_id)
            .order_by(DiscoveryGraphEdgeORM.graph_edge_id.asc())
        )
    )
    outgoing = list(
        session.scalars(
            select(DiscoveryGraphEdgeORM)
            .where(DiscoveryGraphEdgeORM.parent_candidate_id == candidate_id)
            .order_by(DiscoveryGraphEdgeORM.graph_edge_id.asc())
        )
    )
    revisions = list(
        session.scalars(
            select(SourceCandidateRevisionORM).where(
                SourceCandidateRevisionORM.candidate_id == candidate_id
            )
        )
    )
    campaign_ids = sorted(
        {revision.campaign_id for revision in revisions if revision.campaign_id is not None}
    )
    run_ids = sorted(
        {
            revision.discovery_run_id
            for revision in revisions
            if revision.discovery_run_id is not None
        }
    )
    campaigns = (
        list(
            session.scalars(
                select(DiscoveryCampaignORM)
                .where(DiscoveryCampaignORM.campaign_id.in_(campaign_ids))
                .order_by(DiscoveryCampaignORM.campaign_id.asc())
            )
        )
        if campaign_ids
        else []
    )
    runs = (
        list(
            session.scalars(
                select(DiscoveryRunORM)
                .where(DiscoveryRunORM.discovery_run_id.in_(run_ids))
                .order_by(DiscoveryRunORM.discovery_run_id.asc())
            )
        )
        if run_ids
        else []
    )
    return {
        "candidate": candidate,
        "campaigns": campaigns,
        "runs": runs,
        "incoming_edges": incoming,
        "outgoing_edges": outgoing,
        "ancestor_candidate_ids": sorted(
            {
                edge.parent_candidate_id
                for edge in incoming
                if edge.parent_candidate_id is not None
            }
        ),
        "descendant_candidate_ids": sorted({edge.child_candidate_id for edge in outgoing}),
    }


def latest_candidate_health(
    session: Session,
    candidate_id: int,
) -> CandidateHealthCheckORM | None:
    return session.scalar(
        select(CandidateHealthCheckORM)
        .where(CandidateHealthCheckORM.candidate_id == candidate_id)
        .order_by(CandidateHealthCheckORM.health_check_id.desc())
        .limit(1)
    )


def promote_source_candidate(
    session: Session,
    candidate_id: int,
    payload: object,
    *,
    actor: str = "system",
) -> dict[str, object]:
    from src.schemas import ScheduledTaskCreate, SourceDefinitionCreate
    from src.services.scheduler_service import create_scheduled_task
    from src.services.source_service import create_source_definition

    reconcile_expired_suppressions(session, actor=actor)
    candidate = session.get(SourceCandidateORM, candidate_id)
    if candidate is None:
        raise ValueError(f"Source candidate {candidate_id} does not exist.")
    if candidate.status in {"suppressed", "quarantined"}:
        raise ValueError(
            f"Source candidate {candidate_id} is {candidate.status}; clear that disposition before promotion."
        )
    request_data = payload_changes(payload)
    requested_kind = (
        request_data.get("source_kind")
        or request_data.get("recommended_source_kind")
        or (candidate.promotion_json or {}).get("recommended_source_kind")
    )
    if requested_kind is None:
        raise ValueError(f"Source candidate {candidate_id} has no promotable managed-source mapping.")
    source_kind = str(requested_kind)
    if source_kind not in RUNNABLE_SOURCE_KINDS | REFERENCE_ONLY_SOURCE_KINDS:
        raise ValueError(f"Unsupported discovery promotion source kind: {source_kind}")

    source = (
        session.get(SourceDefinitionORM, candidate.promoted_source_id)
        if candidate.promoted_source_id is not None
        else find_source_for_candidate(session, candidate)
    )
    if source is not None and source.source_kind != source_kind:
        raise ValueError(
            f"Source candidate {candidate_id} resolves to existing source {source.source_id} "
            f"with kind {source.source_kind}; refusing conflicting promotion as {source_kind}."
        )
    scheduled_task: ScheduledTaskORM | None = None
    runnable = source_kind in RUNNABLE_SOURCE_KINDS
    requested_enabled = bool(request_data.get("enabled", True))
    enabled = requested_enabled and runnable
    campaign = (
        session.get(DiscoveryCampaignORM, candidate.last_campaign_id)
        if candidate.last_campaign_id is not None
        else None
    )
    layer_key = str(
        request_data.get("layer_key")
        or (campaign.layer_key if campaign is not None else None)
        or "web-discovery"
    )
    interval_seconds = int(
        request_data.get("schedule_interval_seconds")
        or (candidate.promotion_json or {}).get("recommended_schedule_seconds")
        or 86_400
    )
    interval_seconds = max(60, interval_seconds)
    latest_health = latest_candidate_health(session, candidate_id)
    health_risks = {
        "latest_status": latest_health.status if latest_health else "never_checked",
        "reachable": latest_health.reachable if latest_health else False,
        "failure_count": candidate.failure_count,
        "last_error_text": candidate.last_error_text,
    }
    operator_metadata = dict(request_data.get("metadata_json") or {})
    private_runtime_requested = bool(operator_metadata.get("allow_private_networks", False))
    source_metadata = {
        # Promotion metadata is evidence, not an alternate runtime configuration
        # channel.  Keeping it nested prevents a caller from replacing the enforced
        # fetch policy or injecting credential-bearing source headers.
        "operator_metadata": operator_metadata,
        "discovery": {
            "candidate_id": candidate.candidate_id,
            "campaign_id": candidate.last_campaign_id,
            "discovery_run_id": candidate.last_run_id,
            "canonical_url": candidate.canonical_url,
            "candidate_type": candidate.candidate_type,
            "format_hint": candidate.format_hint,
            "score": candidate.score,
            "score_bucket": candidate.score_bucket,
            "score_breakdown": candidate.score_breakdown_json or {},
            "geo_hints": candidate.geo_hints_json or {},
            "trust_hints": candidate.trust_hints_json or {},
            "health_risks": health_risks,
            "recommended_schedule_seconds": interval_seconds,
        },
        "runtime_support": "runnable" if runnable else "reference_only",
        "block_private_networks": True,
        "allow_private_networks": (
            private_runtime_requested
            and bool(get_settings().discovery_allow_private_networks)
        ),
        "max_response_bytes": 20 * 1024 * 1024,
        "request_timeout_seconds": 30.0,
        "retry_attempts": 3,
        "retry_backoff_seconds": 1.0,
        "skip_unchanged": True,
    }
    if source is None:
        source = create_source_definition(
            session,
            SourceDefinitionCreate(
                name=unique_promoted_source_name(
                    session,
                    str(request_data.get("source_name") or build_promoted_source_name(candidate)),
                ),
                source_kind=source_kind,
                layer_key=layer_key,
                target_uri=candidate.canonical_url,
                enabled=enabled,
                integrity_source=bool(request_data.get("integrity_source", False)),
                notes=(
                    str(request_data.get("reason") or "Promoted from discovery candidate.")
                    + (" Reference-only source; batch execution is disabled." if not runnable else "")
                ).strip(),
                metadata_json=source_metadata,
            ),
            actor=actor,
            commit=False,
        )
    else:
        source.metadata_json = {**(source.metadata_json or {}), **source_metadata}
        source.enabled = (
            requested_enabled and runnable
            if "enabled" in request_data
            else source.enabled and runnable
        )
    candidate.promoted_source_id = source.source_id
    candidate.status = "promoted"
    candidate.promotion_json = {
        **(candidate.promotion_json or {}),
        "promoted": True,
        "promoted_source_id": source.source_id,
        "source_kind": source.source_kind,
        "runtime_support": "runnable" if runnable else "reference_only",
        "recommended_schedule_seconds": interval_seconds,
    }
    decision = CandidatePromotionDecisionORM(
        candidate_id=candidate.candidate_id,
        source_id=source.source_id,
        discovery_run_id=candidate.last_run_id,
        decision="promoted" if runnable else "promoted_reference_only",
        recommended_source_kind=source_kind,
        recommended_schedule_json={
            "interval_seconds": interval_seconds,
            "created": False,
            "runnable": runnable,
        },
        score=candidate.score,
        score_bucket=candidate.score_bucket,
        score_breakdown_json=candidate.score_breakdown_json or {},
        reason=str(request_data.get("reason") or "Operator promoted discovery candidate."),
        trust_reasoning_json=candidate.trust_hints_json or {},
        integrity_reasoning_json={
            "integrity_source": source.integrity_source,
            "official_domain": is_official_domain(candidate.normalized_domain),
        },
        health_risks_json=health_risks,
        geo_relevance_json=(candidate.score_breakdown_json or {}).get("components", {}).get(
            "geospatial_relevance", candidate.geo_hints_json or {}
        ),
        evidence_json={
            "candidate_revision_count": count_candidate_revisions(session, candidate_id),
            "latest_health_check_id": latest_health.health_check_id if latest_health else None,
            "canonical_url_hash": candidate.canonical_url_hash,
        },
        actor=actor,
        metadata_json={"requested_enabled": requested_enabled},
    )
    session.add(decision)
    session.flush()
    if bool(request_data.get("create_schedule", True)) and runnable and source.enabled:
        scheduled_task = session.scalar(
            select(ScheduledTaskORM)
            .where(
                ScheduledTaskORM.task_type == "source_sync",
                ScheduledTaskORM.source_id == source.source_id,
            )
            .order_by(ScheduledTaskORM.task_id.asc())
            .limit(1)
        )
        schedule_reused = scheduled_task is not None
        if scheduled_task is None:
            scheduled_task = create_scheduled_task(
                session,
                ScheduledTaskCreate(
                    name=unique_schedule_name(
                        session,
                        f"discovery-source-{candidate.candidate_id}-sync",
                    ),
                    task_type="source_sync",
                    interval_seconds=interval_seconds,
                    retry_attempts=3,
                    retry_backoff_seconds=5.0,
                    source_id=source.source_id,
                    notes=f"Created from discovery promotion decision {decision.promotion_decision_id}.",
                ),
                actor=actor,
                commit=False,
            )
        decision.recommended_schedule_json = {
            **decision.recommended_schedule_json,
            "created": not schedule_reused,
            "task_id": scheduled_task.task_id,
            "reused_existing": schedule_reused,
        }
    camera_source_id = upsert_promoted_camera_candidate(
        session,
        candidate,
        layer_key=layer_key,
        source_kind=source_kind,
        actor=actor,
    )
    if camera_source_id is not None:
        decision.evidence_json = {
            **decision.evidence_json,
            "camera_source_inventory_id": camera_source_id,
        }
    append_candidate_revision(
        session,
        candidate,
        campaign_id=candidate.last_campaign_id,
        run_id=candidate.last_run_id,
        revision_kind="promotion",
        changed=True,
        reason=decision.reason,
    )
    session.add_all(
        [
            CustodyLogORM(
                object_type="source_candidate",
                object_id=str(candidate.candidate_id),
                action="candidate_promoted",
                actor=actor,
                details_json={
                    "source_id": source.source_id,
                    "promotion_decision_id": decision.promotion_decision_id,
                    "source_kind": source.source_kind,
                    "runtime_support": "runnable" if runnable else "reference_only",
                },
            ),
            CustodyLogORM(
                object_type="source_definition",
                object_id=str(source.source_id),
                action="source_promoted_from_discovery",
                actor=actor,
                details_json={
                    "candidate_id": candidate.candidate_id,
                    "promotion_decision_id": decision.promotion_decision_id,
                    "score": candidate.score,
                },
            ),
        ]
    )
    session.commit()
    session.refresh(candidate)
    session.refresh(decision)
    session.refresh(source)
    if scheduled_task is not None:
        session.refresh(scheduled_task)
    return {
        "candidate": candidate,
        "decision": decision,
        "source": source,
        "scheduled_task": scheduled_task,
    }


def find_source_for_candidate(
    session: Session,
    candidate: SourceCandidateORM,
) -> SourceDefinitionORM | None:
    for source in session.scalars(select(SourceDefinitionORM)):
        try:
            if canonicalize_url(source.target_uri) == candidate.canonical_url:
                return source
        except (TypeError, ValueError):
            continue
    return None


def build_promoted_source_name(candidate: SourceCandidateORM) -> str:
    domain = candidate.normalized_domain.replace(".", "-")[:70]
    kind = candidate.candidate_type.replace("_", "-")[:40]
    return f"discovered-{domain}-{kind}-{candidate.candidate_id}"[:160]


def unique_promoted_source_name(session: Session, base: str) -> str:
    candidate = base[:160]
    suffix = 1
    while session.scalar(select(SourceDefinitionORM).where(SourceDefinitionORM.name == candidate)):
        suffix += 1
        trailer = f"-{suffix}"
        candidate = f"{base[:160 - len(trailer)]}{trailer}"
    return candidate


def unique_schedule_name(session: Session, base: str) -> str:
    candidate = base[:160]
    suffix = 1
    while session.scalar(select(ScheduledTaskORM).where(ScheduledTaskORM.name == candidate)):
        suffix += 1
        trailer = f"-{suffix}"
        candidate = f"{base[:160 - len(trailer)]}{trailer}"
    return candidate


def count_candidate_revisions(session: Session, candidate_id: int) -> int:
    return int(
        session.scalar(
            select(func.count()).select_from(SourceCandidateRevisionORM).where(
                SourceCandidateRevisionORM.candidate_id == candidate_id
            )
        )
        or 0
    )


def upsert_promoted_camera_candidate(
    session: Session,
    candidate: SourceCandidateORM,
    *,
    layer_key: str,
    source_kind: str,
    actor: str,
) -> int | None:
    if source_kind not in {"camera_image", "camera_stream"}:
        return None
    candidate_key = f"discovery-{candidate.canonical_url_hash[:32]}"
    record = session.scalar(
        select(CameraSourceInventoryORM).where(
            CameraSourceInventoryORM.candidate_key == candidate_key
        )
    )
    latest_health = latest_candidate_health(session, candidate.candidate_id)
    verification_state = (
        "reachable" if latest_health is not None and latest_health.reachable else "failed"
    )
    endpoint_kind = "image" if source_kind == "camera_image" else "stream"
    if record is None:
        record = CameraSourceInventoryORM(
            candidate_key=candidate_key,
            name=str((candidate.metadata_json or {}).get("title") or candidate.canonical_url)[:200],
            source_domain=candidate.normalized_domain,
            layer_key=layer_key,
            provider=candidate.normalized_domain,
            endpoint_kind=endpoint_kind,
            endpoint_url=candidate.canonical_url,
            status="graduated",
            verification_state=verification_state,
            active=verification_state == "reachable",
            last_observed_at=candidate.last_seen_at,
            last_checked_at=candidate.last_checked_at,
            confidence_score=min(1.0, candidate.score / 100.0),
            graduation_score=min(1.0, candidate.score / 100.0),
            metadata_json={"source_candidate_id": candidate.candidate_id},
        )
        session.add(record)
        session.flush()
    else:
        record.status = "graduated"
        record.verification_state = verification_state
        record.active = verification_state == "reachable"
        record.last_checked_at = candidate.last_checked_at
        record.metadata_json = {
            **(record.metadata_json or {}),
            "source_candidate_id": candidate.candidate_id,
        }
    session.add(
        CustodyLogORM(
            object_type="camera_source_inventory",
            object_id=str(record.camera_source_inventory_id),
            action="camera_source_promoted_from_discovery",
            actor=actor,
            details_json={
                "source_candidate_id": candidate.candidate_id,
                "endpoint_kind": endpoint_kind,
                "verification_state": verification_state,
            },
        )
    )
    return record.camera_source_inventory_id


def suppress_source_candidate(
    session: Session,
    candidate_id: int,
    payload: object,
    *,
    actor: str = "system",
) -> CandidateSuppressionORM:
    reconcile_expired_suppressions(session, actor=actor)
    candidate = session.get(SourceCandidateORM, candidate_id)
    if candidate is None:
        raise ValueError(f"Source candidate {candidate_id} does not exist.")
    data = payload_changes(payload)
    scope = str(data.get("scope") or "candidate")
    if scope not in {"candidate", "domain"}:
        raise ValueError("Candidate suppression scope must be 'candidate' or 'domain'.")
    requested_domain = normalize_domain(data.get("normalized_domain"))
    normalized_domain = (
        requested_domain or candidate.normalized_domain if scope == "domain" else None
    )
    if scope == "domain" and normalized_domain and not domain_matches(
        candidate.normalized_domain,
        normalized_domain,
    ):
        raise ValueError(
            f"Candidate domain {candidate.normalized_domain} is outside suppression domain "
            f"{normalized_domain}."
        )
    suppression = CandidateSuppressionORM(
        candidate_id=candidate.candidate_id,
        normalized_domain=normalized_domain,
        scope=scope,
        status="active",
        reason_code=str(data.get("reason_code") or "operator_suppressed"),
        reason=str(data.get("reason") or ""),
        actor=actor,
        expires_at=data.get("expires_at"),
        metadata_json=dict(data.get("metadata_json") or {}),
    )
    session.add(suppression)
    session.flush()
    affected = [candidate]
    if scope == "domain" and normalized_domain:
        affected = list(
            session.scalars(
                select(SourceCandidateORM).where(
                    or_(
                        SourceCandidateORM.normalized_domain == normalized_domain,
                        SourceCandidateORM.normalized_domain.endswith(
                            f".{normalized_domain}",
                            autoescape=True,
                        ),
                    )
                )
            )
        )
    for row in affected:
        row.status = "suppressed"
        if row.promoted_source_id is not None:
            promoted_source = session.get(SourceDefinitionORM, row.promoted_source_id)
            if promoted_source is not None:
                promoted_source.enabled = False
                session.add(
                    CustodyLogORM(
                        object_type="source_definition",
                        object_id=str(promoted_source.source_id),
                        action="source_disabled_by_candidate_suppression",
                        actor=actor,
                        details_json={
                            "candidate_id": row.candidate_id,
                            "suppression_id": suppression.suppression_id,
                            "scope": scope,
                        },
                    )
                )
        append_candidate_revision(
            session,
            row,
            campaign_id=row.last_campaign_id,
            run_id=row.last_run_id,
            revision_kind="suppression",
            changed=True,
            reason=suppression.reason or suppression.reason_code,
        )
    session.add(
        CustodyLogORM(
            object_type="source_candidate",
            object_id=str(candidate.candidate_id),
            action="candidate_suppressed",
            actor=actor,
            details_json={
                "suppression_id": suppression.suppression_id,
                "scope": scope,
                "normalized_domain": normalized_domain,
                "reason_code": suppression.reason_code,
                "reason": suppression.reason,
                "affected_candidate_ids": [row.candidate_id for row in affected],
            },
        )
    )
    session.commit()
    session.refresh(suppression)
    return suppression


def check_candidate_health(
    session: Session,
    candidate_id: int,
    *,
    actor: str = "discovery_health",
) -> CandidateHealthCheckORM:
    reconcile_expired_suppressions(session, actor=actor)
    candidate = session.get(SourceCandidateORM, candidate_id)
    if candidate is None:
        raise ValueError(f"Source candidate {candidate_id} does not exist.")
    campaign = (
        session.get(DiscoveryCampaignORM, candidate.last_campaign_id)
        if candidate.last_campaign_id is not None
        else None
    )
    if campaign is None:
        raise ValueError(f"Source candidate {candidate_id} has no discovery campaign lineage.")
    campaign_policy = effective_campaign_policy(campaign)
    domain_policy = ensure_domain_policy(session, candidate.normalized_domain, campaign_policy)
    prior_health = latest_candidate_health(session, candidate_id)
    now = discovery_now()
    expected_interval = int(
        (candidate.promotion_json or {}).get("recommended_schedule_seconds") or 86_400
    )
    if (
        candidate.promoted_source_id is not None
        and candidate.last_checked_at is not None
        and normalize_timestamp(candidate.last_checked_at)
        < now - timedelta(seconds=max(300, expected_interval * 2))
    ):
        emit_discovery_alert(
            session,
            alert_type="promoted_source_stale",
            dedupe_scope=f"source:{candidate.promoted_source_id}",
            severity="warning",
            message=f"Promoted discovery source {candidate.promoted_source_id} is stale.",
            basis={
                "candidate_id": candidate.candidate_id,
                "source_id": candidate.promoted_source_id,
                "last_checked_at": candidate.last_checked_at.isoformat(),
                "expected_interval_seconds": expected_interval,
            },
            actor=actor,
        )
    try:
        allowed, block_reason = url_allowed_for_campaign(
            campaign,
            candidate.canonical_url,
            domain_policy=domain_policy,
        )
        if not allowed:
            raise UnsafeTargetError(block_reason or "candidate_blocked_by_policy")
        fetch_policy = effective_fetch_policy(campaign_policy, domain_policy)
        validate_fetch_url(
            candidate.canonical_url,
            allow_private_networks=fetch_policy.allow_private_networks,
        )
        robots_allowed, robots_observation = check_robots_permission(
            session,
            campaign,
            None,
            candidate.canonical_url,
            domain_policy,
        )
        if not robots_allowed:
            candidate.last_checked_at = now
            candidate.last_revisited_at = now
            candidate.revisit_count += 1
            candidate.failure_count += 1
            candidate.last_failure_at = now
            candidate.last_error_text = "robots_disallowed"
            candidate.next_revisit_at = (
                robots_observation.expires_at
                if robots_observation is not None and robots_observation.expires_at is not None
                else now + timedelta(hours=24)
            )
            if candidate.status not in {"promoted", "suppressed"}:
                candidate.status = "robots_blocked"
            check = CandidateHealthCheckORM(
                candidate_id=candidate.candidate_id,
                checked_at=now,
                status="robots_blocked",
                reachable=False,
                error_type="RobotsPolicyDenied",
                error_text="robots_disallowed",
                robots_allowed=False,
                metadata_json={
                    "robots_observation_id": (
                        robots_observation.robots_observation_id
                        if robots_observation is not None
                        else None
                    )
                },
            )
            session.add(check)
            session.flush()
            append_candidate_revision(
                session,
                candidate,
                campaign_id=candidate.last_campaign_id,
                run_id=candidate.last_run_id,
                revision_kind="health_robots_blocked",
                changed=False,
                reason="Robots policy denied the health/revisit fetch.",
            )
            synchronize_camera_health(session, candidate, check, actor=actor)
            emit_discovery_alert(
                session,
                alert_type="robots_block",
                dedupe_scope=f"candidate:{candidate.candidate_id}:health",
                severity="critical" if candidate.promoted_source_id is not None else "warning",
                message=(
                    f"Robots policy blocked health checks for discovery candidate "
                    f"{candidate.candidate_id}."
                ),
                basis={
                    "candidate_id": candidate.candidate_id,
                    "source_id": candidate.promoted_source_id,
                    "robots_observation_id": (
                        robots_observation.robots_observation_id
                        if robots_observation is not None
                        else None
                    ),
                },
                actor=actor,
            )
            session.commit()
            session.refresh(check)
            return check
        apply_domain_pacing(domain_policy.last_fetch_at, domain_policy.crawl_delay_seconds)
        result = fetch_url(
            candidate.canonical_url,
            policy=fetch_policy,
        )
        domain_policy.last_fetch_at = now
        domain_policy.next_allowed_at = now + timedelta(
            seconds=max(0.0, domain_policy.crawl_delay_seconds)
        )
        analysis = analyze_document(
            result.final_url,
            result.payload,
            content_type=result.content_type,
            headers=result.headers,
        )
        content_hash = hashlib.sha256(result.payload).hexdigest()
        schema_hash = analysis.schema_fingerprint
        changed = content_hash != candidate.content_hash
        schema_changed = bool(candidate.schema_hash and schema_hash != candidate.schema_hash)
        trust_level = str((candidate.trust_hints_json or {}).get("trust_level") or "neutral")
        path_pattern = normalize_path_pattern(candidate.canonical_url)
        score_result = score_candidate_analysis(
            session,
            campaign,
            analysis,
            domain=candidate.normalized_domain,
            trust_level=trust_level,
            is_new=False,
            canonical_url=candidate.canonical_url,
            path_pattern=path_pattern,
            robots_allowed=robots_allowed,
        )
        observation_score = float(score_result.get("total_score", candidate.score))
        observation_bucket = str(score_result.get("bucket", candidate.score_bucket))
        candidate_metadata = (
            candidate.metadata_json if isinstance(candidate.metadata_json, dict) else {}
        )
        score_owner_campaign_id = int(
            candidate_metadata.get("best_score_campaign_id")
            or candidate.last_campaign_id
            or campaign.campaign_id
        )
        use_observation_score = (
            score_owner_campaign_id == campaign.campaign_id
            or observation_score >= candidate.score
        )
        previous_status = candidate.status
        candidate.candidate_type = analysis.document_type
        candidate.format_hint = str(analysis.format_hints.get("detected_format") or "unknown")
        candidate.geo_hints_json = analysis.geo_hints
        candidate.temporal_hints_json = analysis.temporal_hints
        candidate.format_hints_json = {
            **analysis.format_hints,
            "structural": analysis.structural_hints,
        }
        candidate.operational_hints_json = {
            **analysis.operational_hints,
            "http_status": result.status_code,
            "latency_ms": result.elapsed_ms,
            "schema_changed": schema_changed,
        }
        candidate.content_hash = content_hash
        candidate.schema_hash = schema_hash
        candidate.last_checked_at = now
        candidate.last_revisited_at = now
        candidate.last_seen_at = now
        candidate.revisit_count += 1
        candidate.failure_count = 0
        candidate.last_error_text = None
        if use_observation_score:
            candidate.score = observation_score
            candidate.score_bucket = observation_bucket
            candidate.score_breakdown_json = {
                **score_result,
                "score_provenance": {
                    "campaign_id": campaign.campaign_id,
                    "discovery_run_id": candidate.last_run_id,
                    "observed_at": now.isoformat(),
                    "observation_kind": "health_revisit",
                },
            }
            candidate.metadata_json = {
                **candidate_metadata,
                "best_score_campaign_id": campaign.campaign_id,
                "best_score_run_id": candidate.last_run_id,
                "best_score_observed_at": now.isoformat(),
            }
        candidate.promotion_json = {
            **(candidate.promotion_json or {}),
            "recommended_source_kind": recommend_source_kind(analysis),
            "score_bucket": candidate.score_bucket,
        }
        if previous_status not in {"promoted", "suppressed"}:
            candidate.status = status_for_score(
                candidate.score_bucket,
                trust_level=trust_level,
                existing_status=previous_status,
            )
        if changed:
            candidate.last_changed_at = now
        interval = int(
            (candidate.promotion_json or {}).get("recommended_schedule_seconds") or 86_400
        )
        candidate.next_revisit_at = compute_next_revisit_at(interval, now)
        check = CandidateHealthCheckORM(
            candidate_id=candidate.candidate_id,
            checked_at=now,
            status="schema_changed" if schema_changed else "changed" if changed else "healthy",
            reachable=True,
            http_status=result.status_code,
            latency_ms=result.elapsed_ms,
            content_type=result.content_type,
            content_length=len(result.payload),
            content_hash=content_hash,
            schema_hash=schema_hash,
            changed=changed,
            redirect_url=(result.final_url if result.final_url != result.requested_url else None),
            robots_allowed=robots_allowed,
            metadata_json={
                "schema_changed": schema_changed,
                "attempt_count": result.attempt_count,
                "response_headers": result.headers,
            },
        )
        session.add(check)
        session.flush()
        append_candidate_revision(
            session,
            candidate,
            campaign_id=candidate.last_campaign_id,
            run_id=candidate.last_run_id,
            revision_kind="health_revisit",
            changed=changed or schema_changed,
            reason=(
                "Endpoint schema changed."
                if schema_changed
                else "Endpoint content changed."
                if changed
                else "Health revisit found no material change."
            ),
            score_override=observation_score,
            score_bucket_override=observation_bucket,
            score_breakdown_override=score_result,
        )
        synchronize_camera_health(session, candidate, check, actor=actor)
        if schema_changed:
            emit_discovery_alert(
                session,
                alert_type="candidate_schema_changed",
                dedupe_scope=f"candidate:{candidate.candidate_id}:schema:{schema_hash}",
                severity="warning",
                message=f"Discovery candidate {candidate.candidate_id} changed schema.",
                basis={
                    "candidate_id": candidate.candidate_id,
                    "source_id": candidate.promoted_source_id,
                    "schema_hash": schema_hash,
                },
                actor=actor,
            )
        if result.final_url != result.requested_url:
            emit_discovery_alert(
                session,
                alert_type="candidate_endpoint_changed",
                dedupe_scope=(
                    f"candidate:{candidate.candidate_id}:redirect:"
                    f"{canonical_url_hash(result.final_url)[:16]}"
                ),
                severity=(
                    "warning"
                    if candidate.promoted_source_id is not None
                    or (candidate.trust_hints_json or {}).get("trust_level") == "trusted"
                    else "info"
                ),
                message=f"Discovery candidate {candidate.candidate_id} redirected to a new endpoint.",
                basis={
                    "candidate_id": candidate.candidate_id,
                    "source_id": candidate.promoted_source_id,
                    "previous_url": result.requested_url,
                    "redirect_url": result.final_url,
                },
                actor=actor,
            )
        if prior_health is not None and not prior_health.reachable:
            emit_discovery_alert(
                session,
                alert_type="candidate_reachable",
                dedupe_scope=f"candidate:{candidate.candidate_id}:reachable",
                severity="info",
                message=f"Discovery candidate {candidate.candidate_id} became reachable again.",
                basis={"candidate_id": candidate.candidate_id},
                actor=actor,
            )
        session.commit()
        session.refresh(check)
        return check
    except Exception as exc:
        session.rollback()
        candidate = session.get(SourceCandidateORM, candidate_id)
        assert candidate is not None
        prior_health = latest_candidate_health(session, candidate_id)
        candidate.last_checked_at = now
        candidate.last_revisited_at = now
        candidate.revisit_count += 1
        candidate.failure_count += 1
        candidate.last_failure_at = now
        candidate.last_error_text = str(exc)[:4000]
        backoff_hours = min(7 * 24, max(1, 2 ** min(candidate.failure_count, 8)))
        candidate.next_revisit_at = now + timedelta(hours=backoff_hours)
        if candidate.status not in {"promoted", "suppressed"}:
            candidate.status = "failing"
        check = CandidateHealthCheckORM(
            candidate_id=candidate.candidate_id,
            checked_at=now,
            status="quarantined" if isinstance(exc, UnsafeTargetError) else "unreachable",
            reachable=False,
            error_type=type(exc).__name__,
            error_text=str(exc)[:4000],
            robots_allowed=None,
            metadata_json={"failure_count": candidate.failure_count},
        )
        session.add(check)
        session.flush()
        append_candidate_revision(
            session,
            candidate,
            campaign_id=candidate.last_campaign_id,
            run_id=candidate.last_run_id,
            revision_kind="health_failure",
            changed=False,
            reason=str(exc)[:1000],
        )
        synchronize_camera_health(session, candidate, check, actor=actor)
        if prior_health is None or prior_health.reachable:
            emit_discovery_alert(
                session,
                alert_type="candidate_unreachable",
                dedupe_scope=f"candidate:{candidate.candidate_id}:unreachable",
                severity="warning" if candidate.promoted_source_id is None else "critical",
                message=f"Discovery candidate {candidate.candidate_id} became unreachable.",
                basis={
                    "candidate_id": candidate.candidate_id,
                    "source_id": candidate.promoted_source_id,
                    "official_domain": is_official_domain(candidate.normalized_domain),
                    "error_type": type(exc).__name__,
                },
                actor=actor,
            )
        if candidate.promoted_source_id is not None:
            emit_discovery_alert(
                session,
                alert_type="promoted_source_failing",
                dedupe_scope=f"source:{candidate.promoted_source_id}",
                severity="critical",
                message=f"Promoted discovery source {candidate.promoted_source_id} is failing health checks.",
                basis={
                    "candidate_id": candidate.candidate_id,
                    "source_id": candidate.promoted_source_id,
                    "failure_count": candidate.failure_count,
                },
                actor=actor,
            )
        if is_official_domain(candidate.normalized_domain):
            fallback = session.scalar(
                select(SourceCandidateORM)
                .where(
                    SourceCandidateORM.candidate_id != candidate.candidate_id,
                    SourceCandidateORM.candidate_type == candidate.candidate_type,
                    SourceCandidateORM.normalized_domain != candidate.normalized_domain,
                    SourceCandidateORM.status.not_in(
                        ("suppressed", "quarantined", "ignored", "failing")
                    ),
                    SourceCandidateORM.failure_count == 0,
                )
                .order_by(SourceCandidateORM.score.desc())
                .limit(1)
            )
            if fallback is not None:
                emit_discovery_alert(
                    session,
                    alert_type="official_source_fallback_available",
                    dedupe_scope=f"candidate:{candidate.candidate_id}:fallback:{fallback.candidate_id}",
                    severity="critical",
                    message=(
                        f"Official source candidate {candidate.candidate_id} disappeared; "
                        f"fallback candidate {fallback.candidate_id} is available for review."
                    ),
                    basis={
                        "candidate_id": candidate.candidate_id,
                        "source_id": candidate.promoted_source_id,
                        "fallback_candidate_id": fallback.candidate_id,
                        "fallback_score": fallback.score,
                    },
                    actor=actor,
                )
        session.commit()
        session.refresh(check)
        return check


def synchronize_camera_health(
    session: Session,
    candidate: SourceCandidateORM,
    check: CandidateHealthCheckORM,
    *,
    actor: str,
) -> None:
    camera_sources = list(
        session.scalars(
            select(CameraSourceInventoryORM).where(
                CameraSourceInventoryORM.endpoint_url == candidate.canonical_url
            )
        )
    )
    for source in camera_sources:
        previous = source.verification_state
        source.verification_state = "reachable" if check.reachable else "failed"
        source.active = check.reachable
        source.last_checked_at = check.checked_at
        if previous != source.verification_state:
            session.add(
                CustodyLogORM(
                    object_type="camera_source_inventory",
                    object_id=str(source.camera_source_inventory_id),
                    action="camera_source_health_changed",
                    actor=actor,
                    details_json={
                        "source_candidate_id": candidate.candidate_id,
                        "previous_verification_state": previous,
                        "verification_state": source.verification_state,
                        "health_check_id": check.health_check_id,
                    },
                )
            )


def scan_candidate_health(
    session: Session,
    payload: object,
    *,
    actor: str = "discovery_health_scan",
) -> dict[str, object]:
    candidate_id = payload_value(payload, "candidate_id")
    domain = normalize_domain(payload_value(payload, "normalized_domain"))
    campaign_id = payload_value(payload, "campaign_id")
    limit = max(1, min(int(payload_value(payload, "limit", 100)), 5000))
    statement = select(SourceCandidateORM).order_by(
        SourceCandidateORM.next_revisit_at.asc().nullsfirst(),
        SourceCandidateORM.candidate_id.asc(),
    )
    if candidate_id is not None:
        statement = statement.where(SourceCandidateORM.candidate_id == int(candidate_id))
    if domain:
        statement = statement.where(SourceCandidateORM.normalized_domain == domain)
    if campaign_id is not None:
        statement = statement.where(
            or_(
                SourceCandidateORM.first_campaign_id == int(campaign_id),
                SourceCandidateORM.last_campaign_id == int(campaign_id),
            )
        )
    candidates = list(session.scalars(statement.limit(limit)))
    checks = [
        check_candidate_health(session, candidate.candidate_id, actor=actor)
        for candidate in candidates
    ]
    return {
        "checked_at": discovery_now(),
        "checked_count": len(checks),
        "reachable_count": sum(1 for check in checks if check.reachable),
        "failing_count": sum(1 for check in checks if not check.reachable),
        "changed_count": sum(1 for check in checks if check.changed),
        "checks": checks,
    }


def revisit_discovery(
    session: Session,
    payload: object,
    *,
    candidate_id: int | None = None,
    actor: str = "discovery_revisit",
) -> dict[str, object]:
    requested_at = discovery_now()
    reconcile_expired_suppressions(session, now=requested_at, actor=actor)
    selected_candidate_id = candidate_id or payload_value(payload, "candidate_id")
    normalized_domain = normalize_domain(payload_value(payload, "normalized_domain"))
    campaign_id = payload_value(payload, "campaign_id")
    force = bool(payload_value(payload, "force", False))
    include_suppressed = bool(payload_value(payload, "include_suppressed", False))
    priority = float(payload_value(payload, "priority", 90.0) or 90.0)
    statement = select(SourceCandidateORM).order_by(SourceCandidateORM.candidate_id.asc())
    if selected_candidate_id is not None:
        statement = statement.where(SourceCandidateORM.candidate_id == int(selected_candidate_id))
    if normalized_domain:
        statement = statement.where(SourceCandidateORM.normalized_domain == normalized_domain)
    if campaign_id is not None:
        statement = statement.where(
            or_(
                SourceCandidateORM.first_campaign_id == int(campaign_id),
                SourceCandidateORM.last_campaign_id == int(campaign_id),
            )
        )
    candidates = list(session.scalars(statement.limit(5000)))
    if selected_candidate_id is not None and not candidates:
        raise ValueError(f"Source candidate {selected_candidate_id} does not exist.")
    runs_by_campaign: dict[int, DiscoveryRunORM] = {}
    queued_candidate_ids: list[int] = []
    frontier_ids: list[int] = []
    skipped_ids: list[int] = []
    for candidate in candidates:
        if candidate.status == "suppressed" and not include_suppressed:
            skipped_ids.append(candidate.candidate_id)
            continue
        if (
            not force
            and candidate.next_revisit_at is not None
            and normalize_timestamp(candidate.next_revisit_at) > requested_at
        ):
            skipped_ids.append(candidate.candidate_id)
            continue
        selected_campaign_id = int(campaign_id or candidate.last_campaign_id or 0)
        campaign = session.get(DiscoveryCampaignORM, selected_campaign_id)
        if campaign is None:
            skipped_ids.append(candidate.candidate_id)
            continue
        run = runs_by_campaign.get(selected_campaign_id)
        if run is None:
            run = session.scalar(
                select(DiscoveryRunORM)
                .where(
                    DiscoveryRunORM.campaign_id == selected_campaign_id,
                    DiscoveryRunORM.status.in_(("checkpointed", "paused")),
                )
                .order_by(DiscoveryRunORM.discovery_run_id.desc())
                .limit(1)
            )
        if run is None:
            run = DiscoveryRunORM(
                campaign_id=selected_campaign_id,
                mode="revisit",
                status="checkpointed",
                trigger_kind="revisit",
                actor=actor,
                request_snapshot_json={"revisit": True},
                policy_snapshot_json=effective_campaign_policy(campaign),
            )
            session.add(run)
            session.flush()
        runs_by_campaign[selected_campaign_id] = run
        entry = enqueue_frontier(
            session,
            campaign,
            run,
            candidate.canonical_url,
            discovery_method="revisit",
            depth=0,
            priority=priority,
            parent_url=candidate.canonical_url,
            parent_candidate_id=candidate.candidate_id,
            metadata={"revisit_candidate_id": candidate.candidate_id},
        )
        if entry is None:
            skipped_ids.append(candidate.candidate_id)
            continue
        candidate.next_revisit_at = requested_at
        queued_candidate_ids.append(candidate.candidate_id)
        frontier_ids.append(entry.frontier_entry_id)
    session.add(
        CustodyLogORM(
            object_type="discovery_revisit",
            object_id=requested_at.isoformat(),
            action="discovery_revisit_queued",
            actor=actor,
            details_json={
                "candidate_ids": queued_candidate_ids,
                "frontier_entry_ids": frontier_ids,
                "skipped_candidate_ids": skipped_ids,
            },
        )
    )
    session.commit()
    return {
        "requested_at": requested_at,
        "queued_count": len(frontier_ids),
        "candidate_ids": queued_candidate_ids,
        "frontier_entry_ids": frontier_ids,
        "skipped_candidate_ids": skipped_ids,
    }


def latest_health_by_candidate(session: Session) -> dict[int, CandidateHealthCheckORM]:
    checks = list(
        session.scalars(
            select(CandidateHealthCheckORM).order_by(
                CandidateHealthCheckORM.candidate_id.asc(),
                CandidateHealthCheckORM.health_check_id.desc(),
            )
        )
    )
    latest: dict[int, CandidateHealthCheckORM] = {}
    for check in checks:
        latest.setdefault(check.candidate_id, check)
    return latest


def count_by_value(values: Iterable[Any]) -> dict[str, int]:
    counts = Counter(str(value or "unknown") for value in values)
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def build_candidate_inventory_summary(
    session: Session,
    *,
    stale_after_hours: float = 24.0,
) -> dict[str, object]:
    now = discovery_now()
    stale_before = now - timedelta(hours=max(0.0, stale_after_hours))
    candidates = list(
        session.scalars(
            select(SourceCandidateORM).order_by(SourceCandidateORM.candidate_id.asc())
        )
    )
    latest_health = latest_health_by_candidate(session)
    return {
        "generated_at": now,
        "total_count": len(candidates),
        "active_count": sum(
            1
            for candidate in candidates
            if candidate.status not in {"suppressed", "ignored", "quarantined", "retired"}
        ),
        "promoted_count": sum(1 for candidate in candidates if candidate.status == "promoted"),
        "suppressed_count": sum(
            1 for candidate in candidates if candidate.status == "suppressed"
        ),
        "failing_count": sum(
            1
            for candidate in candidates
            if candidate.failure_count > 0
            or (
                candidate.candidate_id in latest_health
                and not latest_health[candidate.candidate_id].reachable
                and latest_health[candidate.candidate_id].status != "observed_reference"
            )
        ),
        "stale_count": sum(
            1
            for candidate in candidates
            if normalize_timestamp(candidate.last_checked_at) is None
            or normalize_timestamp(candidate.last_checked_at) < stale_before
        ),
        "due_revisit_count": sum(
            1
            for candidate in candidates
            if candidate.next_revisit_at is not None
            and normalize_timestamp(candidate.next_revisit_at) <= now
        ),
        "status_counts": count_by_value(candidate.status for candidate in candidates),
        "score_bucket_counts": count_by_value(
            candidate.score_bucket for candidate in candidates
        ),
        "type_counts": count_by_value(candidate.candidate_type for candidate in candidates),
        "format_counts": count_by_value(candidate.format_hint for candidate in candidates),
        "domain_counts": count_by_value(candidate.normalized_domain for candidate in candidates),
    }


def build_discovery_health_summary(
    session: Session,
    *,
    stale_after_hours: float = 24.0,
) -> dict[str, object]:
    reconcile_expired_suppressions_for_read(session)
    now = discovery_now()
    stale_before = now - timedelta(hours=max(0.0, stale_after_hours))
    campaigns = list(session.scalars(select(DiscoveryCampaignORM)))
    runs = list(session.scalars(select(DiscoveryRunORM)))
    candidates = list(session.scalars(select(SourceCandidateORM)))
    latest_health = latest_health_by_candidate(session)
    reachable_count = sum(1 for check in latest_health.values() if check.reachable)
    failing_candidates = [
        candidate
        for candidate in candidates
        if candidate.failure_count > 0
        or (
            candidate.candidate_id in latest_health
            and not latest_health[candidate.candidate_id].reachable
            and latest_health[candidate.candidate_id].status != "observed_reference"
        )
    ]
    stale_candidates = [
        candidate
        for candidate in candidates
        if normalize_timestamp(candidate.last_checked_at) is None
        or normalize_timestamp(candidate.last_checked_at) < stale_before
    ]
    due_revisit_count = sum(
        1
        for candidate in candidates
        if candidate.next_revisit_at is not None
        and normalize_timestamp(candidate.next_revisit_at) <= now
    )
    queued_count = int(
        session.scalar(
            select(func.count()).select_from(DiscoveryFrontierEntryORM).where(
                DiscoveryFrontierEntryORM.state.in_(("queued", "retry_wait", "deferred"))
            )
        )
        or 0
    )
    dead_letter_count = int(
        session.scalar(
            select(func.count()).select_from(DiscoveryFrontierEntryORM).where(
                DiscoveryFrontierEntryORM.state == "dead_letter"
            )
        )
        or 0
    )
    robots_block_count = int(
        session.scalar(
            select(func.count()).select_from(DiscoveryFrontierEntryORM).where(
                DiscoveryFrontierEntryORM.state == "robots_blocked"
            )
        )
        or 0
    )
    warnings: list[str] = []
    if failing_candidates:
        warnings.append(f"{len(failing_candidates)} candidates have failing health state.")
    if dead_letter_count:
        warnings.append(f"{dead_letter_count} frontier entries are dead-lettered.")
    stale_running = [
        run
        for run in runs
        if run.status == "running"
        and normalize_timestamp(run.updated_at) < now - timedelta(hours=1)
    ]
    if stale_running:
        warnings.append(f"{len(stale_running)} discovery runs appear stuck in running state.")
    status = "degraded" if warnings else "ok"
    return {
        "generated_at": now,
        "status": status,
        "campaign_count": len(campaigns),
        "active_campaign_count": sum(
            1 for campaign in campaigns if campaign.enabled and campaign.status != "disabled"
        ),
        "running_run_count": sum(1 for run in runs if run.status == "running"),
        "candidate_count": len(candidates),
        "reachable_candidate_count": reachable_count,
        "failing_candidate_count": len(failing_candidates),
        "stale_candidate_count": len(stale_candidates),
        "due_revisit_count": due_revisit_count,
        "queued_frontier_count": queued_count,
        "dead_letter_frontier_count": dead_letter_count,
        "robots_block_count": robots_block_count,
        "warning_count": len(warnings),
        "warnings": warnings,
    }


def build_discovery_ops_summary(
    session: Session,
    *,
    stale_after_hours: float = 24.0,
    limit: int = 25,
) -> dict[str, object]:
    now = discovery_now()
    stale_before = now - timedelta(hours=max(0.0, stale_after_hours))
    candidates = list(
        session.scalars(
            select(SourceCandidateORM).order_by(
                SourceCandidateORM.score.desc(),
                SourceCandidateORM.candidate_id.asc(),
            )
        )
    )
    latest_health = latest_health_by_candidate(session)
    failing = [
        candidate
        for candidate in candidates
        if candidate.failure_count > 0
        or (
            candidate.candidate_id in latest_health
            and not latest_health[candidate.candidate_id].reachable
            and latest_health[candidate.candidate_id].status != "observed_reference"
        )
    ]
    stale = [
        candidate
        for candidate in candidates
        if normalize_timestamp(candidate.last_checked_at) is None
        or normalize_timestamp(candidate.last_checked_at) < stale_before
    ]
    due = [
        candidate
        for candidate in candidates
        if candidate.next_revisit_at is not None
        and normalize_timestamp(candidate.next_revisit_at) <= now
    ]
    campaigns = list(session.scalars(select(DiscoveryCampaignORM)))
    runs = list(
        session.scalars(
            select(DiscoveryRunORM).order_by(DiscoveryRunORM.discovery_run_id.desc())
        )
    )
    frontier_states = dict(
        session.execute(
            select(DiscoveryFrontierEntryORM.state, func.count()).group_by(
                DiscoveryFrontierEntryORM.state
            )
        ).all()
    )
    return {
        "generated_at": now,
        "health_summary": build_discovery_health_summary(
            session, stale_after_hours=stale_after_hours
        ),
        "inventory_summary": build_candidate_inventory_summary(
            session, stale_after_hours=stale_after_hours
        ),
        "campaign_status_counts": count_by_value(campaign.status for campaign in campaigns),
        "run_status_counts": count_by_value(run.status for run in runs),
        "frontier_state_counts": {
            str(key): int(value) for key, value in frontier_states.items()
        },
        "candidate_status_counts": count_by_value(candidate.status for candidate in candidates),
        "score_bucket_counts": count_by_value(
            candidate.score_bucket for candidate in candidates
        ),
        "domain_candidate_counts": count_by_value(
            candidate.normalized_domain for candidate in candidates
        ),
        "failing_candidates": failing[:limit],
        "stale_candidates": stale[:limit],
        "due_revisit_candidates": due[:limit],
        "recent_runs": runs[:limit],
    }


def build_discovery_export_summary(
    session: Session,
    *,
    stale_after_hours: float = 24.0,
    candidate_limit: int = 500,
    report_limit: int = 25,
) -> dict[str, object]:
    generated_at = discovery_now()
    return {
        "generated_at": generated_at,
        "filters_json": {
            "stale_after_hours": stale_after_hours,
            "candidate_limit": candidate_limit,
            "report_limit": report_limit,
        },
        "ops_summary": build_discovery_ops_summary(
            session,
            stale_after_hours=stale_after_hours,
            limit=report_limit,
        ),
        "campaigns": list_discovery_campaigns(session, limit=5000),
        "runs": list_discovery_runs(session, limit=report_limit),
        "candidates": list_source_candidates(session, limit=candidate_limit),
        "promotion_decisions": list(
            session.scalars(
                select(CandidatePromotionDecisionORM)
                .order_by(CandidatePromotionDecisionORM.promotion_decision_id.desc())
                .limit(candidate_limit)
            )
        ),
    }


def revisions_for_run(
    session: Session,
    run_id: int,
    *,
    campaign_id: int | None = None,
) -> dict[int, SourceCandidateRevisionORM]:
    statement = (
        select(SourceCandidateRevisionORM)
        .where(SourceCandidateRevisionORM.discovery_run_id == run_id)
        .order_by(
            SourceCandidateRevisionORM.candidate_id.asc(),
            SourceCandidateRevisionORM.candidate_revision_id.desc(),
        )
    )
    if campaign_id is not None:
        statement = statement.where(SourceCandidateRevisionORM.campaign_id == campaign_id)
    latest: dict[int, SourceCandidateRevisionORM] = {}
    for revision in session.scalars(statement):
        latest.setdefault(revision.candidate_id, revision)
    return latest


def diff_discovery_inventories(
    session: Session,
    *,
    from_run_id: int,
    to_run_id: int,
    campaign_id: int | None = None,
) -> dict[str, object]:
    from_run = session.get(DiscoveryRunORM, from_run_id)
    to_run = session.get(DiscoveryRunORM, to_run_id)
    if from_run is None:
        raise ValueError(f"Discovery run {from_run_id} does not exist.")
    if to_run is None:
        raise ValueError(f"Discovery run {to_run_id} does not exist.")
    if campaign_id is not None and (
        from_run.campaign_id != campaign_id or to_run.campaign_id != campaign_id
    ):
        raise ValueError("Discovery inventory diff runs do not both belong to the campaign.")
    before = revisions_for_run(session, from_run_id, campaign_id=campaign_id)
    after = revisions_for_run(session, to_run_id, campaign_id=campaign_id)
    before_ids = set(before)
    after_ids = set(after)
    added_ids = sorted(after_ids - before_ids)
    disappeared_ids = sorted(before_ids - after_ids)
    changed_ids = sorted(
        candidate_id
        for candidate_id in before_ids.intersection(after_ids)
        if (
            before[candidate_id].content_hash != after[candidate_id].content_hash
            or before[candidate_id].schema_hash != after[candidate_id].schema_hash
            or before[candidate_id].score_bucket != after[candidate_id].score_bucket
            or abs(before[candidate_id].score - after[candidate_id].score) >= 0.01
        )
    )
    promoted_ids = sorted(
        candidate_id
        for candidate_id, revision in after.items()
        if revision.status == "promoted"
        and (candidate_id not in before or before[candidate_id].status != "promoted")
    )
    suppressed_ids = sorted(
        candidate_id
        for candidate_id, revision in after.items()
        if revision.status == "suppressed"
        and (candidate_id not in before or before[candidate_id].status != "suppressed")
    )
    return {
        "generated_at": discovery_now(),
        "from_at": from_run.finished_at or from_run.started_at,
        "to_at": to_run.finished_at or to_run.started_at,
        "added_count": len(added_ids),
        "changed_count": len(changed_ids),
        "promoted_count": len(promoted_ids),
        "suppressed_count": len(suppressed_ids),
        "disappeared_count": len(disappeared_ids),
        "added_candidate_ids": added_ids,
        "changed_candidate_ids": changed_ids,
        "promoted_candidate_ids": promoted_ids,
        "suppressed_candidate_ids": suppressed_ids,
        "disappeared_candidate_ids": disappeared_ids,
    }
