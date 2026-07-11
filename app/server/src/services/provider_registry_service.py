"""Deterministic policy gates for configured public research providers.

The registry deliberately models capability and approved URL prefixes separately from
network execution.  A positive decision here is necessary but not sufficient to fetch:
workers must still apply the hardened fetch policy at connection time.
"""

from __future__ import annotations

import ipaddress
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Mapping
from urllib.parse import urlsplit

from sqlalchemy import select
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from src.models import CustodyLogORM, ResearchProviderORM, utcnow


ALLOWED_CAPABILITIES = frozenset(
    {"search", "fetch_feed", "fetch_static", "parse_document", "browser_render", "source_health"}
)
ALLOWED_PROVIDER_KINDS = frozenset(
    {
        "search_api",
        "rss_atom",
        "sitemap",
        "static_html",
        "document_repository",
        "structured_dataset",
        "activitypub",
        "video_metadata",
        "browser_rendered",
    }
)
ALLOWED_ACCESS_MODES = frozenset({"public_no_login", "operator_supplied_public_feed", "disabled"})
ALLOWED_ROBOTS_MODES = frozenset(
    {"required", "not_applicable", "provider_terms_override_documented"}
)
ALLOWED_CAPTURE_MODES = frozenset(
    {"metadata_only", "normalized_text", "raw_and_normalized", "evidence_candidate"}
)
USABLE_HEALTH_STATUSES = frozenset({"healthy"})
MAX_BUDGET = {
    "max_requests_per_run": 1_000,
    "max_requests_per_day": 10_000,
    "max_concurrency": 20,
    "max_response_bytes": 100_000_000,
    "request_timeout_seconds": 120.0,
    "retry_ceiling": 5,
}
MIN_BUDGET = {
    "max_requests_per_run": 1,
    "max_requests_per_day": 1,
    "max_concurrency": 1,
    "max_response_bytes": 1_024,
    "request_timeout_seconds": 0.1,
    "retry_ceiling": 0,
}
PROVIDER_KEY_RE = re.compile(r"^[a-z][a-z0-9-]{0,119}$")


@dataclass(frozen=True)
class ProviderPolicyDecision:
    allowed: bool
    reason_code: str
    provider: str | None
    url: str | None
    capability: str | None
    applied_budget: dict[str, int | float]

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


class ProviderPauseRequest(BaseModel):
    """Small command payload kept beside the action it authorizes."""

    model_config = {"extra": "forbid"}

    reason: str = Field(min_length=1, max_length=1_000)


def _payload_dict(payload: Any, *, exclude_unset: bool = False) -> dict[str, Any]:
    if hasattr(payload, "model_dump"):
        return dict(payload.model_dump(exclude_unset=exclude_unset))
    if isinstance(payload, Mapping):
        return dict(payload)
    if isinstance(payload, ResearchProviderORM):
        return {
            field: getattr(payload, field)
            for field in (
                "provider_key", "display_name", "provider_kind", "capabilities_json", "base_urls_json",
                "access_mode", "terms_url", "license_note", "robots_mode", "jurisdictions_json",
                "languages_json", "request_budget_json", "artifact_capture_mode",
            )
        }
    raise TypeError("Provider payload must be a Pydantic model or mapping.")


def _error(message: str) -> ValueError:
    return ValueError(message)


def _utcnow() -> datetime:
    return utcnow()


def normalize_provider_key(value: str) -> str:
    key = value.strip().lower()
    if not PROVIDER_KEY_RE.fullmatch(key):
        raise _error("provider_key must be a lowercase slug (letters, digits, or hyphens).")
    return key


def normalize_budget(value: Any) -> dict[str, int | float]:
    if not isinstance(value, Mapping):
        raise _error("request_budget_json must be an object.")
    missing = sorted(set(MAX_BUDGET) - set(value))
    if missing:
        raise _error(f"request_budget_json is missing required fields: {', '.join(missing)}.")
    unknown = sorted(set(value) - set(MAX_BUDGET))
    if unknown:
        raise _error(f"request_budget_json has unsupported fields: {', '.join(unknown)}.")
    normalized: dict[str, int | float] = {}
    for key, maximum in MAX_BUDGET.items():
        raw = value[key]
        try:
            number: int | float = float(raw) if isinstance(maximum, float) else int(raw)
        except (TypeError, ValueError) as exc:
            raise _error(f"request_budget_json.{key} must be numeric.") from exc
        if isinstance(number, float) and not number.is_integer() and not isinstance(maximum, float):
            raise _error(f"request_budget_json.{key} must be an integer.")
        if number < MIN_BUDGET[key]:
            raise _error(f"request_budget_json.{key} must be at least {MIN_BUDGET[key]}.")
        normalized[key] = min(number, maximum)
    return normalized


def validate_approved_origin(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _error("Provider base URLs must be non-empty strings.")
    candidate = value.strip()
    if "*" in candidate:
        raise _error("Provider base URLs may not contain wildcard origins.")
    parsed = urlsplit(candidate)
    if parsed.scheme.lower() not in {"http", "https"}:
        raise _error("Provider base URLs must use http or https.")
    if not parsed.hostname or parsed.username is not None or parsed.password is not None:
        raise _error("Provider base URLs must have a hostname and no userinfo.")
    if parsed.query or parsed.fragment:
        raise _error("Provider base URLs must not contain a query or fragment.")
    hostname = parsed.hostname.rstrip(".").lower()
    if hostname in {"localhost", "localhost.localdomain"} or hostname.endswith((".localhost", ".local")):
        raise _error("Provider base URLs may not target local hosts.")
    try:
        literal = ipaddress.ip_address(hostname.split("%", 1)[0])
    except ValueError:
        literal = None
    if literal is not None and (
        literal.is_private
        or literal.is_loopback
        or literal.is_link_local
        or literal.is_reserved
        or literal.is_unspecified
        or literal.is_multicast
    ):
        raise _error("Provider base URLs may not target private, reserved, or loopback addresses.")
    try:
        port = parsed.port
    except ValueError as exc:
        raise _error("Provider base URL contains an invalid port.") from exc
    authority = hostname if port is None else f"{hostname}:{port}"
    path = parsed.path.rstrip("/")
    return f"{parsed.scheme.lower()}://{authority}{path}" or candidate


def validate_public_request_url(value: Any) -> str:
    """Apply the no-credentials/public-target portion of origin validation to a request URL."""

    if not isinstance(value, str) or not value.strip():
        raise _error("Provider request URL must be a non-empty string.")
    parsed = urlsplit(value.strip())
    # Origin validation does the difficult hostname/IP checks; add the request parts
    # back afterwards because search and dataset endpoints commonly require a query.
    validate_approved_origin(
        f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    )
    return value.strip()


def normalize_origins(value: Any) -> list[str]:
    if not isinstance(value, list) or not value:
        raise _error("base_urls_json must contain at least one approved origin or prefix.")
    return sorted({validate_approved_origin(item) for item in value})


def validate_provider_configuration(payload: Any) -> dict[str, Any]:
    """Validate and normalize a provider configuration without mutating persistence."""

    data = _payload_dict(payload)
    normalized: dict[str, Any] = dict(data)
    normalized["provider_key"] = normalize_provider_key(str(data.get("provider_key") or ""))
    if not str(data.get("display_name") or "").strip():
        raise _error("display_name is required.")
    if data.get("provider_kind") not in ALLOWED_PROVIDER_KINDS:
        raise _error("provider_kind is not supported.")
    capabilities = data.get("capabilities_json")
    if not isinstance(capabilities, list) or not capabilities:
        raise _error("capabilities_json must contain at least one capability.")
    normalized_capabilities = sorted({str(item).strip().lower() for item in capabilities})
    unsupported = sorted(set(normalized_capabilities) - ALLOWED_CAPABILITIES)
    if unsupported:
        raise _error(f"Unsupported provider capabilities: {', '.join(unsupported)}.")
    normalized["capabilities_json"] = normalized_capabilities
    normalized["base_urls_json"] = normalize_origins(data.get("base_urls_json"))
    if data.get("access_mode") not in ALLOWED_ACCESS_MODES:
        raise _error("access_mode is not supported.")
    if not str(data.get("terms_url") or "").strip():
        raise _error("terms_url is required before a provider can be configured.")
    normalized["terms_url"] = validate_approved_origin(data["terms_url"])
    if not str(data.get("license_note") or "").strip():
        raise _error("license_note is required before a provider can be configured.")
    if data.get("robots_mode") not in ALLOWED_ROBOTS_MODES:
        raise _error("robots_mode is not supported.")
    if data.get("artifact_capture_mode") not in ALLOWED_CAPTURE_MODES:
        raise _error("artifact_capture_mode is not supported.")
    normalized["request_budget_json"] = normalize_budget(data.get("request_budget_json"))
    for field_name in ("jurisdictions_json", "languages_json"):
        value = data.get(field_name, [])
        if not isinstance(value, list) or any(not isinstance(item, str) or not item.strip() for item in value):
            raise _error(f"{field_name} must be a list of non-empty strings.")
        normalized[field_name] = sorted({item.strip() for item in value})
    return normalized


def _provider_id(provider: ResearchProviderORM) -> str:
    return str(provider.provider_id)


def _record_custody(
    session: Session,
    *,
    provider: ResearchProviderORM,
    action: str,
    actor: str,
    details: Mapping[str, Any] | None = None,
) -> None:
    session.add(
        CustodyLogORM(
            object_type="research_provider",
            object_id=_provider_id(provider),
            action=action,
            actor=actor[:80],
            details_json=dict(details or {}),
        )
    )


def create_provider(session: Session, payload: Any, *, actor: str) -> ResearchProviderORM:
    data = validate_provider_configuration(payload)
    existing = session.scalar(select(ResearchProviderORM).where(ResearchProviderORM.provider_key == data["provider_key"]))
    if existing is not None:
        raise _error(f"Provider '{data['provider_key']}' already exists.")
    # Activation is intentionally a separate, admin-scoped action even when the
    # configuration is complete and currently healthy.
    data["enabled"] = False
    data["paused_at"] = None
    data["disabled_reason"] = "awaiting_explicit_admin_enable"
    data["created_by"] = actor[:80]
    data["approved_by"] = None
    provider = ResearchProviderORM(**data)
    session.add(provider)
    session.flush()
    _record_custody(
        session,
        provider=provider,
        action="research_provider_created",
        actor=actor,
        details={"provider_key": provider.provider_key, "enabled": False},
    )
    session.commit()
    session.refresh(provider)
    return provider


def get_provider(session: Session, provider_id: int) -> ResearchProviderORM:
    provider = session.get(ResearchProviderORM, provider_id)
    if provider is None:
        raise _error(f"Research provider {provider_id} does not exist.")
    return provider


def list_providers(
    session: Session,
    *,
    provider_kind: str | None = None,
    enabled: bool | None = None,
    health: str | None = None,
    capability: str | None = None,
    jurisdiction: str | None = None,
    limit: int = 200,
) -> list[ResearchProviderORM]:
    statement = select(ResearchProviderORM).order_by(ResearchProviderORM.provider_key.asc())
    if provider_kind:
        statement = statement.where(ResearchProviderORM.provider_kind == provider_kind)
    if enabled is not None:
        statement = statement.where(ResearchProviderORM.enabled.is_(enabled))
    if health:
        statement = statement.where(ResearchProviderORM.health_status == health)
    providers = list(session.scalars(statement.limit(max(1, min(limit, 5_000)))))
    if capability:
        providers = [item for item in providers if capability in (item.capabilities_json or [])]
    if jurisdiction:
        providers = [item for item in providers if jurisdiction in (item.jurisdictions_json or [])]
    return providers


def update_provider(session: Session, provider_id: int, payload: Any, *, actor: str) -> ResearchProviderORM:
    provider = get_provider(session, provider_id)
    updates = _payload_dict(payload, exclude_unset=True)
    if not updates:
        return provider
    if "provider_key" in updates and updates["provider_key"] != provider.provider_key:
        raise _error("provider_key is immutable after creation.")
    candidate = {
        column: getattr(provider, column)
        for column in (
            "provider_key", "display_name", "provider_kind", "capabilities_json", "base_urls_json",
            "access_mode", "terms_url", "license_note", "robots_mode", "jurisdictions_json",
            "languages_json", "request_budget_json", "artifact_capture_mode",
        )
    }
    candidate.update(updates)
    normalized = validate_provider_configuration(candidate)
    changed = sorted(key for key, value in normalized.items() if getattr(provider, key, None) != value)
    for key in changed:
        setattr(provider, key, normalized[key])
    # Configuration changes must be explicitly re-approved before scheduling resumes.
    if changed:
        provider.enabled = False
        provider.paused_at = None
        provider.disabled_reason = "configuration_changed_reapproval_required"
        provider.approved_by = None
        _record_custody(
            session,
            provider=provider,
            action="research_provider_updated",
            actor=actor,
            details={"changed_fields": changed, "enabled": False},
        )
        session.commit()
        session.refresh(provider)
    return provider


def mark_provider_health(
    session: Session,
    provider_id: int,
    *,
    health_status: str,
    reason: str | None,
    actor: str,
) -> ResearchProviderORM:
    provider = get_provider(session, provider_id)
    provider.health_status = health_status
    provider.health_reason = (reason or "")[:1_000] or None
    provider.last_checked_at = _utcnow()
    if health_status not in USABLE_HEALTH_STATUSES:
        provider.enabled = False
        provider.disabled_reason = f"health_{health_status}"
    _record_custody(
        session,
        provider=provider,
        action="research_provider_health_updated",
        actor=actor,
        details={"health_status": health_status, "reason": provider.health_reason},
    )
    session.commit()
    session.refresh(provider)
    return provider


def enable_provider(session: Session, provider_id: int, *, actor: str) -> ResearchProviderORM:
    provider = get_provider(session, provider_id)
    validate_provider_configuration(provider)
    if provider.access_mode == "disabled":
        raise _error("A provider with access_mode 'disabled' cannot be enabled.")
    if provider.paused_at is not None:
        raise _error("A paused provider must be reconfigured or unpaused before enable.")
    if provider.health_status not in USABLE_HEALTH_STATUSES:
        raise _error("Only a healthy provider can be enabled.")
    provider.enabled = True
    provider.disabled_reason = None
    provider.approved_by = actor[:80]
    _record_custody(
        session,
        provider=provider,
        action="research_provider_enabled",
        actor=actor,
        details={"provider_key": provider.provider_key},
    )
    session.commit()
    session.refresh(provider)
    return provider


def pause_provider(session: Session, provider_id: int, *, reason: str, actor: str) -> ResearchProviderORM:
    if not reason.strip():
        raise _error("A non-empty pause reason is required.")
    provider = get_provider(session, provider_id)
    provider.enabled = False
    provider.paused_at = _utcnow()
    provider.disabled_reason = reason.strip()[:1_000]
    _record_custody(
        session,
        provider=provider,
        action="research_provider_paused",
        actor=actor,
        details={"reason": provider.disabled_reason},
    )
    session.commit()
    session.refresh(provider)
    return provider


def _url_matches_approved_prefix(url: str, origins: list[str]) -> bool:
    try:
        candidate = validate_public_request_url(url)
    except ValueError:
        return False
    candidate_parts = urlsplit(candidate)
    for origin in origins:
        origin_parts = urlsplit(origin)
        if (
            candidate_parts.scheme.lower() != origin_parts.scheme.lower()
            or candidate_parts.hostname != origin_parts.hostname
            or candidate_parts.port != origin_parts.port
        ):
            continue
        required_path = origin_parts.path.rstrip("/")
        actual_path = candidate_parts.path.rstrip("/")
        if not required_path or actual_path == required_path or actual_path.startswith(f"{required_path}/"):
            return True
    return False


def evaluate_provider_request(
    session: Session,
    *,
    provider_id: int,
    url: str,
    capability: str,
) -> ProviderPolicyDecision:
    provider = get_provider(session, provider_id)
    budget = normalize_budget(provider.request_budget_json or {})
    common = {"provider": provider.provider_key, "url": url, "capability": capability, "applied_budget": budget}
    if provider.paused_at is not None:
        return ProviderPolicyDecision(False, "domain_paused", **common)
    if not provider.enabled:
        return ProviderPolicyDecision(False, "provider_disabled", **common)
    if provider.health_status not in USABLE_HEALTH_STATUSES:
        return ProviderPolicyDecision(False, "schema_quarantined" if provider.health_status == "schema_quarantined" else "provider_disabled", **common)
    if capability not in ALLOWED_CAPABILITIES or capability not in (provider.capabilities_json or []):
        return ProviderPolicyDecision(False, "unsupported_capability", **common)
    try:
        validate_public_request_url(url)
    except ValueError:
        return ProviderPolicyDecision(False, "unsafe_target", **common)
    if not _url_matches_approved_prefix(url, provider.base_urls_json or []):
        return ProviderPolicyDecision(False, "origin_not_allowed", **common)
    return ProviderPolicyDecision(True, "allowed", **common)


def provider_coverage_summary(session: Session) -> dict[str, object]:
    providers = list_providers(session, limit=5_000)
    all_capabilities = sorted(ALLOWED_CAPABILITIES)
    by_capability: dict[str, dict[str, object]] = {}
    for capability in all_capabilities:
        matching = [provider for provider in providers if capability in (provider.capabilities_json or [])]
        available = [provider for provider in matching if provider.enabled and provider.paused_at is None and provider.health_status in USABLE_HEALTH_STATUSES]
        by_capability[capability] = {
            "configured_count": len(matching),
            "available_count": len(available),
            "provider_keys": [provider.provider_key for provider in matching],
            "available_provider_keys": [provider.provider_key for provider in available],
            "coverage_gap": not bool(available),
        }
    return {
        "provider_count": len(providers),
        "enabled_count": sum(1 for provider in providers if provider.enabled),
        "paused_count": sum(1 for provider in providers if provider.paused_at is not None),
        "capabilities": by_capability,
    }
