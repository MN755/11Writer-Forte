from __future__ import annotations

from urllib.parse import urlparse

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import CustodyLogORM
from src.models import SourceTrustProfileORM
from src.schemas import SourceTrustProfileCreate
from src.schemas import SourceTrustProfileUpdate

DEFAULT_INTEGRITY_SOURCES = (
    ("nytimes.com", "trusted", "auto_approve_stable", True, "Seeded starter integrity source."),
    ("npr.org", "trusted", "auto_approve_stable", True, "Seeded starter integrity source."),
    ("bbc.com", "trusted", "auto_approve_stable", True, "Seeded starter integrity source."),
    ("bbc.co.uk", "trusted", "auto_approve_stable", True, "Seeded starter integrity source."),
    ("smithsonianmag.com", "trusted", "auto_approve_stable", True, "Seeded starter integrity source."),
    ("smithsonian.org", "trusted", "auto_approve_stable", True, "Seeded starter integrity source."),
)

TRUST_SCORES = {
    "trusted": 0.85,
    "neutral": 0.5,
    "blocked": 0.05,
}


def normalize_domain(value: str | None) -> str | None:
    if not value:
        return None
    candidate = value.strip().lower()
    if "://" in candidate:
        parsed = urlparse(candidate)
        candidate = (parsed.hostname or parsed.netloc).lower()
    if "@" in candidate:
        candidate = candidate.rsplit("@", 1)[-1]
    if candidate.startswith("[") and "]" in candidate:
        candidate = candidate[1:candidate.index("]")]
    elif ":" in candidate and candidate.count(":") == 1:
        candidate = candidate.split(":", 1)[0]
    candidate = candidate.split("/")[0]
    if candidate.startswith("www."):
        candidate = candidate[4:]
    return candidate or None


def get_trust_profile(session: Session, domain: str | None) -> SourceTrustProfileORM | None:
    normalized = normalize_domain(domain)
    if not normalized:
        return None
    exact = session.scalar(
        select(SourceTrustProfileORM).where(SourceTrustProfileORM.domain == normalized)
    )
    if exact is not None:
        return exact
    parents = [
        profile
        for profile in session.scalars(select(SourceTrustProfileORM))
        if normalized.endswith(f".{profile.domain}")
    ]
    return max(parents, key=lambda profile: len(profile.domain), default=None)


def resolve_trust(session: Session, domain: str | None) -> tuple[str, str, float]:
    profile = get_trust_profile(session, domain)
    if profile is None:
        return ("neutral", "manual_review", TRUST_SCORES["neutral"])
    return (
        profile.trust_level,
        profile.approval_policy,
        TRUST_SCORES.get(profile.trust_level, TRUST_SCORES["neutral"]),
    )


def create_source_trust_profile(
    session: Session,
    payload: SourceTrustProfileCreate,
    *,
    actor: str = "system",
    commit: bool = True,
) -> SourceTrustProfileORM:
    normalized_domain = normalize_domain(payload.domain) or payload.domain.lower()
    existing = session.scalar(
        select(SourceTrustProfileORM).where(SourceTrustProfileORM.domain == normalized_domain)
    )
    if existing is not None:
        raise ValueError(f"Source trust profile for domain '{normalized_domain}' already exists.")
    record = SourceTrustProfileORM(
        **payload.model_dump(exclude={"domain"}),
        domain=normalized_domain,
    )
    session.add(record)
    session.flush()
    session.add(
        CustodyLogORM(
            object_type="source_trust_profile",
            object_id=str(record.trust_profile_id),
            action="source_trust_profile_created",
            actor=actor,
            details_json={
                "trust_profile_id": record.trust_profile_id,
                "domain": record.domain,
                "trust_level": record.trust_level,
                "approval_policy": record.approval_policy,
                "integrity_source": record.integrity_source,
            },
        )
    )
    if commit:
        session.commit()
        session.refresh(record)
    return record


def update_source_trust_profile(
    session: Session,
    trust_profile_id: int,
    payload: SourceTrustProfileUpdate,
    *,
    actor: str = "system",
    commit: bool = True,
) -> SourceTrustProfileORM:
    record = session.get(SourceTrustProfileORM, trust_profile_id)
    if record is None:
        raise ValueError(f"Source trust profile {trust_profile_id} does not exist.")

    changes = payload.model_dump(exclude_unset=True, exclude_none=True)
    if not changes:
        return record

    change_log: dict[str, dict[str, object]] = {}
    if "domain" in changes and changes["domain"] is not None:
        normalized_domain = normalize_domain(str(changes["domain"])) or str(changes["domain"]).lower()
        existing = session.scalar(
            select(SourceTrustProfileORM).where(
                SourceTrustProfileORM.domain == normalized_domain,
                SourceTrustProfileORM.trust_profile_id != trust_profile_id,
            )
        )
        if existing is not None:
            raise ValueError(f"Source trust profile for domain '{normalized_domain}' already exists.")
        if normalized_domain != record.domain:
            change_log["domain"] = {"old": record.domain, "new": normalized_domain}
            record.domain = normalized_domain

    for field_name in ("trust_level", "approval_policy", "integrity_source", "notes"):
        if field_name not in changes:
            continue
        new_value = changes[field_name]
        old_value = getattr(record, field_name)
        if new_value == old_value:
            continue
        change_log[field_name] = {"old": old_value, "new": new_value}
        setattr(record, field_name, new_value)

    if not change_log:
        return record

    session.add(
        CustodyLogORM(
            object_type="source_trust_profile",
            object_id=str(record.trust_profile_id),
            action="source_trust_profile_updated",
            actor=actor,
            details_json={
                "trust_profile_id": record.trust_profile_id,
                "changes": change_log,
            },
        )
    )
    if commit:
        session.commit()
        session.refresh(record)
    return record


def seed_default_integrity_sources(
    session: Session,
    *,
    actor: str = "system",
    commit: bool = True,
) -> list[str]:
    created_domains: list[str] = []
    for domain, trust_level, approval_policy, integrity_source, notes in DEFAULT_INTEGRITY_SOURCES:
        existing = get_trust_profile(session, domain)
        if existing is not None:
            continue
        create_source_trust_profile(
            session,
            SourceTrustProfileCreate(
                domain=domain,
                trust_level=trust_level,
                approval_policy=approval_policy,
                integrity_source=integrity_source,
                notes=notes,
            ),
            actor=actor,
            commit=False,
        )
        created_domains.append(domain)
    session.add(
        CustodyLogORM(
            object_type="source_trust_seed",
            object_id="default_integrity_sources",
            action="integrity_sources_seeded",
            actor=actor,
            details_json={
                "created_count": len(created_domains),
                "domains": created_domains,
            },
        )
    )
    if commit:
        session.commit()
    return created_domains
