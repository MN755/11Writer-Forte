from __future__ import annotations

from urllib.parse import urlparse

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import SourceTrustProfileORM

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


def seed_default_integrity_sources(session: Session) -> list[str]:
    created_domains: list[str] = []
    for domain, trust_level, approval_policy, integrity_source, notes in DEFAULT_INTEGRITY_SOURCES:
        existing = get_trust_profile(session, domain)
        if existing is not None:
            continue
        session.add(
            SourceTrustProfileORM(
                domain=domain,
                trust_level=trust_level,
                approval_policy=approval_policy,
                integrity_source=integrity_source,
                notes=notes,
            )
        )
        created_domains.append(domain)
    session.commit()
    return created_domains
