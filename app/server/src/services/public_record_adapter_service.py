"""Conservative adapters and resolution primitives for public records.

This module intentionally has no database or HTTP dependencies.  Callers supply only
already-obtained records from a permitted public source, then persist either accepted
normalized records or the returned quarantine result.  Keeping that boundary explicit
prevents a source-format change from silently becoming graph evidence.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
import ipaddress
import re
import unicodedata
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse


ALLOWED_PUBLIC_SOURCE_TYPES = frozenset(
    {"government", "court", "financial", "news", "dataset", "website", "rss", "api", "social", "video"}
)
SOURCE_RELIABILITY = {
    "government": 0.95,
    "court": 0.95,
    "financial": 0.90,
    "dataset": 0.82,
    "news": 0.70,
    "api": 0.70,
    "rss": 0.65,
    "website": 0.55,
    "social": 0.40,
    "video": 0.40,
}
STRONG_IDENTIFIER_KEYS = frozenset(
    {"organization_registration_id", "imo", "mmsi", "vessel_callsign", "aircraft_registration"}
)
ENTITY_IDENTIFIER_KEYS = {
    "legal_case": frozenset({"case_id"}),
    "document": frozenset({"document_id"}),
    "organization": frozenset({"organization_registration_id"}),
    "vessel": frozenset({"imo", "mmsi", "vessel_callsign"}),
    "aircraft": frozenset({"aircraft_registration"}),
}
_CONTEXT_KEYS = frozenset({"jurisdiction", "event_date", "role", "public_institution"})

# Unicode does not include transliteration.  This small, deterministic table covers
# common public-record scripts while leaving unmapped characters intact rather than
# inventing a potentially misleading spelling.
_TRANSLITERATION = str.maketrans(
    {
        **dict(zip("АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ", [
            "A", "B", "V", "G", "D", "E", "E", "Zh", "Z", "I", "I", "K", "L", "M", "N", "O",
            "P", "R", "S", "T", "U", "F", "Kh", "Ts", "Ch", "Sh", "Shch", "", "Y", "", "E", "Yu", "Ya",
        ])),
        **dict(zip("абвгдеёжзийклмнопрстуфхцчшщъыьэюя", [
            "a", "b", "v", "g", "d", "e", "e", "zh", "z", "i", "i", "k", "l", "m", "n", "o",
            "p", "r", "s", "t", "u", "f", "kh", "ts", "ch", "sh", "shch", "", "y", "", "e", "yu", "ya",
        ])),
        **dict(zip("ΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΤΥΦΧΨΩ", [
            "A", "V", "G", "D", "E", "Z", "I", "Th", "I", "K", "L", "M", "N", "X", "O", "P", "R", "S", "T", "Y", "F", "Ch", "Ps", "O",
        ])),
        **dict(zip("αβγδεζηθικλμνξοπρσςτυφχψω", [
            "a", "v", "g", "d", "e", "z", "i", "th", "i", "k", "l", "m", "n", "x", "o", "p", "r", "s", "s", "t", "y", "f", "ch", "ps", "o",
        ])),
    }
)
_IDENTIFIER_PATTERNS: dict[str, re.Pattern[str]] = {
    "imo": re.compile(r"\bIMO\s*[:#-]?\s*(\d{7})\b", re.IGNORECASE),
    "mmsi": re.compile(r"\bMMSI\s*[:#-]?\s*(\d{9})\b", re.IGNORECASE),
    "vessel_callsign": re.compile(r"\b(?:CALL\s*SIGN|CALLSIGN)\s*[:#-]?\s*([A-Z0-9]{3,8})\b", re.IGNORECASE),
    "aircraft_registration": re.compile(r"\b(?:REG(?:ISTRATION)?|TAIL(?:\s*NUMBER)?)\s*[:#-]?\s*([A-Z]{1,2}-?[A-Z0-9]{2,6})\b", re.IGNORECASE),
    "organization_registration_id": re.compile(
        r"\b(?:COMPANY|ORGANIZATION|BUSINESS|REG(?:ISTRATION)?)(?:\s+(?:NO|NUMBER|ID))?\s*[:#-]?\s*([A-Z0-9][A-Z0-9./-]{3,31})\b",
        re.IGNORECASE,
    ),
    "case_id": re.compile(r"\b(?:CASE|DOCKET)\s*(?:NO\.?|NUMBER|ID)?\s*[:#-]?\s*([A-Z0-9][A-Z0-9./:-]{3,63})\b", re.IGNORECASE),
    "document_id": re.compile(r"\b(?:DOCUMENT|FILING|RECORD)\s*(?:NO\.?|NUMBER|ID)?\s*[:#-]?\s*([A-Z0-9][A-Z0-9./:-]{3,63})\b", re.IGNORECASE),
}


class PublicRecordAdapterError(ValueError):
    """Base error for a record that must not be admitted as graph evidence."""


class SourcePolicyError(PublicRecordAdapterError):
    pass


class AdapterSchemaMismatchError(PublicRecordAdapterError):
    pass


@dataclass(frozen=True)
class SourcePolicyDecision:
    allowed: bool
    reason: str | None
    normalized_domain: str | None
    reliability: float


@dataclass(frozen=True)
class AdapterValidationResult:
    status: str
    record: dict[str, Any] | None
    quarantine_reason: str | None = None

    @property
    def accepted(self) -> bool:
        return self.status == "accepted"


@dataclass(frozen=True)
class PublicRecordAdapter:
    """Versioned, allow-listed schema for one public source format."""

    source_name: str
    schema_version: str
    required_fields: frozenset[str]
    source_type: str
    allowed_fields: frozenset[str] | None = None

    def validate(
        self,
        record: Mapping[str, Any],
        *,
        actual_schema_version: str | None = None,
        strict: bool = False,
    ) -> AdapterValidationResult:
        try:
            if not isinstance(record, Mapping):
                raise AdapterSchemaMismatchError("Record must be a JSON object.")
            if actual_schema_version != self.schema_version:
                raise AdapterSchemaMismatchError(
                    f"{self.source_name} schema version mismatch: expected {self.schema_version!r}, "
                    f"received {actual_schema_version!r}."
                )
            missing = sorted(field for field in self.required_fields if record.get(field) in (None, ""))
            if missing:
                raise AdapterSchemaMismatchError(f"Missing required fields: {', '.join(missing)}.")
            if self.allowed_fields is not None:
                unexpected = sorted(set(record) - self.allowed_fields)
                if unexpected:
                    raise AdapterSchemaMismatchError(
                        f"Unexpected fields for schema {self.schema_version}: {', '.join(unexpected)}."
                    )
            return AdapterValidationResult(status="accepted", record=dict(record))
        except AdapterSchemaMismatchError as exc:
            if strict:
                raise
            return AdapterValidationResult(status="quarantined", record=None, quarantine_reason=str(exc))


@dataclass(frozen=True)
class ResolutionSignal:
    key: str
    value: str
    strength: str
    source: str = "record"


@dataclass(frozen=True)
class EntityCandidate:
    entity_type: str
    canonical_name: str
    identifiers: Mapping[str, str] = field(default_factory=dict)
    aliases: tuple[str, ...] = ()
    context: Mapping[str, str] = field(default_factory=dict)
    source_citations: tuple[str, ...] = ()
    source_domains: tuple[str, ...] = ()
    source_reliability: float = 0.5


@dataclass(frozen=True)
class MergeDecision:
    action: str
    reason: str
    matched_identifiers: tuple[str, ...] = ()
    corroborated_context: tuple[str, ...] = ()

    @property
    def should_merge(self) -> bool:
        return self.action == "merge"


def transliterate_text(value: object) -> str:
    """Return a stable best-effort transliteration without network/model calls."""
    text = unicodedata.normalize("NFKD", str(value)).translate(_TRANSLITERATION)
    return "".join(char for char in text if not unicodedata.combining(char))


def canonicalize_text(value: object) -> str:
    """Canonical text for equality checks; display text must always remain separate."""
    transliterated = transliterate_text(value).casefold()
    return " ".join(re.sub(r"[^\w]+", " ", transliterated, flags=re.UNICODE).split())


def normalize_identifier(key: str, value: object) -> str | None:
    text = str(value).strip()
    if not text:
        return None
    normalized = re.sub(r"[^A-Z0-9]", "", transliterate_text(text).upper())
    if key == "imo":
        if len(normalized) == 10 and normalized.startswith("IMO"):
            normalized = normalized[3:]
        return normalized if len(normalized) == 7 and normalized.isdigit() else None
    if key == "mmsi":
        return normalized if len(normalized) == 9 and normalized.isdigit() else None
    if key in {"vessel_callsign", "aircraft_registration", "organization_registration_id", "case_id", "document_id"}:
        return normalized or None
    return canonicalize_text(text) or None


def extract_identifiers(value: object) -> dict[str, str]:
    """Extract labeled public identifiers; unlabeled number strings are deliberately ignored."""
    text = str(value)
    extracted: dict[str, str] = {}
    for key, pattern in _IDENTIFIER_PATTERNS.items():
        match = pattern.search(text)
        if match:
            normalized = normalize_identifier(key, match.group(1))
            if normalized:
                extracted[key] = normalized
    return extracted


def extract_resolution_signals(record: Mapping[str, Any]) -> list[ResolutionSignal]:
    signals: dict[tuple[str, str], ResolutionSignal] = {}
    for key, raw in record.items():
        normalized_key = str(key).strip().lower()
        if raw is None:
            continue
        if normalized_key in _IDENTIFIER_PATTERNS or normalized_key in ENTITY_IDENTIFIER_KEYS.get("vessel", frozenset()):
            normalized = normalize_identifier(normalized_key, raw)
            if normalized:
                signals[(normalized_key, normalized)] = ResolutionSignal(normalized_key, normalized, "strong")
        elif normalized_key in {"name", "full_name", "organization", "alias", "aliases"}:
            values: Iterable[object] = raw if isinstance(raw, (list, tuple, set)) else (raw,)
            for candidate in values:
                canonical = canonicalize_text(candidate)
                if canonical:
                    signals[("name", canonical)] = ResolutionSignal("name", canonical, "weak")
        elif normalized_key in _CONTEXT_KEYS:
            normalized = _normalize_context(normalized_key, raw)
            if normalized:
                signals[(normalized_key, normalized)] = ResolutionSignal(normalized_key, normalized, "context")
        elif isinstance(raw, str):
            for identifier_key, identifier_value in extract_identifiers(raw).items():
                signals[(identifier_key, identifier_value)] = ResolutionSignal(identifier_key, identifier_value, "strong")
    return sorted(signals.values(), key=lambda signal: (signal.key, signal.value))


def evaluate_public_source(
    source_url: str,
    *,
    source_type: str,
    public_access: bool = True,
    requires_login: bool = False,
    requires_payment: bool = False,
    bypasses_access_controls: bool = False,
    terms_permit_access: bool = True,
    domain_reliability: Mapping[str, float] | None = None,
) -> SourcePolicyDecision:
    """Allow only legal, no-login public sources; policy uncertainty denies admission."""
    parsed = urlparse(source_url)
    domain = (parsed.hostname or "").rstrip(".").lower() or None
    source_type = source_type.strip().lower()
    if parsed.scheme not in {"http", "https"} or not domain or parsed.username or parsed.password:
        return _policy_denied("source URL must be a credential-free HTTP(S) URL", domain, source_type)
    if domain == "localhost" or domain.endswith(".localhost") or domain.endswith(".local"):
        return _policy_denied("local source addresses are not public sources", domain, source_type)
    try:
        if ipaddress.ip_address(domain).is_private or ipaddress.ip_address(domain).is_loopback:
            return _policy_denied("private or loopback source addresses are not public sources", domain, source_type)
    except ValueError:
        pass
    if source_type not in ALLOWED_PUBLIC_SOURCE_TYPES:
        return _policy_denied("source type is not approved for public-record adapters", domain, source_type)
    if not public_access or requires_login or requires_payment or bypasses_access_controls or not terms_permit_access:
        return _policy_denied("source must be legally accessible without login, payment, or bypass", domain, source_type)
    return SourcePolicyDecision(True, None, domain, source_domain_reliability(domain, source_type, domain_reliability))


def validate_source_and_record(
    adapter: PublicRecordAdapter,
    source_url: str,
    record: Mapping[str, Any],
    *,
    actual_schema_version: str | None,
    public_access: bool = True,
    requires_login: bool = False,
    requires_payment: bool = False,
    bypasses_access_controls: bool = False,
    terms_permit_access: bool = True,
    domain_reliability: Mapping[str, float] | None = None,
    strict: bool = False,
) -> AdapterValidationResult:
    policy = evaluate_public_source(
        source_url,
        source_type=adapter.source_type,
        public_access=public_access,
        requires_login=requires_login,
        requires_payment=requires_payment,
        bypasses_access_controls=bypasses_access_controls,
        terms_permit_access=terms_permit_access,
        domain_reliability=domain_reliability,
    )
    if not policy.allowed:
        if strict:
            raise SourcePolicyError(policy.reason or "Public source policy denied this source.")
        return AdapterValidationResult("quarantined", None, policy.reason)
    return adapter.validate(record, actual_schema_version=actual_schema_version, strict=strict)


def decide_merge(left: EntityCandidate, right: EntityCandidate) -> MergeDecision:
    """Return a conservative explainable merge decision; never merge on name alone."""
    if left.entity_type != right.entity_type:
        return MergeDecision("do_not_merge", "entity types differ")
    left_ids = _normalized_identifiers(left.identifiers)
    right_ids = _normalized_identifiers(right.identifiers)
    applicable = ENTITY_IDENTIFIER_KEYS.get(left.entity_type, STRONG_IDENTIFIER_KEYS)
    left_strong = {key: value for key, value in left_ids.items() if key in applicable}
    right_strong = {key: value for key, value in right_ids.items() if key in applicable}
    shared = tuple(sorted(key for key in set(left_strong) & set(right_strong) if left_strong[key] == right_strong[key]))
    conflicts = sorted(key for key in set(left_strong) & set(right_strong) if left_strong[key] != right_strong[key])
    if conflicts:
        return MergeDecision("do_not_merge", f"conflicting strong identifiers: {', '.join(conflicts)}")
    if shared:
        if not left.source_citations or not right.source_citations:
            return MergeDecision("ambiguous", "shared strong identifier lacks cited evidence", shared)
        if min(left.source_reliability, right.source_reliability) < 0.5:
            return MergeDecision("ambiguous", "shared strong identifier has insufficient source reliability", shared)
        return MergeDecision("merge", "matching cited strong public identifier", shared)
    if not _candidate_name_forms(left).intersection(_candidate_name_forms(right)):
        return MergeDecision("do_not_merge", "canonical names differ and no strong identifier matches")
    context = _shared_context(left.context, right.context)
    domains = {domain.lower() for domain in left.source_domains + right.source_domains if domain}
    if len(context) >= 2 and len(domains) >= 2 and left.source_citations and right.source_citations:
        return MergeDecision("merge", "same name with independently corroborated cited context", corroborated_context=context)
    return MergeDecision(
        "ambiguous",
        "same-name candidates lack a matching strong identifier or independently corroborated context",
        corroborated_context=context,
    )


def _normalized_identifiers(identifiers: Mapping[str, str]) -> dict[str, str]:
    return {
        key: normalized
        for key, value in identifiers.items()
        for normalized in [normalize_identifier(key, value)]
        if normalized
    }


def source_domain_reliability(
    domain: str | None,
    source_type: str,
    overrides: Mapping[str, float] | None = None,
) -> float:
    """Return a bounded source reliability, honoring the most-specific domain override."""
    base = SOURCE_RELIABILITY.get(source_type.strip().lower(), 0.0)
    if not domain or not overrides:
        return base
    normalized = domain.rstrip(".").lower()
    matches = [
        (configured.rstrip(".").lower(), score)
        for configured, score in overrides.items()
        if normalized == configured.rstrip(".").lower()
        or normalized.endswith(f".{configured.rstrip('.').lower()}")
    ]
    if not matches:
        return base
    _, score = max(matches, key=lambda item: len(item[0]))
    try:
        return max(0.0, min(1.0, float(score)))
    except (TypeError, ValueError):
        return base


def _candidate_name_forms(candidate: EntityCandidate) -> set[str]:
    return {
        canonical
        for raw in (candidate.canonical_name, *candidate.aliases)
        for canonical in [canonicalize_text(raw)]
        if canonical
    }


def _shared_context(left: Mapping[str, str], right: Mapping[str, str]) -> tuple[str, ...]:
    matched = []
    for key in sorted(_CONTEXT_KEYS & set(left) & set(right)):
        if _normalize_context(key, left[key]) == _normalize_context(key, right[key]) and _normalize_context(key, left[key]):
            matched.append(key)
    return tuple(matched)


def _normalize_context(key: str, value: object) -> str | None:
    if key == "event_date":
        if isinstance(value, (date, datetime)):
            return value.isoformat()[:10]
        text = str(value).strip()
        try:
            return date.fromisoformat(text.replace("Z", "+00:00")[:10]).isoformat()
        except ValueError:
            return None
    return canonicalize_text(value) or None


def _policy_denied(reason: str, domain: str | None, source_type: str) -> SourcePolicyDecision:
    return SourcePolicyDecision(False, reason, domain, SOURCE_RELIABILITY.get(source_type, 0.0))
