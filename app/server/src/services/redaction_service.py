from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


REDACTION_ORDER = {
    "public": 0,
    "restricted": 1,
    "confidential": 2,
    "secret": 3,
}

REDACTED_VALUE = "***REDACTED***"
SENSITIVE_KEY_FRAGMENTS = (
    "access_key",
    "api_key",
    "authorization",
    "cookie",
    "credential",
    "password",
    "private_key",
    "secret",
    "token",
)
SENSITIVE_HEADER_NAMES = {
    "authorization",
    "cookie",
    "proxy-authorization",
    "set-cookie",
    "x-api-key",
}
URL_FIELD_FRAGMENTS = ("url", "uri")


class RedactableRecord(Protocol):
    redaction_level: str


def normalize_redaction_level(value: str | None) -> str:
    candidate = (value or "public").strip().lower()
    if not candidate:
        return "public"
    return candidate


def redaction_rank(value: str | None) -> int:
    normalized = normalize_redaction_level(value)
    return REDACTION_ORDER.get(normalized, REDACTION_ORDER["confidential"])


def is_visible_at_level(record_level: str | None, max_redaction_level: str | None) -> bool:
    if max_redaction_level is None:
        return True
    return redaction_rank(record_level) <= redaction_rank(max_redaction_level)


def enforce_export_redaction(record: RedactableRecord, max_redaction_level: str | None) -> None:
    if is_visible_at_level(record.redaction_level, max_redaction_level):
        return
    raise ValueError(
        f"Object with redaction level '{record.redaction_level}' cannot be exported at "
        f"'{normalize_redaction_level(max_redaction_level)}'."
    )


def filter_records_by_redaction_level(
    records: list[object],
    max_redaction_level: str | None,
) -> list[object]:
    if max_redaction_level is None:
        return records
    return [
        record
        for record in records
        if is_visible_at_level(getattr(record, "redaction_level", "public"), max_redaction_level)
    ]


def sanitize_for_observability(value: Any, *, field_name: str | None = None) -> Any:
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        for key, item in value.items():
            key_name = str(key)
            lowered_key = key_name.lower()
            if is_sensitive_field_name(lowered_key):
                sanitized[key_name] = REDACTED_VALUE
                continue
            if lowered_key == "headers" and isinstance(item, dict):
                sanitized[key_name] = sanitize_headers(item)
                continue
            sanitized[key_name] = sanitize_for_observability(item, field_name=key_name)
        return sanitized
    if isinstance(value, list):
        return [sanitize_for_observability(item, field_name=field_name) for item in value]
    if isinstance(value, tuple):
        return [sanitize_for_observability(item, field_name=field_name) for item in value]
    if isinstance(value, Path):
        return value.name
    if isinstance(value, str):
        if should_sanitize_url(field_name):
            return sanitize_url(value)
        return value
    return value


def sanitize_headers(headers: dict[str, Any]) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}
    for key, value in headers.items():
        normalized = str(key)
        lowered = normalized.lower()
        if lowered in SENSITIVE_HEADER_NAMES or is_sensitive_field_name(lowered):
            sanitized[normalized] = REDACTED_VALUE
            continue
        sanitized[normalized] = sanitize_for_observability(value, field_name=normalized)
    return sanitized


def sanitize_url(value: str) -> str:
    stripped = value.strip()
    if not stripped:
        return stripped
    if stripped.startswith("sqlite:///"):
        filename = Path(stripped.removeprefix("sqlite:///")).name
        return f"sqlite:///{filename}" if filename else "sqlite:///"

    parsed = urlsplit(stripped)
    if not parsed.scheme:
        return stripped

    hostname = parsed.hostname or ""
    port = f":{parsed.port}" if parsed.port else ""
    if parsed.username or parsed.password:
        netloc = f"{REDACTED_VALUE}@{hostname}{port}"
    else:
        netloc = parsed.netloc

    query_pairs = []
    for key, item in parse_qsl(parsed.query, keep_blank_values=True):
        if is_sensitive_field_name(key.lower()):
            query_pairs.append((key, REDACTED_VALUE))
        else:
            query_pairs.append((key, item))
    query = urlencode(query_pairs, doseq=True)
    return urlunsplit((parsed.scheme, netloc, parsed.path, query, parsed.fragment))


def is_sensitive_field_name(value: str) -> bool:
    return any(fragment in value for fragment in SENSITIVE_KEY_FRAGMENTS)


def should_sanitize_url(field_name: str | None) -> bool:
    if field_name is None:
        return False
    lowered = field_name.lower()
    return any(fragment in lowered for fragment in URL_FIELD_FRAGMENTS)
