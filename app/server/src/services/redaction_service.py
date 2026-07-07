from __future__ import annotations

from typing import Protocol


REDACTION_ORDER = {
    "public": 0,
    "restricted": 1,
    "confidential": 2,
    "secret": 3,
}


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
