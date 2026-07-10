from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.config import get_settings
from src.models import CustodyLogORM, LocalImportRunORM, ObservationORM
from src.services.geospatial_service import geometry_to_wkt
from src.services.layer_service import ensure_data_layer
from src.services.observation_service import parse_timestamp_value
from src.services.storage_service import register_import_storage_object
from src.services.trust_service import normalize_domain, resolve_trust


@dataclass(frozen=True)
class IngestionEnvelope:
    record_format: str
    source_type: str
    source_domain: str | None
    content_text: str
    content_json: dict[str, Any]
    location_geojson: dict[str, Any] | None
    raw_hash: str
    metadata_json: dict[str, Any] = field(default_factory=dict)


def infer_source_format(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".json", ".jsonl"}:
        return "json"
    if suffix in {".txt", ".log", ".csv", ".xml", ".rss"}:
        return "txt"
    if suffix in {".sqlite", ".sqlite3", ".db"}:
        return "sqlite"
    return "txt"


def read_path_as_envelopes(path: Path, source_format: str, *, source_type: str) -> list[IngestionEnvelope]:
    if source_format == "json":
        return parse_json_envelopes(path, source_type=source_type)
    if source_format == "sqlite":
        return parse_sqlite_envelopes(path, source_type=source_type)
    return parse_text_envelopes(path, source_type=source_type)


def parse_json_envelopes(path: Path, *, source_type: str) -> list[IngestionEnvelope]:
    if path.suffix.lower() == ".jsonl":
        return [
            normalize_payload(json.loads(line), "jsonl", source_type=source_type)
            for line in path.read_text(encoding="utf-8-sig").splitlines()
            if line.strip()
        ]
    raw_text = path.read_text(encoding="utf-8-sig")
    payload = json.loads(raw_text)
    if isinstance(payload, dict):
        items = payload.get("items") if isinstance(payload.get("items"), list) else [payload]
    elif isinstance(payload, list):
        items = payload
    else:
        items = [{"value": payload}]
    return [normalize_payload(item, "json", source_type=source_type) for item in items if isinstance(item, dict)]


def parse_text_envelopes(path: Path, *, source_type: str) -> list[IngestionEnvelope]:
    rows: list[IngestionEnvelope] = []
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        text = line.strip()
        if not text:
            continue
        rows.append(normalize_payload({"text": text, "url": text}, "txt", source_type=source_type))
    return rows


def parse_sqlite_envelopes(path: Path, *, source_type: str) -> list[IngestionEnvelope]:
    rows: list[IngestionEnvelope] = []
    connection = sqlite3.connect(path)
    try:
        table_names = [
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        ]
        for table_name in table_names:
            cursor = connection.execute(f'SELECT * FROM "{table_name}"')
            columns = [column[0] for column in cursor.description or []]
            for record in cursor.fetchall():
                payload = dict(zip(columns, record, strict=False))
                payload["table_name"] = table_name
                rows.append(normalize_payload(payload, "sqlite", source_type=source_type))
    finally:
        connection.close()
    return rows


def normalize_payload(payload: dict[str, Any], record_format: str, *, source_type: str) -> IngestionEnvelope:
    source_domain = extract_domain(payload)
    content_text = payload.get("text") or payload.get("title") or json.dumps(payload, default=str)
    raw_hash = hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    return IngestionEnvelope(
        record_format=record_format,
        source_type=source_type,
        source_domain=source_domain,
        content_text=str(content_text),
        content_json=payload,
        location_geojson=extract_location(payload),
        raw_hash=raw_hash,
        metadata_json={},
    )


def persist_envelopes_as_import_run(
    session: Session,
    *,
    source_path: str,
    source_format: str,
    layer_key: str,
    notes: str,
    actor: str,
    source_type: str,
    envelopes: list[IngestionEnvelope],
) -> LocalImportRunORM:
    ensure_data_layer(session, layer_key, actor=actor)
    run = LocalImportRunORM(
        source_path=source_path,
        source_format=source_format,
        layer_key=layer_key,
        status="running",
        notes=notes,
        chain_of_custody_json=[
            {
                "step": "queued",
                "path": source_path,
                "source_format": source_format,
                "source_type": source_type,
                "actor": actor,
            }
        ],
    )
    session.add(run)
    session.flush()

    parsed = list(envelopes)
    row_limit = get_settings().import_row_limit
    if len(parsed) > row_limit:
        parsed = parsed[:row_limit]
        run.notes = f"{notes} Import truncated at {row_limit} records.".strip()

    run.records_seen = len(parsed)
    run.status = "completed"
    imported_count = 0
    skipped_count = 0
    existing_hashes = existing_observation_hashes(session, layer_key, {item.raw_hash for item in parsed})
    seen_hashes: set[str] = set()

    for item in parsed:
        if item.raw_hash in existing_hashes or item.raw_hash in seen_hashes:
            skipped_count += 1
            continue
        trust_level, approval_policy, confidence_score = resolve_trust(session, item.source_domain)
        observation = ObservationORM(
            import_run_id=run.import_run_id,
            layer_key=layer_key,
            source_domain=item.source_domain,
            source_type=item.source_type,
            record_format=item.record_format,
            trust_level=trust_level,
            approval_policy=approval_policy,
            confidence_score=confidence_score,
            observed_at=extract_observed_at(item.content_json),
            location_geojson=item.location_geojson,
            location_wkt=geometry_to_wkt(item.location_geojson),
            content_text=item.content_text,
            content_json=item.content_json,
            raw_hash=item.raw_hash,
        )
        session.add(observation)
        seen_hashes.add(item.raw_hash)
        imported_count += 1

    run.records_imported = imported_count
    run.records_skipped = skipped_count
    run.chain_of_custody_json.append(
        {
            "step": "parsed",
            "records_seen": run.records_seen,
            "records_imported": run.records_imported,
            "records_skipped": run.records_skipped,
            "source_type": source_type,
            "actor": actor,
        }
    )

    session.add(
        CustodyLogORM(
            object_type="local_import_run",
            object_id=str(run.import_run_id),
            action="import_completed",
            actor=actor,
            details_json={
                "source_path": source_path,
                "source_format": source_format,
                "source_type": source_type,
                "records_seen": run.records_seen,
                "records_imported": run.records_imported,
                "records_skipped": run.records_skipped,
            },
        )
    )
    register_import_storage_object(session, run, actor=actor)
    session.commit()
    session.refresh(run)
    return run


def extract_domain(payload: dict[str, Any]) -> str | None:
    for key in ("url", "source_url", "link", "domain", "page_url", "image_url", "stream_url"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return normalize_domain(value)
    return None


def extract_location(payload: dict[str, Any]) -> dict[str, Any] | None:
    latitude = coerce_float(payload.get("latitude", payload.get("lat")))
    longitude = coerce_float(payload.get("longitude", payload.get("lon")))
    if latitude is not None and longitude is not None:
        return {"type": "Point", "coordinates": [longitude, latitude]}
    geometry = payload.get("geometry")
    if isinstance(geometry, dict) and geometry.get("type") and geometry.get("coordinates"):
        return geometry
    return None


def extract_observed_at(payload: dict[str, Any]) -> object | None:
    for key in (
        "observed_at",
        "occurred_at",
        "event_time",
        "timestamp",
        "published_at",
        "datetime",
        "detected_at",
    ):
        if key in payload:
            return parse_timestamp_value(payload.get(key))
    return None


def coerce_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            return float(value)
        except ValueError:
            return None
    return None


def existing_observation_hashes(session: Session, layer_key: str, hashes: set[str]) -> set[str]:
    if not hashes:
        return set()
    statement = select(ObservationORM.raw_hash).where(
        ObservationORM.layer_key == layer_key,
        ObservationORM.raw_hash.in_(hashes),
    )
    return set(session.scalars(statement))
