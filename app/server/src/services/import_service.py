from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from src.config import get_settings
from src.models import CustodyLogORM, LocalImportRunORM, ObservationORM
from src.services.geospatial_service import geometry_to_wkt
from src.services.trust_service import normalize_domain, resolve_trust


@dataclass
class ParsedObservation:
    record_format: str
    source_domain: str | None
    content_text: str
    content_json: dict[str, Any]
    location_geojson: dict[str, Any] | None


def import_local_path(
    session: Session,
    source_path: str,
    layer_key: str,
    notes: str,
    actor: str = "system",
) -> LocalImportRunORM:
    path = Path(source_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"Input path does not exist: {path}")

    source_format = infer_source_format(path)
    run = LocalImportRunORM(
        source_path=str(path),
        source_format=source_format,
        layer_key=layer_key,
        status="running",
        notes=notes,
        chain_of_custody_json=[
            {
                "step": "queued",
                "path": str(path),
                "source_format": source_format,
                "actor": actor,
            }
        ],
    )
    session.add(run)
    session.flush()

    parsed = list(iter_parsed_observations(path, source_format))
    row_limit = get_settings().import_row_limit
    if len(parsed) > row_limit:
        parsed = parsed[:row_limit]
        run.notes = f"{notes} Import truncated at {row_limit} records.".strip()

    run.records_seen = len(parsed)
    run.records_imported = len(parsed)
    run.status = "completed"
    run.chain_of_custody_json.append(
        {
            "step": "parsed",
            "records_seen": run.records_seen,
            "records_imported": run.records_imported,
            "actor": actor,
        }
    )

    for item in parsed:
        trust_level, approval_policy, confidence_score = resolve_trust(session, item.source_domain)
        observation = ObservationORM(
            import_run_id=run.import_run_id,
            layer_key=layer_key,
            source_domain=item.source_domain,
            source_type="local_import",
            record_format=item.record_format,
            trust_level=trust_level,
            approval_policy=approval_policy,
            confidence_score=confidence_score,
            location_geojson=item.location_geojson,
            location_wkt=geometry_to_wkt(item.location_geojson),
            content_text=item.content_text,
            content_json=item.content_json,
            raw_hash=hashlib.sha256(
                json.dumps(item.content_json, sort_keys=True, default=str).encode("utf-8")
            ).hexdigest(),
        )
        session.add(observation)

    session.add(
        CustodyLogORM(
            object_type="local_import_run",
            object_id=str(run.import_run_id),
            action="import_completed",
            actor=actor,
            details_json={
                "source_path": str(path),
                "source_format": source_format,
                "records_imported": run.records_imported,
            },
        )
    )
    session.commit()
    session.refresh(run)
    return run


def infer_source_format(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".json", ".jsonl"}:
        return "json"
    if suffix in {".txt", ".log", ".csv"}:
        return "txt"
    if suffix in {".sqlite", ".sqlite3", ".db"}:
        return "sqlite"
    return "txt"


def iter_parsed_observations(path: Path, source_format: str) -> list[ParsedObservation]:
    if source_format == "json":
        return parse_json_observations(path)
    if source_format == "sqlite":
        return parse_sqlite_observations(path)
    return parse_text_observations(path)


def parse_json_observations(path: Path) -> list[ParsedObservation]:
    if path.suffix.lower() == ".jsonl":
        return [
            normalize_payload(json.loads(line), "json")
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    raw_text = path.read_text(encoding="utf-8")
    payload = json.loads(raw_text)
    if isinstance(payload, dict):
        items = payload.get("items") if isinstance(payload.get("items"), list) else [payload]
    elif isinstance(payload, list):
        items = payload
    else:
        items = [{"value": payload}]
    return [normalize_payload(item, "json") for item in items if isinstance(item, dict)]


def parse_text_observations(path: Path) -> list[ParsedObservation]:
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        rows.append(
            ParsedObservation(
                record_format="txt",
                source_domain=extract_domain({"url": text}),
                content_text=text,
                content_json={"text": text},
                location_geojson=None,
            )
        )
    return rows


def parse_sqlite_observations(path: Path) -> list[ParsedObservation]:
    rows: list[ParsedObservation] = []
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
                rows.append(normalize_payload(payload, "sqlite"))
    finally:
        connection.close()
    return rows


def normalize_payload(payload: dict[str, Any], record_format: str) -> ParsedObservation:
    source_domain = extract_domain(payload)
    content_text = payload.get("text") or payload.get("title") or json.dumps(payload, default=str)
    return ParsedObservation(
        record_format=record_format,
        source_domain=source_domain,
        content_text=str(content_text),
        content_json=payload,
        location_geojson=extract_location(payload),
    )


def extract_domain(payload: dict[str, Any]) -> str | None:
    for key in ("url", "source_url", "link", "domain"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return normalize_domain(value)
    return None


def extract_location(payload: dict[str, Any]) -> dict[str, Any] | None:
    latitude = payload.get("latitude", payload.get("lat"))
    longitude = payload.get("longitude", payload.get("lon"))
    if isinstance(latitude, (int, float)) and isinstance(longitude, (int, float)):
        return {
            "type": "Point",
            "coordinates": [float(longitude), float(latitude)],
        }
    geometry = payload.get("geometry")
    if isinstance(geometry, dict) and geometry.get("type") and geometry.get("coordinates"):
        return geometry
    return None
