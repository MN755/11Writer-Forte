from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
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

IMPORT_JSON_FILE_SUFFIXES = {".json", ".jsonl"}
IMPORT_TEXT_FILE_SUFFIXES = {".txt", ".log", ".csv", ".md"}
IMPORT_SQLITE_FILE_SUFFIXES = {".sqlite", ".sqlite3", ".db"}
SOURCE_CODE_LANGUAGE_BY_SUFFIX = {
    ".c": "c",
    ".cc": "cpp",
    ".cpp": "cpp",
    ".cxx": "cpp",
    ".h": "c_header",
    ".hpp": "cpp_header",
    ".go": "go",
    ".java": "java",
    ".js": "javascript",
    ".jsx": "javascript_react",
    ".php": "php",
    ".ps1": "powershell",
    ".py": "python",
    ".r": "r",
    ".rb": "ruby",
    ".rs": "rust",
    ".sh": "shell",
    ".sql": "sql",
    ".ts": "typescript",
    ".tsx": "typescript_react",
}
SUPPORTED_DIRECTORY_IMPORT_SUFFIXES = (
    IMPORT_JSON_FILE_SUFFIXES
    | IMPORT_TEXT_FILE_SUFFIXES
    | IMPORT_SQLITE_FILE_SUFFIXES
    | set(SOURCE_CODE_LANGUAGE_BY_SUFFIX)
)


@dataclass
class ParsedObservation:
    record_format: str
    source_domain: str | None
    content_text: str
    content_json: dict[str, Any]
    location_geojson: dict[str, Any] | None
    raw_hash: str


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
    import_paths, ignored_paths = collect_import_paths(path)
    if not import_paths:
        if path.is_dir():
            raise ValueError(
                f"Directory import at '{path}' did not contain any supported files. "
                "Supported suffixes include json, jsonl, txt, log, csv, md, sqlite, and source-code artifacts."
            )
        raise ValueError(f"Input path is not importable: {path}")

    ensure_data_layer(session, layer_key, actor=actor)
    source_format = "directory" if path.is_dir() else infer_source_format(path)
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
                "source_file_count": len(import_paths),
                "ignored_file_count": len(ignored_paths),
                "actor": actor,
            }
        ],
    )
    session.add(run)
    session.flush()

    try:
        parsed = []
        for import_path in import_paths:
            parsed.extend(iter_parsed_observations_for_path(import_path))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Could not parse {source_format} input at '{path}': {exc}") from exc
    except sqlite3.Error as exc:
        raise ValueError(f"Could not parse {source_format} input at '{path}': {exc}") from exc
    except UnicodeDecodeError as exc:
        raise ValueError(f"Could not decode {source_format} input at '{path}': {exc}") from exc
    except OSError as exc:
        raise RuntimeError(f"Could not read {source_format} input at '{path}': {exc}") from exc
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
            source_type="local_import",
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
            "source_file_count": len(import_paths),
            "ignored_file_count": len(ignored_paths),
            "source_paths_sample": [str(item) for item in import_paths[:10]],
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
                "source_path": str(path),
                "source_format": source_format,
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


def infer_source_format(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in IMPORT_JSON_FILE_SUFFIXES:
        return "json"
    if suffix in SOURCE_CODE_LANGUAGE_BY_SUFFIX:
        return "source_code"
    if suffix in IMPORT_TEXT_FILE_SUFFIXES:
        return "txt"
    if suffix in IMPORT_SQLITE_FILE_SUFFIXES:
        return "sqlite"
    return "txt"


def iter_parsed_observations(path: Path, source_format: str) -> list[ParsedObservation]:
    if source_format == "json":
        return parse_json_observations(path)
    if source_format == "sqlite":
        return parse_sqlite_observations(path)
    if source_format == "source_code":
        return parse_source_code_observations(path)
    return parse_text_observations(path)


def iter_parsed_observations_for_path(path: Path) -> list[ParsedObservation]:
    return iter_parsed_observations(path, infer_source_format(path))


def collect_import_paths(path: Path) -> tuple[list[Path], list[Path]]:
    if path.is_file():
        return [path], []
    supported: list[Path] = []
    ignored: list[Path] = []
    for candidate in sorted((item for item in path.rglob("*") if item.is_file()), key=lambda item: str(item).lower()):
        if candidate.suffix.lower() in SUPPORTED_DIRECTORY_IMPORT_SUFFIXES:
            supported.append(candidate)
        else:
            ignored.append(candidate)
    return supported, ignored


def parse_json_observations(path: Path) -> list[ParsedObservation]:
    if path.suffix.lower() == ".jsonl":
        rows: list[ParsedObservation] = []
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if not line.strip():
                continue
            rows.append(
                normalize_payload(
                    json.loads(line),
                    "json",
                    source_path=path,
                    volatile_metadata={"local_source_line_number": line_number},
                )
            )
        return rows
    raw_text = path.read_text(encoding="utf-8")
    payload = json.loads(raw_text)
    if isinstance(payload, dict):
        items = payload.get("items") if isinstance(payload.get("items"), list) else [payload]
    elif isinstance(payload, list):
        items = payload
    else:
        items = [{"value": payload}]
    return [
        normalize_payload(
            item,
            "json",
            source_path=path,
            volatile_metadata={"local_source_record_index": index},
        )
        for index, item in enumerate(items, start=1)
        if isinstance(item, dict)
    ]


def parse_text_observations(path: Path) -> list[ParsedObservation]:
    rows = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        text = line.strip()
        if not text:
            continue
        rows.append(
            normalize_payload(
                {"text": text, "url": text},
                "txt",
                source_path=path,
                volatile_metadata={"local_source_line_number": line_number},
            )
        )
    return rows


def parse_source_code_observations(path: Path) -> list[ParsedObservation]:
    raw_text = path.read_text(encoding="utf-8")
    if not raw_text.strip():
        return []
    lines = raw_text.splitlines()
    payload = {
        "entry_kind": "source_code_file",
        "title": path.name,
        "file_name": path.name,
        "file_extension": path.suffix.lower(),
        "language": infer_source_code_language(path),
        "line_count": len(lines),
        "character_count": len(raw_text),
        "text": raw_text,
    }
    return [normalize_payload(payload, "source_code", source_path=path)]


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
            for row_number, record in enumerate(cursor.fetchall(), start=1):
                payload = dict(zip(columns, record, strict=False))
                payload["table_name"] = table_name
                rows.append(
                    normalize_payload(
                        payload,
                        "sqlite",
                        source_path=path,
                        volatile_metadata={"local_source_row_number": row_number},
                    )
                )
    finally:
        connection.close()
    return rows


def infer_source_code_language(path: Path) -> str:
    return SOURCE_CODE_LANGUAGE_BY_SUFFIX.get(path.suffix.lower(), "source_code")


def normalize_payload(
    payload: dict[str, Any],
    record_format: str,
    *,
    source_path: Path | None = None,
    volatile_metadata: dict[str, Any] | None = None,
) -> ParsedObservation:
    stable_payload = dict(payload)
    if source_path is not None:
        stable_payload.setdefault("local_source_path", str(source_path))
        stable_payload.setdefault("local_source_name", source_path.name)
    content_payload = dict(stable_payload)
    for key, value in (volatile_metadata or {}).items():
        content_payload.setdefault(key, value)
    source_domain = extract_domain(payload)
    content_text = content_payload.get("text") or content_payload.get("title") or json.dumps(content_payload, default=str)
    raw_hash = hashlib.sha256(
        json.dumps(stable_payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    return ParsedObservation(
        record_format=record_format,
        source_domain=source_domain,
        content_text=str(content_text),
        content_json=content_payload,
        location_geojson=extract_location(content_payload),
        raw_hash=raw_hash,
    )


def extract_domain(payload: dict[str, Any]) -> str | None:
    for key in ("url", "source_url", "link", "domain", "page_url", "image_url", "stream_url"):
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


def existing_observation_hashes(
    session: Session,
    layer_key: str,
    hashes: set[str],
) -> set[str]:
    if not hashes:
        return set()
    statement = select(ObservationORM.raw_hash).where(
        ObservationORM.layer_key == layer_key,
        ObservationORM.raw_hash.in_(hashes),
    )
    return set(session.scalars(statement))
