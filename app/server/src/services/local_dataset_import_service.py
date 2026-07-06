from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import select

from src.config.settings import Settings
from src.services.ops_audit_service import init_ops_audit_db, record_provenance_event
from src.services.source_discovery_service import SourceDiscoveryService
from src.source_discovery.db import session_scope
from src.source_discovery.models import SourceContentSnapshotORM, SourceImportRunORM
from src.types.local_import import (
    LocalDatasetImportListResponse,
    LocalDatasetImportRequest,
    LocalDatasetImportResponse,
    LocalDatasetImportRunSummary,
    LocalDatasetImportTableSummary,
)
from src.types.source_discovery import (
    SourceDiscoveryCandidateSeed,
    SourceDiscoveryContentSnapshotRequest,
    SourceDiscoveryMemory,
    SourceDiscoveryScopeHints,
)


LOCAL_IMPORT_CAVEATS = [
    "Local dataset import preserves operator-supplied file contents with provenance but does not prove truth, freshness, or legality of the underlying data.",
    "JSON, TXT, and SQLite imports are schema-light and intentionally bounded for deterministic backend ingestion.",
]


@dataclass
class _ImportedRecord:
    title: str
    text: str
    url_suffix: str
    content_type: str
    normalization_notes: list[str]
    has_geospatial_signal: bool


@dataclass
class _LoadedDataset:
    file_format: str
    records: list[_ImportedRecord]
    tables: list[LocalDatasetImportTableSummary]
    file_sha256: str
    file_size_bytes: int
    loader_caveats: list[str]


class LocalDatasetImportService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._source_discovery = SourceDiscoveryService(settings)

    def import_dataset(self, request: LocalDatasetImportRequest) -> LocalDatasetImportResponse:
        path = Path(request.file_path).expanduser()
        if not path.exists() or not path.is_file():
            raise ValueError(f"Local file not found: {request.file_path}")

        source_id = request.source_id or _generated_source_id(path)
        import_run_id = f"local-import:{_safe_id(source_id)}:{_compact_timestamp(_utc_now())}"
        run = self._create_import_run(import_run_id, source_id, request, status="running")
        try:
            loaded = _load_dataset(path, request)
            memory = self._source_discovery.upsert_candidate(
                SourceDiscoveryCandidateSeed(
                    source_id=source_id,
                    title=request.title or path.stem,
                    url=path.resolve().as_uri(),
                    parent_domain="local-file",
                    source_type=request.source_kind,
                    source_class=request.source_class,
                    lifecycle_state="candidate",
                    source_health="unknown",
                    policy_state="manual_review",
                    access_result="local_file",
                    machine_readable_result="machine_readable",
                    auth_requirement="no_auth",
                    captcha_requirement="no_captcha",
                    intake_disposition="public_no_auth",
                    caveats=[*LOCAL_IMPORT_CAVEATS, *request.caveats],
                    discovery_methods=["local_dataset_import"],
                    structure_hints=["local_file_import", f"format:{loaded.file_format}"],
                    discovery_role="derived",
                    seed_family="other",
                    source_family_tags=sorted(set([loaded.file_format, *request.tags])),
                    scope_hints=SourceDiscoveryScopeHints(
                        spatial=["contains_coordinate_records"] if any(record.has_geospatial_signal for record in loaded.records) else [],
                        language=[],
                        topic=["local_import"],
                    ),
                )
            )
            snapshot_count = 0
            duplicate_snapshot_count = 0
            geospatial_record_count = 0
            seen_snapshot_ids: set[str] = set()
            file_uri = path.resolve().as_uri()
            for record in loaded.records:
                if record.has_geospatial_signal:
                    geospatial_record_count += 1
                snapshot_id = _snapshot_id_for_text(source_id, record.text)
                if snapshot_id in seen_snapshot_ids or self._snapshot_exists(snapshot_id):
                    duplicate_snapshot_count += 1
                    continue
                seen_snapshot_ids.add(snapshot_id)
                self._source_discovery.store_content_snapshot(
                    SourceDiscoveryContentSnapshotRequest(
                        source_id=source_id,
                        url=f"{file_uri}{record.url_suffix}",
                        title=record.title,
                        raw_text=record.text,
                        content_type=record.content_type,
                        retrieval_origin="local",
                        retrieved_from_url=file_uri,
                        resolved_original_url=file_uri,
                        normalization_notes=record.normalization_notes,
                        request_budget=0,
                        caveats=[*LOCAL_IMPORT_CAVEATS, *loaded.loader_caveats, *request.caveats],
                    )
                )
                snapshot_count += 1

            provenance = record_provenance_event(
                self._settings,
                subsystem="local_dataset_import",
                event_kind="local_dataset_import",
                operation="import_dataset",
                status="completed",
                actor=request.requested_by,
                subject_type="source_memory",
                subject_id=memory.source_id,
                source_uri=file_uri,
                input_refs=[file_uri],
                output_refs=[memory.source_id],
                chain_of_custody=[
                    f"format={loaded.file_format}",
                    f"imported_record_count={len(loaded.records)}",
                    f"snapshot_count={snapshot_count}",
                    f"duplicate_snapshot_count={duplicate_snapshot_count}",
                    f"geospatial_record_count={geospatial_record_count}",
                    f"file_sha256={loaded.file_sha256}",
                ],
                metadata={
                    "file_path": str(path.resolve()),
                    "tables": [table.model_dump() for table in loaded.tables],
                    "requested_metadata": request.metadata,
                },
                summary=f"Imported {len(loaded.records)} records from local {loaded.file_format} dataset into {memory.source_id}.",
            )
            finished_run = self._complete_import_run(
                import_run_id,
                source_id=memory.source_id,
                file_format=loaded.file_format,
                file_sha256=loaded.file_sha256,
                file_size_bytes=loaded.file_size_bytes,
                imported_record_count=len(loaded.records),
                snapshot_count=snapshot_count,
                duplicate_snapshot_count=duplicate_snapshot_count,
                geospatial_record_count=geospatial_record_count,
                tables=loaded.tables,
                metadata=request.metadata,
                provenance_event_id=provenance.provenance_event_id,
                caveats=[*LOCAL_IMPORT_CAVEATS, *loaded.loader_caveats, *request.caveats],
            )
            return LocalDatasetImportResponse(
                run=finished_run,
                memory=memory,
                caveats=LOCAL_IMPORT_CAVEATS,
            )
        except Exception as exc:  # noqa: BLE001
            failed_run = self._fail_import_run(import_run_id, error_summary=str(exc), caveats=[*LOCAL_IMPORT_CAVEATS, *request.caveats])
            raise ValueError(f"Local dataset import failed: {failed_run.error_summary}") from exc

    def list_import_runs(self, *, limit: int = 25, source_id: str | None = None) -> LocalDatasetImportListResponse:
        with session_scope(self._settings.source_discovery_database_url) as session:
            stmt = select(SourceImportRunORM).order_by(SourceImportRunORM.started_at.desc(), SourceImportRunORM.import_run_id.desc())
            if source_id:
                stmt = stmt.where(SourceImportRunORM.source_id == source_id)
            rows = list(session.scalars(stmt.limit(max(1, limit))))
            return LocalDatasetImportListResponse(
                count=len(rows),
                runs=[_serialize_import_run(row) for row in rows],
                caveats=LOCAL_IMPORT_CAVEATS,
            )

    def _snapshot_exists(self, snapshot_id: str) -> bool:
        with session_scope(self._settings.source_discovery_database_url) as session:
            return session.get(SourceContentSnapshotORM, snapshot_id) is not None

    def _create_import_run(
        self,
        import_run_id: str,
        source_id: str,
        request: LocalDatasetImportRequest,
        *,
        status: str,
    ) -> LocalDatasetImportRunSummary:
        init_ops_audit_db(self._settings.source_discovery_database_url)
        with session_scope(self._settings.source_discovery_database_url) as session:
            row = SourceImportRunORM(
                import_run_id=import_run_id,
                source_id=source_id,
                file_path=str(Path(request.file_path).expanduser()),
                file_format=request.format,
                source_kind=request.source_kind,
                requested_by=request.requested_by,
                status=status,
                file_sha256=None,
                file_size_bytes=None,
                imported_record_count=0,
                snapshot_count=0,
                duplicate_snapshot_count=0,
                geospatial_record_count=0,
                tables_json=json.dumps([]),
                metadata_json=json.dumps(request.metadata),
                caveats_json=json.dumps([*LOCAL_IMPORT_CAVEATS, *request.caveats]),
                error_summary=None,
                provenance_event_id=None,
                started_at=_utc_now(),
                finished_at=None,
            )
            session.add(row)
            session.flush()
            return _serialize_import_run(row)

    def _complete_import_run(
        self,
        import_run_id: str,
        *,
        source_id: str,
        file_format: str,
        file_sha256: str,
        file_size_bytes: int,
        imported_record_count: int,
        snapshot_count: int,
        duplicate_snapshot_count: int,
        geospatial_record_count: int,
        tables: list[LocalDatasetImportTableSummary],
        metadata: dict[str, object],
        provenance_event_id: str,
        caveats: list[str],
    ) -> LocalDatasetImportRunSummary:
        with session_scope(self._settings.source_discovery_database_url) as session:
            row = session.get(SourceImportRunORM, import_run_id)
            if row is None:
                raise ValueError(f"Unknown import_run_id: {import_run_id}")
            row.source_id = source_id
            row.file_format = file_format
            row.status = "completed"
            row.file_sha256 = file_sha256
            row.file_size_bytes = file_size_bytes
            row.imported_record_count = imported_record_count
            row.snapshot_count = snapshot_count
            row.duplicate_snapshot_count = duplicate_snapshot_count
            row.geospatial_record_count = geospatial_record_count
            row.tables_json = json.dumps([table.model_dump() for table in tables])
            row.metadata_json = json.dumps(metadata)
            row.caveats_json = json.dumps(caveats)
            row.provenance_event_id = provenance_event_id
            row.finished_at = _utc_now()
            session.flush()
            return _serialize_import_run(row)

    def _fail_import_run(
        self,
        import_run_id: str,
        *,
        error_summary: str,
        caveats: list[str],
    ) -> LocalDatasetImportRunSummary:
        with session_scope(self._settings.source_discovery_database_url) as session:
            row = session.get(SourceImportRunORM, import_run_id)
            if row is None:
                raise ValueError(f"Unknown import_run_id: {import_run_id}")
            row.status = "failed"
            row.error_summary = error_summary[:300]
            row.caveats_json = json.dumps(caveats)
            row.finished_at = _utc_now()
            session.flush()
            return _serialize_import_run(row)


def _load_dataset(path: Path, request: LocalDatasetImportRequest) -> _LoadedDataset:
    file_format = _detect_format(path, request.format)
    file_bytes = path.read_bytes()
    file_sha256 = hashlib.sha256(file_bytes).hexdigest()
    file_size_bytes = len(file_bytes)
    if file_format == "json":
        records, caveats = _load_json_records(path, request)
        return _LoadedDataset(file_format=file_format, records=records, tables=[], file_sha256=file_sha256, file_size_bytes=file_size_bytes, loader_caveats=caveats)
    if file_format == "jsonl":
        records, caveats = _load_jsonl_records(path, request)
        return _LoadedDataset(file_format=file_format, records=records, tables=[], file_sha256=file_sha256, file_size_bytes=file_size_bytes, loader_caveats=caveats)
    if file_format == "txt":
        records, caveats = _load_text_records(path, request)
        return _LoadedDataset(file_format=file_format, records=records, tables=[], file_sha256=file_sha256, file_size_bytes=file_size_bytes, loader_caveats=caveats)
    if file_format == "sqlite":
        records, tables, caveats = _load_sqlite_records(path, request)
        return _LoadedDataset(file_format=file_format, records=records, tables=tables, file_sha256=file_sha256, file_size_bytes=file_size_bytes, loader_caveats=caveats)
    raise ValueError(f"Unsupported local dataset format: {file_format}")


def _load_json_records(path: Path, request: LocalDatasetImportRequest) -> tuple[list[_ImportedRecord], list[str]]:
    payload = json.loads(path.read_text(encoding=request.encoding))
    items = _json_items(payload)
    records = [_build_record(item, index=index + 1, content_type="application/json") for index, item in enumerate(items[: max(1, request.max_records)])]
    caveats = []
    if len(items) > request.max_records:
        caveats.append(f"JSON import was bounded to the first {request.max_records} records.")
    return records, caveats


def _load_jsonl_records(path: Path, request: LocalDatasetImportRequest) -> tuple[list[_ImportedRecord], list[str]]:
    records: list[_ImportedRecord] = []
    for index, line in enumerate(path.read_text(encoding=request.encoding).splitlines(), start=1):
        stripped = line.strip()
        if not stripped:
            continue
        records.append(_build_record(json.loads(stripped), index=len(records) + 1, content_type="application/x-ndjson"))
        if len(records) >= max(1, request.max_records):
            break
    caveats = ["JSONL import skips blank lines and respects max_records bounds."]
    return records, caveats


def _load_text_records(path: Path, request: LocalDatasetImportRequest) -> tuple[list[_ImportedRecord], list[str]]:
    lines = [line.strip() for line in path.read_text(encoding=request.encoding).splitlines() if line.strip()]
    if not lines:
        return [], ["Text import found no non-empty lines."]
    if len(lines) == 1:
        return [_ImportedRecord(title=path.stem, text=lines[0], url_suffix="#record-1", content_type="text/plain", normalization_notes=["local_text_single_line"], has_geospatial_signal=_has_geospatial_signal(lines[0]))], []
    limited = lines[: max(1, request.max_records)]
    records = [
        _ImportedRecord(
            title=f"{path.stem} line {index}",
            text=line,
            url_suffix=f"#line-{index}",
            content_type="text/plain",
            normalization_notes=["local_text_line_import"],
            has_geospatial_signal=_has_geospatial_signal(line),
        )
        for index, line in enumerate(limited, start=1)
    ]
    caveats = ["Text import maps each non-empty line to one bounded snapshot."]
    if len(lines) > request.max_records:
        caveats.append(f"Text import was bounded to the first {request.max_records} non-empty lines.")
    return records, caveats


def _load_sqlite_records(
    path: Path,
    request: LocalDatasetImportRequest,
) -> tuple[list[_ImportedRecord], list[LocalDatasetImportTableSummary], list[str]]:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    try:
        available_tables = [
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            ).fetchall()
        ]
        selected_tables = request.table_names or available_tables
        missing_tables = sorted(set(selected_tables) - set(available_tables))
        if missing_tables:
            raise ValueError(f"SQLite tables not found: {', '.join(missing_tables)}")
        records: list[_ImportedRecord] = []
        tables: list[LocalDatasetImportTableSummary] = []
        remaining = max(1, request.max_records)
        for table_name in selected_tables:
            if remaining <= 0:
                break
            rows = connection.execute(f'SELECT * FROM "{table_name}" LIMIT ?', (remaining,)).fetchall()
            tables.append(LocalDatasetImportTableSummary(table_name=table_name, imported_row_count=len(rows)))
            for row_index, row in enumerate(rows, start=1):
                row_dict = {key: row[key] for key in row.keys()}
                records.append(
                    _build_record(
                        row_dict,
                        index=len(records) + 1,
                        content_type="application/vnd.sqlite3.record+json",
                        url_suffix=f"#table-{_safe_id(table_name)}-row-{row_index}",
                        normalization_notes=[f"sqlite_table:{table_name}", f"sqlite_row:{row_index}"],
                        title_prefix=f"{table_name} row",
                    )
                )
                remaining -= 1
                if remaining <= 0:
                    break
        caveats = ["SQLite import reads selected tables as bounded row dictionaries."]
        return records, tables, caveats
    finally:
        connection.close()


def _json_items(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("features", "records", "items", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
    return [payload]


def _build_record(
    value: Any,
    *,
    index: int,
    content_type: str,
    url_suffix: str | None = None,
    normalization_notes: list[str] | None = None,
    title_prefix: str = "record",
) -> _ImportedRecord:
    text = _normalize_import_text(value)
    title = _record_title(value, fallback=f"{title_prefix} {index}")
    return _ImportedRecord(
        title=title,
        text=text,
        url_suffix=url_suffix or f"#record-{index}",
        content_type=content_type,
        normalization_notes=normalization_notes or [],
        has_geospatial_signal=_has_geospatial_signal(value),
    )


def _normalize_import_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    return json.dumps(value, sort_keys=True, ensure_ascii=True, default=str)


def _record_title(value: Any, *, fallback: str) -> str:
    if isinstance(value, dict):
        for key in ("title", "name", "label", "id", "event_id", "record_id"):
            candidate = value.get(key)
            if candidate not in (None, ""):
                return str(candidate)
    return fallback


def _has_geospatial_signal(value: Any) -> bool:
    if isinstance(value, str):
        return bool(_extract_lat_lon_from_text(value))
    if not isinstance(value, dict):
        return False
    if _extract_lat_lon_from_mapping(value) is not None:
        return True
    geometry = value.get("geometry")
    if isinstance(geometry, dict):
        geometry_type = str(geometry.get("type") or "").casefold()
        coordinates = geometry.get("coordinates")
        if geometry_type == "point" and isinstance(coordinates, list) and len(coordinates) >= 2:
            return True
    return False


def _extract_lat_lon_from_mapping(value: dict[str, Any]) -> tuple[float, float] | None:
    lowered = {str(key).casefold(): item for key, item in value.items()}
    lat = _coerce_float(lowered.get("lat") or lowered.get("latitude") or lowered.get("y"))
    lon = _coerce_float(lowered.get("lon") or lowered.get("lng") or lowered.get("longitude") or lowered.get("x"))
    if lat is None or lon is None:
        return None
    if -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0:
        return lat, lon
    return None


def _extract_lat_lon_from_text(value: str) -> tuple[float, float] | None:
    parts = [token.strip(",") for token in value.replace("=", " ").split()]
    floats: list[float] = []
    for part in parts:
        number = _coerce_float(part)
        if number is not None:
            floats.append(number)
        if len(floats) >= 2:
            lat, lon = floats[0], floats[1]
            if -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0:
                return lat, lon
    return None


def _detect_format(path: Path, requested: str) -> str:
    if requested != "auto":
        return requested
    suffix = path.suffix.casefold()
    if suffix in {".json"}:
        return "json"
    if suffix in {".jsonl", ".ndjson"}:
        return "jsonl"
    if suffix in {".txt", ".log"}:
        return "txt"
    if suffix in {".sqlite", ".sqlite3", ".db"}:
        return "sqlite"
    raise ValueError(f"Unable to infer import format from file extension: {path.name}")


def _generated_source_id(path: Path) -> str:
    return f"local-source:{_safe_id(path.stem)}:{_compact_timestamp(_utc_now())}"


def _snapshot_id_for_text(source_id: str, text: str) -> str:
    text_hash = hashlib.sha256(text.strip().encode("utf-8")).hexdigest()
    return f"source-snapshot:{_safe_id(source_id)}:{text_hash[:16]}"


def _serialize_import_run(row: SourceImportRunORM) -> LocalDatasetImportRunSummary:
    tables: list[LocalDatasetImportTableSummary] = []
    for item in _loads_list_of_dicts(row.tables_json):
        table_name = item.get("tableName") or item.get("table_name")
        imported_row_count = item.get("importedRowCount") or item.get("imported_row_count")
        if table_name in (None, ""):
            continue
        tables.append(
            LocalDatasetImportTableSummary(
                table_name=str(table_name),
                imported_row_count=int(imported_row_count or 0),
            )
        )
    return LocalDatasetImportRunSummary(
        import_run_id=row.import_run_id,
        source_id=row.source_id,
        file_path=row.file_path,
        file_format=row.file_format,
        source_kind=row.source_kind,
        requested_by=row.requested_by,
        status=row.status,
        file_sha256=row.file_sha256,
        file_size_bytes=row.file_size_bytes,
        imported_record_count=row.imported_record_count,
        snapshot_count=row.snapshot_count,
        duplicate_snapshot_count=row.duplicate_snapshot_count,
        geospatial_record_count=row.geospatial_record_count,
        tables=tables,
        metadata=_loads_dict(row.metadata_json),
        error_summary=row.error_summary,
        provenance_event_id=row.provenance_event_id,
        started_at=row.started_at,
        finished_at=row.finished_at,
        caveats=_loads_list(row.caveats_json),
    )


def _loads_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return []
    return [str(item) for item in value] if isinstance(value, list) else []


def _loads_dict(raw: str | None) -> dict[str, object]:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _loads_list_of_dicts(raw: str | None) -> list[dict[str, Any]]:
    if not raw:
        return []
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return []
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _coerce_float(value: object) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _compact_timestamp(value: str) -> str:
    return "".join(character for character in value if character.isdigit())[:20]


def _safe_id(value: str) -> str:
    import re

    return re.sub(r"[^a-zA-Z0-9]+", "-", value).strip("-").lower() or "unknown"
