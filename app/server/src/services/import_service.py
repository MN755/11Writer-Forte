from __future__ import annotations

from pathlib import Path

from src.models import LocalImportRunORM
from src.services.ingestion_service import (
    IngestionEnvelope,
    extract_domain,
    extract_location,
    infer_source_format,
    normalize_payload,
    parse_json_envelopes,
    parse_sqlite_envelopes,
    parse_text_envelopes,
    persist_envelopes_as_import_run,
    read_path_as_envelopes,
)


def import_local_path(
    session,
    source_path: str,
    layer_key: str,
    notes: str,
    actor: str = "system",
) -> LocalImportRunORM:
    path = Path(source_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"Input path does not exist: {path}")

    source_format = infer_source_format(path)
    envelopes = read_path_as_envelopes(path, source_format, source_type="local_import")
    return persist_envelopes_as_import_run(
        session,
        source_path=str(path),
        source_format=source_format,
        layer_key=layer_key,
        notes=notes,
        actor=actor,
        source_type="local_import",
        envelopes=envelopes,
    )


__all__ = [
    "IngestionEnvelope",
    "extract_domain",
    "extract_location",
    "import_local_path",
    "infer_source_format",
    "normalize_payload",
    "parse_json_envelopes",
    "parse_sqlite_envelopes",
    "parse_text_envelopes",
    "persist_envelopes_as_import_run",
    "read_path_as_envelopes",
]
