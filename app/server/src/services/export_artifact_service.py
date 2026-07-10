from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from src.services.storage_service import register_export_storage_object


def write_json_export_artifact(
    session: Session,
    *,
    output_path: Path,
    payload: dict[str, Any],
    object_kind: str,
    owner_type: str,
    owner_id: str,
    source_uri: str | None = None,
    observed_at: datetime | None = None,
    metadata_json: dict[str, Any] | None = None,
    actor: str = "cli_export",
) -> object:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    record = register_export_storage_object(
        session,
        object_kind=object_kind,
        owner_type=owner_type,
        owner_id=owner_id,
        output_path=output_path,
        source_uri=source_uri,
        observed_at=observed_at,
        metadata_json=metadata_json,
        actor=actor,
    )
    session.commit()
    session.refresh(record)
    return record


def write_text_export_artifact(
    session: Session,
    *,
    output_path: Path,
    body_text: str,
    object_kind: str,
    owner_type: str,
    owner_id: str,
    source_uri: str | None = None,
    observed_at: datetime | None = None,
    metadata_json: dict[str, Any] | None = None,
    actor: str = "cli_export",
) -> object:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(body_text, encoding="utf-8")
    record = register_export_storage_object(
        session,
        object_kind=object_kind,
        owner_type=owner_type,
        owner_id=owner_id,
        output_path=output_path,
        source_uri=source_uri,
        observed_at=observed_at,
        metadata_json=metadata_json,
        actor=actor,
    )
    session.commit()
    session.refresh(record)
    return record


def write_binary_export_artifact(
    session: Session,
    *,
    output_path: Path,
    payload: bytes,
    object_kind: str,
    owner_type: str,
    owner_id: str,
    source_uri: str | None = None,
    observed_at: datetime | None = None,
    metadata_json: dict[str, Any] | None = None,
    actor: str = "cli_export",
) -> object:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(payload)
    record = register_export_storage_object(
        session,
        object_kind=object_kind,
        owner_type=owner_type,
        owner_id=owner_id,
        output_path=output_path,
        source_uri=source_uri,
        observed_at=observed_at,
        metadata_json=metadata_json,
        actor=actor,
    )
    session.commit()
    session.refresh(record)
    return record
