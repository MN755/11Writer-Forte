"""Explicit, local-only media intake, inference, and visual-change API contract."""

from __future__ import annotations

import base64
import binascii
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
import shutil
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from src.config import get_settings
from src.db import get_db
from src.schemas import (
    InferenceRequest,
    MediaDerivativeRequest,
    MediaIntakeRequest,
    OfflineTranscriptionRequest,
    TesseractOcrRequest,
    VisualChangeRequest,
    WebImageReferenceRequest,
)
from src.services.artifact_store_service import ArtifactOwner, ArtifactStore
from src.services.local_media_runtime_service import (
    LocalMediaRuntimeError,
    generate_image_derivatives,
    generate_video_derivatives,
    transcribe_offline,
    tesseract_ocr_offline,
)
from src.services.media_intake_service import MediaIntakeService
from src.services.storage_service import register_storage_object
from src.services.visual_change_service import (
    LocalInferenceAdapter,
    MaterialVisualChangePipeline,
    ModelManifest,
    VisualObservation,
)

router = APIRouter(prefix="/media-intelligence", tags=["media-intelligence"])


def _media_service() -> MediaIntakeService:
    return MediaIntakeService(get_settings().data_dir / "media_intake")


def _artifact_store() -> ArtifactStore:
    return ArtifactStore(get_settings().data_dir / "artifact_store")


@router.get("/health")
def media_storage_health() -> dict[str, Any]:
    """Expose disk-pressure controls before intake turns the machine into a brick."""
    root = get_settings().data_dir
    root.mkdir(parents=True, exist_ok=True)
    usage = shutil.disk_usage(root)
    free_ratio = usage.free / usage.total if usage.total else 0.0
    if free_ratio < 0.10:
        status, action = "critical", "pause_noncritical_sources"
    elif free_ratio < 0.20:
        status, action = "warning", "lower_sampling_retain_features_over_bytes"
    else:
        status, action = "ok", "normal_collection"
    return {
        "status": status,
        "recommended_action": action,
        "data_dir": str(root.resolve()),
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "free_ratio": round(free_ratio, 6),
        "policy": {
            "evidence_originals": "retain",
            "non_evidence_bytes": "tombstone_before_collection",
            "features": "retain_before_non_evidence_bytes",
        },
    }


@router.post("/derivatives")
def generate_derivatives(payload: MediaDerivativeRequest) -> dict[str, Any]:
    """Create registered derivative artifacts; originals remain immutable evidence."""
    store = _artifact_store()
    try:
        parent = store.get_artifact(payload.artifact_uid)
        source = store.root / parent.blob_path
        output_dir = store.root / "transforms" / parent.artifact_uid / payload.derivative_kind
        paths = (
            generate_image_derivatives(source, output_dir)
            if payload.derivative_kind == "image"
            else generate_video_derivatives(source, output_dir)
        )
        derivatives = {
            name: asdict(
                store.ingest_file(
                    path,
                    role="derived",
                    owner=ArtifactOwner("artifact", parent.artifact_uid, "derived_from"),
                    original_uri=parent.original_uri,
                    media_type="image/jpeg" if path.endswith((".jpg", ".jpeg")) else "application/json" if path.endswith(".json") else "video/mp4",
                    retention_class=parent.retention_class,
                    transform_chain=[f"{payload.derivative_kind}:{name}"],
                    provenance={"parent_artifact_uid": parent.artifact_uid},
                    parent_artifact_uid=parent.artifact_uid,
                    custody_actor="api_media_intelligence",
                )
            )
            for name, path in paths.items()
        }
        return {"parent_artifact_uid": parent.artifact_uid, "derivatives": derivatives}
    except (LocalMediaRuntimeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/transcribe")
def transcribe_local_audio(payload: OfflineTranscriptionRequest) -> dict[str, Any]:
    """Run a checksum-approved local Whisper model with runtime networking disabled."""
    try:
        _assert_under_data_dir(payload.audio_path)
        _assert_under_data_dir(payload.approval_path)
        return asdict(
            transcribe_offline(
                payload.audio_path,
                approval_path=payload.approval_path,
                prefer_gpu=payload.prefer_gpu,
            )
        )
    except LocalMediaRuntimeError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/ocr")
def ocr_local_image(payload: TesseractOcrRequest) -> dict[str, Any]:
    """Run the approved local Tesseract LSTM engine over a data-dir image."""
    try:
        _assert_under_data_dir(payload.image_path)
        _assert_under_data_dir(payload.approval_path)
        return asdict(
            tesseract_ocr_offline(
                payload.image_path,
                approval_path=payload.approval_path,
                language=payload.language,
                page_segmentation_mode=payload.page_segmentation_mode,
            )
        )
    except LocalMediaRuntimeError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/intake")
def intake_media(payload: MediaIntakeRequest, session: Session = Depends(get_db)) -> dict[str, Any]:
    """Ingest already-acquired bytes; this endpoint never fetches a supplied URL."""
    try:
        raw = base64.b64decode(payload.payload_base64, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise HTTPException(status_code=422, detail="payload_base64 must be valid base64") from exc

    result = _media_service().ingest_bytes(
        raw,
        claimed_mime=payload.claimed_mime,
        source_uri=payload.source_uri,
        source_page_uri=payload.source_page_uri,
    )
    response = result.to_dict()
    if not result.accepted:
        return response

    record = _artifact_store().ingest_bytes(
        raw,
        role="raw",
        owner=ArtifactOwner(payload.owner_type, payload.owner_id),
        media_type=result.media_type,
        original_uri=payload.source_uri,
        retention_class=payload.retention_class,
        transform_chain=[str(step.get("step", "unknown")) for step in result.transform_chain],
        provenance={
            "source_page_uri": payload.source_page_uri,
            "media_intake_sha256": result.sha256,
            "media_intake_blake2b_256": result.blake2b_256,
            "metadata": result.metadata,
        },
        custody_actor="api_media_intelligence",
    )
    storage_object = register_storage_object(
        session,
        object_key=f"artifact:{record.artifact_uid}",
        object_kind=f"media_{result.media_kind}",
        owner_type=payload.owner_type,
        owner_id=payload.owner_id,
        object_uri=str(_artifact_store().root / record.blob_path),
        source_uri=payload.source_uri,
        content_hash=record.sha256,
        media_type=result.media_type,
        retention_class=payload.retention_class,
        byte_size=record.byte_size,
        metadata_json={
            "artifact_uid": record.artifact_uid,
            "artifact_role": record.role,
            "blake3": record.blake3,
            "blake3_algorithm": record.blake3_algorithm,
            "perceptual_hash": result.perceptual_hash,
            "duplicate_of": result.duplicate_of,
            "near_duplicate_of": result.near_duplicate_of,
            "transform_chain": list(result.transform_chain),
        },
        actor="api_media_intelligence",
    )
    session.commit()
    response["artifact"] = asdict(record)
    response["artifact"]["owner_references"] = [asdict(owner) for owner in record.owner_references]
    response["storage_object_id"] = storage_object.storage_object_id
    return response


@router.post("/web-image-reference")
def validate_web_image_reference(payload: WebImageReferenceRequest) -> dict[str, Any]:
    """Validate/canonicalize a public image/page reference without fetching either URL."""
    try:
        return _media_service().ingest_web_image_reference(
            payload.image_url, source_page_url=payload.source_page_url
        ).to_dict()
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/inference")
def run_local_inference(payload: InferenceRequest) -> dict[str, Any]:
    """Run a security-gated local adapter contract. Runtime model downloads are impossible here."""
    try:
        manifest = ModelManifest(**payload.model_manifest)
        record = LocalInferenceAdapter(manifest).infer(
            artifact_id=payload.artifact_id,
            input_features=payload.input_features,
            prefer_gpu=payload.prefer_gpu,
        )
        return asdict(record)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/visual-change")
def score_visual_change(payload: VisualChangeRequest) -> dict[str, Any]:
    """Classify local visual evidence and return a bounded review packet when warranted."""
    try:
        observation = _visual_observation(payload.observation)
        baseline = _visual_observation(payload.baseline) if payload.baseline else None
        candidate = MaterialVisualChangePipeline().evaluate(
            observation,
            baseline=baseline,
            historical=[_visual_observation(item) for item in payload.historical],
            llm_quota_available=payload.llm_quota_available,
            confirmed=payload.confirmed,
        )
        return asdict(candidate)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _visual_observation(value: dict[str, Any]) -> VisualObservation:
    normalized = dict(value)
    captured = normalized.get("capture_time")
    if isinstance(captured, str):
        normalized["capture_time"] = datetime.fromisoformat(captured.replace("Z", "+00:00"))
    for key in (
        "named_locations",
        "target_location_names",
        "scene_embedding",
        "object_labels",
        "keyword_hits",
        "derived_asset_ids",
    ):
        if key in normalized and isinstance(normalized[key], list):
            normalized[key] = tuple(normalized[key])
    return VisualObservation(**normalized)


def _assert_under_data_dir(value: str) -> None:
    path, root = Path(value).resolve(), get_settings().data_dir.resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise LocalMediaRuntimeError("Local model and audio paths must be inside data_dir.") from exc
