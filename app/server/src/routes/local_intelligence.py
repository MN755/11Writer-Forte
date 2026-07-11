"""Operator-facing approval, benchmark, and artifact-only local inference API."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from fastapi import APIRouter, HTTPException

from src.config import get_settings
from src.services.local_intelligence_service import LocalIntelligenceError, LocalIntelligenceRuntime, ModelRegistry

router = APIRouter(prefix="/local-intelligence", tags=["local-intelligence"])


def _registry() -> ModelRegistry:
    return ModelRegistry(get_settings().data_dir)


@router.get("/health")
def health() -> dict[str, Any]:
    manifests = _registry().list()
    return {
        "status": "ok" if manifests else "blocked_no_approved_models",
        "registry_version": 1,
        "approved_model_count": len(manifests),
        "fixture_inference": "rejected",
        "runtime_network": "blocked_during_inference",
    }


@router.get("/models")
def list_models() -> list[dict[str, Any]]:
    try:
        return [asdict(manifest) for manifest in _registry().list()]
    except LocalIntelligenceError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/models/approve", status_code=201)
def approve_model(payload: dict[str, Any]) -> dict[str, Any]:
    try:
        return asdict(_registry().approve(payload))
    except (LocalIntelligenceError, TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/infer")
def infer(payload: dict[str, Any]) -> dict[str, Any]:
    allowed = {"artifact_id", "model_id", "model_version", "prefer_gpu"}
    forbidden = sorted(set(payload) - allowed)
    if forbidden:
        raise HTTPException(status_code=422, detail=f"Unsupported runtime fields: {', '.join(forbidden)}.")
    try:
        record = LocalIntelligenceRuntime(get_settings().data_dir).run(
            artifact_id=str(payload.get("artifact_id", "")),
            model_id=str(payload.get("model_id", "")),
            model_version=str(payload["model_version"]) if payload.get("model_version") is not None else None,
            prefer_gpu=bool(payload.get("prefer_gpu", True)),
        )
        return asdict(record)
    except (LocalIntelligenceError, TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
