"""Operator endpoint for an isolated, pre-admitted browser capture worker."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, HTTPException

from src.config import get_settings
from src.services.browser_collection_service import (
    BrowserCollectionError,
    browser_collection_status,
    collect_browser_capture,
)


router = APIRouter(prefix="/browser-collection", tags=["browser-collection"])


@router.get("/health")
def health() -> dict[str, object]:
    return browser_collection_status()


@router.post("/collect", status_code=201)
def collect(payload: dict[str, Any] = Body(...)) -> dict[str, object]:
    admission = payload.get("admission")
    if not isinstance(admission, dict):
        raise HTTPException(status_code=422, detail="A policy-admitted admission object is required.")
    if set(payload) != {"admission"}:
        raise HTTPException(status_code=422, detail="Only the admission field is accepted.")
    try:
        return collect_browser_capture(admission=admission, data_dir=get_settings().data_dir).to_dict()
    except BrowserCollectionError as exc:
        status_code = 503 if "unavailable" in type(exc).__name__.lower() else 422
        raise HTTPException(status_code=status_code, detail=str(exc)) from exc
