from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from src.db import get_db
from src.services.investigation_control_service import (
    assess_investigation_readiness,
    build_investigation_plan,
    evaluate_llm_gate,
    validate_model_assistance,
)

router = APIRouter(prefix="/investigations", tags=["investigation-control"])


class LlmGateRequest(BaseModel):
    telemetry_at: datetime | None = None
    eligible_work_units: int = Field(ge=0)
    llm_work_units: int = Field(ge=0)
    requested_llm_work_units: int = Field(ge=1)
    quota_limit_tokens: int = Field(gt=0)
    used_tokens: int = Field(ge=0)
    estimated_tokens: int = Field(ge=0)
    model: str


class ModelAssistanceRequest(BaseModel):
    response: dict[str, Any]
    evidence_ids: set[str]


@router.get("/{investigation_id}/plan")
def get_deterministic_plan(investigation_id: int, session: Session = Depends(get_db)) -> dict[str, object]:
    try:
        return build_investigation_plan(session, investigation_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/{investigation_id}/readiness")
def get_readiness(investigation_id: int, session: Session = Depends(get_db)) -> dict[str, object]:
    try:
        return assess_investigation_readiness(session, investigation_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/llm-gate")
def gate_llm_work(payload: LlmGateRequest) -> dict[str, object]:
    return evaluate_llm_gate(**payload.model_dump())


@router.post("/validate-model-assistance")
def validate_assistance(payload: ModelAssistanceRequest) -> dict[str, object]:
    try:
        return validate_model_assistance(payload.response, evidence_ids=payload.evidence_ids)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
