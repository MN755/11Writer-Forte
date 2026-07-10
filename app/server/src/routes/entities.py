from pathlib import Path

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db import get_db
from src.models import EntityORM, EntityObservationLinkORM
from src.routes.error_helpers import missing_resource_error, translate_service_error
from src.schemas import (
    EntitySummaryArtifactExportRequest,
    EntityInventorySummaryRead,
    EntityObservationLinkRead,
    EntityOpsExportSummaryRead,
    EntityOpsReportIndexRead,
    EntityRead,
    EntityResolutionRequest,
    EntityResolutionResponse,
    EntityResolutionResultRead,
    ExportArtifactWriteResultRead,
)
from src.services.export_artifact_service import persist_typed_json_export_artifact
from src.services.entity_resolution_service import materialize_entities
from src.services.entity_service import (
    build_entity_inventory_summary,
    build_entity_ops_export_summary,
    build_entity_ops_report_index,
    list_entity_records,
)

router = APIRouter(prefix="/entities", tags=["entities"])


@router.get("", response_model=list[EntityRead])
def list_entities(
    entity_type: str | None = None,
    redaction_level: str | None = None,
    min_confidence_score: float | None = None,
    limit: int = 200,
    session: Session = Depends(get_db),
) -> list[EntityORM]:
    return list_entity_records(
        session,
        entity_type=entity_type,
        redaction_level=redaction_level,
        min_confidence_score=min_confidence_score,
        limit=limit,
    )


@router.get("/summary", response_model=EntityInventorySummaryRead)
def entity_summary(
    entity_type: str | None = None,
    redaction_level: str | None = None,
    min_confidence_score: float | None = None,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    return build_entity_inventory_summary(
        session,
        entity_type=entity_type,
        redaction_level=redaction_level,
        min_confidence_score=min_confidence_score,
    )


@router.get("/report-index", response_model=EntityOpsReportIndexRead)
def entity_report_index(
    entity_type: str | None = None,
    redaction_level: str | None = None,
    min_confidence_score: float | None = None,
    limit: int = 25,
    conflict_limit: int = 25,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    return build_entity_ops_report_index(
        session,
        entity_type=entity_type,
        redaction_level=redaction_level,
        min_confidence_score=min_confidence_score,
        limit=limit,
        conflict_limit=conflict_limit,
    )


@router.get("/export/summary", response_model=EntityOpsExportSummaryRead)
def entity_export_summary(
    entity_type: str | None = None,
    redaction_level: str | None = None,
    min_confidence_score: float | None = None,
    entity_limit: int = 500,
    report_limit: int = 25,
    conflict_limit: int = 25,
    session: Session = Depends(get_db),
    ) -> dict[str, object]:
    return build_entity_ops_export_summary(
        session,
        entity_type=entity_type,
        redaction_level=redaction_level,
        min_confidence_score=min_confidence_score,
        entity_limit=entity_limit,
        report_limit=report_limit,
        conflict_limit=conflict_limit,
    )


@router.post("/export/summary/artifact", response_model=ExportArtifactWriteResultRead)
def entity_export_summary_artifact(
    payload: EntitySummaryArtifactExportRequest,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        report = build_entity_ops_export_summary(
            session,
            entity_type=payload.entity_type,
            redaction_level=payload.redaction_level,
            min_confidence_score=payload.min_confidence_score,
            entity_limit=payload.entity_limit,
            report_limit=payload.report_limit,
            conflict_limit=payload.conflict_limit,
        )
        result = persist_typed_json_export_artifact(
            session,
            output_path=Path(payload.output_path),
            payload=report,
            response_model=EntityOpsExportSummaryRead,
            object_kind="entity_summary_export",
            owner_type="entity_export",
            owner_id=str(payload.entity_type or payload.redaction_level or "scoped"),
            source_uri="/api/entities/export/summary",
            observed_at=report["generated_at"],
            metadata_json=report["filters_json"],
            actor="api_export",
        )
    except ValueError as exc:
        raise translate_service_error(
            exc,
            action="entity_export_summary_artifact",
            context={"output_path": payload.output_path},
        ) from exc
    except OSError as exc:
        raise translate_service_error(
            exc,
            action="entity_export_summary_artifact",
            context={"output_path": payload.output_path},
        ) from exc
    return {
        "output_path": result["output_path"],
        "storage_object": result["storage_object"],
    }


@router.post("/resolve", response_model=EntityResolutionResponse)
def resolve_entities(
    payload: EntityResolutionRequest,
    session: Session = Depends(get_db),
) -> EntityResolutionResponse:
    results = materialize_entities(session, payload)
    return EntityResolutionResponse(
        created_entity_count=sum(1 for result in results if result.created_new),
        entity_results=[
            EntityResolutionResultRead(
                entity_id=result.entity.entity_id,
                slug=result.entity.slug,
                entity_type=result.entity.entity_type,
                canonical_name=result.entity.canonical_name,
                observation_count=result.observation_count,
                signal_count=result.signal_count,
                confidence_score=result.confidence_score,
                created_new=result.created_new,
            )
            for result in results
        ],
    )


@router.get("/{entity_id}/observations", response_model=list[EntityObservationLinkRead])
def list_entity_observations(
    entity_id: int,
    session: Session = Depends(get_db),
) -> list[EntityObservationLinkORM]:
    entity = session.get(EntityORM, entity_id)
    if entity is None:
        raise missing_resource_error(
            resource_name="Entity",
            resource_id=entity_id,
            id_field="entity_id",
            action="list_entity_observations",
        )
    statement = select(EntityObservationLinkORM).where(EntityObservationLinkORM.entity_id == entity_id)
    return list(session.scalars(statement.order_by(EntityObservationLinkORM.entity_observation_link_id.asc())))
