from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db import get_db
from src.models import EventORM, EventObservationLinkORM, SituationProductORM
from src.routes.error_helpers import missing_resource_error, translate_service_error
from src.schemas import (
    DataLayerCreate,
    DataLayerRead,
    EventSummaryArtifactExportRequest,
    EventCreate,
    EventExportBundleRead,
    EventFusionRequest,
    EventFusionResponse,
    EventFusionResultRead,
    EventInventorySummaryRead,
    EventObservationLinkRead,
    EventOpsExportSummaryRead,
    EventOpsReportIndexRead,
    EventRead,
    ExportArtifactWriteResultRead,
    SituationProductRead,
)
from src.services.export_artifact_service import persist_typed_json_export_artifact
from src.services.event_export_service import build_event_export_bundle
from src.services.event_fusion_service import materialize_fused_events
from src.services.event_service import (
    build_event_inventory_summary,
    build_event_ops_export_summary,
    build_event_ops_report_index,
    list_event_records,
)
from src.services.layer_service import create_data_layer, list_data_layers
from src.services.redaction_service import enforce_export_redaction, filter_records_by_redaction_level

router = APIRouter(prefix="/events", tags=["events"])
layer_router = APIRouter(prefix="/layers", tags=["layers"])


@router.get("", response_model=list[EventRead])
def list_events(
    status: str | None = None,
    redaction_level: str | None = None,
    limit: int = 200,
    session: Session = Depends(get_db),
) -> list[EventORM]:
    return list_event_records(
        session,
        status=status,
        redaction_level=redaction_level,
        limit=limit,
    )


@router.get("/summary", response_model=EventInventorySummaryRead)
def event_summary(
    status: str | None = None,
    redaction_level: str | None = None,
    stale_after_hours: float = 24.0,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    return build_event_inventory_summary(
        session,
        status=status,
        redaction_level=redaction_level,
        stale_after_hours=stale_after_hours,
    )


@router.get("/report-index", response_model=EventOpsReportIndexRead)
def event_report_index(
    status: str | None = None,
    redaction_level: str | None = None,
    stale_after_hours: float = 24.0,
    limit: int = 25,
    stale_event_limit: int = 25,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    return build_event_ops_report_index(
        session,
        status=status,
        redaction_level=redaction_level,
        stale_after_hours=stale_after_hours,
        limit=limit,
        stale_event_limit=stale_event_limit,
    )


@router.get("/export/summary", response_model=EventOpsExportSummaryRead)
def event_export_summary(
    status: str | None = None,
    redaction_level: str | None = None,
    stale_after_hours: float = 24.0,
    event_limit: int = 500,
    report_limit: int = 25,
    stale_event_limit: int = 25,
    session: Session = Depends(get_db),
    ) -> dict[str, object]:
    return build_event_ops_export_summary(
        session,
        status=status,
        redaction_level=redaction_level,
        stale_after_hours=stale_after_hours,
        event_limit=event_limit,
        report_limit=report_limit,
        stale_event_limit=stale_event_limit,
    )


@router.post("/export/summary/artifact", response_model=ExportArtifactWriteResultRead)
def event_export_summary_artifact(
    payload: EventSummaryArtifactExportRequest,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        report = build_event_ops_export_summary(
            session,
            status=payload.status,
            redaction_level=payload.redaction_level,
            stale_after_hours=payload.stale_after_hours,
            event_limit=payload.event_limit,
            report_limit=payload.report_limit,
            stale_event_limit=payload.stale_event_limit,
        )
        result = persist_typed_json_export_artifact(
            session,
            output_path=Path(payload.output_path),
            payload=report,
            response_model=EventOpsExportSummaryRead,
            object_kind="event_summary_export",
            owner_type="event_export",
            owner_id=str(payload.status or payload.redaction_level or "scoped"),
            source_uri="/api/events/export/summary",
            observed_at=report["generated_at"],
            metadata_json=report["filters_json"],
            actor="api_export",
        )
    except ValueError as exc:
        raise translate_service_error(
            exc,
            action="event_export_summary_artifact",
            context={"output_path": payload.output_path},
        ) from exc
    except OSError as exc:
        raise translate_service_error(
            exc,
            action="event_export_summary_artifact",
            context={"output_path": payload.output_path},
        ) from exc
    return {
        "output_path": result["output_path"],
        "storage_object": result["storage_object"],
    }


@router.post("", response_model=EventRead)
def create_event(payload: EventCreate, session: Session = Depends(get_db)) -> EventORM:
    record = EventORM(**payload.model_dump())
    session.add(record)
    session.commit()
    session.refresh(record)
    return record


@router.post("/fuse", response_model=EventFusionResponse)
def fuse_events(payload: EventFusionRequest, session: Session = Depends(get_db)) -> EventFusionResponse:
    results = materialize_fused_events(session, payload)
    return EventFusionResponse(
        created_event_count=sum(1 for result in results if result.created_new),
        event_results=[
            EventFusionResultRead(
                event_id=result.event.event_id,
                slug=result.event.slug,
                title=result.event.title,
                observation_count=result.observation_count,
                product_count=result.product_count,
                verification_score=result.verification_score,
            )
            for result in results
        ],
    )


@router.get("/{event_id}/observations", response_model=list[EventObservationLinkRead])
def list_event_observations(event_id: int, session: Session = Depends(get_db)) -> list[EventObservationLinkORM]:
    event = session.get(EventORM, event_id)
    if event is None:
        raise missing_resource_error(
            resource_name="Event",
            resource_id=event_id,
            id_field="event_id",
            action="list_event_observations",
        )
    statement = select(EventObservationLinkORM).where(EventObservationLinkORM.event_id == event_id)
    return list(session.scalars(statement.order_by(EventObservationLinkORM.event_observation_link_id.asc())))


@router.get("/{event_id}/products", response_model=list[SituationProductRead])
def list_event_products(
    event_id: int,
    max_redaction_level: str | None = None,
    session: Session = Depends(get_db),
) -> list[SituationProductORM]:
    event = session.get(EventORM, event_id)
    if event is None:
        raise missing_resource_error(
            resource_name="Event",
            resource_id=event_id,
            id_field="event_id",
            action="list_event_products",
        )
    try:
        enforce_export_redaction(event, max_redaction_level)
    except ValueError as exc:
        raise translate_service_error(
            exc,
            action="list_event_products",
            context={"event_id": event_id, "max_redaction_level": max_redaction_level},
            forbidden_tokens=("cannot be exported",),
        ) from exc
    statement = select(SituationProductORM).where(SituationProductORM.event_id == event_id)
    rows = list(session.scalars(statement.order_by(SituationProductORM.product_id.asc())))
    if max_redaction_level is None:
        return rows
    return filter_records_by_redaction_level(rows, max_redaction_level)


@router.get("/{event_id}/export", response_model=EventExportBundleRead)
def export_event_bundle(
    event_id: int,
    max_redaction_level: str | None = None,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        return build_event_export_bundle(session, event_id, max_redaction_level=max_redaction_level)
    except ValueError as exc:
        raise translate_service_error(
            exc,
            action="export_event_bundle",
            context={"event_id": event_id, "max_redaction_level": max_redaction_level},
            forbidden_tokens=("cannot be exported",),
        ) from exc


@layer_router.get("", response_model=list[DataLayerRead])
def list_layers(session: Session = Depends(get_db)) -> list[object]:
    return list_data_layers(session)


@layer_router.post("", response_model=DataLayerRead)
def create_layer(payload: DataLayerCreate, session: Session = Depends(get_db)) -> object:
    try:
        return create_data_layer(session, payload, actor="api")
    except ValueError as exc:
        raise translate_service_error(
            exc,
            action="create_layer",
            context={"layer_key": payload.key},
        ) from exc
