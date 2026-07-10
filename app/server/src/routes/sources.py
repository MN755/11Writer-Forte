from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session

from src.db import get_db
from src.routes.error_helpers import translate_service_error
from src.schemas import (
    ExportArtifactWriteResultRead,
    SourceCheckpointRead,
    SourceDeadLetterRead,
    SourceDefinitionCreate,
    SourceDefinitionRead,
    SourceInventorySummaryRead,
    SourceOpsExportSummaryRead,
    SourceOpsReportIndexRead,
    SourceSummaryArtifactExportRequest,
    SourceDefinitionUpdate,
    SourceOpsDetailRead,
    SourceRunRead,
    WebSourceRunRequest,
    WebSourceRunResponse,
    WebSearchProviderRead,
)
from src.services.export_artifact_service import persist_typed_json_export_artifact
from src.services.source_service import (
    SourceExecutionError,
    build_source_inventory_summary,
    build_source_ops_detail,
    build_source_ops_export_summary,
    build_source_ops_report_index,
    create_source_definition,
    ingest_webhook_payload,
    list_supported_web_search_providers,
    list_source_checkpoints,
    list_source_dead_letters,
    list_source_definitions,
    list_source_runs,
    replay_dead_letter_record,
    run_source_definition,
    run_source_runtime_cycle,
    upsert_and_run_source_definition,
    update_source_definition,
)

router = APIRouter(prefix="/sources", tags=["sources"])


@router.get("", response_model=list[SourceDefinitionRead])
def list_sources(session: Session = Depends(get_db)) -> list[object]:
    return list_source_definitions(session)


@router.get("/web-search/providers", response_model=list[WebSearchProviderRead])
def list_web_search_providers() -> list[dict[str, object]]:
    return list_supported_web_search_providers()


@router.post("/web/run", response_model=WebSourceRunResponse)
def run_web_source_now(payload: WebSourceRunRequest, session: Session = Depends(get_db)) -> dict[str, object]:
    try:
        return upsert_and_run_source_definition(
            session,
            SourceDefinitionCreate(
                name=payload.name,
                source_kind=payload.source_kind,
                layer_key=payload.layer_key,
                target_uri=payload.target_uri,
                enabled=payload.enabled,
                integrity_source=payload.integrity_source,
                notes=payload.notes,
                metadata_json=payload.metadata_json,
            ),
            actor="api_web_run",
            allow_update=payload.upsert_existing,
        )
    except ValueError as exc:
        raise translate_source_error(exc) from exc
    except SourceExecutionError as exc:
        raise translate_source_execution_error(exc) from exc


@router.post("", response_model=SourceDefinitionRead)
def create_source(payload: SourceDefinitionCreate, session: Session = Depends(get_db)) -> object:
    try:
        return create_source_definition(session, payload)
    except ValueError as exc:
        raise translate_source_error(exc) from exc


@router.patch("/{source_id}", response_model=SourceDefinitionRead)
def update_source(source_id: int, payload: SourceDefinitionUpdate, session: Session = Depends(get_db)) -> object:
    try:
        return update_source_definition(session, source_id, payload)
    except ValueError as exc:
        raise translate_source_error(exc) from exc


@router.get("/runs", response_model=list[SourceRunRead])
def list_runs(session: Session = Depends(get_db)) -> list[object]:
    return list_source_runs(session)


@router.get("/checkpoints", response_model=list[SourceCheckpointRead])
def source_checkpoints(session: Session = Depends(get_db)) -> list[object]:
    return list_source_checkpoints(session)


@router.get("/dead-letters", response_model=list[SourceDeadLetterRead])
def source_dead_letters(
    source_id: int | None = None,
    status: str | None = None,
    limit: int = Query(default=200, ge=1, le=1000),
    session: Session = Depends(get_db),
) -> list[object]:
    return list_source_dead_letters(session, source_id=source_id, status=status, limit=limit)


@router.get("/summary", response_model=SourceInventorySummaryRead)
def source_summary(
    stale_after_hours: float = 24.0,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    return build_source_inventory_summary(session, stale_after_hours=stale_after_hours)


@router.get("/report-index", response_model=SourceOpsReportIndexRead)
def source_report_index(
    stale_after_hours: float = 24.0,
    limit: int = 25,
    stale_source_limit: int = 25,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    return build_source_ops_report_index(
        session,
        stale_after_hours=stale_after_hours,
        limit=limit,
        stale_source_limit=stale_source_limit,
    )


@router.get("/export/summary", response_model=SourceOpsExportSummaryRead)
def source_export_summary(
    stale_after_hours: float = 24.0,
    source_limit: int = 500,
    report_limit: int = 25,
    stale_source_limit: int = 25,
    session: Session = Depends(get_db),
    ) -> dict[str, object]:
    return build_source_ops_export_summary(
        session,
        stale_after_hours=stale_after_hours,
        source_limit=source_limit,
        report_limit=report_limit,
        stale_source_limit=stale_source_limit,
    )


@router.post("/export/summary/artifact", response_model=ExportArtifactWriteResultRead)
def source_export_summary_artifact(
    payload: SourceSummaryArtifactExportRequest,
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        report = build_source_ops_export_summary(
            session,
            stale_after_hours=payload.stale_after_hours,
            source_limit=payload.source_limit,
            report_limit=payload.report_limit,
            stale_source_limit=payload.stale_source_limit,
        )
        result = persist_typed_json_export_artifact(
            session,
            output_path=Path(payload.output_path),
            payload=report,
            response_model=SourceOpsExportSummaryRead,
            object_kind="source_summary_export",
            owner_type="source_export",
            owner_id="scoped",
            source_uri="/api/sources/export/summary",
            observed_at=report["generated_at"],
            metadata_json=report["filters_json"],
            actor="api_export",
        )
    except ValueError as exc:
        raise translate_service_error(
            exc,
            action="source_export_summary_artifact",
            context={"output_path": payload.output_path},
        ) from exc
    except OSError as exc:
        raise translate_service_error(
            exc,
            action="source_export_summary_artifact",
            context={"output_path": payload.output_path},
        ) from exc
    return {
        "output_path": result["output_path"],
        "storage_object": result["storage_object"],
    }


@router.get("/{source_id}/ops", response_model=SourceOpsDetailRead)
def source_ops(source_id: int, session: Session = Depends(get_db)) -> dict[str, object]:
    try:
        return build_source_ops_detail(session, source_id)
    except ValueError as exc:
        raise translate_source_error(exc) from exc


@router.post("/{source_id}/run", response_model=SourceRunRead)
def run_source(source_id: int, session: Session = Depends(get_db)) -> object:
    try:
        return run_source_definition(session, source_id)
    except ValueError as exc:
        raise translate_source_error(exc) from exc
    except SourceExecutionError as exc:
        raise translate_source_execution_error(exc) from exc


@router.post("/{source_id}/runtime", response_model=dict[str, object])
def run_source_runtime(source_id: int, session: Session = Depends(get_db)) -> dict[str, object]:
    try:
        return run_source_runtime_cycle(session, source_id=source_id)
    except ValueError as exc:
        raise translate_source_error(exc) from exc
    except SourceExecutionError as exc:
        raise translate_source_execution_error(exc) from exc


@router.post("/{source_id}/webhook", response_model=SourceRunRead)
async def source_webhook(
    source_id: int,
    request: Request,
    session: Session = Depends(get_db),
) -> object:
    try:
        payload = await request.body()
        return ingest_webhook_payload(
            session,
            source_id,
            payload=payload,
            content_type=request.headers.get("Content-Type"),
        )
    except ValueError as exc:
        raise translate_source_error(exc) from exc
    except SourceExecutionError as exc:
        raise translate_source_execution_error(exc) from exc


@router.post("/dead-letters/{source_dead_letter_id}/replay", response_model=SourceDeadLetterRead)
def replay_source_dead_letter_route(
    source_dead_letter_id: int,
    session: Session = Depends(get_db),
) -> object:
    try:
        return replay_dead_letter_record(session, source_dead_letter_id)
    except ValueError as exc:
        raise translate_source_error(exc) from exc


def translate_source_error(exc: ValueError) -> HTTPException:
    detail = str(exc)
    status_code = 404 if "does not exist" in detail else 409
    return HTTPException(status_code=status_code, detail=detail)


def translate_source_execution_error(exc: SourceExecutionError) -> HTTPException:
    cause = exc.cause
    if isinstance(cause, FileNotFoundError):
        status_code = 404
    elif isinstance(cause, ValueError):
        status_code = 409
    elif isinstance(cause, (RuntimeError, OSError)):
        status_code = 502
    else:
        status_code = 500
    return HTTPException(
        status_code=status_code,
        detail={
            "message": str(cause),
            "source_id": exc.source_id,
            "source_run_id": exc.source_run_id,
            "source_kind": exc.source_kind,
            "error_type": cause.__class__.__name__,
        },
    )
