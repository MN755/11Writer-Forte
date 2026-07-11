from __future__ import annotations

from xml.etree import ElementTree

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from sqlalchemy.orm import Session

from src.db import get_db
from src.services.watch_feed_service import (
    ensure_local_feed_access, generate_report, get_feed_update, list_activity,
    list_feed_updates, list_reports, archive_watch, serialize_report, watch_state,
)

router = APIRouter(prefix="/v1/watch-feed", tags=["watch-feed"])


@router.get("/updates")
def get_updates(request: Request, watch_id: int | None = None, status: str | None = None, update_type: str | None = None, cursor: str | None = None, limit: int = Query(default=50, ge=1, le=200), view: str = Query(default="operator", pattern="^(operator|public)$"), session: Session = Depends(get_db)) -> dict[str, object]:
    ensure_local_feed_access(request)
    try:
        return list_feed_updates(session, watch_id=watch_id, status=status, update_type=update_type, cursor=cursor, limit=limit, view=view)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/updates/{event_id}")
def get_update(event_id: str, request: Request, view: str = Query(default="operator", pattern="^(operator|public)$"), session: Session = Depends(get_db)) -> dict[str, object]:
    ensure_local_feed_access(request)
    try:
        return get_feed_update(session, event_id, view=view)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/watches/{watch_id}")
def get_watch_state(watch_id: int, request: Request, session: Session = Depends(get_db)) -> dict[str, object]:
    ensure_local_feed_access(request)
    try:
        return watch_state(session, watch_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/watches/{watch_id}/activity")
def get_watch_activity(watch_id: int, request: Request, limit: int = Query(default=100, ge=1, le=500), session: Session = Depends(get_db)) -> list[dict[str, object]]:
    ensure_local_feed_access(request)
    try:
        return list_activity(session, watch_id, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/watches/{watch_id}/reports")
def get_reports(watch_id: int, request: Request, session: Session = Depends(get_db)) -> list[dict[str, object]]:
    ensure_local_feed_access(request)
    try:
        return [serialize_report(report) for report in list_reports(session, watch_id)]
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/watches/{watch_id}/reports")
def create_report(watch_id: int, request: Request, session: Session = Depends(get_db)) -> object:
    ensure_local_feed_access(request)
    try:
        return generate_report(session, watch_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/watches/{watch_id}/archive")
def archive_watch_route(watch_id: int, request: Request, session: Session = Depends(get_db)) -> dict[str, object]:
    ensure_local_feed_access(request)
    try:
        return archive_watch(session, watch_id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/rss", response_class=Response)
def get_public_rss(request: Request, watch_id: int | None = None, limit: int = Query(default=50, ge=1, le=200), session: Session = Depends(get_db)) -> Response:
    ensure_local_feed_access(request)
    feed = list_feed_updates(session, watch_id=watch_id, status=None, update_type=None, cursor=None, limit=limit, view="public")
    root = ElementTree.Element("rss", version="2.0")
    channel = ElementTree.SubElement(root, "channel")
    ElementTree.SubElement(channel, "title").text = "11Writer Forte public watch summaries"
    ElementTree.SubElement(channel, "description").text = "Material cited updates only; non-public artifacts and local paths are excluded."
    ElementTree.SubElement(channel, "link").text = str(request.url_for("get_public_rss"))
    for update in feed["items"]:  # type: ignore[index]
        item = ElementTree.SubElement(channel, "item")
        ElementTree.SubElement(item, "guid").text = update["event_id"]  # type: ignore[index]
        ElementTree.SubElement(item, "title").text = f"{update['watch_slug']}: {update['update_type']}"  # type: ignore[index]
        ElementTree.SubElement(item, "description").text = update["summary"]  # type: ignore[index]
        citation = update["citations"][0]  # type: ignore[index]
        ElementTree.SubElement(item, "link").text = citation["uri"]
    return Response(ElementTree.tostring(root, encoding="unicode", xml_declaration=True), media_type="application/rss+xml")
