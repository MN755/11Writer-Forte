from __future__ import annotations

import json
import sys
from collections.abc import Callable
from typing import Any

from sqlalchemy import func, select

from src.db import get_session_factory, init_db
from src.models import AlertORM, CustodyLogORM, EventORM, ObservationORM, SourceDefinitionORM

SERVER_NAME = "elevenwriter-forte"
PROTOCOL_VERSION = "2025-03-26"
MAX_LIMIT = 100

TOOLS: list[dict[str, Any]] = [
    {
        "name": "forte_runtime_inventory",
        "description": "Return local Forte record counts. Read-only.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "forte_list_sources",
        "description": "List managed sources and their local configuration. Read-only.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "enabled": {"type": "boolean"},
                "limit": {"type": "integer", "minimum": 1, "maximum": MAX_LIMIT},
            },
        },
    },
    {
        "name": "forte_list_alerts",
        "description": "List local rule-generated or analyst alerts. Read-only.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "status": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": MAX_LIMIT},
            },
        },
    },
    {
        "name": "forte_list_events",
        "description": "List local fused and analyst-created events. Read-only.",
        "inputSchema": {
            "type": "object",
            "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": MAX_LIMIT}},
        },
    },
    {
        "name": "forte_list_recent_observations",
        "description": "List recent local observations with clipped evidence text. Read-only.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "layer_key": {"type": "string"},
                "source_domain": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": MAX_LIMIT},
            },
        },
    },
    {
        "name": "forte_list_custody",
        "description": "List chain-of-custody actions for local evidence and operations. Read-only.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "object_type": {"type": "string"},
                "object_id": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": MAX_LIMIT},
            },
        },
    },
]


def bounded_limit(arguments: dict[str, Any]) -> int:
    value = arguments.get("limit", 25)
    if not isinstance(value, int):
        raise ValueError("limit must be an integer.")
    return max(1, min(value, MAX_LIMIT))


def clip(value: str, limit: int = 1200) -> str:
    return value if len(value) <= limit else value[:limit] + "..."


def iso(value: Any) -> str | None:
    return value.isoformat() if value is not None else None


def run_tool(name: str, arguments: dict[str, Any]) -> Any:
    handlers: dict[str, Callable[[dict[str, Any]], Any]] = {
        "forte_runtime_inventory": runtime_inventory,
        "forte_list_sources": list_sources,
        "forte_list_alerts": list_alerts,
        "forte_list_events": list_events,
        "forte_list_recent_observations": list_recent_observations,
        "forte_list_custody": list_custody,
    }
    handler = handlers.get(name)
    if handler is None:
        raise ValueError(f"Unknown tool: {name}")
    return handler(arguments)


def runtime_inventory(_: dict[str, Any]) -> dict[str, int]:
    session = get_session_factory()()
    try:
        return {
            "sources": session.scalar(select(func.count()).select_from(SourceDefinitionORM)) or 0,
            "observations": session.scalar(select(func.count()).select_from(ObservationORM)) or 0,
            "events": session.scalar(select(func.count()).select_from(EventORM)) or 0,
            "alerts": session.scalar(select(func.count()).select_from(AlertORM)) or 0,
        }
    finally:
        session.close()


def list_sources(arguments: dict[str, Any]) -> list[dict[str, Any]]:
    session = get_session_factory()()
    try:
        statement = select(SourceDefinitionORM).order_by(SourceDefinitionORM.updated_at.desc())
        if isinstance(arguments.get("enabled"), bool):
            statement = statement.where(SourceDefinitionORM.enabled == arguments["enabled"])
        rows = session.scalars(statement.limit(bounded_limit(arguments)))
        return [
            {
                "source_id": row.source_id,
                "name": row.name,
                "source_kind": row.source_kind,
                "layer_key": row.layer_key,
                "target_uri": row.target_uri,
                "enabled": row.enabled,
                "integrity_source": row.integrity_source,
                "notes": clip(row.notes, 500),
                "updated_at": iso(row.updated_at),
            }
            for row in rows
        ]
    finally:
        session.close()


def list_alerts(arguments: dict[str, Any]) -> list[dict[str, Any]]:
    session = get_session_factory()()
    try:
        statement = select(AlertORM).order_by(AlertORM.updated_at.desc())
        status = arguments.get("status")
        if isinstance(status, str) and status:
            statement = statement.where(AlertORM.status == status)
        rows = session.scalars(statement.limit(bounded_limit(arguments)))
        return [
            {
                "alert_id": row.alert_id,
                "event_id": row.event_id,
                "geofence_id": row.geofence_id,
                "severity": row.severity,
                "status": row.status,
                "message": clip(row.message),
                "created_at": iso(row.created_at),
                "updated_at": iso(row.updated_at),
            }
            for row in rows
        ]
    finally:
        session.close()


def list_events(arguments: dict[str, Any]) -> list[dict[str, Any]]:
    session = get_session_factory()()
    try:
        rows = session.scalars(
            select(EventORM).order_by(EventORM.updated_at.desc()).limit(bounded_limit(arguments))
        )
        return [
            {
                "event_id": row.event_id,
                "slug": row.slug,
                "title": row.title,
                "summary": clip(row.summary),
                "status": row.status,
                "occurred_at": iso(row.occurred_at),
                "updated_at": iso(row.updated_at),
            }
            for row in rows
        ]
    finally:
        session.close()


def list_recent_observations(arguments: dict[str, Any]) -> list[dict[str, Any]]:
    session = get_session_factory()()
    try:
        statement = select(ObservationORM).order_by(ObservationORM.updated_at.desc())
        for key in ("layer_key", "source_domain"):
            value = arguments.get(key)
            if isinstance(value, str) and value:
                statement = statement.where(getattr(ObservationORM, key) == value)
        rows = session.scalars(statement.limit(bounded_limit(arguments)))
        return [
            {
                "observation_id": row.observation_id,
                "event_id": row.event_id,
                "layer_key": row.layer_key,
                "source_domain": row.source_domain,
                "source_type": row.source_type,
                "trust_level": row.trust_level,
                "confidence_score": row.confidence_score,
                "content_text": clip(row.content_text),
                "content_json": row.content_json,
                "updated_at": iso(row.updated_at),
            }
            for row in rows
        ]
    finally:
        session.close()


def list_custody(arguments: dict[str, Any]) -> list[dict[str, Any]]:
    session = get_session_factory()()
    try:
        statement = select(CustodyLogORM).order_by(CustodyLogORM.created_at.desc())
        for key in ("object_type", "object_id"):
            value = arguments.get(key)
            if isinstance(value, str) and value:
                statement = statement.where(getattr(CustodyLogORM, key) == value)
        rows = session.scalars(statement.limit(bounded_limit(arguments)))
        return [
            {
                "custody_log_id": row.custody_log_id,
                "object_type": row.object_type,
                "object_id": row.object_id,
                "action": row.action,
                "actor": row.actor,
                "details_json": row.details_json,
                "created_at": iso(row.created_at),
            }
            for row in rows
        ]
    finally:
        session.close()


def handle_message(request: dict[str, Any]) -> dict[str, Any] | None:
    request_id = request.get("id")
    method = request.get("method")
    if method == "notifications/initialized":
        return None
    if method == "initialize":
        return result_response(
            request_id,
            {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": SERVER_NAME, "version": "0.1.0"},
            },
        )
    if method == "tools/list":
        return result_response(request_id, {"tools": TOOLS})
    if method == "tools/call":
        params = request.get("params") or {}
        try:
            tool_result = run_tool(str(params.get("name", "")), params.get("arguments") or {})
            return result_response(
                request_id,
                {"content": [{"type": "text", "text": json.dumps(tool_result, default=str)}]},
            )
        except ValueError as exc:
            return result_response(
                request_id,
                {"content": [{"type": "text", "text": str(exc)}], "isError": True},
            )
    if request_id is None:
        return None
    return error_response(request_id, -32601, f"Method not found: {method}")


def result_response(request_id: Any, result: dict[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def error_response(request_id: Any, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}


def main() -> None:
    init_db()
    for raw_line in sys.stdin:
        try:
            response = handle_message(json.loads(raw_line))
            if response is not None:
                sys.stdout.write(json.dumps(response) + "\n")
                sys.stdout.flush()
        except json.JSONDecodeError:
            continue


if __name__ == "__main__":
    main()
