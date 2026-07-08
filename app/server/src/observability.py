from __future__ import annotations

import json
import logging
from contextvars import ContextVar, Token
from datetime import datetime, timezone
from typing import Any

from src.config import Settings
from src.services.redaction_service import sanitize_for_observability

REQUEST_ID_CONTEXT: ContextVar[str | None] = ContextVar("elevenwriter_request_id", default=None)
LOGGING_HANDLER_NAME = "elevenwriter_json"


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        request_id = REQUEST_ID_CONTEXT.get()
        if request_id:
            payload["request_id"] = request_id

        event = getattr(record, "event", None)
        if isinstance(event, str) and event:
            payload["event"] = event

        fields = getattr(record, "fields", None)
        if isinstance(fields, dict):
            payload.update(sanitize_for_observability(fields))

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str, separators=(",", ":"), sort_keys=True)


def configure_logging(settings: Settings) -> None:
    root_logger = logging.getLogger()
    desired_level = getattr(logging, settings.log_level.upper(), logging.INFO)
    root_logger.setLevel(desired_level)

    handler = next(
        (existing for existing in root_logger.handlers if getattr(existing, "name", "") == LOGGING_HANDLER_NAME),
        None,
    )
    if handler is None:
        handler = logging.StreamHandler()
        handler.name = LOGGING_HANDLER_NAME
        handler.setFormatter(JsonLogFormatter())
        root_logger.addHandler(handler)
    else:
        handler.setFormatter(JsonLogFormatter())


def bind_request_id(request_id: str) -> Token[str | None]:
    return REQUEST_ID_CONTEXT.set(request_id)


def reset_request_id(token: Token[str | None]) -> None:
    REQUEST_ID_CONTEXT.reset(token)


def log_event(logger: logging.Logger, level: int, event: str, **fields: Any) -> None:
    logger.log(level, event, extra={"event": event, "fields": sanitize_for_observability(fields)})
