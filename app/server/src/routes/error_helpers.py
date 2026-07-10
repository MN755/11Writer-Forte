from __future__ import annotations

from collections.abc import Iterable

from fastapi import HTTPException

DEFAULT_NOT_FOUND_TOKENS = ("does not exist",)


def structured_http_error(
    *,
    status_code: int,
    message: str,
    action: str,
    error_type: str,
    context: dict[str, object] | None = None,
) -> HTTPException:
    detail = {
        "message": message,
        "action": action,
        "error_type": error_type,
    }
    if context:
        detail.update({key: value for key, value in context.items() if value is not None})
    return HTTPException(status_code=status_code, detail=detail)


def missing_resource_error(
    *,
    resource_name: str,
    resource_id: object,
    id_field: str,
    action: str,
    context: dict[str, object] | None = None,
) -> HTTPException:
    detail_context = {id_field: resource_id}
    if context:
        detail_context.update(context)
    return structured_http_error(
        status_code=404,
        message=f"{resource_name} {resource_id} does not exist.",
        action=action,
        error_type="ValueError",
        context=detail_context,
    )


def translate_service_error(
    exc: Exception,
    *,
    action: str,
    context: dict[str, object] | None = None,
    value_error_status: int = 409,
    runtime_status: int = 502,
    not_found_tokens: Iterable[str] = DEFAULT_NOT_FOUND_TOKENS,
    forbidden_tokens: Iterable[str] = (),
) -> HTTPException:
    message = str(exc)
    if isinstance(exc, FileNotFoundError) or contains_any_token(message, not_found_tokens):
        status_code = 404
    elif isinstance(exc, PermissionError) or contains_any_token(message, forbidden_tokens):
        status_code = 403
    elif isinstance(exc, ValueError):
        status_code = value_error_status
    elif isinstance(exc, (RuntimeError, OSError)):
        status_code = runtime_status
    else:
        status_code = 500
    return structured_http_error(
        status_code=status_code,
        message=message,
        action=action,
        error_type=exc.__class__.__name__,
        context=context,
    )


def contains_any_token(message: str, tokens: Iterable[str]) -> bool:
    lowered = message.lower()
    return any(token.lower() in lowered for token in tokens if token)
