from __future__ import annotations

from dataclasses import dataclass
from hmac import compare_digest

from fastapi import Request

from src.config import Settings, get_settings

PUBLIC_PATHS = {"/health"}
PROTECTED_PREFIXES = ("/api",)


@dataclass(frozen=True)
class RequestAuthResult:
    enforced: bool
    allowed: bool
    status_code: int | None = None
    detail: str | None = None
    subject: str | None = None


def authenticate_request(request: Request) -> RequestAuthResult:
    settings = get_settings()
    return authenticate_request_for_settings(request, settings)


def authenticate_request_for_settings(request: Request, settings: Settings) -> RequestAuthResult:
    path = request.url.path
    if not should_enforce_auth(path, settings):
        return RequestAuthResult(enforced=False, allowed=True)

    if settings.api_auth_misconfigured:
        return RequestAuthResult(
            enforced=True,
            allowed=False,
            status_code=503,
            detail="API auth is required, but ELEVENWRITER_API_KEY is not configured.",
        )

    presented_key = request.headers.get(settings.api_key_header)
    expected_key = settings.api_key or ""
    if presented_key is None or not compare_digest(presented_key, expected_key):
        return RequestAuthResult(
            enforced=True,
            allowed=False,
            status_code=401,
            detail="Invalid or missing API key.",
        )
    return RequestAuthResult(enforced=True, allowed=True, subject="api_key")


def should_enforce_auth(path: str, settings: Settings) -> bool:
    if path in PUBLIC_PATHS:
        return False
    if not settings.api_auth_enabled:
        return False
    if settings.metrics_enabled and path == settings.metrics_path:
        return True
    return any(path.startswith(prefix) for prefix in PROTECTED_PREFIXES)


def build_api_auth_diagnostics() -> dict[str, object]:
    settings = get_settings()
    return {
        "enabled": settings.api_auth_enabled,
        "mode": settings.api_auth_mode,
        "header_name": settings.api_key_header,
        "misconfigured": settings.api_auth_misconfigured,
        "protected_prefixes": list(PROTECTED_PREFIXES),
        "metrics_protected": bool(settings.metrics_enabled and settings.api_auth_enabled),
    }
