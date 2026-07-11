from __future__ import annotations

import secrets
from dataclasses import dataclass
from typing import Literal

from fastapi import Request

from src.config import OperatorScope, Settings


RequiredScope = Literal["read", "operate", "admin"]
_SCOPE_RANK: dict[RequiredScope, int] = {"read": 1, "operate": 2, "admin": 3}
_ADMIN_PATHS = {
    "/api/operations/runtime/restore",
    "/api/operations/runtime/bundle/restore",
    "/api/operations/clickhouse/provision",
    "/api/operations/clickhouse/archive",
    "/api/operations/clickhouse/rehydrate",
    "/api/storage/sweep",
}


@dataclass(frozen=True)
class OperatorPrincipal:
    principal_id: str
    scopes: frozenset[OperatorScope]

    def allows(self, required_scope: RequiredScope) -> bool:
        return max((_SCOPE_RANK[scope] for scope in self.scopes), default=0) >= _SCOPE_RANK[
            required_scope
        ]


def required_scope_for_request(request: Request) -> RequiredScope:
    if request.url.path in _ADMIN_PATHS:
        return "admin"
    if request.method.upper() in {"GET", "HEAD", "OPTIONS"}:
        return "read"
    return "operate"


def authenticate_operator(request: Request, settings: Settings) -> OperatorPrincipal | None:
    """Authenticate a bearer token without persisting or returning the secret."""
    if settings.auth_mode == "disabled":
        return OperatorPrincipal("local-operator", frozenset({"admin"}))

    scheme, separator, token = request.headers.get("authorization", "").partition(" ")
    if scheme.lower() != "bearer" or not separator or not token:
        return None

    for principal_id, credential in settings.configured_operator_tokens().items():
        if secrets.compare_digest(token, credential.token.get_secret_value()):
            return OperatorPrincipal(principal_id, frozenset(credential.scopes))
    return None
