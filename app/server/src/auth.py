"""Small, centralized API-scope dependency for operator-only endpoints.

This project does not yet provide identity authentication.  The header handled here is
therefore an explicit deployment boundary, not proof of a caller's identity.  It keeps
scope classification out of individual routes so a future authenticated principal can
replace this implementation in one place.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, Header, HTTPException, status


@dataclass(frozen=True)
class OperatorPrincipal:
    """The declared operator context used for custody attribution and scope checks."""

    actor: str
    scopes: frozenset[str]


def _parse_scopes(raw_scopes: str | None) -> frozenset[str]:
    return frozenset(
        scope.strip().lower()
        for scope in (raw_scopes or "").split(",")
        if scope.strip()
    )


def get_operator_principal(
    scopes: str | None = Header(default=None, alias="X-ElevenWriter-Scopes"),
    actor: str | None = Header(default=None, alias="X-ElevenWriter-Actor"),
) -> OperatorPrincipal:
    parsed_scopes = _parse_scopes(scopes)
    if not parsed_scopes:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="X-ElevenWriter-Scopes is required for this endpoint.",
        )
    return OperatorPrincipal(actor=(actor or "api_operator").strip()[:80] or "api_operator", scopes=parsed_scopes)


def require_scope(required_scope: str) -> Callable[..., OperatorPrincipal]:
    """Return a reusable FastAPI dependency that denies undeclared scopes by default."""

    normalized_scope = required_scope.strip().lower()
    if not normalized_scope:
        raise ValueError("A non-empty scope is required.")

    def dependency(
        principal: Annotated[OperatorPrincipal, Depends(get_operator_principal)],
    ) -> OperatorPrincipal:
        return scoped_principal(principal, normalized_scope)

    # Keep the public factory for classification/inspection and attach the target scope.
    setattr(dependency, "required_scope", normalized_scope)
    return dependency


def scoped_principal(
    principal: OperatorPrincipal,
    required_scope: str,
) -> OperatorPrincipal:
    """Validate the already-injected principal for a centrally classified scope."""

    normalized_scope = required_scope.strip().lower()
    if normalized_scope not in principal.scopes:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"The '{normalized_scope}' operator scope is required for this endpoint.",
        )
    return principal
