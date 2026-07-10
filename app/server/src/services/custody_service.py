from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import CustodyLogORM


def list_custody_logs(
    session: Session,
    *,
    object_type: str | None = None,
    object_id: str | None = None,
    action: str | None = None,
    actor: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = 200,
) -> list[CustodyLogORM]:
    statement = select(CustodyLogORM)
    if object_type:
        statement = statement.where(CustodyLogORM.object_type == object_type)
    if object_id:
        statement = statement.where(CustodyLogORM.object_id == object_id)
    if action:
        statement = statement.where(CustodyLogORM.action == action)
    if actor:
        statement = statement.where(CustodyLogORM.actor == actor)
    if since is not None:
        statement = statement.where(CustodyLogORM.created_at >= since)
    if until is not None:
        statement = statement.where(CustodyLogORM.created_at <= until)
    statement = statement.order_by(
        CustodyLogORM.created_at.desc(),
        CustodyLogORM.custody_log_id.desc(),
    ).limit(max(1, min(limit, 5000)))
    return list(session.scalars(statement))
