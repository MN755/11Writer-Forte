from datetime import datetime

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from src.db import get_db
from src.schemas import CustodyLogRead
from src.services.custody_service import list_custody_logs

router = APIRouter(prefix="/custody", tags=["custody"])


@router.get("/logs", response_model=list[CustodyLogRead])
def custody_logs(
    object_type: str | None = None,
    object_id: str | None = None,
    action: str | None = None,
    actor: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = Query(default=200, ge=1, le=5000),
    session: Session = Depends(get_db),
) -> list[object]:
    return list_custody_logs(
        session,
        object_type=object_type,
        object_id=object_id,
        action=action,
        actor=actor,
        since=since,
        until=until,
        limit=limit,
    )
