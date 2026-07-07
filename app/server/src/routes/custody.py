from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db import get_db
from src.models import CustodyLogORM
from src.schemas import CustodyLogRead

router = APIRouter(prefix="/custody", tags=["custody"])


@router.get("/logs", response_model=list[CustodyLogRead])
def list_custody_logs(session: Session = Depends(get_db)) -> list[CustodyLogORM]:
    statement = select(CustodyLogORM).order_by(CustodyLogORM.created_at.desc())
    return list(session.scalars(statement))
