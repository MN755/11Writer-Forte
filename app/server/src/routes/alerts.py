from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db import get_db
from src.models import AlertORM
from src.schemas import AlertCreate, AlertRead

router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("", response_model=list[AlertRead])
def list_alerts(session: Session = Depends(get_db)) -> list[AlertORM]:
    return list(session.scalars(select(AlertORM).order_by(AlertORM.created_at.desc())))


@router.post("", response_model=AlertRead)
def create_alert(payload: AlertCreate, session: Session = Depends(get_db)) -> AlertORM:
    record = AlertORM(**payload.model_dump())
    session.add(record)
    session.commit()
    session.refresh(record)
    return record

