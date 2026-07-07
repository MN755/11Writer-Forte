from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db import get_db
from src.models import GeofenceORM
from src.schemas import GeofenceCreate, GeofenceRead
from src.services.geospatial_service import geometry_to_wkt

router = APIRouter(prefix="/geofences", tags=["geofences"])


@router.get("", response_model=list[GeofenceRead])
def list_geofences(session: Session = Depends(get_db)) -> list[GeofenceORM]:
    return list(session.scalars(select(GeofenceORM).order_by(GeofenceORM.name.asc())))


@router.post("", response_model=GeofenceRead)
def create_geofence(payload: GeofenceCreate, session: Session = Depends(get_db)) -> GeofenceORM:
    record = GeofenceORM(
        **payload.model_dump(),
        geometry_wkt=geometry_to_wkt(payload.geometry_geojson),
    )
    session.add(record)
    session.commit()
    session.refresh(record)
    return record
