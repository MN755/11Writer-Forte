from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from src.db import get_db
from src.schemas import GeofenceCreate, GeofenceRead
from src.services.geofence_service import create_geofence, list_geofences

router = APIRouter(prefix="/geofences", tags=["geofences"])


@router.get("", response_model=list[GeofenceRead])
def get_geofences(session: Session = Depends(get_db)) -> list[GeofenceRead]:
    return list_geofences(session)


@router.post("", response_model=GeofenceRead)
def post_geofence(payload: GeofenceCreate, session: Session = Depends(get_db)) -> GeofenceRead:
    return create_geofence(session, payload, actor="api_geofence")
