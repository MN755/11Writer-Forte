from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import CustodyLogORM, GeofenceORM
from src.schemas import GeofenceCreate
from src.services.geospatial_service import geometry_to_wkt


def list_geofences(session: Session) -> list[GeofenceORM]:
    statement = select(GeofenceORM).order_by(GeofenceORM.name.asc(), GeofenceORM.geofence_id.asc())
    return list(session.scalars(statement))


def create_geofence(
    session: Session,
    payload: GeofenceCreate,
    *,
    actor: str = "system",
) -> GeofenceORM:
    record = GeofenceORM(
        **payload.model_dump(),
        geometry_wkt=geometry_to_wkt(payload.geometry_geojson),
    )
    session.add(record)
    session.flush()
    session.add(
        CustodyLogORM(
            object_type="geofence",
            object_id=str(record.geofence_id),
            action="geofence_created",
            actor=actor,
            details_json={
                **payload.model_dump(),
                "geofence_id": record.geofence_id,
                "geometry_wkt": record.geometry_wkt,
            },
        )
    )
    session.commit()
    session.refresh(record)
    return record
