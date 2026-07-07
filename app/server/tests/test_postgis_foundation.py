from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from src.db import get_session_factory
from src.models import GeofenceORM, ObservationORM
from src.services.geospatial_service import (
    build_bbox_sql_filter,
    build_contains_geometry_sql_filter,
)


def test_spatial_wkt_is_persisted_for_observations_and_geofences(
    client: TestClient,
    tmp_path: Path,
) -> None:
    fixture = tmp_path / "point.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Port position",
                    "url": "https://alpha.example.com/port",
                    "lat": 29.76,
                    "lon": -95.36,
                }
            ]
        ),
        encoding="utf-8",
    )

    client.post("/api/imports/local", json={"source_path": str(fixture), "layer_key": "marine-track"})
    geofence_response = client.post(
        "/api/geofences",
        json={
            "name": "Port polygon",
            "geometry_geojson": {
                "type": "Polygon",
                "coordinates": [[[-96.0, 29.0], [-94.0, 29.0], [-94.0, 31.0], [-96.0, 31.0], [-96.0, 29.0]]],
            },
        },
    )
    assert geofence_response.status_code == 200

    session = get_session_factory()()
    try:
        observation = session.scalar(select(ObservationORM).order_by(ObservationORM.observation_id.asc()))
        geofence = session.scalar(select(GeofenceORM).order_by(GeofenceORM.geofence_id.asc()))
        assert observation is not None
        assert geofence is not None
        assert observation.location_wkt == "POINT (-95.36 29.76)"
        assert geofence.geometry_wkt == "POLYGON ((-96 29, -94 29, -94 31, -96 31, -96 29))"
    finally:
        session.close()


def test_postgis_bbox_filter_compiles_to_spatial_sql() -> None:
    statement = select(ObservationORM).where(
        build_bbox_sql_filter(
            ObservationORM.location_wkt,
            min_lon=-96.0,
            min_lat=29.0,
            max_lon=-94.0,
            max_lat=31.0,
        )
    )
    compiled = str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "ST_Intersects" in compiled
    assert "ST_MakeEnvelope(-96.0, 29.0, -94.0, 31.0, 4326)" in compiled
    assert "ST_GeomFromText(observations.location_wkt, 4326)" in compiled


def test_postgis_contains_filter_compiles_to_spatial_sql() -> None:
    statement = select(ObservationORM).where(
        build_contains_geometry_sql_filter(
            "POLYGON ((-96 29, -94 29, -94 31, -96 31, -96 29))",
            ObservationORM.location_wkt,
        )
    )
    compiled = str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "ST_Contains" in compiled
    assert "POLYGON ((-96 29, -94 29, -94 31, -96 31, -96 29))" in compiled
    assert "ST_GeomFromText(observations.location_wkt, 4326)" in compiled
