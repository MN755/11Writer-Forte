from __future__ import annotations

from typing import Any

from sqlalchemy import and_, func
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import ColumnElement


SRID = 4326


def point_in_geometry(point: tuple[float, float], geometry: dict[str, Any]) -> bool:
    geometry_type = geometry.get("type")
    coordinates = geometry.get("coordinates")
    if geometry_type == "Point" and isinstance(coordinates, list) and len(coordinates) >= 2:
        return tuple(coordinates[:2]) == point
    if geometry_type == "Polygon" and isinstance(coordinates, list):
        return point_in_polygon(point, coordinates)
    if geometry_type == "MultiPolygon" and isinstance(coordinates, list):
        return any(point_in_polygon(point, polygon) for polygon in coordinates)
    return False


def point_in_polygon(point: tuple[float, float], polygon: list[list[list[float]]]) -> bool:
    if not polygon:
        return False
    if not ray_cast(point, polygon[0]):
        return False
    for hole in polygon[1:]:
        if ray_cast(point, hole):
            return False
    return True


def ray_cast(point: tuple[float, float], ring: list[list[float]]) -> bool:
    if len(ring) < 3:
        return False
    x, y = point
    inside = False
    previous = ring[-1]
    for current in ring:
        x1, y1 = previous[:2]
        x2, y2 = current[:2]
        intersects = (y1 > y) != (y2 > y)
        if intersects:
            slope_x = (x2 - x1) * (y - y1) / ((y2 - y1) or 1e-12) + x1
            if x < slope_x:
                inside = not inside
        previous = current
    return inside


def uses_postgis(session: Session) -> bool:
    bind = session.get_bind()
    return bind.dialect.name == "postgresql"


def geometry_to_wkt(geometry: dict[str, Any] | None) -> str | None:
    if not isinstance(geometry, dict):
        return None
    geometry_type = geometry.get("type")
    coordinates = geometry.get("coordinates")
    if geometry_type == "Point" and isinstance(coordinates, list) and len(coordinates) >= 2:
        lon = format_wkt_number(float(coordinates[0]))
        lat = format_wkt_number(float(coordinates[1]))
        return f"POINT ({lon} {lat})"
    if geometry_type == "Polygon" and isinstance(coordinates, list):
        rings = [ring_to_wkt(ring) for ring in coordinates if isinstance(ring, list) and ring]
        if not rings:
            return None
        return f"POLYGON ({', '.join(rings)})"
    if geometry_type == "MultiPolygon" and isinstance(coordinates, list):
        polygons = []
        for polygon in coordinates:
            if not isinstance(polygon, list):
                continue
            rings = [ring_to_wkt(ring) for ring in polygon if isinstance(ring, list) and ring]
            if rings:
                polygons.append(f"({', '.join(rings)})")
        if not polygons:
            return None
        return f"MULTIPOLYGON ({', '.join(polygons)})"
    return None


def ring_to_wkt(ring: list[list[float]]) -> str:
    points = []
    for point in ring:
        if not isinstance(point, list) or len(point) < 2:
            continue
        lon = format_wkt_number(float(point[0]))
        lat = format_wkt_number(float(point[1]))
        points.append(f"{lon} {lat}")
    return f"({', '.join(points)})"


def format_wkt_number(value: float) -> str:
    return f"{value:.12g}"


def build_bbox_sql_filter(
    location_wkt_column: ColumnElement[str | None],
    *,
    min_lon: float,
    min_lat: float,
    max_lon: float,
    max_lat: float,
) -> ColumnElement[bool]:
    return and_(
        location_wkt_column.is_not(None),
        func.ST_Intersects(
            func.ST_GeomFromText(location_wkt_column, SRID),
            func.ST_MakeEnvelope(min_lon, min_lat, max_lon, max_lat, SRID),
        ),
    )


def build_contains_geometry_sql_filter(
    geometry_wkt: str,
    location_wkt_column: ColumnElement[str | None],
) -> ColumnElement[bool]:
    return and_(
        location_wkt_column.is_not(None),
        func.ST_Contains(
            func.ST_GeomFromText(geometry_wkt, SRID),
            func.ST_GeomFromText(location_wkt_column, SRID),
        ),
    )
