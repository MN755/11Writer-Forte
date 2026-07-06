from __future__ import annotations

from typing import Any


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
