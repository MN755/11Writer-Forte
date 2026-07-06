from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import select

from src.config.settings import Settings
from src.reference.geometry import haversine_distance_m, point_in_polygon
from src.reference.service import ReferenceService
from src.services.ops_audit_service import init_ops_audit_db, record_provenance_event, upsert_alert_record
from src.source_discovery.db import session_scope
from src.source_discovery.models import GeofenceEvaluationORM, GeofenceORM
from src.types.entities import ReferenceObjectSummary
from src.types.geofence import (
    GeofenceCreateRequest,
    GeofenceDetail,
    GeofenceEvaluationListResponse,
    GeofenceEvaluationRequest,
    GeofenceEvaluationResponse,
    GeofenceEvaluationSummary,
    GeofenceListResponse,
    GeofenceSummary,
)


GEOFENCE_CAVEATS = [
    "Geofence evaluations are deterministic spatial checks over saved perimeter geometry and do not prove intent, causation, or complete source coverage.",
    "Reference-object context is a bounded spatial aid and may omit relevant non-reference data layers.",
]


class GeofenceService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._reference = ReferenceService(settings.reference_database_url)

    def list_geofences(self, *, enabled_only: bool = False) -> GeofenceListResponse:
        with session_scope(self._settings.source_discovery_database_url) as session:
            stmt = select(GeofenceORM).order_by(GeofenceORM.updated_at.desc(), GeofenceORM.geofence_id.asc())
            if enabled_only:
                stmt = stmt.where(GeofenceORM.enabled.is_(True))
            rows = list(session.scalars(stmt))
            return GeofenceListResponse(
                count=len(rows),
                geofences=[_serialize_geofence_summary(row) for row in rows],
                caveats=GEOFENCE_CAVEATS,
            )

    def get_geofence(self, geofence_id: str) -> GeofenceDetail:
        with session_scope(self._settings.source_discovery_database_url) as session:
            row = session.get(GeofenceORM, geofence_id)
            if row is None:
                raise ValueError(f"Unknown geofence_id: {geofence_id}")
            return _serialize_geofence_detail(row)

    def create_geofence(self, request: GeofenceCreateRequest) -> GeofenceDetail:
        init_ops_audit_db(self._settings.source_discovery_database_url)
        now = _utc_now()
        shape = _normalized_shape(request)
        with session_scope(self._settings.source_discovery_database_url) as session:
            existing = session.get(GeofenceORM, request.geofence_id)
            if existing is not None:
                raise ValueError(f"geofence_id already exists: {request.geofence_id}")
            row = GeofenceORM(
                geofence_id=request.geofence_id,
                name=request.name,
                description=request.description,
                shape_kind=request.shape_kind,
                redaction_level=request.redaction_level,
                enabled=request.enabled,
                min_lat=shape["min_lat"],
                min_lon=shape["min_lon"],
                max_lat=shape["max_lat"],
                max_lon=shape["max_lon"],
                center_lat=shape["center_lat"],
                center_lon=shape["center_lon"],
                radius_m=shape["radius_m"],
                geometry_json=shape["geometry_json"],
                tags_json=json.dumps(sorted({tag.strip() for tag in request.tags if tag.strip()})),
                metadata_json=json.dumps(request.metadata),
                created_by=request.created_by,
                created_at=now,
                updated_at=now,
                caveats_json=json.dumps(GEOFENCE_CAVEATS),
            )
            session.add(row)
            session.flush()
            record_provenance_event(
                self._settings,
                subsystem="geofences",
                event_kind="geofence_create",
                operation="create_geofence",
                status="completed",
                actor=request.created_by,
                subject_type="geofence",
                subject_id=row.geofence_id,
                summary=f"Created geofence {row.geofence_id}.",
                output_refs=[row.geofence_id],
                chain_of_custody=[
                    f"shape_kind={row.shape_kind}",
                    f"redaction_level={row.redaction_level}",
                    f"enabled={row.enabled}",
                ],
                metadata={"name": row.name, "tags": _loads_list(row.tags_json)},
                session=session,
            )
            session.flush()
            return _serialize_geofence_detail(row)

    def evaluate_point(self, geofence_id: str, request: GeofenceEvaluationRequest) -> GeofenceEvaluationResponse:
        init_ops_audit_db(self._settings.source_discovery_database_url)
        with session_scope(self._settings.source_discovery_database_url) as session:
            geofence = session.get(GeofenceORM, geofence_id)
            if geofence is None:
                raise ValueError(f"Unknown geofence_id: {geofence_id}")
            observed_at = request.observed_at or _utc_now()
            matched, match_method, distance_to_center_m, evaluation_caveats = _match_geofence(geofence, request.lat, request.lon)
            reference_matches = _reference_context(self._reference, geofence, object_types=request.reference_object_types, limit=request.reference_limit)
            now = _utc_now()
            evaluation = GeofenceEvaluationORM(
                evaluation_id=(
                    f"geofence-eval:{_compact_timestamp(now)}:"
                    f"{geofence.geofence_id.replace(':', '-')[:32]}:{uuid4().hex[:8]}"
                ),
                geofence_id=geofence.geofence_id,
                subject_type=request.subject_type,
                subject_id=request.subject_id,
                observation_label=request.observation_label,
                observed_lat=request.lat,
                observed_lon=request.lon,
                observed_at=observed_at,
                matched=matched,
                match_method=match_method,
                distance_to_center_m=distance_to_center_m,
                reference_match_count=len(reference_matches),
                matched_reference_ref_ids_json=json.dumps([item.ref_id for item in reference_matches]),
                alert_id=None,
                provenance_event_id=None,
                metadata_json=json.dumps(request.metadata),
                caveats_json=json.dumps(evaluation_caveats),
                created_at=now,
            )
            session.add(evaluation)
            session.flush()
            provenance = record_provenance_event(
                self._settings,
                subsystem="geofences",
                event_kind="geofence_evaluation",
                operation="evaluate_point",
                status="completed" if matched else "skipped",
                actor=request.requested_by,
                subject_type=request.subject_type,
                subject_id=request.subject_id or geofence.geofence_id,
                summary=(
                    f"Observation matched geofence {geofence.geofence_id}."
                    if matched
                    else f"Observation did not match geofence {geofence.geofence_id}."
                ),
                input_refs=[geofence.geofence_id],
                output_refs=[evaluation.evaluation_id],
                evidence_refs=[item.ref_id for item in reference_matches],
                chain_of_custody=[
                    f"observed_lat={request.lat}",
                    f"observed_lon={request.lon}",
                    f"match_method={match_method}",
                    f"reference_match_count={len(reference_matches)}",
                ],
                metadata={"matched": matched, "observation_label": request.observation_label},
                session=session,
            )
            evaluation.provenance_event_id = provenance.provenance_event_id
            if matched:
                alert = upsert_alert_record(
                    self._settings,
                    dedupe_key=f"geofence-hit:{geofence.geofence_id}:{request.subject_type}:{request.subject_id or 'anonymous'}",
                    subsystem="geofences",
                    alert_type="geofence_match",
                    severity="medium",
                    status="open",
                    title=f"Geofence matched: {geofence.name}",
                    summary=request.observation_label or f"Observation entered geofence {geofence.name}.",
                    subject_type=request.subject_type,
                    subject_id=request.subject_id or geofence.geofence_id,
                    source_event_id=provenance.provenance_event_id,
                    evidence_refs=[item.ref_id for item in reference_matches],
                    metadata={"geofence_id": geofence.geofence_id, "match_method": match_method},
                    caveats=GEOFENCE_CAVEATS,
                    session=session,
                )
                evaluation.alert_id = alert.alert_id
            session.flush()
            return GeofenceEvaluationResponse(
                geofence=_serialize_geofence_summary(geofence),
                evaluation=_serialize_evaluation(evaluation, reference_matches),
                caveats=GEOFENCE_CAVEATS,
            )

    def list_evaluations(self, geofence_id: str, *, limit: int = 25) -> GeofenceEvaluationListResponse:
        with session_scope(self._settings.source_discovery_database_url) as session:
            if session.get(GeofenceORM, geofence_id) is None:
                raise ValueError(f"Unknown geofence_id: {geofence_id}")
            rows = list(
                session.scalars(
                    select(GeofenceEvaluationORM)
                    .where(GeofenceEvaluationORM.geofence_id == geofence_id)
                    .order_by(GeofenceEvaluationORM.created_at.desc(), GeofenceEvaluationORM.evaluation_id.desc())
                    .limit(max(1, limit))
                )
            )
            return GeofenceEvaluationListResponse(
                count=len(rows),
                evaluations=[_serialize_evaluation(row, []) for row in rows],
                caveats=GEOFENCE_CAVEATS,
            )


def _normalized_shape(request: GeofenceCreateRequest) -> dict[str, object]:
    if request.shape_kind == "bbox":
        if None in {request.min_lat, request.min_lon, request.max_lat, request.max_lon}:
            raise ValueError("bbox geofences require min/max latitude and longitude.")
        min_lat = min(float(request.min_lat), float(request.max_lat))
        max_lat = max(float(request.min_lat), float(request.max_lat))
        min_lon = min(float(request.min_lon), float(request.max_lon))
        max_lon = max(float(request.min_lon), float(request.max_lon))
        return {
            "min_lat": min_lat,
            "min_lon": min_lon,
            "max_lat": max_lat,
            "max_lon": max_lon,
            "center_lat": (min_lat + max_lat) / 2,
            "center_lon": (min_lon + max_lon) / 2,
            "radius_m": None,
            "geometry_json": None,
        }
    if request.shape_kind == "circle":
        if None in {request.center_lat, request.center_lon, request.radius_m}:
            raise ValueError("circle geofences require center latitude, center longitude, and radius_m.")
        radius_m = float(request.radius_m)
        if radius_m <= 0:
            raise ValueError("circle geofence radius_m must be positive.")
        lat_delta = radius_m / 111_320.0
        lon_delta = radius_m / max(1e-9, 111_320.0 * abs(math.cos(math.radians(float(request.center_lat)))))
        return {
            "min_lat": float(request.center_lat) - lat_delta,
            "min_lon": float(request.center_lon) - lon_delta,
            "max_lat": float(request.center_lat) + lat_delta,
            "max_lon": float(request.center_lon) + lon_delta,
            "center_lat": float(request.center_lat),
            "center_lon": float(request.center_lon),
            "radius_m": radius_m,
            "geometry_json": None,
        }
    if request.shape_kind == "polygon":
        if len(request.polygon_points) < 3:
            raise ValueError("polygon geofences require at least three [lon, lat] points.")
        points = [[float(point[0]), float(point[1])] for point in request.polygon_points]
        if points[0] != points[-1]:
            points.append([points[0][0], points[0][1]])
        lons = [point[0] for point in points]
        lats = [point[1] for point in points]
        geometry_json = json.dumps({"type": "Polygon", "coordinates": [points]})
        return {
            "min_lat": min(lats),
            "min_lon": min(lons),
            "max_lat": max(lats),
            "max_lon": max(lons),
            "center_lat": (min(lats) + max(lats)) / 2,
            "center_lon": (min(lons) + max(lons)) / 2,
            "radius_m": None,
            "geometry_json": geometry_json,
        }
    raise ValueError(f"Unsupported shape_kind: {request.shape_kind}")


def _match_geofence(geofence: GeofenceORM, lat: float, lon: float) -> tuple[bool, str, float | None, list[str]]:
    if geofence.shape_kind == "bbox":
        matched = bool(
            geofence.min_lat is not None
            and geofence.max_lat is not None
            and geofence.min_lon is not None
            and geofence.max_lon is not None
            and geofence.min_lat <= lat <= geofence.max_lat
            and geofence.min_lon <= lon <= geofence.max_lon
        )
        return matched, "bbox", None, []
    if geofence.shape_kind == "circle":
        if None in {geofence.center_lat, geofence.center_lon, geofence.radius_m}:
            return False, "circle", None, ["Circle geofence is missing center or radius geometry."]
        distance = haversine_distance_m(lat, lon, float(geofence.center_lat), float(geofence.center_lon))
        return distance <= float(geofence.radius_m), "circle", distance, []
    if geofence.shape_kind == "polygon":
        return point_in_polygon(lat, lon, geofence.geometry_json), "polygon", None, []
    return False, geofence.shape_kind, None, ["Unknown geofence shape kind prevented evaluation."]


def _reference_context(
    reference: ReferenceService,
    geofence: GeofenceORM,
    *,
    object_types: list[str],
    limit: int,
) -> list[ReferenceObjectSummary]:
    if geofence.shape_kind == "circle" and None not in {geofence.center_lat, geofence.center_lon, geofence.radius_m}:
        response = reference.nearby(
            lat=float(geofence.center_lat),
            lon=float(geofence.center_lon),
            radius_m=float(geofence.radius_m),
            object_types=object_types or None,
            limit=max(1, limit),
        )
        return [item.summary for item in response.results]
    if None not in {geofence.min_lat, geofence.min_lon, geofence.max_lat, geofence.max_lon}:
        response = reference.in_bounds(
            lamin=float(geofence.min_lat),
            lamax=float(geofence.max_lat),
            lomin=float(geofence.min_lon),
            lomax=float(geofence.max_lon),
            object_types=object_types or None,
            limit=max(1, limit),
        )
        return response.results
    return []


def _serialize_geofence_summary(row: GeofenceORM) -> GeofenceSummary:
    return GeofenceSummary(
        geofence_id=row.geofence_id,
        name=row.name,
        shape_kind=row.shape_kind,  # type: ignore[arg-type]
        redaction_level=row.redaction_level,  # type: ignore[arg-type]
        enabled=row.enabled,
        created_by=row.created_by,
        created_at=row.created_at,
        updated_at=row.updated_at,
        description=row.description,
        min_lat=row.min_lat,
        min_lon=row.min_lon,
        max_lat=row.max_lat,
        max_lon=row.max_lon,
        center_lat=row.center_lat,
        center_lon=row.center_lon,
        radius_m=row.radius_m,
        tags=_loads_list(row.tags_json),
    )


def _serialize_geofence_detail(row: GeofenceORM) -> GeofenceDetail:
    return GeofenceDetail(
        **_serialize_geofence_summary(row).model_dump(),
        geometry_json=row.geometry_json,
        metadata=_loads_dict(row.metadata_json),
        caveats=_loads_list(row.caveats_json),
    )


def _serialize_evaluation(
    row: GeofenceEvaluationORM,
    reference_matches: list[ReferenceObjectSummary],
) -> GeofenceEvaluationSummary:
    return GeofenceEvaluationSummary(
        evaluation_id=row.evaluation_id,
        geofence_id=row.geofence_id,
        subject_type=row.subject_type,
        subject_id=row.subject_id,
        observation_label=row.observation_label,
        observed_lat=row.observed_lat,
        observed_lon=row.observed_lon,
        observed_at=row.observed_at,
        matched=row.matched,
        match_method=row.match_method,
        distance_to_center_m=row.distance_to_center_m,
        reference_match_count=row.reference_match_count,
        matched_reference_objects=reference_matches,
        alert_id=row.alert_id,
        provenance_event_id=row.provenance_event_id,
        metadata=_loads_dict(row.metadata_json),
        caveats=_loads_list(row.caveats_json),
    )


def _loads_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return []
    return [str(item) for item in value] if isinstance(value, list) else []


def _loads_dict(raw: str | None) -> dict[str, object]:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _compact_timestamp(value: str) -> str:
    return "".join(character for character in value if character.isdigit())[:20]
