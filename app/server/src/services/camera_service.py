from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import CameraInventoryORM, CustodyLogORM, ObservationORM
from src.services.geospatial_service import build_bbox_sql_filter, geometry_to_wkt, uses_postgis
from src.services.observation_service import (
    extract_observation_timestamp,
    has_complete_bbox,
    query_observations,
)
from src.services.trust_service import normalize_domain


@dataclass(frozen=True)
class CameraCandidate:
    camera_key: str
    external_id: str | None
    name: str
    source_domain: str | None
    layer_key: str
    provider: str
    road_name: str
    status: str
    active: bool
    image_url: str | None
    stream_url: str | None
    page_url: str | None
    location_geojson: dict[str, Any] | None
    confidence_score: float
    last_observed_at: datetime | None
    metadata_json: dict[str, Any]


def list_cameras(
    session: Session,
    *,
    layer_key: str | None = None,
    source_domain: str | None = None,
    status: str | None = None,
    active: bool | None = None,
    min_lon: float | None = None,
    min_lat: float | None = None,
    max_lon: float | None = None,
    max_lat: float | None = None,
    limit: int = 200,
) -> list[CameraInventoryORM]:
    statement = select(CameraInventoryORM).order_by(
        CameraInventoryORM.last_observed_at.desc().nullslast(),
        CameraInventoryORM.updated_at.desc(),
    )
    if layer_key:
        statement = statement.where(CameraInventoryORM.layer_key == layer_key)
    if source_domain:
        statement = statement.where(CameraInventoryORM.source_domain == source_domain)
    if status:
        statement = statement.where(CameraInventoryORM.status == status)
    if active is not None:
        statement = statement.where(CameraInventoryORM.active == active)

    if has_complete_bbox(min_lon=min_lon, min_lat=min_lat, max_lon=max_lon, max_lat=max_lat) and uses_postgis(session):
        statement = statement.where(
            build_bbox_sql_filter(
                CameraInventoryORM.location_wkt,
                min_lon=min_lon,
                min_lat=min_lat,
                max_lon=max_lon,
                max_lat=max_lat,
            )
        ).limit(limit)
        return list(session.scalars(statement))

    rows = list(session.scalars(statement))
    if has_complete_bbox(min_lon=min_lon, min_lat=min_lat, max_lon=max_lon, max_lat=max_lat):
        rows = [
            row
            for row in rows
            if camera_in_bbox(
                row,
                min_lon=min_lon,
                min_lat=min_lat,
                max_lon=max_lon,
                max_lat=max_lat,
            )
        ]
    return rows[:limit]


def materialize_camera_inventory(
    session: Session,
    *,
    layer_key: str | None = None,
    source_domain: str | None = None,
    limit: int = 500,
    actor: str = "camera_registry",
) -> dict[str, object]:
    observations = query_observations(
        session,
        layer_key=layer_key,
        source_domain=source_domain,
        limit=limit,
    )
    created_count = 0
    updated_count = 0
    touched_keys: list[str] = []
    processed_keys: set[str] = set()

    for observation in observations:
        candidate = extract_camera_candidate(observation)
        if candidate is None:
            continue
        if candidate.camera_key in processed_keys:
            continue
        processed_keys.add(candidate.camera_key)
        record = session.scalar(
            select(CameraInventoryORM).where(CameraInventoryORM.camera_key == candidate.camera_key)
        )
        if record is None:
            record = CameraInventoryORM(
                camera_key=candidate.camera_key,
                observation_id=observation.observation_id,
                external_id=candidate.external_id,
                name=candidate.name,
                source_domain=candidate.source_domain,
                layer_key=candidate.layer_key,
                provider=candidate.provider,
                road_name=candidate.road_name,
                status=candidate.status,
                active=candidate.active,
                image_url=candidate.image_url,
                stream_url=candidate.stream_url,
                page_url=candidate.page_url,
                location_geojson=candidate.location_geojson,
                location_wkt=geometry_to_wkt(candidate.location_geojson),
                confidence_score=candidate.confidence_score,
                last_observed_at=candidate.last_observed_at,
                metadata_json=candidate.metadata_json,
            )
            session.add(record)
            session.flush()
            created_count += 1
            touched_keys.append(record.camera_key)
            session.add(
                CustodyLogORM(
                    object_type="camera_inventory",
                    object_id=str(record.camera_inventory_id),
                    action="camera_registered",
                    actor=actor,
                    details_json={
                        "camera_key": record.camera_key,
                        "observation_id": observation.observation_id,
                        "source_domain": record.source_domain,
                        "layer_key": record.layer_key,
                    },
                )
            )
            continue

        if (
            record.last_observed_at is not None
            and candidate.last_observed_at is not None
            and normalize_timestamp(candidate.last_observed_at) < normalize_timestamp(record.last_observed_at)
        ):
            continue

        change_details = apply_camera_candidate(record, candidate, observation)
        if not change_details:
            continue
        updated_count += 1
        touched_keys.append(record.camera_key)
        session.add(
            CustodyLogORM(
                object_type="camera_inventory",
                object_id=str(record.camera_inventory_id),
                action="camera_updated",
                actor=actor,
                details_json={
                    "camera_key": record.camera_key,
                    "observation_id": observation.observation_id,
                    "changes": to_json_safe(change_details),
                },
            )
        )

    session.add(
        CustodyLogORM(
            object_type="camera_inventory_materialization",
            object_id=f"{layer_key or 'all'}:{source_domain or 'all'}",
            action="camera_materialization_completed",
            actor=actor,
            details_json={
                "layer_key": layer_key,
                "source_domain": source_domain,
                "limit": limit,
                "scanned_count": len(observations),
                "created_count": created_count,
                "updated_count": updated_count,
                "camera_keys": touched_keys,
            },
        )
    )
    session.commit()

    touched_cameras = (
        list(
            session.scalars(
                select(CameraInventoryORM)
                .where(CameraInventoryORM.camera_key.in_(touched_keys))
                .order_by(CameraInventoryORM.camera_inventory_id.asc())
            )
        )
        if touched_keys
        else []
    )
    return {
        "created_count": created_count,
        "updated_count": updated_count,
        "scanned_count": len(observations),
        "cameras": touched_cameras,
    }


def extract_camera_candidate(observation: ObservationORM) -> CameraCandidate | None:
    payload = observation.content_json if isinstance(observation.content_json, dict) else {}
    external_id = first_string(payload, ("camera_id", "cameraId", "siteId", "device_id", "id"))
    name = first_string(payload, ("camera_name", "cameraName", "name", "title", "description"))
    image_url = first_string(
        payload,
        ("image_url", "imageUrl", "snapshot_url", "snapshotUrl", "still_image_url", "jpg_url"),
    )
    stream_url = first_string(
        payload,
        ("stream_url", "streamUrl", "video_url", "videoUrl", "hls_url", "hlsUrl", "stream"),
    )
    page_url = first_string(payload, ("page_url", "pageUrl", "url", "source_url", "link"))
    road_name = first_string(payload, ("road_name", "roadName", "route", "route_designator", "roadway")) or ""
    provider = first_string(payload, ("provider", "agency", "source_agency")) or (observation.source_domain or "")
    status = (first_string(payload, ("status", "camera_status", "availability")) or "unknown").lower()
    location_geojson = observation.location_geojson
    camera_source_domain = resolve_camera_source_domain(
        image_url=image_url,
        stream_url=stream_url,
        page_url=page_url,
        fallback=observation.source_domain,
    )

    if not is_camera_candidate(
        observation=observation,
        payload=payload,
        name=name,
        image_url=image_url,
        stream_url=stream_url,
    ):
        return None

    camera_key = build_camera_key(
        source_domain=camera_source_domain,
        external_id=external_id,
        name=name,
        image_url=image_url,
        stream_url=stream_url,
        location_geojson=location_geojson,
    )
    metadata_json = {
        "record_format": observation.record_format,
        "source_type": observation.source_type,
        "raw_hash": observation.raw_hash,
        "provider": provider,
    }
    return CameraCandidate(
        camera_key=camera_key,
        external_id=external_id,
        name=name or external_id or observation.content_text[:120] or "Unnamed camera",
        source_domain=camera_source_domain,
        layer_key=observation.layer_key,
        provider=provider,
        road_name=road_name,
        status=status,
        active=is_active_camera(status, image_url=image_url, stream_url=stream_url),
        image_url=image_url,
        stream_url=stream_url,
        page_url=page_url,
        location_geojson=location_geojson,
        confidence_score=observation.confidence_score,
        last_observed_at=extract_observation_timestamp(observation) or observation.created_at,
        metadata_json=metadata_json,
    )


def is_camera_candidate(
    *,
    observation: ObservationORM,
    payload: dict[str, Any],
    name: str | None,
    image_url: str | None,
    stream_url: str | None,
) -> bool:
    if image_url or stream_url:
        return True
    if first_string(payload, ("camera_id", "cameraId", "siteId")):
        return True
    searchable = " ".join(
        part.lower()
        for part in (observation.layer_key, name or "", observation.content_text or "")
        if part
    )
    return "camera" in searchable or "webcam" in searchable or "cctv" in searchable


def build_camera_key(
    *,
    source_domain: str | None,
    external_id: str | None,
    name: str | None,
    image_url: str | None,
    stream_url: str | None,
    location_geojson: dict[str, Any] | None,
) -> str:
    coordinates = (location_geojson or {}).get("coordinates")
    coordinate_fragment = ""
    if isinstance(coordinates, list) and len(coordinates) >= 2:
        coordinate_fragment = f"{float(coordinates[0]):.6f},{float(coordinates[1]):.6f}"
    raw_key = "|".join(
        [
            source_domain or "",
            external_id or "",
            image_url or "",
            stream_url or "",
            (name or "").strip().lower(),
            coordinate_fragment,
        ]
    )
    digest = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()[:16]
    prefix = external_id or (name or "camera").strip().lower().replace(" ", "-")[:48] or "camera"
    prefix = "".join(ch for ch in prefix if ch.isalnum() or ch in {"-", "_"}).strip("-_") or "camera"
    return f"{prefix}-{digest}"


def first_string(payload: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, (int, float)):
            return str(value)
    return None


def resolve_camera_source_domain(
    *,
    image_url: str | None,
    stream_url: str | None,
    page_url: str | None,
    fallback: str | None,
) -> str | None:
    for value in (image_url, stream_url, page_url, fallback):
        if isinstance(value, str) and value.strip():
            normalized = normalize_domain(value)
            if normalized:
                return normalized
    return fallback


def is_active_camera(status: str, *, image_url: str | None, stream_url: str | None) -> bool:
    if status in {"offline", "inactive", "down", "disabled", "unavailable"}:
        return False
    return bool(image_url or stream_url or status in {"active", "online", "open", "ok", "unknown"})


def apply_camera_candidate(
    record: CameraInventoryORM,
    candidate: CameraCandidate,
    observation: ObservationORM,
) -> dict[str, dict[str, object]]:
    changes: dict[str, dict[str, object]] = {}
    field_mapping = {
        "observation_id": observation.observation_id,
        "external_id": candidate.external_id,
        "name": candidate.name,
        "source_domain": candidate.source_domain,
        "layer_key": candidate.layer_key,
        "provider": candidate.provider,
        "road_name": candidate.road_name,
        "status": candidate.status,
        "active": candidate.active,
        "image_url": candidate.image_url,
        "stream_url": candidate.stream_url,
        "page_url": candidate.page_url,
        "location_geojson": candidate.location_geojson,
        "location_wkt": geometry_to_wkt(candidate.location_geojson),
        "confidence_score": candidate.confidence_score,
        "last_observed_at": candidate.last_observed_at,
        "metadata_json": candidate.metadata_json,
    }
    for field_name, new_value in field_mapping.items():
        old_value = getattr(record, field_name)
        if values_equal(old_value, new_value):
            continue
        setattr(record, field_name, new_value)
        changes[field_name] = {"old": old_value, "new": new_value}
    return changes


def camera_in_bbox(
    camera: CameraInventoryORM,
    *,
    min_lon: float | None,
    min_lat: float | None,
    max_lon: float | None,
    max_lat: float | None,
) -> bool:
    if None in {min_lon, min_lat, max_lon, max_lat}:
        return True
    coordinates = (camera.location_geojson or {}).get("coordinates")
    if not isinstance(coordinates, list) or len(coordinates) < 2:
        return False
    lon = float(coordinates[0])
    lat = float(coordinates[1])
    return min_lon <= lon <= max_lon and min_lat <= lat <= max_lat


def normalize_timestamp(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def to_json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: to_json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [to_json_safe(item) for item in value]
    return value


def values_equal(old_value: Any, new_value: Any) -> bool:
    if isinstance(old_value, datetime) and isinstance(new_value, datetime):
        return normalize_timestamp(old_value) == normalize_timestamp(new_value)
    return old_value == new_value
