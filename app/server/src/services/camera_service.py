from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import (
    CameraInventoryORM,
    CustodyLogORM,
    LocalImportRunORM,
    ObservationORM,
    ScheduledTaskORM,
    ScheduledTaskRunORM,
)
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
    rows = query_camera_inventory(
        session,
        layer_key=layer_key,
        source_domain=source_domain,
        status=status,
        active=active,
        min_lon=min_lon,
        min_lat=min_lat,
        max_lon=max_lon,
        max_lat=max_lat,
        limit=limit,
    )
    return rows[:limit]


def query_camera_inventory(
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
    limit: int | None = None,
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
        )
        if limit is not None:
            statement = statement.limit(limit)
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
    if limit is None:
        return rows
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


def build_camera_inventory_summary(
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
    stale_after_hours: float = 24.0,
) -> dict[str, object]:
    generated_at = datetime.now(timezone.utc)
    stale_before = generated_at - timedelta(hours=max(0.0, stale_after_hours))
    cameras = query_camera_inventory(
        session,
        layer_key=layer_key,
        source_domain=source_domain,
        status=status,
        active=active,
        min_lon=min_lon,
        min_lat=min_lat,
        max_lon=max_lon,
        max_lat=max_lat,
        limit=None,
    )

    return {
        "generated_at": generated_at,
        "stale_before": stale_before,
        "total_count": len(cameras),
        "active_count": sum(1 for camera in cameras if camera.active),
        "inactive_count": sum(1 for camera in cameras if not camera.active),
        "stale_count": sum(1 for camera in cameras if is_stale_camera(camera, stale_before)),
        "layer_counts": build_camera_summary_buckets(cameras, lambda camera: camera.layer_key, stale_before),
        "source_domain_counts": build_camera_summary_buckets(
            cameras,
            lambda camera: camera.source_domain or "unknown",
            stale_before,
        ),
        "provider_counts": build_camera_summary_buckets(
            cameras,
            lambda camera: camera.provider or "unknown",
            stale_before,
        ),
        "status_counts": build_camera_summary_buckets(cameras, lambda camera: camera.status or "unknown", stale_before),
    }


def build_camera_inventory_ops_detail(
    session: Session,
    camera_inventory_id: int,
) -> dict[str, object]:
    camera = session.get(CameraInventoryORM, camera_inventory_id)
    if camera is None:
        raise ValueError(f"Camera inventory record {camera_inventory_id} does not exist.")

    latest_observation = session.get(ObservationORM, camera.observation_id) if camera.observation_id is not None else None
    latest_import_run = (
        session.get(LocalImportRunORM, latest_observation.import_run_id)
        if latest_observation is not None and latest_observation.import_run_id is not None
        else None
    )
    custody_logs = list(
        session.scalars(
            select(CustodyLogORM)
            .where(
                CustodyLogORM.object_type == "camera_inventory",
                CustodyLogORM.object_id == str(camera.camera_inventory_id),
            )
            .order_by(CustodyLogORM.created_at.desc())
            .limit(50)
        )
    )
    refresh_tasks = list(
        session.scalars(
            select(ScheduledTaskORM)
            .where(ScheduledTaskORM.task_type == "camera_inventory_refresh")
            .order_by(ScheduledTaskORM.task_id.asc())
        )
    )
    matching_tasks = [
        task
        for task in refresh_tasks
        if camera_matches_refresh_task(camera, task)
    ]
    return {
        "camera": camera,
        "latest_observation": latest_observation,
        "latest_import_run": latest_import_run,
        "custody_logs": custody_logs,
        "refresh_tasks": matching_tasks,
    }


def build_camera_ops_report_index(
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
    stale_after_hours: float = 24.0,
    limit: int = 25,
    stale_camera_limit: int = 25,
) -> dict[str, object]:
    generated_at = datetime.now(timezone.utc)
    summary = build_camera_inventory_summary(
        session,
        layer_key=layer_key,
        source_domain=source_domain,
        status=status,
        active=active,
        min_lon=min_lon,
        min_lat=min_lat,
        max_lon=max_lon,
        max_lat=max_lat,
        stale_after_hours=stale_after_hours,
    )
    stale_before = summary["stale_before"]
    scoped_cameras = query_camera_inventory(
        session,
        layer_key=layer_key,
        source_domain=source_domain,
        status=status,
        active=active,
        min_lon=min_lon,
        min_lat=min_lat,
        max_lon=max_lon,
        max_lat=max_lat,
        limit=None,
    )
    stale_cameras = [
        camera
        for camera in scoped_cameras
        if is_stale_camera(camera, stale_before)
    ][:stale_camera_limit]

    refresh_tasks = list(
        session.scalars(
            select(ScheduledTaskORM)
            .where(ScheduledTaskORM.task_type == "camera_inventory_refresh")
            .order_by(ScheduledTaskORM.task_id.asc())
        )
    )
    matching_refresh_tasks = [
        task
        for task in refresh_tasks
        if refresh_task_matches_scope(task, layer_key=layer_key, source_domain=source_domain)
    ]
    task_lookup = {task.task_id: task for task in matching_refresh_tasks}
    task_ids = list(task_lookup)

    all_refresh_runs: list[ScheduledTaskRunORM] = []
    recent_refresh_runs: list[dict[str, object]] = []
    if task_ids:
        all_refresh_runs = list(
            session.scalars(
                select(ScheduledTaskRunORM)
                .where(ScheduledTaskRunORM.task_id.in_(task_ids))
                .order_by(ScheduledTaskRunORM.task_run_id.desc())
            )
        )
        recent_refresh_runs = [
            serialize_refresh_run(task_lookup[run.task_id], run)
            for run in all_refresh_runs[:limit]
            if run.task_id in task_lookup
        ]

    materialization_logs = list(
        session.scalars(
            select(CustodyLogORM)
            .where(CustodyLogORM.object_type == "camera_inventory_materialization")
            .order_by(CustodyLogORM.created_at.desc())
            .limit(max(limit * 5, 50))
        )
    )
    recent_materializations = [
        log
        for log in materialization_logs
        if materialization_matches_scope(log, layer_key=layer_key, source_domain=source_domain)
    ][:limit]
    latest_materialization_at = recent_materializations[0].created_at if recent_materializations else None

    return {
        "generated_at": generated_at,
        "stale_after_hours": stale_after_hours,
        "latest_materialization_at": latest_materialization_at,
        "inventory_summary": summary,
        "refresh_task_count": len(matching_refresh_tasks),
        "refresh_run_count": len(all_refresh_runs),
        "refresh_failure_count": sum(1 for run in all_refresh_runs if run.status == "failed"),
        "refresh_tasks": matching_refresh_tasks,
        "recent_refresh_runs": recent_refresh_runs,
        "recent_materializations": recent_materializations,
        "stale_cameras": stale_cameras,
    }


def build_camera_ops_export_summary(
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
    stale_after_hours: float = 24.0,
    camera_limit: int = 500,
    report_limit: int = 25,
    stale_camera_limit: int = 25,
) -> dict[str, object]:
    generated_at = datetime.now(timezone.utc)
    cameras = query_camera_inventory(
        session,
        layer_key=layer_key,
        source_domain=source_domain,
        status=status,
        active=active,
        min_lon=min_lon,
        min_lat=min_lat,
        max_lon=max_lon,
        max_lat=max_lat,
        limit=camera_limit,
    )
    return {
        "generated_at": generated_at,
        "filters_json": {
            "layer_key": layer_key,
            "source_domain": source_domain,
            "status": status,
            "active": active,
            "min_lon": min_lon,
            "min_lat": min_lat,
            "max_lon": max_lon,
            "max_lat": max_lat,
            "stale_after_hours": stale_after_hours,
            "camera_limit": camera_limit,
            "report_limit": report_limit,
            "stale_camera_limit": stale_camera_limit,
        },
        "report_index": build_camera_ops_report_index(
            session,
            layer_key=layer_key,
            source_domain=source_domain,
            status=status,
            active=active,
            min_lon=min_lon,
            min_lat=min_lat,
            max_lon=max_lon,
            max_lat=max_lat,
            stale_after_hours=stale_after_hours,
            limit=report_limit,
            stale_camera_limit=stale_camera_limit,
        ),
        "cameras": cameras,
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


def is_stale_camera(camera: CameraInventoryORM, stale_before: datetime) -> bool:
    if camera.last_observed_at is None:
        return True
    return normalize_timestamp(camera.last_observed_at) < stale_before


def build_camera_summary_buckets(
    cameras: list[CameraInventoryORM],
    key_fn,
    stale_before: datetime,
) -> list[dict[str, object]]:
    counts: dict[str, dict[str, object]] = {}
    for camera in cameras:
        key = str(key_fn(camera) or "unknown")
        bucket = counts.setdefault(
            key,
            {
                "key": key,
                "total_count": 0,
                "active_count": 0,
                "inactive_count": 0,
                "stale_count": 0,
            },
        )
        bucket["total_count"] += 1
        if camera.active:
            bucket["active_count"] += 1
        else:
            bucket["inactive_count"] += 1
        if is_stale_camera(camera, stale_before):
            bucket["stale_count"] += 1
    return sorted(
        counts.values(),
        key=lambda item: (-int(item["total_count"]), str(item["key"]).lower()),
    )


def camera_matches_refresh_task(camera: CameraInventoryORM, task: ScheduledTaskORM) -> bool:
    if task.layer_key is not None and task.layer_key != camera.layer_key:
        return False
    payload = task.payload_json if isinstance(task.payload_json, dict) else {}
    task_source_domain = payload.get("source_domain")
    if not isinstance(task_source_domain, str) or not task_source_domain.strip():
        return True
    normalized_task_domain = normalize_domain(task_source_domain)
    normalized_camera_domain = normalize_domain(camera.source_domain) if camera.source_domain else None
    if not normalized_task_domain or not normalized_camera_domain:
        return False
    return (
        normalized_camera_domain == normalized_task_domain
        or normalized_camera_domain.endswith(f".{normalized_task_domain}")
        or normalized_task_domain.endswith(f".{normalized_camera_domain}")
    )


def refresh_task_matches_scope(
    task: ScheduledTaskORM,
    *,
    layer_key: str | None,
    source_domain: str | None,
) -> bool:
    if layer_key is not None and task.layer_key is not None and task.layer_key != layer_key:
        return False
    payload = task.payload_json if isinstance(task.payload_json, dict) else {}
    task_source_domain = payload.get("source_domain")
    if source_domain is None:
        return True
    if not isinstance(task_source_domain, str) or not task_source_domain.strip():
        return True
    normalized_scope_domain = normalize_domain(source_domain)
    normalized_task_domain = normalize_domain(task_source_domain)
    if not normalized_scope_domain or not normalized_task_domain:
        return False
    return (
        normalized_scope_domain == normalized_task_domain
        or normalized_scope_domain.endswith(f".{normalized_task_domain}")
        or normalized_task_domain.endswith(f".{normalized_scope_domain}")
    )


def materialization_matches_scope(
    log: CustodyLogORM,
    *,
    layer_key: str | None,
    source_domain: str | None,
) -> bool:
    details = log.details_json if isinstance(log.details_json, dict) else {}
    detail_layer = details.get("layer_key")
    detail_source_domain = details.get("source_domain")
    if layer_key is not None and detail_layer is not None and detail_layer != layer_key:
        return False
    if source_domain is None:
        return True
    if not isinstance(detail_source_domain, str) or not detail_source_domain.strip():
        return True
    normalized_scope_domain = normalize_domain(source_domain)
    normalized_detail_domain = normalize_domain(detail_source_domain)
    if not normalized_scope_domain or not normalized_detail_domain:
        return False
    return (
        normalized_scope_domain == normalized_detail_domain
        or normalized_scope_domain.endswith(f".{normalized_detail_domain}")
        or normalized_detail_domain.endswith(f".{normalized_scope_domain}")
    )


def serialize_refresh_run(task: ScheduledTaskORM, run: ScheduledTaskRunORM) -> dict[str, object]:
    payload = task.payload_json if isinstance(task.payload_json, dict) else {}
    return {
        "task_run_id": run.task_run_id,
        "task_id": task.task_id,
        "task_name": task.name,
        "layer_key": task.layer_key,
        "source_domain": payload.get("source_domain") if isinstance(payload.get("source_domain"), str) else None,
        "status": run.status,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "records_affected": run.records_affected,
        "error_text": run.error_text,
        "output_json": run.output_json,
    }


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
