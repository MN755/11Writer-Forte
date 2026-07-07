from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import ObservationORM
from src.services.geospatial_service import build_bbox_sql_filter, uses_postgis


@dataclass
class CrossVerificationCluster:
    observation_ids: list[int] = field(default_factory=list)
    layer_keys: set[str] = field(default_factory=set)
    source_domains: set[str] = field(default_factory=set)
    confidence_scores: list[float] = field(default_factory=list)
    timestamps: list[datetime] = field(default_factory=list)
    coordinates: list[tuple[float, float]] = field(default_factory=list)

    def add(self, observation: ObservationORM, point: tuple[float, float]) -> None:
        self.observation_ids.append(observation.observation_id)
        self.layer_keys.add(observation.layer_key)
        if observation.source_domain:
            self.source_domains.add(observation.source_domain)
        self.confidence_scores.append(observation.confidence_score)
        self.timestamps.append(observation.created_at)
        self.coordinates.append(point)

    @property
    def centroid(self) -> tuple[float, float]:
        lon = sum(point[0] for point in self.coordinates) / len(self.coordinates)
        lat = sum(point[1] for point in self.coordinates) / len(self.coordinates)
        return (lon, lat)

    @property
    def started_at(self) -> datetime:
        return min(self.timestamps)

    @property
    def ended_at(self) -> datetime:
        return max(self.timestamps)


def query_observations(
    session: Session,
    *,
    layer_key: str | None = None,
    source_domain: str | None = None,
    trust_level: str | None = None,
    min_lon: float | None = None,
    min_lat: float | None = None,
    max_lon: float | None = None,
    max_lat: float | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = 200,
) -> list[ObservationORM]:
    statement = select(ObservationORM).order_by(ObservationORM.created_at.desc())
    if layer_key:
        statement = statement.where(ObservationORM.layer_key == layer_key)
    if source_domain:
        statement = statement.where(ObservationORM.source_domain == source_domain)
    if trust_level:
        statement = statement.where(ObservationORM.trust_level == trust_level)
    if since:
        statement = statement.where(ObservationORM.created_at >= since)
    if until:
        statement = statement.where(ObservationORM.created_at <= until)

    if has_complete_bbox(min_lon=min_lon, min_lat=min_lat, max_lon=max_lon, max_lat=max_lat) and uses_postgis(session):
        statement = statement.where(
            build_bbox_sql_filter(
                ObservationORM.location_wkt,
                min_lon=min_lon,
                min_lat=min_lat,
                max_lon=max_lon,
                max_lat=max_lat,
            )
        ).limit(limit)
        return list(session.scalars(statement))

    if None in {min_lon, min_lat, max_lon, max_lat}:
        statement = statement.limit(limit)
        return list(session.scalars(statement))

    observations = list(session.scalars(statement))
    observations = [
        observation
        for observation in observations
        if observation_in_bbox(
            observation,
            min_lon=min_lon,
            min_lat=min_lat,
            max_lon=max_lon,
            max_lat=max_lat,
        )
    ]
    return observations[:limit]


def has_complete_bbox(
    *,
    min_lon: float | None,
    min_lat: float | None,
    max_lon: float | None,
    max_lat: float | None,
) -> bool:
    return None not in {min_lon, min_lat, max_lon, max_lat}


def observation_in_bbox(
    observation: ObservationORM,
    *,
    min_lon: float | None,
    min_lat: float | None,
    max_lon: float | None,
    max_lat: float | None,
) -> bool:
    if None in {min_lon, min_lat, max_lon, max_lat}:
        return True
    coordinates = (observation.location_geojson or {}).get("coordinates")
    if not isinstance(coordinates, list) or len(coordinates) < 2:
        return False
    lon = float(coordinates[0])
    lat = float(coordinates[1])
    return min_lon <= lon <= max_lon and min_lat <= lat <= max_lat


def build_cross_verification_summaries(
    observations: list[ObservationORM],
    *,
    time_window_minutes: int = 60,
    distance_km: float = 25.0,
    min_independent_signals: int = 2,
) -> list[dict[str, object]]:
    clusters: list[CrossVerificationCluster] = []
    for observation in sorted(observations, key=lambda row: row.created_at):
        point = extract_point(observation)
        if point is None:
            continue
        matched_cluster = None
        for cluster in clusters:
            if within_cluster(cluster, observation.created_at, point, time_window_minutes, distance_km):
                matched_cluster = cluster
                break
        if matched_cluster is None:
            matched_cluster = CrossVerificationCluster()
            clusters.append(matched_cluster)
        matched_cluster.add(observation, point)

    summaries: list[dict[str, object]] = []
    for index, cluster in enumerate(clusters, start=1):
        source_count = len(cluster.source_domains)
        layer_count = len(cluster.layer_keys)
        independent_signals = max(source_count, layer_count)
        if independent_signals < min_independent_signals:
            continue
        avg_confidence = (
            sum(cluster.confidence_scores) / len(cluster.confidence_scores)
            if cluster.confidence_scores
            else 0.0
        )
        verification_score = min(
            0.99,
            avg_confidence + 0.15 * max(source_count - 1, 0) + 0.1 * max(layer_count - 1, 0),
        )
        centroid_lon, centroid_lat = cluster.centroid
        summaries.append(
            {
                "cluster_id": f"cluster-{index}",
                "observation_ids": cluster.observation_ids,
                "observation_count": len(cluster.observation_ids),
                "source_domain_count": source_count,
                "layer_count": layer_count,
                "independent_signal_count": independent_signals,
                "verification_score": round(verification_score, 4),
                "started_at": cluster.started_at,
                "ended_at": cluster.ended_at,
                "centroid_geojson": {
                    "type": "Point",
                    "coordinates": [centroid_lon, centroid_lat],
                },
            }
        )
    return summaries


def extract_point(observation: ObservationORM) -> tuple[float, float] | None:
    coordinates = (observation.location_geojson or {}).get("coordinates")
    if not isinstance(coordinates, list) or len(coordinates) < 2:
        return None
    return (float(coordinates[0]), float(coordinates[1]))


def within_cluster(
    cluster: CrossVerificationCluster,
    timestamp: datetime,
    point: tuple[float, float],
    time_window_minutes: int,
    distance_km: float,
) -> bool:
    if abs((timestamp - cluster.ended_at).total_seconds()) > time_window_minutes * 60:
        return False
    centroid = cluster.centroid
    return haversine_km(centroid, point) <= distance_km


def haversine_km(point_a: tuple[float, float], point_b: tuple[float, float]) -> float:
    lon1, lat1 = map(math.radians, point_a)
    lon2, lat2 = map(math.radians, point_b)
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    term = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 6371.0 * 2 * math.atan2(math.sqrt(term), math.sqrt(1 - term))
