from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import Text, cast, or_, select
from sqlalchemy.orm import Session

from src.models import ObservationORM, SourceTrustProfileORM
from src.services.geospatial_service import build_bbox_sql_filter, uses_postgis


@dataclass
class ObservationQueryRecord:
    observation_id: int
    import_run_id: int | None
    event_id: int | None
    layer_key: str
    source_domain: str | None
    source_type: str
    record_format: str
    trust_level: str
    approval_policy: str
    confidence_score: float
    observed_at: datetime | None
    location_geojson: dict[str, object] | None
    content_text: str
    content_json: dict[str, object]
    raw_hash: str
    created_at: datetime
    updated_at: datetime


@dataclass
class ObservationSearchResult:
    observation: ObservationORM | ObservationQueryRecord
    search_score: float
    matched_terms: list[str]
    title: str | None
    source_url: str | None
    snippet: str | None


@dataclass
class CrossVerificationCluster:
    observation_ids: list[int] = field(default_factory=list)
    layer_keys: set[str] = field(default_factory=set)
    source_domains: set[str] = field(default_factory=set)
    confidence_scores: list[float] = field(default_factory=list)
    timestamps: list[datetime] = field(default_factory=list)
    coordinates: list[tuple[float, float]] = field(default_factory=list)
    trusted_observation_count: int = 0
    ground_truth_count: int = 0

    def add(self, observation: ObservationORM, point: tuple[float, float]) -> None:
        self.observation_ids.append(observation.observation_id)
        self.layer_keys.add(observation.layer_key)
        if observation.source_domain:
            self.source_domains.add(observation.source_domain)
        self.confidence_scores.append(observation.confidence_score)
        self.timestamps.append(extract_observation_timestamp(observation) or normalize_timestamp(observation.created_at))
        self.coordinates.append(point)
        if observation.trust_level == "trusted":
            self.trusted_observation_count += 1
        if is_ground_truth_observation(observation):
            self.ground_truth_count += 1

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


SEARCH_TITLE_KEYS = ("title", "headline", "page_title", "name", "canonical_name")
SEARCH_SUMMARY_KEYS = ("summary", "description", "meta_description", "text_excerpt", "excerpt")
SEARCH_URL_KEYS = ("url", "source_url", "page_url", "canonical_url", "link_url")
SEARCH_SNIPPET_MAX_LENGTH = 220


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
    backend: str = "runtime",
    archive_glob_url: str | None = None,
) -> list[ObservationORM | ObservationQueryRecord]:
    if backend != "runtime":
        from src.services.clickhouse_service import query_clickhouse_observations

        return query_clickhouse_observations(
            layer_key=layer_key,
            source_domain=source_domain,
            trust_level=trust_level,
            min_lon=min_lon,
            min_lat=min_lat,
            max_lon=max_lon,
            max_lat=max_lat,
            since=since,
            until=until,
            limit=limit,
            backend=backend,
            archive_glob_url=archive_glob_url,
        )

    statement = build_runtime_observation_statement(
        layer_key=layer_key,
        source_domain=source_domain,
        trust_level=trust_level,
        since=since,
        until=until,
    )
    statement = statement.order_by(
        ObservationORM.observed_at.desc().nullslast(),
        ObservationORM.created_at.desc(),
    )

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


def search_observations(
    session: Session,
    *,
    query_text: str,
    layer_key: str | None = None,
    source_domain: str | None = None,
    trust_level: str | None = None,
    min_lon: float | None = None,
    min_lat: float | None = None,
    max_lon: float | None = None,
    max_lat: float | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = 50,
    candidate_limit: int = 500,
    backend: str = "runtime",
    archive_glob_url: str | None = None,
) -> list[ObservationSearchResult]:
    normalized_query = normalize_search_query(query_text)
    if not normalized_query:
        raise ValueError("Observation search query must not be empty.")

    terms = tokenize_search_terms(normalized_query)
    effective_candidate_limit = min(max(limit, candidate_limit, limit * 10), 5000)
    if backend != "runtime":
        candidates = query_observations(
            session,
            layer_key=layer_key,
            source_domain=source_domain,
            trust_level=trust_level,
            min_lon=min_lon,
            min_lat=min_lat,
            max_lon=max_lon,
            max_lat=max_lat,
            since=since,
            until=until,
            limit=effective_candidate_limit,
            backend=backend,
            archive_glob_url=archive_glob_url,
        )
        return rank_observation_search_candidates(
            candidates,
            normalized_query=normalized_query,
            terms=terms,
            limit=limit,
        )

    statement = build_runtime_observation_statement(
        layer_key=layer_key,
        source_domain=source_domain,
        trust_level=trust_level,
        since=since,
        until=until,
    )
    statement = apply_runtime_observation_search_filter(
        statement,
        normalized_query=normalized_query,
        terms=terms,
    ).order_by(
        ObservationORM.observed_at.desc().nullslast(),
        ObservationORM.created_at.desc(),
    )
    if has_complete_bbox(min_lon=min_lon, min_lat=min_lat, max_lon=max_lon, max_lat=max_lat) and uses_postgis(session):
        statement = statement.where(
            build_bbox_sql_filter(
                ObservationORM.location_wkt,
                min_lon=min_lon,
                min_lat=min_lat,
                max_lon=max_lon,
                max_lat=max_lat,
            )
        )
    candidates = list(session.scalars(statement.limit(effective_candidate_limit)))
    if has_complete_bbox(min_lon=min_lon, min_lat=min_lat, max_lon=max_lon, max_lat=max_lat) and not uses_postgis(session):
        candidates = [
            observation
            for observation in candidates
            if observation_in_bbox(
                observation,
                min_lon=min_lon,
                min_lat=min_lat,
                max_lon=max_lon,
                max_lat=max_lat,
            )
        ]
    return rank_observation_search_candidates(
        candidates,
        normalized_query=normalized_query,
        terms=terms,
        limit=limit,
    )


def build_runtime_observation_statement(
    *,
    layer_key: str | None = None,
    source_domain: str | None = None,
    trust_level: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
):
    statement = select(ObservationORM)
    if layer_key:
        statement = statement.where(ObservationORM.layer_key == layer_key)
    if source_domain:
        statement = statement.where(ObservationORM.source_domain == source_domain)
    if trust_level:
        statement = statement.where(ObservationORM.trust_level == trust_level)
    if since:
        statement = statement.where(
            or_(
                ObservationORM.observed_at >= since,
                (ObservationORM.observed_at.is_(None) & (ObservationORM.created_at >= since)),
            )
        )
    if until:
        statement = statement.where(
            or_(
                ObservationORM.observed_at <= until,
                (ObservationORM.observed_at.is_(None) & (ObservationORM.created_at <= until)),
            )
        )
    return statement


def apply_runtime_observation_search_filter(
    statement,
    *,
    normalized_query: str,
    terms: list[str],
):
    predicates = []
    for pattern in dedupe_strings([normalized_query, *terms]):
        escaped = f"%{escape_sql_like(pattern)}%"
        predicates.append(
            or_(
                ObservationORM.content_text.ilike(escaped, escape="\\"),
                cast(ObservationORM.content_json, Text).ilike(escaped, escape="\\"),
                ObservationORM.source_domain.ilike(escaped, escape="\\"),
                ObservationORM.layer_key.ilike(escaped, escape="\\"),
            )
        )
    if predicates:
        statement = statement.where(or_(*predicates))
    return statement


def rank_observation_search_candidates(
    candidates: list[ObservationORM | ObservationQueryRecord],
    *,
    normalized_query: str,
    terms: list[str],
    limit: int,
) -> list[ObservationSearchResult]:
    results: list[ObservationSearchResult] = []
    for candidate in candidates:
        ranked = build_observation_search_result(
            candidate,
            normalized_query=normalized_query,
            terms=terms,
        )
        if ranked is not None:
            results.append(ranked)
    results.sort(
        key=lambda row: (
            -row.search_score,
            -(
                extract_observation_timestamp(row.observation)
                or normalize_timestamp(row.observation.created_at)
            ).timestamp(),
            -row.observation.observation_id,
        )
    )
    return results[:limit]


def build_observation_search_result(
    observation: ObservationORM | ObservationQueryRecord,
    *,
    normalized_query: str,
    terms: list[str],
) -> ObservationSearchResult | None:
    title = extract_observation_search_title(observation)
    summary = extract_observation_search_summary(observation)
    source_url = extract_observation_search_source_url(observation)
    content_text = observation.content_text or ""
    json_text = json.dumps(observation.content_json or {}, sort_keys=True, default=str)
    domain_text = observation.source_domain or ""
    layer_text = observation.layer_key or ""

    normalized_fields = {
        "title": normalize_search_query(title),
        "summary": normalize_search_query(summary),
        "source_url": normalize_search_query(source_url),
        "content_text": normalize_search_query(content_text),
        "json_text": normalize_search_query(json_text),
        "domain": normalize_search_query(domain_text),
        "layer": normalize_search_query(layer_text),
    }

    matched_terms: list[str] = []
    score = 0.0
    if normalized_query in normalized_fields["title"]:
        score += 28.0
    if normalized_query in normalized_fields["summary"]:
        score += 18.0
    if normalized_query in normalized_fields["source_url"]:
        score += 16.0
    if normalized_query in normalized_fields["content_text"]:
        score += 14.0
    if normalized_query in normalized_fields["json_text"]:
        score += 6.0

    for term in terms:
        matched = False
        if term in normalized_fields["title"]:
            score += 10.0
            matched = True
        elif term in normalized_fields["summary"]:
            score += 7.0
            matched = True
        elif term in normalized_fields["source_url"]:
            score += 6.0
            matched = True
        elif term in normalized_fields["domain"]:
            score += 4.0
            matched = True
        elif term in normalized_fields["layer"]:
            score += 3.0
            matched = True
        elif term in normalized_fields["content_text"]:
            score += 4.0
            matched = True
        elif term in normalized_fields["json_text"]:
            score += 2.0
            matched = True
        if matched:
            matched_terms.append(term)

    matched_terms = dedupe_strings(matched_terms)
    if score <= 0:
        return None
    if matched_terms and len(matched_terms) == len(terms):
        score += 8.0
    score += min(max(float(observation.confidence_score), 0.0), 1.0)
    if observation.trust_level == "trusted":
        score += 0.25
    if observation.trust_level == "blocked":
        score -= 0.25

    snippet = build_observation_search_snippet(
        summary or content_text or json_text,
        matches=matched_terms or [normalized_query],
    )
    return ObservationSearchResult(
        observation=observation,
        search_score=round(score, 4),
        matched_terms=matched_terms,
        title=title,
        source_url=source_url,
        snippet=snippet,
    )


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
    session: Session | None,
    observations: list[ObservationORM],
    *,
    time_window_minutes: int = 60,
    distance_km: float = 25.0,
    min_independent_signals: int = 2,
) -> list[dict[str, object]]:
    clusters: list[CrossVerificationCluster] = []
    ordered = sorted(
        observations,
        key=lambda row: extract_observation_timestamp(row) or normalize_timestamp(row.created_at),
    )
    for observation in ordered:
        point = extract_point(observation)
        if point is None:
            continue
        timestamp = extract_observation_timestamp(observation) or normalize_timestamp(observation.created_at)
        matched_cluster = None
        for cluster in clusters:
            if within_cluster(cluster, timestamp, point, time_window_minutes, distance_km):
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
        integrity_source_count = count_integrity_sources(session, cluster.source_domains)
        time_span_minutes_value = round(
            max((cluster.ended_at - cluster.started_at).total_seconds(), 0.0) / 60.0,
            3,
        )
        summaries.append(
            {
                "cluster_id": f"cluster-{index}",
                "observation_ids": cluster.observation_ids,
                "observation_count": len(cluster.observation_ids),
                "source_domain_count": source_count,
                "source_domains": sorted(cluster.source_domains),
                "layer_count": layer_count,
                "layer_keys": sorted(cluster.layer_keys),
                "independent_signal_count": independent_signals,
                "verification_score": round(verification_score, 4),
                "trusted_observation_count": cluster.trusted_observation_count,
                "integrity_source_count": integrity_source_count,
                "ground_truth_count": cluster.ground_truth_count,
                "time_span_minutes": time_span_minutes_value,
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


def count_integrity_sources(session: Session | None, source_domains: set[str]) -> int:
    if session is None or not source_domains:
        return 0
    statement = select(SourceTrustProfileORM.domain).where(
        SourceTrustProfileORM.integrity_source.is_(True),
        SourceTrustProfileORM.domain.in_(source_domains),
    )
    return len(set(session.scalars(statement)))


def extract_observation_search_title(observation: ObservationORM | ObservationQueryRecord) -> str | None:
    return as_optional_search_string(find_first_key(observation.content_json, SEARCH_TITLE_KEYS))


def extract_observation_search_summary(observation: ObservationORM | ObservationQueryRecord) -> str | None:
    return as_optional_search_string(find_first_key(observation.content_json, SEARCH_SUMMARY_KEYS))


def extract_observation_search_source_url(observation: ObservationORM | ObservationQueryRecord) -> str | None:
    return as_optional_search_string(find_first_key(observation.content_json, SEARCH_URL_KEYS))


def as_optional_search_string(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    if isinstance(value, (int, float)):
        return str(value)
    return None


def normalize_search_query(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value.strip().lower())


def tokenize_search_terms(value: str) -> list[str]:
    tokens = re.findall(r"[a-z0-9]+", value.lower())
    return [token for token in dedupe_strings(tokens) if len(token) >= 2]


def dedupe_strings(values: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        candidate = value.strip()
        if not candidate or candidate in seen:
            continue
        normalized.append(candidate)
        seen.add(candidate)
    return normalized


def escape_sql_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def build_observation_search_snippet(text: str, *, matches: list[str]) -> str | None:
    normalized_text = " ".join(text.split())
    if not normalized_text:
        return None
    lower_text = normalized_text.lower()
    start_index = 0
    for match in matches:
        lowered_match = match.lower()
        if not lowered_match:
            continue
        found = lower_text.find(lowered_match)
        if found >= 0:
            start_index = max(found - 60, 0)
            break
    snippet = normalized_text[start_index : start_index + SEARCH_SNIPPET_MAX_LENGTH].strip()
    if start_index > 0:
        snippet = f"...{snippet}"
    if start_index + SEARCH_SNIPPET_MAX_LENGTH < len(normalized_text):
        snippet = f"{snippet}..."
    return snippet


def extract_observation_timestamp(observation: ObservationORM) -> datetime | None:
    if observation.observed_at is not None:
        return normalize_timestamp(observation.observed_at)
    return parse_timestamp_value(find_first_key(observation.content_json, TIMESTAMP_KEYS))


def normalize_timestamp(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def is_ground_truth_observation(observation: ObservationORM) -> bool:
    candidate = find_first_key(observation.content_json, GROUND_TRUTH_KEYS)
    if isinstance(candidate, bool):
        return candidate
    if isinstance(candidate, (int, float)):
        return bool(candidate)
    if isinstance(candidate, str):
        return candidate.strip().lower() in {"1", "true", "yes", "verified", "confirmed", "direct"}
    return False


def find_first_key(payload: object, keys: tuple[str, ...]) -> object | None:
    if isinstance(payload, dict):
        for key in keys:
            if key in payload:
                return payload[key]
        for value in payload.values():
            found = find_first_key(value, keys)
            if found is not None:
                return found
        return None
    if isinstance(payload, list):
        for item in payload:
            found = find_first_key(item, keys)
            if found is not None:
                return found
    return None


def parse_timestamp_value(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        return parse_epoch_timestamp(float(value))
    if not isinstance(value, str):
        return None

    stripped = value.strip()
    if not stripped:
        return None
    if stripped.isdigit():
        return parse_epoch_timestamp(float(stripped))

    normalized = stripped.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                parsed = datetime.strptime(stripped, pattern)
                break
            except ValueError:
                parsed = None
        if parsed is None:
            return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def parse_epoch_timestamp(value: float) -> datetime | None:
    if value <= 0:
        return None
    if value > 10_000_000_000:
        value /= 1000.0
    try:
        return datetime.fromtimestamp(value, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


TIMESTAMP_KEYS = (
    "observed_at",
    "occurred_at",
    "event_time",
    "timestamp",
    "published_at",
    "datetime",
    "detected_at",
)


GROUND_TRUTH_KEYS = (
    "ground_truth",
    "is_ground_truth",
    "verified_on_scene",
    "direct_observation",
)
