from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config.settings import Settings
from src.routes.events import (
    recent_bmkg_earthquakes,
    recent_canada_cap_alerts,
    recent_dwd_cap_alerts,
    recent_earthquakes,
    recent_emsc_seismicportal_events,
    recent_eonet_events,
    recent_ga_earthquakes,
    recent_geonet_hazards,
    recent_geosphere_austria_warnings,
    recent_hko_weather,
    recent_ipma_warnings,
    recent_met_eireann_warnings,
    recent_meteoalarm_country_warnings,
    recent_metno_alerts,
    recent_nhc_gis,
    recent_nrc_event_notifications,
    recent_nws_alerts,
    recent_tsunami_alerts,
    recent_uk_flood_events,
    recent_volcano_status,
)

from .models import (
    EventCreate,
    EventFeedSyncFeedResult,
    EventFeedSyncRequest,
    EventFeedSyncResponse,
    ObservationCreate,
    RedactionLevel,
    SourceCreate,
    SourceKind,
)
from .service import IntelService


@dataclass(frozen=True)
class _FeedSpec:
    key: str
    source_id: str
    source_name: str
    source_description: str
    trust_tier: str
    integrity_score: float
    default_confidence: float
    data_latency_seconds: int
    load: Callable[[Settings, int], Awaitable[Any]]
    extract: Callable[[Any], Iterable[dict[str, Any]]]


def _stable_suffix(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:24]


def _stable_id(prefix: str, feed_key: str, external_id: str) -> str:
    return f"{prefix}:{feed_key}:{_stable_suffix(external_id)}"


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _coerce_json(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _coerce_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_coerce_json(item) for item in value]
    if hasattr(value, "model_dump"):
        return _coerce_json(value.model_dump(mode="json"))
    return str(value)


def _summary_from_parts(*parts: str | None) -> str:
    return " | ".join(part for part in parts if part)


def _confidence_for_evidence(evidence_basis: str | None, default_confidence: float) -> float:
    modifier = {
        "observed": 0.08,
        "source-reported": 0.05,
        "advisory": 0.04,
        "contextual": -0.03,
    }.get((evidence_basis or "").strip().lower(), 0.0)
    return max(0.05, min(0.99, round(default_confidence + modifier, 3)))


def _status_for_item(item: dict[str, Any]) -> str:
    raw_status = str(item.get("status") or "").strip().lower()
    if raw_status in {"closed", "inactive", "cancel", "cancelled", "cancellation", "resolved"}:
        return "closed"
    if item.get("is_closed") is True or item.get("closed"):
        return "closed"
    expires_at = _parse_dt(item.get("expires_at"))
    if expires_at is not None and expires_at < datetime.now(tz=timezone.utc):
        return "expired"
    return "active"


def _normalize_records(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [record for record in records if record.get("external_id") and record.get("title")]


def _item_to_record(
    *,
    feed_key: str,
    raw_item: Any,
    external_id: str,
    title: str,
    event_type: str,
    time: str | None,
    updated_at: str | None,
    latitude: float | None,
    longitude: float | None,
    summary: str | None,
    source_url: str | None,
    evidence_basis: str | None,
    tags: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = raw_item.model_dump(mode="json") if hasattr(raw_item, "model_dump") else _coerce_json(raw_item)
    return {
        "feed_key": feed_key,
        "external_id": external_id,
        "title": title,
        "event_type": event_type,
        "time": time,
        "updated_at": updated_at,
        "latitude": latitude,
        "longitude": longitude,
        "summary": summary or title,
        "source_url": source_url,
        "evidence_basis": evidence_basis,
        "tags": tags or [],
        "status": _status_for_item(payload if isinstance(payload, dict) else {}),
        "raw_payload": payload,
        "metadata": metadata or {},
    }


def _extract_earthquakes(response: Any) -> Iterable[dict[str, Any]]:
    for item in response.events:
        yield _item_to_record(
            feed_key="earthquakes",
            raw_item=item,
            external_id=item.event_id,
            title=item.title,
            event_type=item.event_type or "earthquake",
            time=item.time,
            updated_at=item.updated,
            latitude=item.latitude,
            longitude=item.longitude,
            summary=_summary_from_parts(item.place, f"M {item.magnitude}" if item.magnitude is not None else None),
            source_url=item.source_url,
            evidence_basis="source-reported",
            tags=[item.alert or "no-alert", item.status or "unknown"],
        )


def _extract_emsc(response: Any) -> Iterable[dict[str, Any]]:
    for item in response.events:
        yield _item_to_record(
            feed_key="emsc-seismicportal",
            raw_item=item,
            external_id=item.event_id,
            title=item.title,
            event_type="earthquake",
            time=item.event_time or item.observed_at,
            updated_at=item.updated_at,
            latitude=item.latitude,
            longitude=item.longitude,
            summary=_summary_from_parts(item.region, f"M {item.magnitude}" if item.magnitude is not None else None),
            source_url=item.source_url,
            evidence_basis=item.evidence_basis,
            tags=[item.action, item.provider or "unknown-provider"],
        )


def _extract_eonet(response: Any) -> Iterable[dict[str, Any]]:
    for item in response.events:
        event_type = item.category_ids[0] if item.category_ids else "environmental_event"
        yield _item_to_record(
            feed_key="eonet",
            raw_item=item,
            external_id=item.event_id,
            title=item.title,
            event_type=event_type,
            time=item.event_date,
            updated_at=item.updated,
            latitude=item.latitude,
            longitude=item.longitude,
            summary=_summary_from_parts(item.description, item.coordinates_summary),
            source_url=item.source_url,
            evidence_basis="contextual",
            tags=item.category_titles,
        )


def _extract_volcano(response: Any) -> Iterable[dict[str, Any]]:
    for item in response.events:
        yield _item_to_record(
            feed_key="volcano-status",
            raw_item=item,
            external_id=item.event_id,
            title=item.title,
            event_type="volcano_status",
            time=item.issued_at,
            updated_at=item.issued_at,
            latitude=item.latitude,
            longitude=item.longitude,
            summary=_summary_from_parts(item.region, item.alert_level, item.aviation_color_code),
            source_url=item.source_url,
            evidence_basis="advisory",
            tags=[item.status_scope, item.alert_level, item.aviation_color_code],
        )


def _extract_tsunami(response: Any) -> Iterable[dict[str, Any]]:
    for item in response.events:
        yield _item_to_record(
            feed_key="tsunami",
            raw_item=item,
            external_id=item.event_id,
            title=item.title,
            event_type=f"tsunami_{item.alert_type}",
            time=item.issued_at,
            updated_at=item.updated_at,
            latitude=item.latitude,
            longitude=item.longitude,
            summary=_summary_from_parts(item.region, item.basin, item.summary),
            source_url=item.source_url,
            evidence_basis=item.evidence_basis,
            tags=[item.alert_type, item.source_center],
        )


def _extract_uk_floods(response: Any) -> Iterable[dict[str, Any]]:
    for item in response.events:
        yield _item_to_record(
            feed_key="uk-floods",
            raw_item=item,
            external_id=item.event_id,
            title=item.title,
            event_type="flood_alert",
            time=item.issued_at or item.updated_at,
            updated_at=item.updated_at,
            latitude=item.latitude,
            longitude=item.longitude,
            summary=_summary_from_parts(item.area_name, item.river_or_sea, item.description or item.message),
            source_url=item.source_url,
            evidence_basis=item.evidence_basis,
            tags=[item.severity, item.region or "unknown-region"],
        )


def _extract_geonet(response: Any) -> Iterable[dict[str, Any]]:
    for item in response.quakes:
        yield _item_to_record(
            feed_key="geonet",
            raw_item=item,
            external_id=item.event_id,
            title=item.title,
            event_type="earthquake",
            time=item.event_time,
            updated_at=item.updated_at,
            latitude=item.latitude,
            longitude=item.longitude,
            summary=_summary_from_parts(item.region, item.locality, f"M {item.magnitude}" if item.magnitude is not None else None),
            source_url=item.source_url,
            evidence_basis=item.evidence_basis,
            tags=["quake", item.status or "unknown"],
        )
    for item in response.volcano_alerts:
        yield _item_to_record(
            feed_key="geonet",
            raw_item=item,
            external_id=item.volcano_id,
            title=item.title,
            event_type="volcano_alert",
            time=item.issued_at,
            updated_at=item.updated_at,
            latitude=item.latitude,
            longitude=item.longitude,
            summary=_summary_from_parts(item.activity, item.hazards, item.aviation_color_code),
            source_url=item.source_url,
            evidence_basis=item.evidence_basis,
            tags=["volcano", item.aviation_color_code or "unknown-color"],
        )


def _extract_hko(response: Any) -> Iterable[dict[str, Any]]:
    for item in response.warnings:
        yield _item_to_record(
            feed_key="hko-weather",
            raw_item=item,
            external_id=item.event_id,
            title=item.title,
            event_type=f"hko_{item.warning_type.lower()}",
            time=item.issued_at,
            updated_at=item.updated_at,
            latitude=None,
            longitude=None,
            summary=_summary_from_parts(item.summary, item.affected_area, item.warning_level),
            source_url=item.source_url,
            evidence_basis=item.evidence_basis,
            tags=[item.warning_type, item.warning_level or "unknown"],
        )
    cyclone = response.tropical_cyclone
    if cyclone is not None:
        yield _item_to_record(
            feed_key="hko-weather",
            raw_item=cyclone,
            external_id=cyclone.event_id,
            title=cyclone.title,
            event_type="tropical_cyclone_context",
            time=cyclone.issued_at,
            updated_at=cyclone.updated_at,
            latitude=None,
            longitude=None,
            summary=_summary_from_parts(cyclone.summary, cyclone.signal),
            source_url=cyclone.source_url,
            evidence_basis=cyclone.evidence_basis,
            tags=["tropical-cyclone", cyclone.signal or "unknown-signal"],
        )


def _extract_nws(response: Any) -> Iterable[dict[str, Any]]:
    for item in response.alerts:
        yield _item_to_record(
            feed_key="nws-alerts",
            raw_item=item,
            external_id=item.event_id,
            title=item.title,
            event_type=f"nws_{item.alert_type}",
            time=item.sent_at or item.effective_at,
            updated_at=item.updated_at,
            latitude=item.latitude,
            longitude=item.longitude,
            summary=_summary_from_parts(item.headline, item.area_description, item.description),
            source_url=item.source_url,
            evidence_basis=item.evidence_basis,
            tags=[item.alert_type, item.severity, item.urgency or "unknown-urgency"],
        )


def _extract_nhc(response: Any) -> Iterable[dict[str, Any]]:
    for item in response.advisories:
        yield _item_to_record(
            feed_key="nhc-gis",
            raw_item=item,
            external_id=item.event_id,
            title=item.title,
            event_type=f"nhc_{item.product_type}",
            time=item.published_at,
            updated_at=item.updated_at,
            latitude=item.latitude,
            longitude=item.longitude,
            summary=_summary_from_parts(item.headline, item.storm_name, item.description),
            source_url=item.source_url,
            evidence_basis=item.evidence_basis,
            tags=[item.product_type, item.storm_type or "unknown-storm-type"],
        )


def _extract_metno(response: Any) -> Iterable[dict[str, Any]]:
    for item in response.alerts:
        latitude = None
        longitude = None
        if None not in {item.bbox_min_lat, item.bbox_max_lat}:
            latitude = (item.bbox_min_lat + item.bbox_max_lat) / 2.0
        if None not in {item.bbox_min_lon, item.bbox_max_lon}:
            longitude = (item.bbox_min_lon + item.bbox_max_lon) / 2.0
        yield _item_to_record(
            feed_key="metno-alerts",
            raw_item=item,
            external_id=item.event_id,
            title=item.title,
            event_type=f"metno_{item.alert_type}",
            time=item.sent_at or item.effective_at,
            updated_at=item.updated_at,
            latitude=latitude,
            longitude=longitude,
            summary=_summary_from_parts(item.area_description, item.geometry_summary, item.severity),
            source_url=item.source_url,
            evidence_basis=item.evidence_basis,
            tags=[item.alert_type, item.severity, item.msg_type],
        )


def _extract_canada_cap(response: Any) -> Iterable[dict[str, Any]]:
    for item in response.alerts:
        yield _item_to_record(
            feed_key="canada-cap",
            raw_item=item,
            external_id=item.event_id,
            title=item.title,
            event_type=f"canada_cap_{item.alert_type}",
            time=item.sent_at or item.effective_at,
            updated_at=item.updated_at,
            latitude=item.latitude,
            longitude=item.longitude,
            summary=_summary_from_parts(item.area_description, item.province_or_region, item.severity),
            source_url=item.source_url,
            evidence_basis=item.evidence_basis,
            tags=[item.alert_type, item.severity],
        )


def _extract_meteoalarm(response: Any) -> Iterable[dict[str, Any]]:
    for item in response.warnings:
        yield _item_to_record(
            feed_key="meteoalarm",
            raw_item=item,
            external_id=item.entry_id,
            title=item.title,
            event_type="meteoalarm_warning",
            time=item.published_at,
            updated_at=item.updated_at,
            latitude=None,
            longitude=None,
            summary=_summary_from_parts(item.country, item.area_label, item.summary),
            source_url=item.source_url or item.link,
            evidence_basis=item.evidence_basis,
            tags=[item.country, item.area_label or "unknown-area"],
        )


def _extract_dwd(response: Any) -> Iterable[dict[str, Any]]:
    for item in response.alerts:
        yield _item_to_record(
            feed_key="dwd-alerts",
            raw_item=item,
            external_id=item.event_id,
            title=item.title,
            event_type=f"dwd_{item.product_family}",
            time=item.sent_at or item.effective_at,
            updated_at=item.expires_at,
            latitude=None,
            longitude=None,
            summary=_summary_from_parts(item.area_description, item.description, item.instruction),
            source_url=item.source_url,
            evidence_basis=item.evidence_basis,
            tags=[item.product_family, item.severity, item.msg_type or "unknown-msg-type"],
        )


def _extract_bmkg(response: Any) -> Iterable[dict[str, Any]]:
    for item in response.events:
        yield _item_to_record(
            feed_key="bmkg-earthquakes",
            raw_item=item,
            external_id=item.event_id,
            title=item.title,
            event_type="earthquake",
            time=item.event_time,
            updated_at=item.local_time,
            latitude=item.latitude,
            longitude=item.longitude,
            summary=_summary_from_parts(item.region, f"M {item.magnitude}" if item.magnitude is not None else None, item.felt_summary),
            source_url=item.source_url,
            evidence_basis=item.evidence_basis,
            tags=["tsunami" if item.tsunami_flag else "no-tsunami"],
        )


def _extract_ga(response: Any) -> Iterable[dict[str, Any]]:
    for item in response.earthquakes:
        yield _item_to_record(
            feed_key="ga-earthquakes",
            raw_item=item,
            external_id=item.event_id,
            title=item.title,
            event_type="earthquake",
            time=item.event_time,
            updated_at=item.updated_at,
            latitude=item.latitude,
            longitude=item.longitude,
            summary=_summary_from_parts(item.region, f"M {item.magnitude}" if item.magnitude is not None else None, item.evaluation_status),
            source_url=item.source_url,
            evidence_basis=item.evidence_basis,
            tags=[item.evaluation_mode or "unknown-mode"],
        )


def _extract_ipma(response: Any) -> Iterable[dict[str, Any]]:
    for item in response.warnings:
        yield _item_to_record(
            feed_key="ipma-warnings",
            raw_item=item,
            external_id=item.event_id,
            title=item.title,
            event_type=f"ipma_{item.warning_type.lower()}",
            time=item.start_time,
            updated_at=item.end_time,
            latitude=item.latitude,
            longitude=item.longitude,
            summary=_summary_from_parts(item.area_name, item.area_region, item.description),
            source_url=item.source_url,
            evidence_basis=item.evidence_basis,
            tags=[item.warning_type, item.warning_level],
        )


def _extract_met_eireann(response: Any) -> Iterable[dict[str, Any]]:
    for item in response.warnings:
        yield _item_to_record(
            feed_key="met-eireann-warnings",
            raw_item=item,
            external_id=item.event_id,
            title=item.title,
            event_type=f"met_eireann_{(item.warning_type or 'warning').lower()}",
            time=item.issued_at or item.onset_at,
            updated_at=item.updated_at,
            latitude=None,
            longitude=None,
            summary=_summary_from_parts(item.affected_area, item.description, item.level),
            source_url=item.source_url,
            evidence_basis=item.evidence_basis,
            tags=[item.level, item.severity],
        )


def _extract_geosphere(response: Any) -> Iterable[dict[str, Any]]:
    for item in response.warnings:
        latitude = None
        longitude = None
        if None not in {item.bbox_min_y, item.bbox_max_y}:
            latitude = (item.bbox_min_y + item.bbox_max_y) / 2.0
        if None not in {item.bbox_min_x, item.bbox_max_x}:
            longitude = (item.bbox_min_x + item.bbox_max_x) / 2.0
        yield _item_to_record(
            feed_key="geosphere-austria-warnings",
            raw_item=item,
            external_id=item.event_id,
            title=item.warning_type_label or "Geosphere Austria Warning",
            event_type="geosphere_warning",
            time=item.issued_at or item.onset_at,
            updated_at=item.expires_at,
            latitude=latitude,
            longitude=longitude,
            summary=_summary_from_parts(item.warning_type_label, item.level, f"municipalities={item.municipality_count}"),
            source_url=item.source_url,
            evidence_basis=item.evidence_basis,
            tags=[item.level, item.color, item.geometry_type or "unknown-geometry"],
        )


def _extract_nrc(response: Any) -> Iterable[dict[str, Any]]:
    for item in response.notifications:
        external_id = item.event_id or item.record_id
        yield _item_to_record(
            feed_key="nrc-notifications",
            raw_item=item,
            external_id=external_id,
            title=item.title,
            event_type="nrc_notification",
            time=item.published_at,
            updated_at=item.updated_at,
            latitude=None,
            longitude=None,
            summary=_summary_from_parts(item.facility_or_org, item.category_text, item.summary),
            source_url=item.source_url,
            evidence_basis=item.evidence_basis,
            tags=[item.status_text or "unknown-status"],
        )


def _load_specs() -> dict[str, _FeedSpec]:
    return {
        "earthquakes": _FeedSpec(
            key="earthquakes",
            source_id="feed:usgs-earthquakes",
            source_name="USGS Earthquake Hazards Program",
            source_description="Recent earthquake events from the USGS earthquake hazards feed.",
            trust_tier="tier_1",
            integrity_score=0.92,
            default_confidence=0.82,
            data_latency_seconds=300,
            load=lambda settings, limit: recent_earthquakes(
                min_magnitude=None,
                since=None,
                limit=min(limit, 2000),
                bbox=None,
                window="day",
                sort="newest",
                settings=settings,
            ),
            extract=_extract_earthquakes,
        ),
        "emsc-seismicportal": _FeedSpec(
            key="emsc-seismicportal",
            source_id="feed:emsc-seismicportal",
            source_name="EMSC Seismic Portal Realtime",
            source_description="Realtime earthquake event stream from EMSC Seismic Portal.",
            trust_tier="tier_2",
            integrity_score=0.84,
            default_confidence=0.76,
            data_latency_seconds=120,
            load=lambda settings, limit: recent_emsc_seismicportal_events(
                min_magnitude=None,
                limit=min(limit, 1000),
                bbox=None,
                action="all",
                sort="newest",
                settings=settings,
            ),
            extract=_extract_emsc,
        ),
        "eonet": _FeedSpec(
            key="eonet",
            source_id="feed:nasa-eonet",
            source_name="NASA EONET",
            source_description="Open and recent NASA environmental event context records.",
            trust_tier="tier_2",
            integrity_score=0.86,
            default_confidence=0.74,
            data_latency_seconds=3600,
            load=lambda settings, limit: recent_eonet_events(
                category=None,
                status="open",
                limit=min(limit, 2000),
                bbox=None,
                days=30,
                sort="newest",
                settings=settings,
            ),
            extract=_extract_eonet,
        ),
        "volcano-status": _FeedSpec(
            key="volcano-status",
            source_id="feed:usgs-volcano-status",
            source_name="USGS Volcano Status",
            source_description="Recent elevated or monitored volcano status advisories.",
            trust_tier="tier_1",
            integrity_score=0.9,
            default_confidence=0.8,
            data_latency_seconds=600,
            load=lambda settings, limit: recent_volcano_status(
                scope="elevated",
                alert_level="all",
                observatory=None,
                limit=min(limit, 1000),
                bbox=None,
                sort="alert",
                settings=settings,
            ),
            extract=_extract_volcano,
        ),
        "tsunami": _FeedSpec(
            key="tsunami",
            source_id="feed:tsunami-alerts",
            source_name="Tsunami Alert Feed",
            source_description="Recent tsunami warning center alerts.",
            trust_tier="tier_1",
            integrity_score=0.9,
            default_confidence=0.81,
            data_latency_seconds=300,
            load=lambda settings, limit: recent_tsunami_alerts(
                alert_type="all",
                source_center="all",
                limit=min(limit, 1000),
                bbox=None,
                sort="newest",
                settings=settings,
            ),
            extract=_extract_tsunami,
        ),
        "uk-floods": _FeedSpec(
            key="uk-floods",
            source_id="feed:uk-ea-floods",
            source_name="UK Environment Agency Flood Alerts",
            source_description="Recent UK Environment Agency flood alerts and warnings.",
            trust_tier="tier_1",
            integrity_score=0.88,
            default_confidence=0.79,
            data_latency_seconds=600,
            load=lambda settings, limit: recent_uk_flood_events(
                severity="all",
                area=None,
                limit=min(limit, 1000),
                bbox=None,
                include_stations=True,
                sort="newest",
                settings=settings,
            ),
            extract=_extract_uk_floods,
        ),
        "geonet": _FeedSpec(
            key="geonet",
            source_id="feed:geonet-hazards",
            source_name="GeoNet Hazards",
            source_description="Recent GeoNet quake and volcano hazard records.",
            trust_tier="tier_1",
            integrity_score=0.9,
            default_confidence=0.81,
            data_latency_seconds=300,
            load=lambda settings, limit: recent_geonet_hazards(
                event_type="all",
                min_magnitude=None,
                alert_level="all",
                limit=min(limit, 1000),
                bbox=None,
                sort="newest",
                settings=settings,
            ),
            extract=_extract_geonet,
        ),
        "hko-weather": _FeedSpec(
            key="hko-weather",
            source_id="feed:hko-weather",
            source_name="Hong Kong Observatory Weather Warnings",
            source_description="Recent Hong Kong Observatory weather warning records.",
            trust_tier="tier_1",
            integrity_score=0.88,
            default_confidence=0.78,
            data_latency_seconds=300,
            load=lambda settings, limit: recent_hko_weather(
                warning_type="all",
                limit=min(limit, 500),
                sort="newest",
                settings=settings,
            ),
            extract=_extract_hko,
        ),
        "nws-alerts": _FeedSpec(
            key="nws-alerts",
            source_id="feed:nws-alerts",
            source_name="US National Weather Service Alerts",
            source_description="Recent NWS weather alerts.",
            trust_tier="tier_1",
            integrity_score=0.92,
            default_confidence=0.82,
            data_latency_seconds=180,
            load=lambda settings, limit: recent_nws_alerts(
                alert_type="all",
                severity="all",
                area=None,
                zone=None,
                event=None,
                limit=min(limit, 1000),
                sort="newest",
                settings=settings,
            ),
            extract=_extract_nws,
        ),
        "nhc-gis": _FeedSpec(
            key="nhc-gis",
            source_id="feed:nhc-gis",
            source_name="National Hurricane Center GIS Products",
            source_description="Recent NHC Atlantic advisory products.",
            trust_tier="tier_1",
            integrity_score=0.92,
            default_confidence=0.83,
            data_latency_seconds=300,
            load=lambda settings, limit: recent_nhc_gis(
                product_type="all",
                storm_name=None,
                limit=min(limit, 1000),
                sort="newest",
                settings=settings,
            ),
            extract=_extract_nhc,
        ),
        "metno-alerts": _FeedSpec(
            key="metno-alerts",
            source_id="feed:metno-alerts",
            source_name="MET Norway Alerts",
            source_description="Recent MET Norway alerts.",
            trust_tier="tier_1",
            integrity_score=0.88,
            default_confidence=0.79,
            data_latency_seconds=600,
            load=lambda settings, limit: recent_metno_alerts(
                severity="all",
                alert_type=None,
                limit=min(limit, 500),
                sort="newest",
                bbox=None,
                settings=settings,
            ),
            extract=_extract_metno,
        ),
        "canada-cap": _FeedSpec(
            key="canada-cap",
            source_id="feed:canada-cap",
            source_name="Canada CAP Alerts",
            source_description="Recent Canadian CAP alerts.",
            trust_tier="tier_1",
            integrity_score=0.88,
            default_confidence=0.79,
            data_latency_seconds=600,
            load=lambda settings, limit: recent_canada_cap_alerts(
                alert_type="all",
                severity="all",
                province=None,
                limit=min(limit, 1000),
                sort="newest",
                settings=settings,
            ),
            extract=_extract_canada_cap,
        ),
        "meteoalarm": _FeedSpec(
            key="meteoalarm",
            source_id="feed:meteoalarm",
            source_name="Meteoalarm Atom Feed",
            source_description="Country weather warnings from Meteoalarm.",
            trust_tier="tier_2",
            integrity_score=0.84,
            default_confidence=0.74,
            data_latency_seconds=900,
            load=lambda settings, limit: recent_meteoalarm_country_warnings(
                q=None,
                limit=min(limit, 1000),
                sort="newest",
                settings=settings,
            ),
            extract=_extract_meteoalarm,
        ),
        "dwd-alerts": _FeedSpec(
            key="dwd-alerts",
            source_id="feed:dwd-alerts",
            source_name="DWD CAP Alerts",
            source_description="Recent DWD CAP warning alerts.",
            trust_tier="tier_1",
            integrity_score=0.9,
            default_confidence=0.8,
            data_latency_seconds=600,
            load=lambda settings, limit: recent_dwd_cap_alerts(
                severity="all",
                event=None,
                limit=min(limit, 1000),
                sort="newest",
                settings=settings,
            ),
            extract=_extract_dwd,
        ),
        "bmkg-earthquakes": _FeedSpec(
            key="bmkg-earthquakes",
            source_id="feed:bmkg-earthquakes",
            source_name="BMKG Earthquakes",
            source_description="Recent BMKG earthquake alerts.",
            trust_tier="tier_1",
            integrity_score=0.88,
            default_confidence=0.79,
            data_latency_seconds=300,
            load=lambda settings, limit: recent_bmkg_earthquakes(
                min_magnitude=5.0,
                limit=min(limit, 50),
                sort="newest",
                settings=settings,
            ),
            extract=_extract_bmkg,
        ),
        "ga-earthquakes": _FeedSpec(
            key="ga-earthquakes",
            source_id="feed:ga-earthquakes",
            source_name="Geoscience Australia Recent Earthquakes",
            source_description="Recent Geoscience Australia earthquake events.",
            trust_tier="tier_1",
            integrity_score=0.88,
            default_confidence=0.79,
            data_latency_seconds=600,
            load=lambda settings, limit: recent_ga_earthquakes(
                min_magnitude=None,
                limit=min(limit, 500),
                bbox=None,
                sort="newest",
                settings=settings,
            ),
            extract=_extract_ga,
        ),
        "ipma-warnings": _FeedSpec(
            key="ipma-warnings",
            source_id="feed:ipma-warnings",
            source_name="IPMA Warnings",
            source_description="Recent IPMA weather warnings.",
            trust_tier="tier_1",
            integrity_score=0.87,
            default_confidence=0.78,
            data_latency_seconds=600,
            load=lambda settings, limit: recent_ipma_warnings(
                level="all",
                area_id=None,
                warning_type=None,
                active_only=True,
                limit=min(limit, 1000),
                sort="newest",
                settings=settings,
            ),
            extract=_extract_ipma,
        ),
        "met-eireann-warnings": _FeedSpec(
            key="met-eireann-warnings",
            source_id="feed:met-eireann-warnings",
            source_name="Met Eireann Warnings",
            source_description="Recent Met Eireann warning records.",
            trust_tier="tier_1",
            integrity_score=0.87,
            default_confidence=0.78,
            data_latency_seconds=600,
            load=lambda settings, limit: recent_met_eireann_warnings(
                level="all",
                limit=min(limit, 1000),
                sort="newest",
                settings=settings,
            ),
            extract=_extract_met_eireann,
        ),
        "geosphere-austria-warnings": _FeedSpec(
            key="geosphere-austria-warnings",
            source_id="feed:geosphere-austria-warnings",
            source_name="Geosphere Austria Warnings",
            source_description="Recent Geosphere Austria weather warnings.",
            trust_tier="tier_1",
            integrity_score=0.87,
            default_confidence=0.78,
            data_latency_seconds=600,
            load=lambda settings, limit: recent_geosphere_austria_warnings(
                level="all",
                limit=min(limit, 1000),
                sort="newest",
                settings=settings,
            ),
            extract=_extract_geosphere,
        ),
        "nrc-notifications": _FeedSpec(
            key="nrc-notifications",
            source_id="feed:nrc-notifications",
            source_name="US NRC Event Notifications",
            source_description="Recent US NRC event notifications.",
            trust_tier="tier_2",
            integrity_score=0.85,
            default_confidence=0.76,
            data_latency_seconds=900,
            load=lambda settings, limit: recent_nrc_event_notifications(
                q=None,
                limit=min(limit, 500),
                sort="event_id",
                settings=settings,
            ),
            extract=_extract_nrc,
        ),
    }


EVENT_FEED_SPECS = _load_specs()
EVENT_FEED_KEYS = tuple(EVENT_FEED_SPECS.keys())


class EventFeedSyncService:
    def __init__(self, settings: Settings, intel_service: IntelService) -> None:
        self._settings = _prepare_sync_settings(settings)
        self._intel = intel_service

    async def sync(self, request: EventFeedSyncRequest) -> EventFeedSyncResponse:
        started_at = datetime.now(tz=timezone.utc).isoformat()
        feeds = list(request.feeds or EVENT_FEED_KEYS)
        invalid = sorted(feed for feed in feeds if feed not in EVENT_FEED_SPECS)
        if invalid:
            raise ValueError(f"Unsupported feed keys: {', '.join(invalid)}")

        results: list[EventFeedSyncFeedResult] = []
        for feed_key in feeds:
            results.append(await self._sync_feed(EVENT_FEED_SPECS[feed_key], request))

        created_alert_count = 0
        if request.evaluate_geofences:
            created_alert_count = len(self._intel.evaluate_geofences(actor=request.actor))

        completed_at = datetime.now(tz=timezone.utc).isoformat()
        return EventFeedSyncResponse(
            started_at=started_at,
            completed_at=completed_at,
            feed_count=len(feeds),
            synced_feed_count=sum(1 for result in results if result.status == "ok"),
            created_alert_count=created_alert_count,
            results=results,
            caveats=[
                "Event-feed sync writes canonical intel events and observations using deterministic ids so reruns update in place.",
                "This bridge currently normalizes event-bearing feed records; deeper provider-specific side tables still remain in their native subsystems.",
            ],
        )

    async def _sync_feed(self, spec: _FeedSpec, request: EventFeedSyncRequest) -> EventFeedSyncFeedResult:
        try:
            source, _ = self._intel.upsert_source(
                SourceCreate(
                    source_id=spec.source_id,
                    name=spec.source_name,
                    kind=SourceKind.DATA_FEED_SOURCE,
                    description=spec.source_description,
                    trust_tier=spec.trust_tier,
                    integrity_score=spec.integrity_score,
                    default_confidence=spec.default_confidence,
                    data_latency_seconds=spec.data_latency_seconds,
                    formats=["api", "event-sync"],
                    tags=["intel-sync", spec.key],
                    metadata_json={"sync_feed_key": spec.key},
                    actor=request.actor,
                )
            )
            response = await spec.load(self._settings, request.max_records_per_feed)
            raw_records = _normalize_records(spec.extract(response))
            created_events = 0
            updated_events = 0
            created_observations = 0
            updated_observations = 0
            touched_event_ids: list[str] = []
            caveats = list(getattr(response, "caveats", []) or [])
            metadata_payload = (
                response.metadata.model_dump(mode="json")
                if hasattr(response, "metadata") and hasattr(response.metadata, "model_dump")
                else {}
            )

            for record in raw_records:
                external_id = str(record["external_id"])
                event_id = _stable_id("event-feed", spec.key, external_id)
                observation_id = _stable_id("observation-feed", spec.key, external_id)
                confidence = _confidence_for_evidence(record.get("evidence_basis"), spec.default_confidence)
                event, event_created = self._intel.upsert_event(
                    EventCreate(
                        event_id=event_id,
                        event_type=str(record["event_type"]),
                        title=str(record["title"]),
                        status=str(record.get("status") or "active"),
                        summary=str(record.get("summary") or record["title"]),
                        redaction_level=RedactionLevel.PUBLIC,
                        latitude=record.get("latitude"),
                        longitude=record.get("longitude"),
                        confidence_score=confidence,
                        started_at=_parse_dt(record.get("time")),
                        detected_at=_parse_dt(record.get("updated_at")) or _parse_dt(record.get("time")),
                        tags=[spec.key, *(record.get("tags") or [])],
                        metadata_json={
                            "feed_key": spec.key,
                            "external_id": external_id,
                            "source_url": record.get("source_url"),
                            "sync_metadata": record.get("metadata") or {},
                            "response_metadata": metadata_payload,
                        },
                        actor=request.actor,
                    )
                )
                observation, observation_created = self._intel.upsert_observation(
                    ObservationCreate(
                        observation_id=observation_id,
                        source_id=source.source_id,
                        event_id=event.event_id,
                        observation_type="event_feed_record",
                        title=str(record["title"]),
                        summary=str(record.get("summary") or record["title"]),
                        observed_at=_parse_dt(record.get("time")),
                        collected_at=datetime.now(tz=timezone.utc),
                        latitude=record.get("latitude"),
                        longitude=record.get("longitude"),
                        raw_uri=record.get("source_url"),
                        extracted_text=str(record.get("summary") or record["title"]),
                        raw_hash_sha256=hashlib.sha256(
                            json.dumps(record.get("raw_payload"), sort_keys=True, ensure_ascii=True).encode("utf-8")
                        ).hexdigest(),
                        confidence_score=confidence,
                        is_ground_truth=False,
                        tags=[spec.key, "event-sync", *(record.get("tags") or [])],
                        raw_payload_json=_coerce_json(record.get("raw_payload")),
                        metadata_json={
                            "feed_key": spec.key,
                            "external_id": external_id,
                            "evidence_basis": record.get("evidence_basis"),
                            "sync_metadata": record.get("metadata") or {},
                        },
                        actor=request.actor,
                    ),
                    recompute_event_confidence=False,
                )
                created_events += int(event_created)
                updated_events += int(not event_created)
                created_observations += int(observation_created)
                updated_observations += int(not observation_created)
                touched_event_ids.append(event.event_id)

            for event_id in sorted(set(touched_event_ids)):
                self._intel.recompute_event_assessment(event_id, actor=request.actor)

            return EventFeedSyncFeedResult(
                feed_key=spec.key,
                source_id=source.source_id,
                status="ok",
                fetched_count=len(raw_records),
                events_created=created_events,
                events_updated=updated_events,
                observations_created=created_observations,
                observations_updated=updated_observations,
                detail=f"Synced {len(raw_records)} records from {spec.source_name}.",
                event_ids=sorted(set(touched_event_ids))[:25],
                caveats=caveats,
            )
        except Exception as exc:  # noqa: BLE001
            return EventFeedSyncFeedResult(
                feed_key=spec.key,
                source_id=spec.source_id,
                status="error",
                detail=f"{exc.__class__.__name__}: {exc}",
                caveats=[
                    "Feed sync failed before records were committed for this feed." if "Synced" not in str(exc) else str(exc),
                ],
            )


def _prepare_sync_settings(settings: Settings) -> Settings:
    server_root = Path(__file__).resolve().parents[2]
    prepared = settings.model_copy(deep=True)
    fixture_fields = [
        "earthquake_fixture_path",
        "eonet_fixture_path",
        "volcano_fixture_path",
        "tsunami_fixture_path",
        "geonet_fixture_path",
        "hko_fixture_path",
        "metno_metalerts_fixture_path",
        "canada_cap_fixture_path",
        "dwd_cap_fixture_path",
        "ga_recent_earthquakes_fixture_path",
    ]
    for field_name in fixture_fields:
        raw_value = getattr(prepared, field_name, None)
        if not isinstance(raw_value, str) or not raw_value:
            continue
        path = Path(raw_value)
        if path.is_absolute():
            continue
        candidate = server_root / path
        if candidate.exists():
            setattr(prepared, field_name, str(candidate))
    return prepared
