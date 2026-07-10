from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha1
from typing import Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import CustodyLogORM, EntityORM, EntityObservationLinkORM, ObservationORM
from src.schemas import EntityResolutionRequest
from src.services.observation_service import (
    extract_observation_timestamp,
    extract_point,
    haversine_km,
    query_observations,
)


EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
PHONE_RE = re.compile(r"\+?\d[\d\-\s().]{6,}\d")

SIGNAL_SPECS: dict[str, tuple[str, ...]] = {
    "person": (
        "email",
        "phone",
        "handle",
        "username",
        "passport_number",
        "full_name",
        "person_name",
        "display_name",
    ),
    "organization": ("organization", "org_name", "company", "employer", "operator", "owner"),
    "vessel": ("vessel_name", "ship_name", "imo", "mmsi", "callsign"),
    "vehicle": ("tail_number", "registration", "license_plate"),
}
DISPLAY_PRIORITY = (
    "vessel_name",
    "ship_name",
    "full_name",
    "person_name",
    "display_name",
    "organization",
    "org_name",
    "company",
    "operator",
    "owner",
    "employer",
    "callsign",
    "registration",
    "license_plate",
    "tail_number",
    "handle",
    "username",
    "email",
    "phone",
    "mmsi",
    "imo",
    "passport_number",
)
PRIMARY_SIGNAL_PRIORITY = (
    "imo",
    "mmsi",
    "passport_number",
    "email",
    "phone",
    "callsign",
    "tail_number",
    "registration",
    "license_plate",
    "handle",
    "username",
    "vessel_name",
    "ship_name",
    "full_name",
    "person_name",
    "display_name",
    "organization",
    "org_name",
    "company",
    "operator",
    "owner",
    "employer",
)
BASE_CONFIDENCE = {
    "imo": 0.9,
    "mmsi": 0.88,
    "passport_number": 0.88,
    "email": 0.84,
    "phone": 0.8,
    "callsign": 0.78,
    "tail_number": 0.78,
    "registration": 0.76,
    "license_plate": 0.76,
    "handle": 0.74,
    "username": 0.72,
    "vessel_name": 0.62,
    "ship_name": 0.62,
    "full_name": 0.58,
    "person_name": 0.58,
    "display_name": 0.58,
    "organization": 0.58,
    "org_name": 0.58,
    "company": 0.58,
    "operator": 0.58,
    "owner": 0.56,
    "employer": 0.54,
}
HARD_SIGNAL_KEYS = {
    "imo",
    "mmsi",
    "passport_number",
    "email",
    "phone",
    "callsign",
    "tail_number",
    "registration",
    "license_plate",
    "handle",
    "username",
}
SOFT_MATCH_MAX_DISTANCE_KM = 50.0
SOFT_MATCH_MAX_TIME_HOURS = 72.0


@dataclass(frozen=True)
class EntitySignal:
    entity_type: str
    signal_key: str
    normalized_value: str
    raw_value: str


@dataclass(frozen=True)
class EntityObservationProfile:
    entity_type: str
    observation: ObservationORM
    signals: tuple[EntitySignal, ...]
    hard_signals: tuple[EntitySignal, ...]
    soft_signals: tuple[EntitySignal, ...]


@dataclass
class MaterializedEntity:
    entity: EntityORM
    observation_count: int
    signal_count: int
    confidence_score: float
    created_new: bool


def materialize_entities(
    session: Session,
    request: EntityResolutionRequest,
    actor: str = "entity_resolution",
) -> list[MaterializedEntity]:
    observations = query_observations(
        session,
        layer_key=request.layer_key,
        source_domain=request.source_domain,
        trust_level=request.trust_level,
        min_lon=request.min_lon,
        min_lat=request.min_lat,
        max_lon=request.max_lon,
        max_lat=request.max_lat,
        since=request.since,
        until=request.until,
        limit=request.limit,
    )
    components = build_resolution_components(
        observations,
        entity_type=request.entity_type,
        min_observations=request.min_observations,
    )

    results: list[MaterializedEntity] = []
    for component in components:
        slug = build_entity_slug(component)
        entity = session.scalar(select(EntityORM).where(EntityORM.slug == slug))
        created_new = entity is None
        confidence_score = compute_entity_confidence(component)
        primary_signal = choose_primary_signal(component)
        canonical_name = choose_canonical_name(component)
        metadata = build_entity_metadata(component, confidence_score, primary_signal)

        if entity is None:
            entity = EntityORM(
                slug=slug,
                entity_type=component["entity_type"],
                canonical_name=canonical_name,
                resolution_basis="rule_based_evidence_graph",
                confidence_score=confidence_score,
                redaction_level=request.redaction_level,
                metadata_json=metadata,
            )
            session.add(entity)
            session.flush()
        else:
            entity.canonical_name = canonical_name
            entity.confidence_score = confidence_score
            entity.redaction_level = request.redaction_level
            entity.metadata_json = metadata
            entity.resolution_basis = "rule_based_evidence_graph"

        log_entity_resolution(session, entity, component, confidence_score, created_new, actor=actor)
        ensure_entity_links(session, entity, component, confidence_score, primary_signal, actor=actor)
        results.append(
            MaterializedEntity(
                entity=entity,
                observation_count=len(component["observations"]),
                signal_count=len(component["signals"]),
                confidence_score=confidence_score,
                created_new=created_new,
            )
        )

    session.commit()
    for result in results:
        session.refresh(result.entity)
    return results


def build_resolution_components(
    observations: list[ObservationORM],
    *,
    entity_type: str | None,
    min_observations: int,
) -> list[dict[str, object]]:
    profiles = build_entity_observation_profiles(observations, entity_type=entity_type)
    if not profiles:
        return []

    parents = list(range(len(profiles)))
    link_rows: list[dict[str, object]] = []

    for left_index, left_profile in enumerate(profiles):
        for right_index in range(left_index + 1, len(profiles)):
            right_profile = profiles[right_index]
            link_details = classify_profile_link(left_profile, right_profile)
            if link_details is None:
                continue
            union_profile_roots(parents, left_index, right_index)
            link_rows.append({"left_index": left_index, "right_index": right_index, **link_details})

    grouped_indexes: dict[int, list[int]] = defaultdict(list)
    for index in range(len(profiles)):
        grouped_indexes[find_profile_root(parents, index)].append(index)

    rows: list[dict[str, object]] = []
    for component_indexes in grouped_indexes.values():
        component = build_resolution_component(profiles, component_indexes, link_rows)
        if len(component["observations"]) < min_observations:
            continue
        rows.append(component)

    rows.sort(
        key=lambda row: (
            -compute_entity_confidence(row),
            -len(row["observations"]),
            row["entity_type"],
            choose_canonical_name(row).lower(),
        )
    )
    return rows


def build_entity_observation_profiles(
    observations: list[ObservationORM],
    *,
    entity_type: str | None,
) -> list[EntityObservationProfile]:
    profiles: list[EntityObservationProfile] = []
    for observation in observations:
        grouped: dict[str, dict[tuple[str, str, str], EntitySignal]] = defaultdict(dict)
        for signal in extract_entity_signals(observation, entity_type=entity_type):
            grouped[signal.entity_type][(signal.entity_type, signal.signal_key, signal.normalized_value)] = signal
        for resolved_type, signals_by_key in grouped.items():
            signals = tuple(sorted(signals_by_key.values(), key=signal_sort_key))
            hard_signals = tuple(signal for signal in signals if signal.signal_key in HARD_SIGNAL_KEYS)
            soft_signals = tuple(signal for signal in signals if signal.signal_key not in HARD_SIGNAL_KEYS)
            profiles.append(
                EntityObservationProfile(
                    entity_type=resolved_type,
                    observation=observation,
                    signals=signals,
                    hard_signals=hard_signals,
                    soft_signals=soft_signals,
                )
            )
    return profiles


def build_resolution_component(
    profiles: list[EntityObservationProfile],
    component_indexes: list[int],
    link_rows: list[dict[str, object]],
) -> dict[str, object]:
    component_profiles = [profiles[index] for index in component_indexes]
    observations = sorted(
        {profile.observation.observation_id: profile.observation for profile in component_profiles}.values(),
        key=lambda observation: observation.observation_id,
    )
    domains = sorted({observation.source_domain for observation in observations if observation.source_domain})
    layers = sorted({observation.layer_key for observation in observations})
    signal_support = build_signal_support(component_profiles)
    support_rows = sorted_signal_support_rows(signal_support)
    signal_rows = [row["signal"] for row in support_rows]
    hard_signal_keys = sorted({row["signal"].signal_key for row in support_rows if row["signal_class"] == "hard"})
    soft_signal_keys = sorted({row["signal"].signal_key for row in support_rows if row["signal_class"] == "soft"})
    signal_keys = sorted({row["signal"].signal_key for row in support_rows})
    component_edge_rows = [
        row
        for row in link_rows
        if row["left_index"] in component_indexes and row["right_index"] in component_indexes
    ]
    link_reason_counts = Counter(row["reason"] for row in component_edge_rows)
    link_reasons = [
        {"reason": reason, "count": count}
        for reason, count in sorted(link_reason_counts.items(), key=lambda item: (-item[1], item[0]))
    ]
    signal_conflicts = build_signal_conflicts(support_rows)
    return {
        "entity_type": component_profiles[0].entity_type,
        "signals": signal_rows,
        "signal_keys": signal_keys,
        "hard_signal_keys": hard_signal_keys,
        "soft_signal_keys": soft_signal_keys,
        "observations": observations,
        "domains": domains,
        "layers": layers,
        "signal_support": support_rows,
        "signal_conflicts": signal_conflicts,
        "link_reasons": link_reasons,
        "evidence_strength": classify_component_evidence_strength(
            hard_signal_keys=hard_signal_keys,
            signal_support=support_rows,
            domains=domains,
            observations=observations,
            link_reasons=link_reasons,
        ),
    }


def build_signal_support(
    profiles: list[EntityObservationProfile],
) -> dict[tuple[str, str], dict[str, object]]:
    support: dict[tuple[str, str], dict[str, object]] = {}
    for profile in profiles:
        for signal in profile.signals:
            key = (signal.signal_key, signal.normalized_value)
            entry = support.setdefault(
                key,
                {
                    "signal": signal,
                    "signal_class": "hard" if signal.signal_key in HARD_SIGNAL_KEYS else "soft",
                    "observation_ids": set(),
                    "raw_value_counts": Counter(),
                },
            )
            entry["observation_ids"].add(profile.observation.observation_id)
            entry["raw_value_counts"][signal.raw_value] += 1
    return support


def sorted_signal_support_rows(
    support: dict[tuple[str, str], dict[str, object]],
) -> list[dict[str, object]]:
    rows = []
    for entry in support.values():
        raw_values = sorted(entry["raw_value_counts"].items(), key=lambda item: (-item[1], item[0].lower()))
        rows.append(
            {
                "signal": entry["signal"],
                "signal_class": entry["signal_class"],
                "observation_count": len(entry["observation_ids"]),
                "raw_values": [value for value, _ in raw_values],
                "raw_value_counts": dict(entry["raw_value_counts"]),
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            primary_signal_rank(row["signal"].signal_key),
            -int(row["observation_count"]),
            row["signal"].normalized_value,
        ),
    )


def build_signal_conflicts(signal_support: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in signal_support:
        grouped[row["signal"].signal_key].append(row)
    conflicts: list[dict[str, object]] = []
    for signal_key, rows in grouped.items():
        normalized_values = sorted({row["signal"].normalized_value for row in rows})
        if len(normalized_values) <= 1:
            continue
        raw_values = sorted({value for row in rows for value in row["raw_values"]}, key=str.lower)
        conflicts.append(
            {
                "signal_key": signal_key,
                "signal_class": rows[0]["signal_class"],
                "variant_count": len(normalized_values),
                "normalized_values": normalized_values,
                "raw_values": raw_values,
                "observation_count": sum(int(row["observation_count"]) for row in rows),
            }
        )
    return sorted(
        conflicts,
        key=lambda row: (
            row["signal_class"] != "hard",
            primary_signal_rank(str(row["signal_key"])),
            -int(row["variant_count"]),
            str(row["signal_key"]),
        ),
    )


def classify_profile_link(
    left: EntityObservationProfile,
    right: EntityObservationProfile,
) -> dict[str, object] | None:
    if left.entity_type != right.entity_type:
        return None

    shared_hard = collect_shared_signals(left.hard_signals, right.hard_signals)
    if shared_hard:
        return {"reason": "shared_hard_signal", "strength": "anchored", "shared_signals": shared_hard}

    shared_all = collect_shared_signals(left.signals, right.signals)
    if len(shared_all) >= 2:
        return {"reason": "multi_signal_match", "strength": "corroborated", "shared_signals": shared_all}

    shared_soft = collect_shared_signals(left.soft_signals, right.soft_signals)
    if shared_soft and profiles_are_contextually_consistent(left, right):
        return {
            "reason": "soft_signal_context_match",
            "strength": "contextual",
            "shared_signals": shared_soft,
        }

    return None


def collect_shared_signals(
    left_signals: Iterable[EntitySignal],
    right_signals: Iterable[EntitySignal],
) -> list[dict[str, str]]:
    left_map = {(signal.signal_key, signal.normalized_value): signal for signal in left_signals}
    right_map = {(signal.signal_key, signal.normalized_value): signal for signal in right_signals}
    shared_keys = sorted(set(left_map) & set(right_map), key=lambda item: (primary_signal_rank(item[0]), item[1]))
    return [
        {
            "signal_key": signal_key,
            "normalized_value": normalized_value,
            "left_raw_value": left_map[(signal_key, normalized_value)].raw_value,
            "right_raw_value": right_map[(signal_key, normalized_value)].raw_value,
        }
        for signal_key, normalized_value in shared_keys
    ]


def profiles_are_contextually_consistent(
    left: EntityObservationProfile,
    right: EntityObservationProfile,
) -> bool:
    left_time = normalize_timestamp(extract_observation_timestamp(left.observation) or left.observation.created_at)
    right_time = normalize_timestamp(extract_observation_timestamp(right.observation) or right.observation.created_at)
    time_delta_hours = abs((left_time - right_time).total_seconds()) / 3600.0

    left_point = extract_point(left.observation)
    right_point = extract_point(right.observation)
    distance_km = haversine_km(left_point, right_point) if left_point and right_point else None

    if distance_km is not None and time_delta_hours <= SOFT_MATCH_MAX_TIME_HOURS and distance_km <= SOFT_MATCH_MAX_DISTANCE_KM:
        return True
    if distance_km is not None and distance_km <= 10.0:
        return True
    if time_delta_hours <= 6.0 and (
        left.observation.layer_key == right.observation.layer_key
        or left.observation.source_domain == right.observation.source_domain
    ):
        return True
    return False


def extract_entity_signals(
    observation: ObservationORM,
    *,
    entity_type: str | None = None,
) -> list[EntitySignal]:
    payload = observation.content_json if isinstance(observation.content_json, dict) else {}
    signals: dict[tuple[str, str, str], EntitySignal] = {}

    for resolved_type, keys in SIGNAL_SPECS.items():
        if entity_type is not None and resolved_type != entity_type:
            continue
        for key in keys:
            for value in iter_payload_values_for_key(payload, key):
                normalized = normalize_signal_value(key, value)
                if not normalized:
                    continue
                signal = EntitySignal(
                    entity_type=resolved_type,
                    signal_key=key,
                    normalized_value=normalized,
                    raw_value=str(value).strip(),
                )
                signals[(signal.entity_type, signal.signal_key, signal.normalized_value)] = signal

    for email in EMAIL_RE.findall(observation.content_text or ""):
        normalized = normalize_signal_value("email", email)
        if not normalized:
            continue
        signal = EntitySignal(
            entity_type="person",
            signal_key="email",
            normalized_value=normalized,
            raw_value=email,
        )
        signals[(signal.entity_type, signal.signal_key, signal.normalized_value)] = signal

    for phone in PHONE_RE.findall(observation.content_text or ""):
        normalized = normalize_signal_value("phone", phone)
        if not normalized:
            continue
        signal = EntitySignal(
            entity_type="person",
            signal_key="phone",
            normalized_value=normalized,
            raw_value=phone,
        )
        signals[(signal.entity_type, signal.signal_key, signal.normalized_value)] = signal

    return sorted(signals.values(), key=signal_sort_key)


def iter_payload_values_for_key(payload: object, signal_key: str) -> Iterable[object]:
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key == signal_key:
                yield from flatten_signal_values(value)
            if isinstance(value, (dict, list)):
                yield from iter_payload_values_for_key(value, signal_key)
    elif isinstance(payload, list):
        for item in payload:
            yield from iter_payload_values_for_key(item, signal_key)


def flatten_signal_values(value: object) -> Iterable[object]:
    if isinstance(value, list):
        for item in value:
            yield from flatten_signal_values(item)
        return
    if isinstance(value, dict):
        for key in ("value", "text", "name", "display_name"):
            nested = value.get(key)
            if isinstance(nested, str) and nested.strip():
                yield nested
                return
        return
    if isinstance(value, (str, int, float)):
        yield value


def normalize_signal_value(signal_key: str, value: object) -> str | None:
    text = str(value).strip()
    if not text:
        return None
    if signal_key == "email":
        return text.lower()
    if signal_key == "phone":
        digits = "".join(character for character in text if character.isdigit())
        return digits if len(digits) >= 7 else None
    if signal_key in {"handle", "username"}:
        return text.lstrip("@").strip().lower()
    if signal_key in {"imo", "mmsi", "callsign", "tail_number", "registration", "license_plate", "passport_number"}:
        return re.sub(r"[^A-Z0-9]", "", text.upper())
    return " ".join(text.casefold().split())


def choose_canonical_name(component: dict[str, object]) -> str:
    support_rows: list[dict[str, object]] = component["signal_support"]
    for signal_key in DISPLAY_PRIORITY:
        matches = [row for row in support_rows if row["signal"].signal_key == signal_key]
        if not matches:
            continue
        best = sorted(
            matches,
            key=lambda row: (-int(row["observation_count"]), -len(row["raw_values"]), row["raw_values"][0].lower()),
        )[0]
        return best["raw_values"][0]
    return support_rows[0]["raw_values"][0]


def choose_primary_signal(component: dict[str, object]) -> dict[str, object]:
    support_rows: list[dict[str, object]] = component["signal_support"]
    best = sorted(
        support_rows,
        key=lambda row: (
            primary_signal_rank(row["signal"].signal_key),
            -int(row["observation_count"]),
            row["signal"].normalized_value,
        ),
    )[0]
    return {
        "signal_key": best["signal"].signal_key,
        "normalized_value": best["signal"].normalized_value,
        "raw_value": best["raw_values"][0],
        "signal_class": best["signal_class"],
        "observation_count": int(best["observation_count"]),
    }


def build_entity_slug(component: dict[str, object]) -> str:
    entity_type = component["entity_type"]
    primary_signal = choose_primary_signal(component)
    basis = f"{entity_type}:{primary_signal['signal_key']}:{primary_signal['normalized_value']}"
    return f"entity-{sha1(basis.encode('utf-8')).hexdigest()[:12]}"


def compute_entity_confidence(component: dict[str, object]) -> float:
    observations: list[ObservationORM] = component["observations"]
    domains: list[str] = component["domains"]
    layers: list[str] = component["layers"]
    support_rows: list[dict[str, object]] = component["signal_support"]
    primary_signal = choose_primary_signal(component)
    base = BASE_CONFIDENCE.get(primary_signal["signal_key"], 0.55)
    hard_signal_count = sum(1 for row in support_rows if row["signal_class"] == "hard")
    corroborated_signal_count = sum(1 for row in support_rows if int(row["observation_count"]) > 1)
    evidence_strength = str(component["evidence_strength"])
    signal_conflicts: list[dict[str, object]] = component["signal_conflicts"]
    hard_conflict_count = sum(1 for row in signal_conflicts if row["signal_class"] == "hard")
    soft_conflict_count = sum(1 for row in signal_conflicts if row["signal_class"] == "soft")
    trusted_count = sum(1 for observation in observations if observation.trust_level == "trusted")
    ground_truth_count = sum(1 for observation in observations if bool(observation.content_json.get("ground_truth")))
    timeline = build_component_timeline(observations)
    _, max_distance_km = build_component_geospatial_summary(observations)
    time_span_hours = compute_timeline_span_hours(timeline["started_at"], timeline["ended_at"])

    score = base
    score += min(0.16, 0.04 * max(0, len(observations) - 1))
    score += min(0.1, 0.05 * max(0, len(domains) - 1))
    score += min(0.06, 0.03 * max(0, len(layers) - 1))
    score += min(0.06, 0.03 * max(0, hard_signal_count - 1))
    score += min(0.04, 0.02 * max(0, corroborated_signal_count - 1))
    score += min(0.05, 0.025 * trusted_count)
    score += min(0.04, 0.04 * ground_truth_count)

    if evidence_strength == "anchored":
        score += 0.04
    elif evidence_strength == "corroborated":
        score += 0.02
    elif evidence_strength == "soft_correlated":
        score -= 0.04

    if hard_signal_count == 0 and len(domains) < 2:
        score -= 0.05
    if hard_signal_count == 0:
        score = min(score, 0.82)
    if hard_signal_count == 0 and max_distance_km is not None and max_distance_km > 100.0:
        score -= 0.08
    if hard_signal_count == 0 and time_span_hours is not None and time_span_hours > 168.0:
        score -= 0.06
    if len(domains) == 1 and len(observations) >= 3 and hard_signal_count == 0:
        score -= 0.04
    if hard_conflict_count > 0:
        score -= min(0.15, 0.07 * hard_conflict_count)
    if soft_conflict_count > 0:
        score -= min(0.09, 0.03 * soft_conflict_count)

    return round(min(max(score, 0.35), 0.99), 2)


def build_entity_metadata(
    component: dict[str, object],
    confidence_score: float,
    primary_signal: dict[str, object],
) -> dict[str, object]:
    observations: list[ObservationORM] = component["observations"]
    timeline = build_component_timeline(observations)
    centroid_geojson, max_distance_km = build_component_geospatial_summary(observations)
    signal_conflicts: list[dict[str, object]] = component["signal_conflicts"]
    return {
        "confidence_score": confidence_score,
        "observation_ids": [observation.observation_id for observation in observations],
        "signal_keys": component["signal_keys"],
        "hard_signal_keys": component["hard_signal_keys"],
        "soft_signal_keys": component["soft_signal_keys"],
        "domains": component["domains"],
        "layers": component["layers"],
        "observation_count": len(observations),
        "source_domain_count": len(component["domains"]),
        "layer_count": len(component["layers"]),
        "trusted_observation_count": sum(1 for observation in observations if observation.trust_level == "trusted"),
        "ground_truth_count": sum(1 for observation in observations if bool(observation.content_json.get("ground_truth"))),
        "started_at": timeline["started_at"],
        "ended_at": timeline["ended_at"],
        "centroid_geojson": centroid_geojson,
        "max_observation_distance_km": max_distance_km,
        "primary_signal": primary_signal,
        "evidence_strength": component["evidence_strength"],
        "signal_conflict_count": len(signal_conflicts),
        "hard_signal_conflict_count": sum(1 for row in signal_conflicts if row["signal_class"] == "hard"),
        "soft_signal_conflict_count": sum(1 for row in signal_conflicts if row["signal_class"] == "soft"),
        "signal_conflicts": signal_conflicts,
        "link_reasons": component["link_reasons"],
        "analytic_assessment": build_entity_analytic_assessment(
            component,
            confidence_score=confidence_score,
            max_distance_km=max_distance_km,
            started_at=timeline["started_at"],
            ended_at=timeline["ended_at"],
        ),
        "signals": [
            {
                "entity_type": row["signal"].entity_type,
                "signal_key": row["signal"].signal_key,
                "normalized_value": row["signal"].normalized_value,
                "signal_class": row["signal_class"],
                "observation_count": int(row["observation_count"]),
                "raw_values": row["raw_values"],
            }
            for row in component["signal_support"]
        ],
    }


def log_entity_resolution(
    session: Session,
    entity: EntityORM,
    component: dict[str, object],
    confidence_score: float,
    created_new: bool,
    *,
    actor: str,
) -> None:
    observation_ids = [observation.observation_id for observation in component["observations"]]
    details = {
        "entity_type": entity.entity_type,
        "canonical_name": entity.canonical_name,
        "confidence_score": confidence_score,
        "observation_ids": observation_ids,
        "signal_keys": component["signal_keys"],
        "hard_signal_keys": component["hard_signal_keys"],
        "soft_signal_keys": component["soft_signal_keys"],
        "evidence_strength": component["evidence_strength"],
        "signal_conflict_count": len(component["signal_conflicts"]),
        "signal_conflicts": component["signal_conflicts"],
        "link_reasons": component["link_reasons"],
    }
    session.add(
        CustodyLogORM(
            object_type="entity",
            object_id=str(entity.entity_id),
            action="entity_created_from_resolution" if created_new else "entity_updated_from_resolution",
            actor=actor,
            details_json=details,
        )
    )
    session.add(
        CustodyLogORM(
            object_type="entity_resolution",
            object_id=str(entity.entity_id),
            action="resolution_materialized",
            actor=actor,
            details_json=details,
        )
    )


def ensure_entity_links(
    session: Session,
    entity: EntityORM,
    component: dict[str, object],
    confidence_score: float,
    primary_signal: dict[str, object],
    *,
    actor: str,
) -> None:
    observations: list[ObservationORM] = component["observations"]
    existing_ids = {
        link.observation_id
        for link in session.scalars(
            select(EntityObservationLinkORM).where(EntityObservationLinkORM.entity_id == entity.entity_id)
        )
    }
    match_basis = f"{entity.entity_type}:{primary_signal['signal_key']}"
    for observation in observations:
        if observation.observation_id in existing_ids:
            continue
        link = EntityObservationLinkORM(
            entity_id=entity.entity_id,
            observation_id=observation.observation_id,
            match_basis=match_basis,
            confidence_contribution=confidence_score,
        )
        session.add(link)
        session.flush()
        session.add(
            CustodyLogORM(
                object_type="entity_observation_link",
                object_id=str(link.entity_observation_link_id),
                action="entity_link_created",
                actor=actor,
                details_json={
                    "entity_id": entity.entity_id,
                    "observation_id": observation.observation_id,
                    "match_basis": match_basis,
                    "confidence_contribution": confidence_score,
                },
            )
        )
        existing_ids.add(observation.observation_id)


def build_component_timeline(observations: list[ObservationORM]) -> dict[str, str | None]:
    timestamps = [normalize_timestamp(extract_observation_timestamp(observation) or observation.created_at) for observation in observations]
    if not timestamps:
        return {"started_at": None, "ended_at": None}
    return {"started_at": format_time(min(timestamps)), "ended_at": format_time(max(timestamps))}


def build_component_geospatial_summary(
    observations: list[ObservationORM],
) -> tuple[dict[str, object] | None, float | None]:
    points = [point for observation in observations if (point := extract_point(observation)) is not None]
    if not points:
        return None, None
    centroid_lon = sum(point[0] for point in points) / len(points)
    centroid_lat = sum(point[1] for point in points) / len(points)
    centroid = (centroid_lon, centroid_lat)
    max_distance = max(haversine_km(point, centroid) for point in points) if points else None
    return (
        {"type": "Point", "coordinates": [round(centroid_lon, 6), round(centroid_lat, 6)]},
        round(max_distance, 3) if max_distance is not None else None,
    )


def build_entity_analytic_assessment(
    component: dict[str, object],
    *,
    confidence_score: float,
    max_distance_km: float | None,
    started_at: str | None,
    ended_at: str | None,
) -> dict[str, object]:
    observations: list[ObservationORM] = component["observations"]
    signal_support: list[dict[str, object]] = component["signal_support"]
    trusted_count = sum(1 for observation in observations if observation.trust_level == "trusted")
    ground_truth_count = sum(1 for observation in observations if bool(observation.content_json.get("ground_truth")))
    drivers: list[str] = []
    cautions: list[str] = []
    signal_conflicts: list[dict[str, object]] = component["signal_conflicts"]

    if component["hard_signal_keys"]:
        drivers.append(f"Hard identifiers present: {', '.join(component['hard_signal_keys'])}.")
    repeated_rows = [row for row in signal_support if int(row["observation_count"]) > 1]
    if repeated_rows:
        repeated_keys = ", ".join(sorted({row["signal"].signal_key for row in repeated_rows}))
        drivers.append(f"Repeated cross-observation signal support on {repeated_keys}.")
    if len(component["domains"]) >= 2:
        drivers.append(f"Cross-domain corroboration spans {len(component['domains'])} domains.")
    if trusted_count > 0:
        drivers.append(f"Trusted observations contributing: {trusted_count}.")
    if ground_truth_count > 0:
        drivers.append(f"Ground-truth observations contributing: {ground_truth_count}.")

    time_span_hours = compute_timeline_span_hours(started_at, ended_at)
    if not component["hard_signal_keys"]:
        cautions.append("No hard identifier anchors this entity; resolution depends on contextual and repeated soft signals.")
    if len(component["domains"]) <= 1:
        cautions.append("Evidence is concentrated in a single source domain.")
    if max_distance_km is not None and max_distance_km > 100.0:
        cautions.append(f"Spatial dispersion is wide at {max_distance_km:.1f} km.")
    if time_span_hours is not None and time_span_hours > 168.0:
        cautions.append(f"Observation span is long at {time_span_hours:.1f} hours.")
    hard_conflicts = [row for row in signal_conflicts if row["signal_class"] == "hard"]
    soft_conflicts = [row for row in signal_conflicts if row["signal_class"] == "soft"]
    if hard_conflicts:
        cautions.append(
            "Conflicting hard identifiers observed on "
            + ", ".join(sorted(str(row["signal_key"]) for row in hard_conflicts))
            + "."
        )
    elif soft_conflicts:
        cautions.append(
            "Conflicting soft identifiers observed on "
            + ", ".join(sorted(str(row["signal_key"]) for row in soft_conflicts))
            + "."
        )
    if not cautions:
        cautions.append("No major weakening factors were detected in the current entity evidence set.")

    return {
        "confidence_score": confidence_score,
        "drivers": drivers,
        "cautions": cautions,
        "signal_conflict_count": len(signal_conflicts),
        "trusted_observation_count": trusted_count,
        "ground_truth_count": ground_truth_count,
        "source_domain_count": len(component["domains"]),
        "layer_count": len(component["layers"]),
    }


def classify_component_evidence_strength(
    *,
    hard_signal_keys: list[str],
    signal_support: list[dict[str, object]],
    domains: list[str],
    observations: list[ObservationORM],
    link_reasons: list[dict[str, object]],
) -> str:
    if hard_signal_keys and (len(domains) >= 2 or len(observations) >= 2):
        return "anchored"
    if any(row["reason"] == "multi_signal_match" for row in link_reasons):
        return "corroborated"
    if any(int(row["observation_count"]) > 1 for row in signal_support):
        return "soft_correlated"
    return "tentative"


def compute_timeline_span_hours(started_at: str | None, ended_at: str | None) -> float | None:
    if not started_at or not ended_at:
        return None
    try:
        started = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        ended = datetime.fromisoformat(ended_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    return abs((ended - started).total_seconds()) / 3600.0


def find_profile_root(parents: list[int], index: int) -> int:
    if parents[index] != index:
        parents[index] = find_profile_root(parents, parents[index])
    return parents[index]


def union_profile_roots(parents: list[int], left_index: int, right_index: int) -> None:
    left_root = find_profile_root(parents, left_index)
    right_root = find_profile_root(parents, right_index)
    if left_root == right_root:
        return
    parents[right_root] = left_root


def signal_sort_key(signal: EntitySignal) -> tuple[int, str, str]:
    return (primary_signal_rank(signal.signal_key), signal.normalized_value, signal.raw_value.lower())


def primary_signal_rank(signal_key: str) -> int:
    if signal_key in PRIMARY_SIGNAL_PRIORITY:
        return PRIMARY_SIGNAL_PRIORITY.index(signal_key)
    return len(PRIMARY_SIGNAL_PRIORITY)


def normalize_timestamp(value: datetime) -> datetime:
    if value.tzinfo is not None:
        return value
    return value.replace(tzinfo=timezone.utc)


def format_time(value: datetime | None) -> str | None:
    if value is None:
        return None
    return normalize_timestamp(value).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
