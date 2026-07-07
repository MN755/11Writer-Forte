from __future__ import annotations

import re
from dataclasses import dataclass
from hashlib import sha1

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import CustodyLogORM, EntityORM, EntityObservationLinkORM, ObservationORM
from src.schemas import EntityResolutionRequest
from src.services.observation_service import query_observations


EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
PHONE_RE = re.compile(r"\+?\d[\d\-\s().]{6,}\d")

SIGNAL_SPECS: dict[str, tuple[str, ...]] = {
    "person": ("email", "phone", "handle", "username", "passport_number", "full_name", "person_name"),
    "organization": ("organization", "org_name", "company", "employer"),
    "vessel": ("vessel_name", "ship_name", "imo", "mmsi", "callsign"),
    "vehicle": ("tail_number", "registration"),
}
DISPLAY_PRIORITY = (
    "vessel_name",
    "ship_name",
    "full_name",
    "person_name",
    "organization",
    "org_name",
    "company",
    "employer",
    "callsign",
    "registration",
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
    "handle",
    "username",
    "vessel_name",
    "ship_name",
    "full_name",
    "person_name",
    "organization",
    "org_name",
    "company",
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
    "handle": 0.74,
    "username": 0.72,
    "vessel_name": 0.62,
    "ship_name": 0.62,
    "full_name": 0.58,
    "person_name": 0.58,
    "organization": 0.58,
    "org_name": 0.58,
    "company": 0.58,
    "employer": 0.54,
}


@dataclass(frozen=True)
class EntitySignal:
    entity_type: str
    signal_key: str
    normalized_value: str
    raw_value: str


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
        canonical_name = choose_canonical_name(component)
        metadata = build_entity_metadata(component, confidence_score)

        if entity is None:
            entity = EntityORM(
                slug=slug,
                entity_type=component["entity_type"],
                canonical_name=canonical_name,
                resolution_basis="rule_based",
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

        log_entity_resolution(session, entity, component, confidence_score, created_new, actor=actor)
        ensure_entity_links(session, entity, component, confidence_score, actor=actor)
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
    node_details: dict[tuple[str, str, str], EntitySignal] = {}
    node_observations: dict[tuple[str, str, str], set[int]] = {}
    node_domains: dict[tuple[str, str, str], set[str]] = {}
    node_layers: dict[tuple[str, str, str], set[str]] = {}
    observation_index = {observation.observation_id: observation for observation in observations}
    parents: dict[tuple[str, str, str], tuple[str, str, str]] = {}

    for observation in observations:
        grouped: dict[str, list[EntitySignal]] = {}
        for signal in extract_entity_signals(observation, entity_type=entity_type):
            grouped.setdefault(signal.entity_type, []).append(signal)
            node = (signal.entity_type, signal.signal_key, signal.normalized_value)
            node_details[node] = signal
            node_observations.setdefault(node, set()).add(observation.observation_id)
            if observation.source_domain:
                node_domains.setdefault(node, set()).add(observation.source_domain)
            node_layers.setdefault(node, set()).add(observation.layer_key)
            parents.setdefault(node, node)

        for signals in grouped.values():
            if not signals:
                continue
            first = (signals[0].entity_type, signals[0].signal_key, signals[0].normalized_value)
            for signal in signals[1:]:
                current = (signal.entity_type, signal.signal_key, signal.normalized_value)
                union_nodes(parents, first, current)

    components: dict[tuple[str, str, str], dict[str, object]] = {}
    for node, signal in node_details.items():
        root = find_root(parents, node)
        component = components.setdefault(
            root,
            {
                "entity_type": signal.entity_type,
                "signals": [],
                "signal_keys": set(),
                "observations": set(),
                "domains": set(),
                "layers": set(),
            },
        )
        component["signals"].append(signal)
        component["signal_keys"].add(signal.signal_key)
        component["observations"].update(node_observations.get(node, set()))
        component["domains"].update(node_domains.get(node, set()))
        component["layers"].update(node_layers.get(node, set()))

    rows: list[dict[str, object]] = []
    for component in components.values():
        observation_ids = sorted(component["observations"])
        if len(observation_ids) < min_observations:
            continue
        component["observations"] = [
            observation_index[observation_id]
            for observation_id in observation_ids
            if observation_id in observation_index
        ]
        component["domains"] = sorted(component["domains"])
        component["layers"] = sorted(component["layers"])
        component["signal_keys"] = sorted(component["signal_keys"])
        rows.append(component)

    rows.sort(
        key=lambda row: (
            -len(row["observations"]),
            row["entity_type"],
            choose_canonical_name(row).lower(),
        )
    )
    return rows


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
            value = payload.get(key)
            if value is None:
                continue
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

    return list(signals.values())


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
    if signal_key in {"imo", "mmsi", "callsign", "tail_number", "registration", "passport_number"}:
        return re.sub(r"[^A-Z0-9]", "", text.upper())
    return " ".join(text.casefold().split())


def choose_canonical_name(component: dict[str, object]) -> str:
    signals: list[EntitySignal] = component["signals"]
    for signal_key in DISPLAY_PRIORITY:
        for signal in signals:
            if signal.signal_key == signal_key:
                return signal.raw_value
    return signals[0].raw_value


def build_entity_slug(component: dict[str, object]) -> str:
    entity_type = component["entity_type"]
    signals: list[EntitySignal] = component["signals"]
    primary_signal = sorted(
        signals,
        key=lambda signal: (
            PRIMARY_SIGNAL_PRIORITY.index(signal.signal_key)
            if signal.signal_key in PRIMARY_SIGNAL_PRIORITY
            else len(PRIMARY_SIGNAL_PRIORITY),
            signal.normalized_value,
        ),
    )[0]
    basis = f"{entity_type}:{primary_signal.signal_key}:{primary_signal.normalized_value}"
    return f"entity-{sha1(basis.encode('utf-8')).hexdigest()[:12]}"


def compute_entity_confidence(component: dict[str, object]) -> float:
    observations: list[ObservationORM] = component["observations"]
    signals: list[EntitySignal] = component["signals"]
    domains: list[str] = component["domains"]
    layers: list[str] = component["layers"]
    base = max(BASE_CONFIDENCE.get(signal.signal_key, 0.55) for signal in signals)
    score = (
        base
        + min(0.15, 0.05 * max(0, len(observations) - 1))
        + min(0.1, 0.05 * max(0, len(domains) - 1))
        + min(0.06, 0.03 * max(0, len(layers) - 1))
    )
    return round(min(score, 0.99), 2)


def build_entity_metadata(component: dict[str, object], confidence_score: float) -> dict[str, object]:
    observations: list[ObservationORM] = component["observations"]
    signals: list[EntitySignal] = component["signals"]
    return {
        "confidence_score": confidence_score,
        "observation_ids": [observation.observation_id for observation in observations],
        "signal_keys": component["signal_keys"],
        "domains": component["domains"],
        "layers": component["layers"],
        "signals": [
            {
                "entity_type": signal.entity_type,
                "signal_key": signal.signal_key,
                "normalized_value": signal.normalized_value,
                "raw_value": signal.raw_value,
            }
            for signal in signals
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
    match_basis = f"{entity.entity_type}:{component['signal_keys'][0]}"
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


def find_root(
    parents: dict[tuple[str, str, str], tuple[str, str, str]],
    node: tuple[str, str, str],
) -> tuple[str, str, str]:
    parent = parents[node]
    if parent != node:
        parents[node] = find_root(parents, parent)
    return parents[node]


def union_nodes(
    parents: dict[tuple[str, str, str], tuple[str, str, str]],
    left: tuple[str, str, str],
    right: tuple[str, str, str],
) -> None:
    left_root = find_root(parents, left)
    right_root = find_root(parents, right)
    if left_root == right_root:
        return
    parents[right_root] = left_root
