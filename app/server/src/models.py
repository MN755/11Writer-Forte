from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import JSON, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(default=utcnow, onupdate=utcnow)


class DataLayerORM(TimestampMixin, Base):
    __tablename__ = "data_layers"

    layer_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(String(80), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(200))
    description: Mapped[str] = mapped_column(Text, default="")
    temporal_resolution: Mapped[str] = mapped_column(String(80), default="unknown")
    data_latency: Mapped[str] = mapped_column(String(80), default="unknown")
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class EventORM(TimestampMixin, Base):
    __tablename__ = "events"

    event_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    slug: Mapped[str] = mapped_column(String(120), unique=True, index=True)
    title: Mapped[str] = mapped_column(String(200))
    summary: Mapped[str] = mapped_column(Text, default="")
    occurred_at: Mapped[datetime | None] = mapped_column(default=None)
    status: Mapped[str] = mapped_column(String(50), default="open")
    redaction_level: Mapped[str] = mapped_column(String(50), default="public")
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class GeofenceORM(TimestampMixin, Base):
    __tablename__ = "geofences"

    geofence_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(160), unique=True, index=True)
    description: Mapped[str] = mapped_column(Text, default="")
    geometry_geojson: Mapped[dict[str, Any]] = mapped_column(JSON)
    rule_expression: Mapped[str] = mapped_column(Text, default="")
    enabled: Mapped[bool] = mapped_column(default=True)


class AlertORM(TimestampMixin, Base):
    __tablename__ = "alerts"

    alert_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[int | None] = mapped_column(ForeignKey("events.event_id"), default=None)
    geofence_id: Mapped[int | None] = mapped_column(ForeignKey("geofences.geofence_id"), default=None)
    severity: Mapped[str] = mapped_column(String(30), default="info")
    status: Mapped[str] = mapped_column(String(30), default="open")
    message: Mapped[str] = mapped_column(Text)
    trigger_basis_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class SourceTrustProfileORM(TimestampMixin, Base):
    __tablename__ = "source_trust_profiles"

    trust_profile_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    domain: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    trust_level: Mapped[str] = mapped_column(String(30), default="neutral")
    approval_policy: Mapped[str] = mapped_column(String(40), default="manual_review")
    integrity_source: Mapped[bool] = mapped_column(default=False)
    notes: Mapped[str] = mapped_column(Text, default="")


class LocalImportRunORM(TimestampMixin, Base):
    __tablename__ = "local_import_runs"

    import_run_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_path: Mapped[str] = mapped_column(Text)
    source_format: Mapped[str] = mapped_column(String(30))
    layer_key: Mapped[str] = mapped_column(String(80), default="unassigned")
    status: Mapped[str] = mapped_column(String(30), default="queued")
    records_seen: Mapped[int] = mapped_column(Integer, default=0)
    records_imported: Mapped[int] = mapped_column(Integer, default=0)
    notes: Mapped[str] = mapped_column(Text, default="")
    chain_of_custody_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list)

    observations: Mapped[list["ObservationORM"]] = relationship(back_populates="import_run")


class ObservationORM(TimestampMixin, Base):
    __tablename__ = "observations"

    observation_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    import_run_id: Mapped[int | None] = mapped_column(ForeignKey("local_import_runs.import_run_id"))
    event_id: Mapped[int | None] = mapped_column(ForeignKey("events.event_id"), default=None)
    layer_key: Mapped[str] = mapped_column(String(80), index=True)
    source_domain: Mapped[str | None] = mapped_column(String(255), default=None)
    source_type: Mapped[str] = mapped_column(String(40), default="local_import")
    record_format: Mapped[str] = mapped_column(String(30), default="json")
    trust_level: Mapped[str] = mapped_column(String(30), default="neutral")
    approval_policy: Mapped[str] = mapped_column(String(40), default="manual_review")
    confidence_score: Mapped[float] = mapped_column(Float, default=0.5)
    location_geojson: Mapped[dict[str, Any] | None] = mapped_column(JSON, default=None)
    content_text: Mapped[str] = mapped_column(Text, default="")
    content_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    raw_hash: Mapped[str] = mapped_column(String(64), index=True)

    import_run: Mapped[LocalImportRunORM | None] = relationship(back_populates="observations")


class CustodyLogORM(Base):
    __tablename__ = "custody_logs"

    custody_log_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    object_type: Mapped[str] = mapped_column(String(50), index=True)
    object_id: Mapped[str] = mapped_column(String(120), index=True)
    action: Mapped[str] = mapped_column(String(80))
    actor: Mapped[str] = mapped_column(String(80), default="system")
    details_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)
