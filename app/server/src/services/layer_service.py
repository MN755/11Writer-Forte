from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import CustodyLogORM, DataLayerORM
from src.schemas import DataLayerCreate


def list_data_layers(session: Session) -> list[DataLayerORM]:
    statement = select(DataLayerORM).order_by(DataLayerORM.name.asc())
    return list(session.scalars(statement))


def create_data_layer(
    session: Session,
    payload: DataLayerCreate,
    actor: str = "system",
) -> DataLayerORM:
    existing = session.scalar(select(DataLayerORM).where(DataLayerORM.key == payload.key))
    if existing is not None:
        raise ValueError(f"Data layer '{payload.key}' already exists.")

    record = DataLayerORM(**payload.model_dump())
    session.add(record)
    session.flush()
    session.add(
        CustodyLogORM(
            object_type="data_layer",
            object_id=str(record.layer_id),
            action="layer_created",
            actor=actor,
            details_json={
                **payload.model_dump(),
                "layer_id": record.layer_id,
            },
        )
    )
    session.commit()
    session.refresh(record)
    return record


def ensure_data_layer(
    session: Session,
    layer_key: str,
    actor: str = "system",
) -> DataLayerORM:
    existing = session.scalar(select(DataLayerORM).where(DataLayerORM.key == layer_key))
    if existing is not None:
        return existing

    record = DataLayerORM(
        key=layer_key,
        name=build_default_layer_name(layer_key),
        description="Auto-created from ingest workflow.",
        metadata_json={"auto_created": True},
    )
    session.add(record)
    session.flush()
    session.add(
        CustodyLogORM(
            object_type="data_layer",
            object_id=str(record.layer_id),
            action="layer_auto_created",
            actor=actor,
            details_json={
                "layer_id": record.layer_id,
                "key": record.key,
                "name": record.name,
            },
        )
    )
    return record


def build_default_layer_name(layer_key: str) -> str:
    cleaned = layer_key.replace("_", " ").replace("-", " ").strip()
    return cleaned.title() if cleaned else "Unnamed Layer"
