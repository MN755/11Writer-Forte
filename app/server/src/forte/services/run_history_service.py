from sqlmodel import Session, select

from src.forte.models.connector import Connector
from src.forte.models.run_history import RunHistory


def list_runs_for_wave(
    session: Session,
    wave_id: int,
    limit: int = 50,
) -> list[tuple[RunHistory, str]]:
    bounded_limit = max(1, min(limit, 200))
    statement = (
        select(RunHistory, Connector.name)
        .join(Connector, Connector.id == RunHistory.connector_id)
        .where(RunHistory.wave_id == wave_id)
        .order_by(RunHistory.started_at.desc())
        .limit(bounded_limit)
    )
    return list(session.exec(statement).all())


def list_runs_for_connector(
    session: Session,
    connector_id: int,
    limit: int = 50,
) -> list[tuple[RunHistory, str]]:
    bounded_limit = max(1, min(limit, 200))
    statement = (
        select(RunHistory, Connector.name)
        .join(Connector, Connector.id == RunHistory.connector_id)
        .where(RunHistory.connector_id == connector_id)
        .order_by(RunHistory.started_at.desc())
        .limit(bounded_limit)
    )
    return list(session.exec(statement).all())

