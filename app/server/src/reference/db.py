from __future__ import annotations

from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.reference.models import Base as ReferenceBase


def _prepare_sqlite_path(database_url: str) -> None:
    if not database_url.startswith("sqlite:///"):
        return
    raw_path = database_url.removeprefix("sqlite:///")
    if raw_path == ":memory:":
        return
    path = Path(raw_path)
    if not path.is_absolute():
        path = Path.cwd() / path
    path.parent.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=4)
def get_engine(database_url: str) -> Engine:
    _prepare_sqlite_path(database_url)
    connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
    return create_engine(database_url, future=True, connect_args=connect_args)


@lru_cache(maxsize=4)
def get_session_factory(database_url: str) -> sessionmaker[Session]:
    return sessionmaker(bind=get_engine(database_url), autoflush=False, autocommit=False, future=True)


def init_db(database_url: str) -> None:
    engine = get_engine(database_url)
    ReferenceBase.metadata.create_all(engine)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS reference_spatial_index (
                ref_rowid INTEGER PRIMARY KEY,
                min_lat REAL NOT NULL,
                max_lat REAL NOT NULL,
                min_lon REAL NOT NULL,
                max_lon REAL NOT NULL
            )
            """
        )


@contextmanager
def session_scope(database_url: str) -> Iterator[Session]:
    init_db(database_url)
    session = get_session_factory(database_url)()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
