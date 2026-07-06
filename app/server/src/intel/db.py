from __future__ import annotations

from src.config.settings import get_settings
from src.forte.db.session import DatabaseManager

from . import models  # noqa: F401


db = DatabaseManager(get_settings().database_url)


def ensure_current_database() -> None:
    current_url = get_settings().database_url
    if getattr(db, "_url", None) != current_url:
        db.reconfigure(current_url)


def init_db(database: DatabaseManager) -> None:
    from . import models as _models  # noqa: F401

    ensure_current_database()
    database.create_all()
