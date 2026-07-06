from collections.abc import Generator

from sqlmodel import Session

from src.forte.core.settings import settings
from src.forte.db.session import DatabaseManager

db = DatabaseManager(settings.database_url)


def get_session() -> Generator[Session, None, None]:
    yield from db.get_session()

