from src.forte import models  # noqa: F401
from src.forte.db.session import DatabaseManager


def init_db(db: DatabaseManager) -> None:
    db.create_all()
