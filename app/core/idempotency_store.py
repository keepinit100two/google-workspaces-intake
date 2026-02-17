import json
import sqlite3
from pathlib import Path
from typing import Optional

from app.domain.schemas import Event

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "idempotency.sqlite3"


class SQLiteIdempotencyStore:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS idempotency (key TEXT PRIMARY KEY, event_json TEXT NOT NULL)"
            )

    def get(self, key: str) -> Optional[Event]:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT event_json FROM idempotency WHERE key = ?", (key,)).fetchone()
        if not row:
            return None
        data = json.loads(row[0])
        # Pydantic v2:
        return Event.model_validate(data)

    def set(self, key: str, event: Event) -> None:
        event_json = json.dumps(event.model_dump(), default=str)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO idempotency (key, event_json) VALUES (?, ?)",
                (key, event_json),
            )
