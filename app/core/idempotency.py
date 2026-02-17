from typing import Dict
from app.domain.schemas import Event

# In-memory store for local development.
# In production this becomes Redis or a DB table.
_idempotency_store: Dict[str, Event] = {}


def get_event(key: str) -> Event | None:
    return _idempotency_store.get(key)


def set_event(key: str, event: Event) -> None:
    _idempotency_store[key] = event
