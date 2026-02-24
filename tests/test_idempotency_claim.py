from datetime import datetime, timezone
from pathlib import Path

from app.core.idempotency_store import SQLiteIdempotencyStore
from app.domain.schemas import Event


def _make_event(event_id: str = "evt-1") -> Event:
    return Event(
        event_id=event_id,
        event_type="gmail_ingest",
        source="gmail",
        timestamp=datetime.now(timezone.utc),
        actor=None,
        payload={"example": True},
        metadata={},
        gmail=None,
        ordering=None,
        is_late_event=False,
        late_reason=None,
    )


def test_try_claim_first_writer_wins(tmp_path: Path) -> None:
    db_path = tmp_path / "idempotency.sqlite3"
    store = SQLiteIdempotencyStore(db_path=db_path)

    key = "gmail:mailbox=support@example.com:message_id=abc123"
    event = _make_event("evt-claim-1")

    # First claim should succeed
    r1 = store.try_claim(key=key, event=event, lease_seconds=120, owner_id="worker-a")
    assert r1.claimed is True
    assert r1.status == "claimed"
    assert r1.owner_id == "worker-a"

    # Second claim during active lease should fail (another worker owns it)
    r2 = store.try_claim(key=key, event=event, lease_seconds=120, owner_id="worker-b")
    assert r2.claimed is False
    assert r2.status == "claimed"
    assert r2.owner_id == "worker-a"


def test_mark_completed_blocks_reprocessing(tmp_path: Path) -> None:
    db_path = tmp_path / "idempotency.sqlite3"
    store = SQLiteIdempotencyStore(db_path=db_path)

    key = "gmail:mailbox=support@example.com:message_id=done456"
    event = _make_event("evt-done-1")

    r1 = store.try_claim(key=key, event=event, lease_seconds=120, owner_id="worker-a")
    assert r1.claimed is True

    store.mark_completed(key=key, owner_id="worker-a")

    # After completion, no one should be able to claim again
    r2 = store.try_claim(key=key, event=event, lease_seconds=120, owner_id="worker-b")
    assert r2.claimed is False
    assert r2.status == "completed"


def test_mark_failed_allows_reclaim(tmp_path: Path) -> None:
    db_path = tmp_path / "idempotency.sqlite3"
    store = SQLiteIdempotencyStore(db_path=db_path)

    key = "gmail:mailbox=support@example.com:message_id=fail789"
    event = _make_event("evt-fail-1")

    r1 = store.try_claim(key=key, event=event, lease_seconds=120, owner_id="worker-a")
    assert r1.claimed is True

    store.mark_failed(key=key, owner_id="worker-a", error="simulated failure")

    # After failed status, another worker should be able to reclaim immediately
    r2 = store.try_claim(key=key, event=event, lease_seconds=120, owner_id="worker-b")
    assert r2.claimed is True
    assert r2.status == "claimed"
    assert r2.owner_id == "worker-b"