import app.main as main
from app.core.idempotency_store import ClaimResult
from app.domain.schemas import IngestRequest


class FakeIdemStore:
    def __init__(self):
        self.mark_completed_calls = []
        self.mark_failed_calls = []

    def try_claim(self, key, event, lease_seconds=120, owner_id=None):
        return ClaimResult(claimed=True, status="claimed", owner_id="worker-a", lease_until=None, existing_event=None)

    def mark_completed(self, key, owner_id):
        self.mark_completed_calls.append({"key": key, "owner_id": owner_id})

    def mark_failed(self, key, owner_id, error):
        self.mark_failed_calls.append({"key": key, "owner_id": owner_id, "error": error})

    def get(self, key):
        return None

    def set(self, key, event):
        pass


class FakeCursorStore:
    def __init__(self):
        self.try_advance_calls = []

    def try_advance(self, mailbox, new_history_id):
        self.try_advance_calls.append({"mailbox": mailbox, "new_history_id": new_history_id})
        return True

    def check_late(self, mailbox, incoming_history_id):
        raise AssertionError("check_late should not be called in _process_ingest shadow-mode test")


def test_shadow_mode_skips_act_and_cursor_advance(monkeypatch):
    monkeypatch.setenv("SHADOW_MODE", "true")

    fake_idem = FakeIdemStore()
    fake_cursor = FakeCursorStore()

    monkeypatch.setattr(main, "idem_store", fake_idem)
    monkeypatch.setattr(main, "cursor_store", fake_cursor)

    # If execute_decision is called, test should fail
    def boom_execute_decision(event, decision):
        raise RuntimeError("execute_decision should not run in shadow mode")

    monkeypatch.setattr(main, "execute_decision", boom_execute_decision)

    ingest_req = IngestRequest(
        event_type="gmail_ingest",
        source="gmail",
        actor=None,
        payload={"mailbox": "support@example.com", "history_id": "101"},
        metadata={"ordering_signal_missing": False},
    )

    resp = main._process_ingest(ingest_req, idempotency_key="idem-shadow-1")

    # mark_completed must be called
    assert len(fake_idem.mark_completed_calls) == 1
    assert fake_idem.mark_completed_calls[0]["key"] == "idem-shadow-1"

    # cursor must NOT advance in shadow mode
    assert fake_cursor.try_advance_calls == []

    # sanity response
    assert resp.event.event_id is not None
    assert resp.decision.event_id == resp.event.event_id