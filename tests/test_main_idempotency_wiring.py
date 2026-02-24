from datetime import datetime, timezone

import pytest

import app.main as main
from app.core.idempotency_store import ClaimResult
from app.domain.schemas import Event, IngestRequest


class FakeIdemStore:
    def __init__(self, claim: ClaimResult):
        self._claim = claim
        self.try_claim_calls = []
        self.mark_completed_calls = []
        self.mark_failed_calls = []
        self.get_calls = []

    def try_claim(self, key, event, lease_seconds=120, owner_id=None):
        self.try_claim_calls.append(
            {"key": key, "event_id": event.event_id, "lease_seconds": lease_seconds, "owner_id": owner_id}
        )
        return self._claim

    def mark_completed(self, key, owner_id):
        self.mark_completed_calls.append({"key": key, "owner_id": owner_id})

    def mark_failed(self, key, owner_id, error):
        self.mark_failed_calls.append({"key": key, "owner_id": owner_id, "error": error})

    def get(self, key):
        self.get_calls.append({"key": key})
        return None

    def set(self, key, event):
        # not used in new flow, but kept for compatibility
        pass


def _ingest_req() -> IngestRequest:
    return IngestRequest(
        event_type="gmail_ingest",
        source="gmail",
        actor=None,
        payload={"hello": "world"},
        metadata={},
    )


def _event_for_claim() -> Event:
    return Event(
        event_id="evt-claim",
        event_type="gmail_ingest",
        source="gmail",
        timestamp=datetime.now(timezone.utc),
        actor=None,
        payload={"hello": "world"},
        metadata={},
        gmail=None,
        ordering=None,
        is_late_event=False,
        late_reason=None,
    )


def test_process_ingest_calls_try_claim_and_marks_completed(monkeypatch):
    # Arrange: force claim success
    claim = ClaimResult(claimed=True, status="claimed", owner_id="worker-a", lease_until=None, existing_event=None)
    fake_store = FakeIdemStore(claim=claim)

    # Patch the global idem_store used by _process_ingest
    monkeypatch.setattr(main, "idem_store", fake_store)

    # Patch route_event to deterministic output
    def fake_route_event(event):
        return main.route_event(event)

    # We won't patch route_event; existing router is deterministic & tests already cover it.

    # Act
    resp = main._process_ingest(_ingest_req(), idempotency_key="idem-123")

    # Assert: try_claim called
    assert len(fake_store.try_claim_calls) == 1
    assert fake_store.try_claim_calls[0]["key"] == "idem-123"

    # Assert: completed marked on success path
    assert len(fake_store.mark_completed_calls) == 1
    assert fake_store.mark_completed_calls[0]["key"] == "idem-123"
    assert fake_store.mark_completed_calls[0]["owner_id"] == "worker-a"

    # Sanity: response structured
    assert resp.event.event_id is not None
    assert resp.decision.event_id == resp.event.event_id


def test_process_ingest_marks_failed_on_act_exception(monkeypatch):
    # Arrange: force claim success so we enter processing path
    claim = ClaimResult(claimed=True, status="claimed", owner_id="worker-a", lease_until=None, existing_event=None)
    fake_store = FakeIdemStore(claim=claim)
    monkeypatch.setattr(main, "idem_store", fake_store)

    # Patch actuator to raise to simulate Act failure
    def boom_execute_decision(event, decision):
        raise RuntimeError("act exploded")

    monkeypatch.setattr(main, "execute_decision", boom_execute_decision)

    # Act + Assert: exception propagates, but mark_failed is called
    with pytest.raises(RuntimeError):
        main._process_ingest(_ingest_req(), idempotency_key="idem-456")

    assert len(fake_store.try_claim_calls) == 1
    assert fake_store.try_claim_calls[0]["key"] == "idem-456"

    assert len(fake_store.mark_failed_calls) == 1
    assert fake_store.mark_failed_calls[0]["key"] == "idem-456"
    assert fake_store.mark_failed_calls[0]["owner_id"] == "worker-a"
    assert "act exploded" in fake_store.mark_failed_calls[0]["error"]