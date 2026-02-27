import pytest

import app.main as main
from app.domain.schemas import IngestRequest


class FakeSink:
    def __init__(self):
        self.calls = []

    def notify(self, record):
        self.calls.append(record)


def test_missing_idempotency_key_notifies_failure_sink(monkeypatch):
    fake = FakeSink()
    monkeypatch.setattr(main, "failure_sink", fake)

    req = IngestRequest(
        event_type="gmail_ingest",
        source="gmail",
        actor=None,
        payload={},
        metadata={},
    )

    with pytest.raises(Exception):
        main._process_ingest(req, idempotency_key=None)

    assert len(fake.calls) == 1
    assert fake.calls[0].stage == "ingest"
    assert "MISSING_IDEMPOTENCY_KEY" in fake.calls[0].error_code