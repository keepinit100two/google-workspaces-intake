from datetime import datetime, timezone

import app.main as main
from app.domain.schemas import GmailIngestRequest


def test_ingest_gmail_derives_idempotency_and_calls_process(monkeypatch):
    captured = {}

    def fake_process_ingest(ingest_req, idempotency_key):
        captured["event_type"] = ingest_req.event_type
        captured["source"] = ingest_req.source
        captured["idempotency_key"] = idempotency_key
        captured["payload"] = ingest_req.payload
        captured["metadata"] = ingest_req.metadata
        # Return a minimal valid shape by delegating to real function is unnecessary here
        # Just ensure wiring passes correct values.
        return {"ok": True}  # FastAPI response_model is not enforced in unit test call

    monkeypatch.setattr(main, "_process_ingest", fake_process_ingest)

    req = GmailIngestRequest(
        source="gmail",
        mailbox="support@example.com",
        trigger_type="push_notification",
        message_id="msg-123",
        thread_id="thr-456",
        history_id="789",
        trigger_received_at=datetime.now(timezone.utc),
        event_observed_at=None,
        trace_id="trace-abc",
        raw_trigger={"sample": True},
    )

    resp = main.ingest_gmail(req)
    assert resp == {"ok": True}

    assert captured["event_type"] == "gmail_ingest"
    assert captured["source"] == "gmail"
    assert captured["idempotency_key"] == "gmail:mailbox=support@example.com:message_id=msg-123"
    assert captured["payload"]["mailbox"] == "support@example.com"
    assert captured["metadata"]["trace_id"] == "trace-abc"