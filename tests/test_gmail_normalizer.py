from datetime import datetime, timezone

from app.domain.schemas import GmailIngestRequest
from app.services.normalizer import normalize_gmail_ingest


def test_normalize_gmail_ingest_happy_path():
    greq = GmailIngestRequest(
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

    req = normalize_gmail_ingest(greq)

    assert req.event_type == "gmail_ingest"
    assert req.source == "gmail"
    assert req.actor is None

    assert req.payload["mailbox"] == "support@example.com"
    assert req.payload["trigger_type"] == "push_notification"
    assert req.payload["message_id"] == "msg-123"
    assert req.payload["thread_id"] == "thr-456"
    assert req.payload["history_id"] == "789"

    assert req.metadata["trace_id"] == "trace-abc"
    assert req.metadata["raw_trigger"] == {"sample": True}

    ordering = req.metadata["ordering"]
    assert ordering["history_id"] == "789"
    assert ordering["trigger_received_at"] is not None
    assert ordering["event_observed_at"] is None

    assert req.metadata["missing_message_id"] is False
    assert req.metadata["missing_thread_id"] is False
    assert req.metadata["missing_history_id"] is False


def test_normalize_gmail_ingest_missing_message_id_sets_flags():
    greq = GmailIngestRequest(
        source="gmail",
        mailbox="support@example.com",
        trigger_type="polling",
        message_id=None,
        thread_id=None,
        history_id="1000",
        trigger_received_at=datetime.now(timezone.utc),
        event_observed_at=datetime.now(timezone.utc),
        trace_id="trace-missing",
        raw_trigger={"poll": True},
    )

    req = normalize_gmail_ingest(greq)

    assert req.payload["message_id"] is None
    assert req.payload["thread_id"] is None
    assert req.payload["history_id"] == "1000"

    assert req.metadata["missing_message_id"] is True
    assert req.metadata["missing_thread_id"] is True
    assert req.metadata["missing_history_id"] is False

    ordering = req.metadata["ordering"]
    assert ordering["event_observed_at"] is not None