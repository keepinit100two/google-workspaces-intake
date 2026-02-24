from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from app.domain.schemas import GmailIngestRequest, IngestRequest


def _dt_to_iso(dt: datetime) -> str:
    # Deterministic serialization for metadata fields
    return dt.isoformat()


def normalize_gmail_ingest(greq: GmailIngestRequest) -> IngestRequest:
    """
    Normalize a Gmail-specific ingest envelope into the canonical IngestRequest.

    Important invariants:
      - No decisions here. No routing logic. No side effects.
      - Preserve ordering + trace metadata so out-of-order handling is possible later.
      - Payload contains only the canonical identifiers and trigger context.
      - Normalize does NOT fetch email content yet (Gmail API adapter comes later).
    """

    # Canonical payload: identifiers + minimal trigger context
    payload: Dict[str, Any] = {
        "mailbox": greq.mailbox,
        "trigger_type": greq.trigger_type,
        "message_id": greq.message_id,
        "thread_id": greq.thread_id,
        "history_id": greq.history_id,
    }

    # Canonical metadata: audit/debug/ordering (not decision-driving business logic)
    metadata: Dict[str, Any] = {
        "trace_id": greq.trace_id,
        "raw_trigger": greq.raw_trigger,
        "ordering": {
            "trigger_received_at": _dt_to_iso(greq.trigger_received_at),
            "event_observed_at": _dt_to_iso(greq.event_observed_at) if greq.event_observed_at else None,
            "history_id": greq.history_id,
        },
        # Useful flags for downstream deterministic policies
        "missing_message_id": greq.message_id is None,
        "missing_thread_id": greq.thread_id is None,
        "missing_history_id": greq.history_id is None,
    }

    # Canonical request
    # event_type is a deterministic internal type; keep stable for routing/config
    return IngestRequest(
        event_type="gmail_ingest",
        source="gmail",
        actor=None,  # actor is unknown at trigger-time; will be filled after Gmail fetch later
        payload=payload,
        metadata=metadata,
    )


def normalize_gmail_ingest_safe(greq: GmailIngestRequest) -> tuple[Optional[IngestRequest], Optional[Dict[str, Any]]]:
    """
    Safe wrapper for normalize_gmail_ingest.

    Returns:
      (IngestRequest, None) on success
      (None, error_info) on failure

    This is useful later when wiring into an endpoint where we want explicit failure
    artifacts rather than uncaught exceptions.
    """
    try:
        return normalize_gmail_ingest(greq), None
    except Exception as e:
        return None, {
            "error_code": "NORMALIZE_FAILED",
            "error": str(e),
        }