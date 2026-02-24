from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, validator


class IngestRequest(BaseModel):
    event_type: str = Field(
        ...,
        description="Type of event, e.g. support_request, user_message, task_requested",
    )
    source: str = Field(
        "api",
        description="Where this event came from, e.g. api, slack, telegram",
    )
    actor: Optional[str] = Field(
        None,
        description="Who initiated the event (user id/email), if available",
    )
    payload: Dict[str, Any] = Field(
        ...,
        description="The core content of the event",
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Optional extra context for debugging/routing",
    )


class SlackIngestRequest(BaseModel):
    """
    Minimal Slack-style payload for adapter demonstration.
    This is NOT full Slack Events API coverage—just enough to prove reuse.
    """

    text: str = Field(..., description="Slack message text")
    user: Optional[str] = Field(None, description="Slack user id")
    channel: Optional[str] = Field(None, description="Slack channel id")
    ts: Optional[str] = Field(None, description="Slack timestamp/message id")


# =========================
# Gmail-specific ingest envelope (external)
# =========================
class GmailIngestRequest(BaseModel):
    """
    External ingest envelope for Gmail triggers.
    Keep this minimal and trustworthy: it is a trigger signal, not the email itself.
    Normalize is responsible for fetching/expanding message details.
    """

    source: str = Field("gmail", description="Event source")
    mailbox: str = Field(..., description="Monitored mailbox address this trigger pertains to")

    trigger_type: str = Field(
        ...,
        description="How ingest was triggered: push_notification or polling",
    )

    # These may or may not be available depending on trigger strategy
    message_id: Optional[str] = Field(
        None,
        description="Gmail message id if directly available from the trigger",
    )
    thread_id: Optional[str] = Field(
        None,
        description="Gmail thread id if directly available from the trigger",
    )
    history_id: Optional[str] = Field(
        None,
        description="Gmail historyId if using watch/history based ingestion",
    )

    # Ordering / out-of-order protection metadata
    trigger_received_at: datetime = Field(
        ...,
        description="Timestamp when our system received the trigger (used for ordering/debug)",
    )
    event_observed_at: Optional[datetime] = Field(
        None,
        description="Timestamp embedded in the trigger (if any). Not always present.",
    )

    trace_id: str = Field(
        ...,
        description="Correlation id for logs/artifacts across the pipeline",
    )

    raw_trigger: Dict[str, Any] = Field(
        default_factory=dict,
        description="Raw trigger payload as received (for audit/debug; not used for deterministic decisions)",
    )


# =========================
# Canonical ordering metadata (internal)
# =========================
class OrderingMetadata(BaseModel):
    """
    Helps the system handle out-of-order ingest deterministically.
    - history_id is a mailbox-level cursor (when using Gmail watch/history)
    - gmail_internal_date_ms is message time from Gmail
    - trigger_received_at is when we got the signal
    """

    history_id: Optional[str] = Field(
        None, description="Mailbox ordering cursor (Gmail historyId), if available"
    )
    gmail_internal_date_ms: Optional[int] = Field(
        None, description="Gmail internal message timestamp (ms since epoch), if available"
    )
    trigger_received_at: Optional[datetime] = Field(
        None, description="When our system received the trigger"
    )
    fetched_at: Optional[datetime] = Field(
        None, description="When the system fetched full message content from Gmail"
    )


class AttachmentMeta(BaseModel):
    filename: Optional[str] = Field(None, description="Attachment filename")
    mime_type: Optional[str] = Field(None, description="Attachment MIME type")
    size_bytes: Optional[int] = Field(None, description="Attachment size in bytes")
    attachment_id: Optional[str] = Field(None, description="Gmail attachment id")
    sha256: Optional[str] = Field(None, description="Optional content hash if downloaded and hashed")


class GmailMessageMeta(BaseModel):
    """
    Canonical Gmail message identity + lightweight metadata.
    Full raw payload may remain in Event.payload if needed, but deterministic logic
    should prefer these canonical fields.
    """

    mailbox: str = Field(..., description="Monitored mailbox that received the email")
    message_id: str = Field(..., description="Gmail message id (stable identifier)")
    thread_id: Optional[str] = Field(None, description="Gmail thread id")
    from_email: Optional[str] = Field(None, description="From email address")
    from_domain: Optional[str] = Field(None, description="Derived sender domain")
    to_emails: List[str] = Field(default_factory=list, description="To recipients")
    cc_emails: List[str] = Field(default_factory=list, description="CC recipients")
    reply_to: Optional[str] = Field(None, description="Reply-To email address if present")

    subject: Optional[str] = Field(None, description="Email subject")
    snippet: Optional[str] = Field(None, description="Gmail-provided snippet")
    body_plain: Optional[str] = Field(None, description="Extracted plaintext body (optional)")
    body_html: Optional[str] = Field(None, description="Extracted HTML body (optional)")
    body_clean: Optional[str] = Field(
        None,
        description="Deterministically cleaned body (e.g., strip quoted history/signature) if implemented",
    )

    has_attachments: bool = Field(False, description="Whether message has attachments")
    attachments: List[AttachmentMeta] = Field(default_factory=list, description="Attachment metadata list")

    gmail_label_ids: List[str] = Field(default_factory=list, description="Current Gmail label ids")
    gmail_label_names: List[str] = Field(default_factory=list, description="Current Gmail label names (if resolved)")
    in_inbox: Optional[bool] = Field(None, description="Whether INBOX label is present (if computed)")
    is_unread: Optional[bool] = Field(None, description="Whether UNREAD label is present (if computed)")
    internal_date_ms: Optional[int] = Field(None, description="Gmail internalDate in ms since epoch")
    size_estimate_bytes: Optional[int] = Field(None, description="Gmail sizeEstimate in bytes")


class Event(BaseModel):
    event_id: str = Field(..., description="Unique idempotency anchor for this event")
    event_type: str
    source: str
    timestamp: datetime
    actor: Optional[str] = None
    payload: Dict[str, Any]
    metadata: Dict[str, Any]

    # Optional canonical Gmail fields (populated after Normalize for Gmail events)
    gmail: Optional[GmailMessageMeta] = Field(
        None, description="Canonical Gmail message metadata (preferred for deterministic logic)"
    )

    ordering: Optional[OrderingMetadata] = Field(
        None, description="Ordering metadata to handle out-of-order ingest deterministically"
    )

    is_late_event: bool = Field(
        False,
        description="Whether this event arrived out-of-order relative to mailbox/thread cursor policy",
    )
    late_reason: Optional[str] = Field(
        None,
        description="Machine-readable reason for late-event classification (e.g., HISTORY_BEHIND_CURSOR)",
    )


# =========================
# AI enrichment schema gate (strict JSON contract)
# =========================
class AiClassification(BaseModel):
    """
    Strict JSON contract for Gemini classification.
    This is rejectable structured enrichment, never a direct outcome.
    """

    schema_version: str = Field("1.0", description="Schema version for compatibility")
    category: str = Field(..., description="Predicted category (must be in allowed category set)")
    confidence: float = Field(..., description="Confidence score from 0.0 to 1.0")

    # Optional non-authoritative fields (for audit/debug only)
    reason: Optional[str] = Field(None, description="Short rationale (bounded)")
    keywords: List[str] = Field(default_factory=list, description="Optional keywords (bounded)")

    @validator("confidence")
    def confidence_in_range(cls, v: float) -> float:
        if v < 0.0 or v > 1.0:
            raise ValueError("confidence must be between 0.0 and 1.0")
        return v

    @validator("reason")
    def reason_bounded(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if len(v) > 240:
            raise ValueError("reason must be <= 240 characters")
        return v

    @validator("keywords")
    def keywords_bounded(cls, v: List[str]) -> List[str]:
        if len(v) > 20:
            raise ValueError("keywords must have at most 20 items")
        for kw in v:
            if len(kw) > 40:
                raise ValueError("each keyword must be <= 40 characters")
        return v


class AiAttempt(BaseModel):
    attempt: int = Field(..., description="Attempt number (1..N)")
    status: str = Field(..., description="ok | invalid_json | invalid_schema | timeout | other")
    latency_ms: Optional[int] = Field(None, description="Latency for this attempt")
    error_detail: Optional[str] = Field(None, description="Optional bounded error detail for audit/debug")


class Decision(BaseModel):
    decision_id: str = Field(..., description="Unique identifier for this decision")
    event_id: str = Field(..., description="The event this decision was derived from")

    # Existing template field (kept for backwards compatibility)
    route: str = Field(
        ...,
        description="Chosen route, e.g. ESCALATE_HUMAN, REQUEST_MORE_INFO, CREATE_DRAFT_TICKET",
    )

    reason: str = Field(..., description="Human-readable reason for this decision")
    risk_level: str = Field("low", description="Risk level: low, medium, high")

    proposed_action: Dict[str, Any] = Field(
        default_factory=dict,
        description="Optional structured action request",
    )

    # --- Gmail intake classification fields (optional, won't break existing tests) ---
    category: Optional[str] = Field(
        None, description="Operational category for Gmail intake routing (5–7 categories)"
    )
    decision_source: Optional[str] = Field(
        None, description="rule | ai | fallback"
    )
    rule_id: Optional[str] = Field(
        None, description="If deterministic rule fired, record its id"
    )
    confidence: Optional[float] = Field(
        None, description="AI confidence score (0..1) if decision_source=ai"
    )
    threshold_used: Optional[float] = Field(
        None, description="Confidence threshold used for gating"
    )

    allowlist_verdict: Optional[str] = Field(
        None, description="allowed | blocked | needs_review"
    )

    # Late-event handling visibility for ops/audit
    is_late_event: bool = Field(
        False, description="Whether this decision corresponds to a late/out-of-order event"
    )
    late_reason: Optional[str] = Field(
        None, description="Machine-readable reason for late event"
    )

    # AI retry and rejection tracking (bounded, deterministic)
    ai_attempts_count: int = Field(
        0, description="How many AI attempts were made (including format-guard retries)"
    )
    ai_attempts: List[AiAttempt] = Field(
        default_factory=list,
        description="Per-attempt AI status record for audit/debug",
    )
    ai_final_status: Optional[str] = Field(
        None, description="accepted | rejected | not_used"
    )
    ai_reject_reason: Optional[str] = Field(
        None, description="invalid_json | invalid_schema | invalid_category | missing_confidence | timeout | other"
    )

    # --- Error taxonomy fields (optional, for UI/ops automation) ---
    error_code: Optional[str] = Field(
        None,
        description="Machine-readable error code for UI/ops (e.g. MISSING_REQUIRED_FIELD, SECURITY_KEYWORD_DETECTED)",
    )
    missing_fields: List[str] = Field(
        default_factory=list,
        description="If request is incomplete, list missing fields here",
    )
    next_steps: Optional[str] = Field(
        None,
        description="Human-readable guidance for what should happen next",
    )

    @validator("confidence")
    def decision_confidence_in_range(cls, v: Optional[float]) -> Optional[float]:
        if v is None:
            return v
        if v < 0.0 or v > 1.0:
            raise ValueError("confidence must be between 0.0 and 1.0")
        return v


class IngestResponse(BaseModel):
    event: Event
    decision: Decision


class ActionResult(BaseModel):
    action_id: str = Field(..., description="Unique identifier for this action execution")
    event_id: str = Field(..., description="Event the action corresponds to")
    decision_id: str = Field(..., description="Decision that triggered this action")
    action_type: str = Field(..., description="Type of action executed (or attempted)")
    status: str = Field(..., description="Outcome: executed, skipped, noop, failed")

    artifact_path: Optional[str] = Field(
        None,
        description="Where a draft artifact was stored (if any)",
    )

    reason: str = Field(..., description="Human-readable explanation of what happened")

    # Optional action execution metadata to support act-level idempotency later
    action_params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Structured action params used for execution (for replay/idempotency/debug)",
    )
    depends_on: List[str] = Field(
        default_factory=list,
        description="List of action_ids that must succeed before this action can run",
    )
    reversible: bool = Field(
        True,
        description="Whether this action is reversible (best-effort) for safe operations",
    )

    # --- Error taxonomy fields (optional, for UI/ops automation) ---
    error_code: Optional[str] = Field(
        None,
        description="Machine-readable error code for UI/ops if action failed or was skipped for a known reason",
    )
    next_steps: Optional[str] = Field(
        None,
        description="Human-readable guidance for operator or caller",
    )