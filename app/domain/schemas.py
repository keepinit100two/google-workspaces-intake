from pydantic import BaseModel, Field
from typing import Any, Dict, Optional, List
from datetime import datetime


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


class Event(BaseModel):
    event_id: str = Field(..., description="Unique idempotency anchor for this event")
    event_type: str
    source: str
    timestamp: datetime
    actor: Optional[str] = None
    payload: Dict[str, Any]
    metadata: Dict[str, Any]


class Decision(BaseModel):
    decision_id: str = Field(..., description="Unique identifier for this decision")
    event_id: str = Field(..., description="The event this decision was derived from")
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

    # --- Error taxonomy fields (optional, for UI/ops automation) ---
    error_code: Optional[str] = Field(
        None,
        description="Machine-readable error code for UI/ops if action failed or was skipped for a known reason",
    )
    next_steps: Optional[str] = Field(
        None,
        description="Human-readable guidance for operator or caller",
    )
