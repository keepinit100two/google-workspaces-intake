import uuid
from typing import Any

from app.domain.schemas import Event, Decision


def _get_text(event: Event) -> str:
    """
    Extract a best-effort text field from the event payload.
    We keep this defensive because payloads vary across sources/domains.
    """
    payload: Any = event.payload or {}
    if isinstance(payload, dict):
        text = payload.get("text")
        if isinstance(text, str):
            return text
    return ""


def route_event(event: Event) -> Decision:
    """
    Decide what should happen next for an Event using a deterministic routing stack.

    D1 rules (in order):
      1) Security keywords -> ESCALATE_HUMAN (high risk)
      2) Missing required field 'urgency' -> REQUEST_MORE_INFO (medium risk)
      3) Otherwise -> CREATE_DRAFT_TICKET (low risk) with proposed_action populated

    This function performs NO side effects. It returns a reviewable plan only.
    """
    decision_id = str(uuid.uuid4())
    text = _get_text(event).lower()

    # Rule 1: High-risk / security keywords -> escalate
    security_keywords = ["password", "credential", "security", "breach"]
    if any(k in text for k in security_keywords):
        return Decision(
            decision_id=decision_id,
            event_id=event.event_id,
            route="ESCALATE_HUMAN",
            reason="Security-related keyword detected",
            risk_level="high",
            proposed_action={},
        )

    # Rule 2: Missing required info -> request more info
    urgency = None
    if isinstance(event.payload, dict):
        urgency = event.payload.get("urgency")

    if not urgency:
        return Decision(
            decision_id=decision_id,
            event_id=event.event_id,
            route="REQUEST_MORE_INFO",
            reason="Missing required field: urgency",
            risk_level="medium",
            proposed_action={
                "question": "How urgent is this? (low / medium / high)",
                "missing_fields": ["urgency"],
            },
        )

    # Rule 3: Default -> create a draft ticket (still reversible / reviewable)
    summary = "Support request"
    if text:
        summary = text[:80]  # keep short and safe

    return Decision(
        decision_id=decision_id,
        event_id=event.event_id,
        route="CREATE_DRAFT_TICKET",
        reason="Standard support request",
        risk_level="low",
        proposed_action={
            "type": "create_ticket_draft",
            "queue": "IT",
            "priority": str(urgency).lower(),
            "summary": summary,
            "description": (event.payload if isinstance(event.payload, dict) else {"text": text}),
        },
    )
