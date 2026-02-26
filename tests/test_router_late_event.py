from datetime import datetime, timezone

from app.domain.schemas import Event
from app.services.router import route_event


def test_late_event_always_noop_route():
    event = Event(
        event_id="evt-late",
        event_type="gmail_ingest",
        source="gmail",
        timestamp=datetime.now(timezone.utc),
        actor=None,
        payload={"text": "password reset needed", "urgency": "high"},
        metadata={},
        gmail=None,
        ordering=None,
        is_late_event=True,
        late_reason="BEHIND_CURSOR",
    )

    decision = route_event(event)

    assert decision.route == "NOOP_LATE_EVENT"
    assert "Late/out-of-order event" in decision.reason
    assert decision.risk_level == "low"