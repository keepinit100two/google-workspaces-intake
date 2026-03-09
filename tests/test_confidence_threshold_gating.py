from datetime import datetime, timezone

from app.core.config_loader import ConfidenceThresholdConfig, RoutingConfig
from app.domain.schemas import AiAttempt, AiClassification, Event
from app.services.llm_adapter import LLMClassificationOutcome
from app.services.router import route_event


class FakeClassifier:
    def __init__(self, outcome):
        self.outcome = outcome

    def classify(self, text: str, allowed_categories):
        return self.outcome


def _event() -> Event:
    return Event(
        event_id="evt-threshold-1",
        event_type="gmail_ingest",
        source="gmail",
        timestamp=datetime.now(timezone.utc),
        actor=None,
        payload={
            "mailbox": "support@example.com",
            "message_id": "msg-999",
            "text": "Please help with my invoice and billing account.",
        },
        metadata={},
        gmail=None,
        ordering=None,
        is_late_event=False,
        late_reason=None,
    )


def _routing_cfg() -> RoutingConfig:
    return RoutingConfig(
        version="1.0",
        routes=["NEEDS_REVIEW", "CREATE_DRAFT_TICKET", "ESCALATE_HUMAN"],
        categories=["support", "billing", "security", "sales", "other"],
        category_routes={
            "support": "CREATE_DRAFT_TICKET",
            "billing": "CREATE_DRAFT_TICKET",
            "security": "ESCALATE_HUMAN",
            "sales": "NEEDS_REVIEW",
            "other": "NEEDS_REVIEW",
        },
        rules=[],
    )


def _threshold_cfg() -> ConfidenceThresholdConfig:
    return ConfidenceThresholdConfig(
        version="1.0",
        auto_route_threshold=0.85,
        review_threshold=0.60,
    )


def test_llm_above_threshold_auto_routes():
    outcome = LLMClassificationOutcome(
        accepted=True,
        classification=AiClassification(
            schema_version="1.0",
            category="billing",
            confidence=0.91,
            reason="Invoice related",
            keywords=["invoice"],
        ),
        attempts=[AiAttempt(attempt=1, status="ok", latency_ms=None, error_detail=None)],
        final_status="accepted",
        reject_reason=None,
    )

    d = route_event(
        _event(),
        routing_config=_routing_cfg(),
        classifier=FakeClassifier(outcome),
        threshold_config=_threshold_cfg(),
    )

    assert d.route == "CREATE_DRAFT_TICKET"
    assert d.category == "billing"
    assert d.decision_source == "ai"
    assert d.confidence == 0.91
    assert d.threshold_used == 0.85


def test_llm_below_threshold_goes_to_needs_review():
    outcome = LLMClassificationOutcome(
        accepted=True,
        classification=AiClassification(
            schema_version="1.0",
            category="billing",
            confidence=0.72,
            reason="Invoice related",
            keywords=["invoice"],
        ),
        attempts=[AiAttempt(attempt=1, status="ok", latency_ms=None, error_detail=None)],
        final_status="accepted",
        reject_reason=None,
    )

    d = route_event(
        _event(),
        routing_config=_routing_cfg(),
        classifier=FakeClassifier(outcome),
        threshold_config=_threshold_cfg(),
    )

    assert d.route == "NEEDS_REVIEW"
    assert d.category == "billing"
    assert d.decision_source == "ai"
    assert d.confidence == 0.72
    assert d.threshold_used == 0.85
    assert "below auto-route threshold" in d.reason