from datetime import datetime, timezone

from app.core.config_loader import ConfidenceThresholdConfig, RoutingConfig
from app.domain.schemas import AiAttempt, AiClassification, Event
from app.services.llm_adapter import LLMClassificationOutcome
from app.services.router import route_event


class FakeClassifier:
    def __init__(self, outcome):
        self.outcome = outcome
        self.calls = []

    def classify(self, text: str, allowed_categories):
        self.calls.append({"text": text, "allowed_categories": allowed_categories})
        return self.outcome


def _event(**overrides) -> Event:
    base = Event(
        event_id="evt-llm-1",
        event_type="gmail_ingest",
        source="gmail",
        timestamp=datetime.now(timezone.utc),
        actor=None,
        payload={
            "mailbox": "support@example.com",
            "message_id": "msg-123",
            "text": "I have a billing question about my invoice.",
        },
        metadata={},
        gmail=None,
        ordering=None,
        is_late_event=False,
        late_reason=None,
    )
    data = base.model_dump()
    data.update(overrides)
    return Event.model_validate(data)


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


def test_llm_runs_when_no_deterministic_rule_matches():
    outcome = LLMClassificationOutcome(
        accepted=True,
        classification=AiClassification(
            schema_version="1.0",
            category="billing",
            confidence=0.87,
            reason="Invoice-related question",
            keywords=["invoice"],
        ),
        attempts=[
            AiAttempt(attempt=1, status="ok", latency_ms=None, error_detail=None),
        ],
        final_status="accepted",
        reject_reason=None,
    )

    classifier = FakeClassifier(outcome)
    e = _event()

    d = route_event(
        e,
        routing_config=_routing_cfg(),
        classifier=classifier,
        threshold_config=_threshold_cfg(),
    )

    assert len(classifier.calls) == 1
    assert d.route == "CREATE_DRAFT_TICKET"
    assert d.decision_source == "ai"
    assert d.category == "billing"
    assert d.confidence == 0.87
    assert d.threshold_used == 0.85
    assert d.ai_attempts_count == 1
    assert d.ai_final_status == "accepted"
    assert d.ai_reject_reason is None
    assert "accepted above auto-route threshold" in d.reason


def test_llm_rejection_falls_back_to_needs_review():
    outcome = LLMClassificationOutcome(
        accepted=False,
        classification=None,
        attempts=[
            AiAttempt(attempt=1, status="invalid_json", latency_ms=None, error_detail="bad json"),
            AiAttempt(attempt=2, status="invalid_json", latency_ms=None, error_detail="bad json"),
            AiAttempt(attempt=3, status="invalid_json", latency_ms=None, error_detail="bad json"),
        ],
        final_status="rejected",
        reject_reason="invalid_json",
    )

    classifier = FakeClassifier(outcome)
    e = _event()

    d = route_event(
        e,
        routing_config=_routing_cfg(),
        classifier=classifier,
        threshold_config=_threshold_cfg(),
    )

    assert len(classifier.calls) == 1
    assert d.route == "NEEDS_REVIEW"
    assert d.decision_source == "fallback"
    assert d.category is None
    assert d.confidence is None
    assert d.ai_attempts_count == 3
    assert d.ai_final_status == "rejected"
    assert d.ai_reject_reason == "invalid_json"
    assert "LLM classification rejected" in d.reason