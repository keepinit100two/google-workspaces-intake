import uuid
from typing import Any, Dict, Tuple

from app.core.config_loader import load_routing_config, RoutingRule
from app.domain.schemas import Event, Decision


def _get_payload(event: Event) -> Dict[str, Any]:
    payload: Any = event.payload or {}
    if isinstance(payload, dict):
        return payload
    return {}


def _get_text(event: Event) -> str:
    """
    Extract a best-effort text field from the event payload.
    We keep this defensive because payloads vary across sources/domains.
    """
    payload = _get_payload(event)
    text = payload.get("text")
    if isinstance(text, str):
        return text
    return ""


def _platform_rule_match(event: Event, allowed_routes: set[str]) -> Tuple[bool, str, str, Dict[str, Any], str]:
    """
    Platform safety rules (not client business policy).
    Returns:
      matched, route, reason, proposed_action, risk_level
    """

    # Platform Rule 1: Missing ordering signal should bias to human review
    ordering_signal_missing = bool(event.metadata.get("ordering_signal_missing", False))
    if ordering_signal_missing and "NEEDS_REVIEW" in allowed_routes:
        return (
            True,
            "NEEDS_REVIEW",
            "Ordering signal missing (history_id absent); requires human review",
            {
                "type": "needs_review",
                "reason": "ordering_signal_missing",
            },
            "medium",
        )

    # Platform Rule 2: Gmail payload missing message_id should bias to review
    payload = _get_payload(event)
    if event.source == "gmail" and payload.get("message_id") is None and "NEEDS_REVIEW" in allowed_routes:
        return (
            True,
            "NEEDS_REVIEW",
            "Missing Gmail message_id in payload; requires human review",
            {
                "type": "needs_review",
                "reason": "missing_message_id",
            },
            "medium",
        )

    return False, "", "", {}, ""


def _rule_matches(event: Event, rule: RoutingRule) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Returns:
      matched, reason, details
    """
    payload = _get_payload(event)

    if rule.match_type == "always":
        return True, "Rule match_type=always", {}

    if rule.match_type == "field_equals":
        if not rule.field:
            return False, "field_equals missing field", {}
        actual = payload.get(rule.field)
        if actual is None:
            return False, f"payload.{rule.field} missing", {"field": rule.field}
        if rule.value is None:
            return False, "field_equals missing value", {"field": rule.field}
        matched = str(actual).lower() == str(rule.value).lower()
        return (
            matched,
            f"payload.{rule.field} == {rule.value}",
            {"field": rule.field, "actual": actual, "expected": rule.value},
        )

    if rule.match_type == "field_missing":
        if not rule.field:
            return False, "field_missing missing field", {}
        missing = rule.field not in payload or payload.get(rule.field) in (None, "")
        return (
            missing,
            f"payload.{rule.field} missing or empty",
            {"field": rule.field},
        )

    if rule.match_type == "keyword":
        if rule.value is None:
            return False, "keyword missing value", {}
        haystack = _get_text(event).lower()
        needle = str(rule.value).lower()
        matched = needle in haystack
        return (
            matched,
            f"keyword '{needle}' in payload.text",
            {"keyword": needle},
        )

    return False, f"unknown match_type '{rule.match_type}'", {"match_type": rule.match_type}


def route_event(event: Event) -> Decision:
    """
    Decide what should happen next for an Event using a deterministic routing stack.

    Order of precedence:
      D0) Governance override: late event -> NOOP_LATE_EVENT
      D1) Platform safety rules -> NEEDS_REVIEW
      D2) Config-driven deterministic rules -> first match wins
      D3) Fallback -> NEEDS_REVIEW

    This function performs NO side effects. It returns a reviewable plan only.
    """
    decision_id = str(uuid.uuid4())

    # D0: Governance no-op for late events (ordering enforcement)
    if getattr(event, "is_late_event", False):
        return Decision(
            decision_id=decision_id,
            event_id=event.event_id,
            route="NOOP_LATE_EVENT",
            reason=f"Late/out-of-order event: {getattr(event, 'late_reason', None) or 'UNKNOWN'}",
            risk_level="low",
            proposed_action={
                "type": "noop",
                "reason": "late_event",
                "late_reason": getattr(event, "late_reason", None),
            },
            category=None,
            decision_source="fallback",
            confidence=None,
            threshold_used=None,
            rule_id=None,
        )

    cfg = load_routing_config()
    allowed_routes = set(cfg.routes)

    # D1: Platform safety posture
    matched, route, reason, proposed_action, risk_level = _platform_rule_match(event, allowed_routes)
    if matched:
        return Decision(
            decision_id=decision_id,
            event_id=event.event_id,
            route=route,
            reason=reason,
            risk_level=risk_level,
            proposed_action=proposed_action,
            category=None,
            decision_source="rule",
            confidence=1.0,
            threshold_used=None,
            rule_id=proposed_action.get("reason"),
        )

    # D2: Config-driven deterministic rules
    for rule in cfg.rules:
        matched, reason, details = _rule_matches(event, rule)
        if matched:
            proposed_action: Dict[str, Any] = {
                "type": "config_rule_match",
                "rule_id": rule.rule_id,
                "details": details,
            }

            if rule.route == "REQUEST_MORE_INFO":
                proposed_action = {
                    "question": "How urgent is this? (low / medium / high)",
                    "missing_fields": [rule.field] if rule.field else [],
                }

            return Decision(
                decision_id=decision_id,
                event_id=event.event_id,
                route=rule.route,
                reason=reason,
                risk_level=rule.risk_level,
                proposed_action=proposed_action,
                category=rule.category,
                decision_source="rule",
                confidence=1.0,
                threshold_used=None,
                rule_id=rule.rule_id,
            )

    # D3: Deterministic fallback
    return Decision(
        decision_id=decision_id,
        event_id=event.event_id,
        route="NEEDS_REVIEW",
        reason="No deterministic rule matched; requires human review",
        risk_level="medium",
        proposed_action={
            "type": "needs_review",
            "reason": "no_rule_match",
        },
        category=None,
        decision_source="fallback",
        confidence=None,
        threshold_used=None,
        rule_id=None,
    )