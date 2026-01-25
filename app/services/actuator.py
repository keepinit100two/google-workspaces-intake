import json
import uuid
from pathlib import Path
from typing import Any, Dict

from app.domain.schemas import Event, Decision, ActionResult

# Where draft artifacts are stored: <repo_root>/artifacts/drafts/
DRAFT_DIR = Path(__file__).resolve().parents[2] / "artifacts" / "drafts"


def _write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def execute_decision(event: Event, decision: Decision) -> ActionResult:
    """
    Act v0: Execute only safe, reversible actions.
    - CREATE_DRAFT_TICKET -> write a local draft JSON artifact
    - REQUEST_MORE_INFO / ESCALATE_HUMAN -> no side effects (noop)

    This function is intentionally deterministic and side-effect bounded.
    """
    action_id = str(uuid.uuid4())

    # Only execute draft ticket creation (safe, reversible)
    if decision.route == "CREATE_DRAFT_TICKET":
        # Idempotency at the action layer:
        # Draft artifact path is derived from event_id, so repeated runs overwrite the same file.
        artifact_path = DRAFT_DIR / f"{event.event_id}.draft_ticket.json"

        draft_payload = {
            "event_id": event.event_id,
            "decision_id": decision.decision_id,
            "route": decision.route,
            "risk_level": decision.risk_level,
            "reason": decision.reason,
            "proposed_action": decision.proposed_action,
        }

        _write_json(artifact_path, draft_payload)

        return ActionResult(
            action_id=action_id,
            event_id=event.event_id,
            decision_id=decision.decision_id,
            action_type="create_ticket_draft",
            status="executed",
            artifact_path=str(artifact_path),
            reason="Draft ticket artifact written",
        )

    # Everything else: no action executed (still a valid result)
    return ActionResult(
        action_id=action_id,
        event_id=event.event_id,
        decision_id=decision.decision_id,
        action_type="noop",
        status="noop",
        artifact_path=None,
        reason=f"No action executed for route: {decision.route}",
    )
