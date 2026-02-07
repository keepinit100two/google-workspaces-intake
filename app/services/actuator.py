import uuid
from pathlib import Path
from typing import Any, Dict

from app.core.artifacts import LocalArtifactStore
from app.domain.schemas import Event, Decision, ActionResult

# Default local artifact store (template-safe)
DEFAULT_DRAFT_DIR = Path(__file__).resolve().parents[2] / "artifacts" / "drafts"
artifact_store = LocalArtifactStore(DEFAULT_DRAFT_DIR)


def execute_decision(event: Event, decision: Decision) -> ActionResult:
    """
    Act v0: Execute only safe, reversible actions.
    - CREATE_DRAFT_TICKET -> write a local draft JSON artifact via ArtifactStore
    - REQUEST_MORE_INFO / ESCALATE_HUMAN -> noop

    No external side effects.
    """
    action_id = str(uuid.uuid4())

    if decision.route == "CREATE_DRAFT_TICKET":
        relative_path = f"{event.event_id}.draft_ticket.json"

        draft_payload: Dict[str, Any] = {
            "event_id": event.event_id,
            "decision_id": decision.decision_id,
            "route": decision.route,
            "risk_level": decision.risk_level,
            "reason": decision.reason,
            "proposed_action": decision.proposed_action,
        }

        artifact_path = artifact_store.write_json(relative_path, draft_payload)

        return ActionResult(
            action_id=action_id,
            event_id=event.event_id,
            decision_id=decision.decision_id,
            action_type="create_ticket_draft",
            status="executed",
            artifact_path=artifact_path,
            reason="Draft ticket artifact written",
        )

    return ActionResult(
        action_id=action_id,
        event_id=event.event_id,
        decision_id=decision.decision_id,
        action_type="noop",
        status="noop",
        artifact_path=None,
        reason=f"No action executed for route: {decision.route}",
    )
