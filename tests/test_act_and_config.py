import json

from fastapi.testclient import TestClient

from app.main import app
import app.services.actuator as actuator

client = TestClient(app)


def test_create_draft_ticket_writes_artifact(tmp_path, monkeypatch):
    """
    If routing returns CREATE_DRAFT_TICKET, Act v0 must write a draft artifact.
    We redirect draft output to tmp_path so tests don't touch real artifacts/.
    """
    draft_dir = tmp_path / "drafts"
    monkeypatch.setattr(actuator, "DRAFT_DIR", draft_dir)

    payload = {
        "event_type": "support_request",
        "source": "api",
        "actor": "test_user",
        "payload": {"text": "VPN is down", "urgency": "high"},
        "metadata": {"channel": "pytest"},
    }

    headers = {"Idempotency-Key": "act-draft-1"}

    resp = client.post("/ingest/api", json=payload, headers=headers)
    assert resp.status_code == 200

    body = resp.json()
    event_id = body["event"]["event_id"]
    decision_id = body["decision"]["decision_id"]

    assert body["decision"]["route"] == "CREATE_DRAFT_TICKET"

    artifact_path = draft_dir / f"{event_id}.draft_ticket.json"
    assert artifact_path.exists()

    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert artifact["event_id"] == event_id
    assert artifact["decision_id"] == decision_id
    assert artifact["route"] == "CREATE_DRAFT_TICKET"
    assert "proposed_action" in artifact


def test_escalation_does_not_write_artifact(tmp_path, monkeypatch):
    """
    ESCALATE_HUMAN must not produce any draft artifacts.
    """
    draft_dir = tmp_path / "drafts"
    monkeypatch.setattr(actuator, "DRAFT_DIR", draft_dir)

    payload = {
        "event_type": "support_request",
        "source": "api",
        "actor": "test_user",
        "payload": {"text": "We have a security breach", "urgency": "high"},
        "metadata": {},
    }

    headers = {"Idempotency-Key": "act-escalate-1"}

    resp = client.post("/ingest/api", json=payload, headers=headers)
    assert resp.status_code == 200

    decision = resp.json()["decision"]
    assert decision["route"] == "ESCALATE_HUMAN"

    assert not draft_dir.exists() or not any(draft_dir.iterdir())


def test_request_more_info_does_not_write_artifact(tmp_path, monkeypatch):
    """
    REQUEST_MORE_INFO must not produce any draft artifacts.
    """
    draft_dir = tmp_path / "drafts"
    monkeypatch.setattr(actuator, "DRAFT_DIR", draft_dir)

    payload = {
        "event_type": "support_request",
        "source": "api",
        "actor": "test_user",
        "payload": {"text": "VPN is down"},
        "metadata": {},
    }

    headers = {"Idempotency-Key": "act-clarify-1"}

    resp = client.post("/ingest/api", json=payload, headers=headers)
    assert resp.status_code == 200

    decision = resp.json()["decision"]
    assert decision["route"] == "REQUEST_MORE_INFO"

    assert not draft_dir.exists() or not any(draft_dir.iterdir())


# TODO: Add a config regression test later (prefer integration-style: temporarily write configs/routing.json,
# reload router module, assert routing changes, then restore original config).



