from datetime import datetime
import uuid

from fastapi import FastAPI, Header, HTTPException, Depends

from app.core.auth import require_ops_api_key
from app.core.idempotency_store import get_idempotency_store
from app.core.logging import get_logger, log_event
from app.domain.schemas import IngestRequest, IngestResponse, Event, GmailIngestRequest
from app.services.normalizer import normalize_gmail_ingest
from app.services.router import route_event
from app.services.actuator import execute_decision

app = FastAPI(title="AI Control Plane")
logger = get_logger()

# Idempotency store backend is selectable (sqlite for local, firestore for shared)
idem_store = get_idempotency_store()


def _gmail_idempotency_key(greq: GmailIngestRequest) -> str:
    """
    Deterministic idempotency key for Gmail ingest.

    Priority:
      1) message_id (best)
      2) history_id (fallback for watch/history style ingestion)
      3) else reject (cannot guarantee deterministic dedupe across retries)
    """
    mailbox = greq.mailbox.strip().lower()

    if greq.message_id:
        return f"gmail:mailbox={mailbox}:message_id={greq.message_id}"
    if greq.history_id:
        return f"gmail:mailbox={mailbox}:history_id={greq.history_id}"

    # No stable id => cannot guarantee idempotency across retries
    raise HTTPException(
        status_code=400,
        detail="Gmail ingest requires message_id or history_id for deterministic idempotency",
    )


def _process_ingest(ingest_req: IngestRequest, idempotency_key: str | None) -> IngestResponse:
    """
    Canonical ingest pipeline runner:
      - enforce Idempotency-Key (or derived idempotency key for typed ingest endpoints)
      - shared claim/lease (multi-server safe)
      - Decide (router)
      - Act v0 (safe execution)
      - structured logging
      - returns {event, decision}
    """
    # Gate 1: Idempotency-Key is required
    if not idempotency_key:
        log_event(
            logger,
            event_name="ingest_rejected",
            fields={
                "reason": "missing_idempotency_key",
                "event_type": ingest_req.event_type,
                "source": ingest_req.source,
            },
        )
        raise HTTPException(status_code=400, detail="Missing Idempotency-Key header")

    # Create a candidate canonical Event (used if we win the claim)
    candidate_event = Event(
        event_id=str(uuid.uuid4()),
        event_type=ingest_req.event_type,
        source=ingest_req.source,
        timestamp=datetime.utcnow(),
        actor=ingest_req.actor,
        payload=ingest_req.payload,
        metadata=ingest_req.metadata,
    )

    # Gate 2: Shared idempotency claim (multi-server safe)
    claim = idem_store.try_claim(
        key=idempotency_key,
        event=candidate_event,
        lease_seconds=120,
        owner_id=None,
    )

    if not claim.claimed:
        # Deterministic duplicate behavior: do not silently drop.
        existing_event = claim.existing_event or idem_store.get(idempotency_key) or candidate_event

        log_event(
            logger,
            event_name="ingest_duplicate",
            fields={
                "idempotency_key": idempotency_key,
                "status": claim.status,
                "event_id": existing_event.event_id,
                "event_type": existing_event.event_type,
                "source": existing_event.source,
                "owner_id": claim.owner_id,
            },
        )

        # Decide
        decision = route_event(existing_event)
        log_event(
            logger,
            event_name="decision_created",
            fields={
                "decision_id": decision.decision_id,
                "event_id": decision.event_id,
                "route": decision.route,
                "risk_level": decision.risk_level,
                "reason": decision.reason,
                "idempotency_key": idempotency_key,
            },
        )

        # Act (safe execution) — act-level idempotency will be strengthened later
        try:
            action_result = execute_decision(existing_event, decision)
            log_event(
                logger,
                event_name="action_executed" if action_result.status == "executed" else "action_noop",
                fields={
                    "action_id": action_result.action_id,
                    "event_id": action_result.event_id,
                    "decision_id": action_result.decision_id,
                    "action_type": action_result.action_type,
                    "status": action_result.status,
                    "artifact_path": action_result.artifact_path,
                    "reason": action_result.reason,
                    "idempotency_key": idempotency_key,
                },
            )
        except Exception as e:
            log_event(
                logger,
                event_name="action_failed",
                fields={
                    "event_id": existing_event.event_id,
                    "decision_id": decision.decision_id,
                    "route": decision.route,
                    "error": str(e),
                    "idempotency_key": idempotency_key,
                },
            )

        return IngestResponse(event=existing_event, decision=decision)

    # We own the lease: proceed as the single processor
    event = candidate_event

    log_event(
        logger,
        event_name="ingest_claimed",
        fields={
            "idempotency_key": idempotency_key,
            "event_id": event.event_id,
            "event_type": event.event_type,
            "source": event.source,
            "owner_id": claim.owner_id,
        },
    )

    # Decide
    decision = route_event(event)
    log_event(
        logger,
        event_name="decision_created",
        fields={
            "decision_id": decision.decision_id,
            "event_id": decision.event_id,
            "route": decision.route,
            "risk_level": decision.risk_level,
            "reason": decision.reason,
            "idempotency_key": idempotency_key,
        },
    )

    # Act (safe execution)
    try:
        action_result = execute_decision(event, decision)
        log_event(
            logger,
            event_name="action_executed" if action_result.status == "executed" else "action_noop",
            fields={
                "action_id": action_result.action_id,
                "event_id": action_result.event_id,
                "decision_id": action_result.decision_id,
                "action_type": action_result.action_type,
                "status": action_result.status,
                "artifact_path": action_result.artifact_path,
                "reason": action_result.reason,
                "idempotency_key": idempotency_key,
            },
        )

        # Mark completed only after successful Decide+Act path completes
        idem_store.mark_completed(key=idempotency_key, owner_id=claim.owner_id or "unknown")
        log_event(
            logger,
            event_name="ingest_completed",
            fields={
                "idempotency_key": idempotency_key,
                "event_id": event.event_id,
                "decision_id": decision.decision_id,
                "owner_id": claim.owner_id,
            },
        )

    except Exception as e:
        # Explicit failure handling: mark failed so retries can reclaim deterministically
        idem_store.mark_failed(key=idempotency_key, owner_id=claim.owner_id or "unknown", error=str(e))
        log_event(
            logger,
            event_name="ingest_failed",
            fields={
                "idempotency_key": idempotency_key,
                "event_id": event.event_id,
                "decision_id": decision.decision_id,
                "route": decision.route,
                "error": str(e),
                "owner_id": claim.owner_id,
            },
        )
        raise

    return IngestResponse(event=event, decision=decision)


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.get("/ops/ping")
def ops_ping(_: None = Depends(require_ops_api_key)):
    """
    Minimal protected ops endpoint.
    Requires header: X-API-Key: <OPS_API_KEY>
    """
    return {"status": "ok"}


@app.post("/ingest/api", response_model=IngestResponse)
def ingest_api(
    req: IngestRequest,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> IngestResponse:
    """
    Generic ingest endpoint: caller supplies Idempotency-Key header.
    """
    return _process_ingest(req, idempotency_key)


@app.post("/ingest/gmail", response_model=IngestResponse)
def ingest_gmail(req: GmailIngestRequest) -> IngestResponse:
    """
    Gmail ingest endpoint:
      - accepts GmailIngestRequest (trigger envelope)
      - normalizes to canonical IngestRequest
      - derives deterministic idempotency key
      - runs canonical pipeline
    """
    try:
        idempotency_key = _gmail_idempotency_key(req)
    except HTTPException:
        log_event(
            logger,
            event_name="ingest_rejected",
            fields={
                "reason": "missing_gmail_id",
                "mailbox": req.mailbox,
                "trigger_type": req.trigger_type,
                "trace_id": req.trace_id,
            },
        )
        raise

    canonical_req = normalize_gmail_ingest(req)

    log_event(
        logger,
        event_name="gmail_normalized",
        fields={
            "idempotency_key": idempotency_key,
            "mailbox": req.mailbox,
            "trigger_type": req.trigger_type,
            "message_id": req.message_id,
            "thread_id": req.thread_id,
            "history_id": req.history_id,
            "trace_id": req.trace_id,
        },
    )

    return _process_ingest(canonical_req, idempotency_key)