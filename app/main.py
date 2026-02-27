from datetime import datetime
import os
import uuid

from fastapi import FastAPI, Header, HTTPException, Depends

from app.core.auth import require_ops_api_key
from app.core.cursor_store import get_mailbox_cursor_store
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

# Mailbox cursor store backend is selectable (sqlite for local, firestore for shared)
cursor_store = get_mailbox_cursor_store()


def _is_shadow_mode() -> bool:
    """
    Shadow mode = compute decisions + log, but do NOT execute Act or advance cursor.
    Controlled via env var SHADOW_MODE.

    Truthy values: "1", "true", "yes", "on"
    """
    v = os.getenv("SHADOW_MODE", "").strip().lower()
    return v in {"1", "true", "yes", "on"}


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
      - Act v0 (safe execution) unless late-event or shadow mode
      - mailbox cursor advance (if applicable) after successful completion
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

    shadow_mode = _is_shadow_mode()

    # Late-event flags (set by upstream typed ingest endpoints like /ingest/gmail)
    is_late_event = bool(ingest_req.metadata.get("is_late_event", False))
    late_reason = ingest_req.metadata.get("late_reason")

    # Create a candidate canonical Event (used if we win the claim)
    candidate_event = Event(
        event_id=str(uuid.uuid4()),
        event_type=ingest_req.event_type,
        source=ingest_req.source,
        timestamp=datetime.utcnow(),
        actor=ingest_req.actor,
        payload=ingest_req.payload,
        metadata=ingest_req.metadata,
        gmail=None,
        ordering=None,
        is_late_event=is_late_event,
        late_reason=late_reason,
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
                "shadow_mode": shadow_mode,
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
                "shadow_mode": shadow_mode,
            },
        )

        # IMPORTANT:
        # We do NOT enforce shadow-mode behavior on the duplicate responder path here.
        # The canonical behavior is enforced on the single processor path that holds the claim
        # and marks completed. This keeps "one writer" responsible for state mutations.

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
                    "shadow_mode": shadow_mode,
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
                    "shadow_mode": shadow_mode,
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
            "is_late_event": event.is_late_event,
            "late_reason": event.late_reason,
            "shadow_mode": shadow_mode,
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
            "is_late_event": event.is_late_event,
            "late_reason": event.late_reason,
            "ordering_signal_missing": bool(ingest_req.metadata.get("ordering_signal_missing", False)),
            "shadow_mode": shadow_mode,
        },
    )

    # 1) Governance no-op for late events (ordering enforcement)
    if event.is_late_event:
        idem_store.mark_completed(key=idempotency_key, owner_id=claim.owner_id or "unknown")
        log_event(
            logger,
            event_name="late_event_noop",
            fields={
                "idempotency_key": idempotency_key,
                "event_id": event.event_id,
                "decision_id": decision.decision_id,
                "owner_id": claim.owner_id,
                "late_reason": event.late_reason,
                "shadow_mode": shadow_mode,
            },
        )
        return IngestResponse(event=event, decision=decision)

    # 2) Shadow mode no-op (compute + log only; no Act, no cursor advance)
    if shadow_mode:
        idem_store.mark_completed(key=idempotency_key, owner_id=claim.owner_id or "unknown")
        log_event(
            logger,
            event_name="shadow_mode_noop",
            fields={
                "idempotency_key": idempotency_key,
                "event_id": event.event_id,
                "decision_id": decision.decision_id,
                "owner_id": claim.owner_id,
                "route": decision.route,
            },
        )
        return IngestResponse(event=event, decision=decision)

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

        # Mailbox cursor advance (Gmail only), only after completion, only if not-late and not shadow-mode
        # Policy A: if history_id missing, we do not advance cursor.
        if ingest_req.source == "gmail":
            mailbox = ingest_req.payload.get("mailbox")
            history_id = ingest_req.payload.get("history_id")
            if mailbox and history_id:
                advanced = cursor_store.try_advance(mailbox=mailbox, new_history_id=str(history_id))
                log_event(
                    logger,
                    event_name="mailbox_cursor_advanced" if advanced else "mailbox_cursor_unchanged",
                    fields={
                        "mailbox": mailbox,
                        "history_id": history_id,
                        "advanced": advanced,
                        "idempotency_key": idempotency_key,
                        "event_id": event.event_id,
                    },
                )
            elif mailbox and not history_id:
                log_event(
                    logger,
                    event_name="mailbox_cursor_missing_history_id",
                    fields={
                        "mailbox": mailbox,
                        "idempotency_key": idempotency_key,
                        "event_id": event.event_id,
                        "ordering_signal_missing": True,
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
                "shadow_mode": shadow_mode,
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
      - mailbox cursor check (late/out-of-order detection)
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

    # Cursor check (ordering enforcement). This is policy, not just metadata.
    cursor_check = cursor_store.check_late(mailbox=req.mailbox, incoming_history_id=req.history_id)
    is_late_event = bool(cursor_check.is_late)
    late_reason = cursor_check.reason if is_late_event else None

    canonical_req = normalize_gmail_ingest(req)

    # Policy A: missing history_id is allowed but must be signaled deterministically.
    ordering_signal_missing = req.history_id is None
    canonical_req.metadata["ordering_signal_missing"] = ordering_signal_missing

    # Persist late-event determination into canonical metadata so _process_ingest can enforce no-op
    canonical_req.metadata["is_late_event"] = is_late_event
    canonical_req.metadata["late_reason"] = late_reason
    canonical_req.metadata["cursor_check"] = {
        "incoming_history_id": cursor_check.incoming_history_id,
        "current_history_id": cursor_check.current_history_id,
        "reason": cursor_check.reason,
        "is_late": cursor_check.is_late,
    }

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
            "is_late_event": is_late_event,
            "late_reason": late_reason,
            "cursor_reason": cursor_check.reason,
            "cursor_incoming": cursor_check.incoming_history_id,
            "cursor_current": cursor_check.current_history_id,
            "ordering_signal_missing": ordering_signal_missing,
            "shadow_mode": _is_shadow_mode(),
        },
    )

    return _process_ingest(canonical_req, idempotency_key)