from datetime import datetime
import os
import uuid

from fastapi import FastAPI, Header, HTTPException, Depends

from app.core.auth import require_ops_api_key
from app.core.cursor_store import get_mailbox_cursor_store
from app.core.failure_sink import get_failure_sink, make_failure
from app.core.idempotency_store import get_idempotency_store
from app.core.logging import get_logger, log_event
from app.domain.schemas import IngestRequest, IngestResponse, Event, GmailIngestRequest
from app.services.normalizer import normalize_gmail_ingest
from app.services.router import route_event
from app.services.actuator import execute_decision

app = FastAPI(title="AI Control Plane")
logger = get_logger()

idem_store = get_idempotency_store()
cursor_store = get_mailbox_cursor_store()
failure_sink = get_failure_sink()


def _is_shadow_mode() -> bool:
    v = os.getenv("SHADOW_MODE", "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _gmail_idempotency_key(greq: GmailIngestRequest) -> str:
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
        failure_sink.notify(
            make_failure(
                stage="ingest",
                error_code="INGEST_REJECTED_MISSING_IDEMPOTENCY_KEY",
                message="Missing Idempotency-Key header",
                context={
                    "event_type": ingest_req.event_type,
                    "source": ingest_req.source,
                },
            )
        )
        raise HTTPException(status_code=400, detail="Missing Idempotency-Key header")

    shadow_mode = _is_shadow_mode()
    is_late_event = bool(ingest_req.metadata.get("is_late_event", False))
    late_reason = ingest_req.metadata.get("late_reason")

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

    claim = idem_store.try_claim(
        key=idempotency_key,
        event=candidate_event,
        lease_seconds=120,
        owner_id=None,
    )

    if not claim.claimed:
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
            failure_sink.notify(
                make_failure(
                    stage="act",
                    error_code="ACT_FAILED_DUPLICATE_PATH",
                    message=str(e),
                    context={
                        "idempotency_key": idempotency_key,
                        "event_id": existing_event.event_id,
                        "decision_id": decision.decision_id,
                        "route": decision.route,
                    },
                )
            )

        return IngestResponse(event=existing_event, decision=decision)

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
        failure_sink.notify(
            make_failure(
                stage="act",
                error_code="ACT_FAILED_SINGLE_PROCESSOR",
                message=str(e),
                context={
                    "idempotency_key": idempotency_key,
                    "event_id": event.event_id,
                    "decision_id": decision.decision_id,
                    "route": decision.route,
                    "source": ingest_req.source,
                },
            )
        )
        raise

    return IngestResponse(event=event, decision=decision)


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.get("/ops/ping")
def ops_ping(_: None = Depends(require_ops_api_key)):
    return {"status": "ok"}


@app.post("/ingest/api", response_model=IngestResponse)
def ingest_api(
    req: IngestRequest,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> IngestResponse:
    return _process_ingest(req, idempotency_key)


@app.post("/ingest/gmail", response_model=IngestResponse)
def ingest_gmail(req: GmailIngestRequest) -> IngestResponse:
    try:
        idempotency_key = _gmail_idempotency_key(req)
    except HTTPException as e:
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
        failure_sink.notify(
            make_failure(
                stage="ingest",
                error_code="INGEST_REJECTED_MISSING_GMAIL_IDS",
                message=str(e.detail),
                context={
                    "mailbox": req.mailbox,
                    "trigger_type": req.trigger_type,
                    "trace_id": req.trace_id,
                },
            )
        )
        raise

    cursor_check = cursor_store.check_late(mailbox=req.mailbox, incoming_history_id=req.history_id)
    is_late_event = bool(cursor_check.is_late)
    late_reason = cursor_check.reason if is_late_event else None

    canonical_req = normalize_gmail_ingest(req)

    ordering_signal_missing = req.history_id is None
    canonical_req.metadata["ordering_signal_missing"] = ordering_signal_missing
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