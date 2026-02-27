# Ordering Policy (Mailbox Cursor Governance)

This system processes Gmail intake events in a distributed environment.
To prevent out-of-order events from mutating visible system state, we enforce a mailbox cursor policy using Gmail `historyId`.

## Concepts

### Ordering Metadata (Schema)
The ingest envelope and canonical request carry ordering metadata such as:
- trigger_received_at
- event_observed_at
- history_id (optional)

This metadata is passive. It does not enforce ordering by itself.

### Mailbox Cursor Policy (Enforcement)
The cursor store persists the last processed mailbox position:
- last_history_id per mailbox

We compare incoming history_id to the stored cursor:
- incoming < cursor  => late event (no mutation)
- incoming == cursor => not late (duplicate-ish ordering position)
- incoming > cursor  => normal processing

## Late Event Rule (Hard)
If an event is classified as late (incoming historyId behind cursor):
- event is marked late
- Decision.route must be NOOP_LATE_EVENT
- Act must not run (no user-visible mutations)
- idempotency is marked completed (so we do not loop)
- cursor is NOT advanced (cursor already ahead)

## Missing historyId Policy (A)
Sometimes the trigger may not contain a `history_id`.

Policy A:
- Do NOT reject ingest solely due to missing history_id
- Proceed with processing
- Mark canonical metadata:
  - ordering_signal_missing = true
  - cursor_check.reason = NO_INCOMING_HISTORY_ID
- Downstream Decide may choose a safer route (e.g., Needs Review) based on this flag.

Reason:
- Phase 1 prioritizes operational continuity while preserving deterministic safety signals.
- We still maintain dedupe via message_id-based idempotency keys when available.

## Cursor Advance Rule
Cursor is advanced only after:
- non-late event
- successful completion of the pipeline (Act success)
- history_id exists

Cursor is never advanced for:
- late events
- failed events
- missing history_id
- shadow-mode (future policy; see docs)