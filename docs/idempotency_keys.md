# Idempotency Key Contract

This project is a deterministic intake control plane. Idempotency keys are treated as stable primary identifiers.
They MUST remain stable across retries, servers, and deployments.

## Invariants
- Same external message → same idempotency key
- Idempotency keys are deterministic and reproducible
- Keys are stable over time (do not change format once deployed)
- No hidden state in key generation

## Gmail Idempotency Keys

### Preferred (message-level)
Use when `message_id` is available.

Format:
gmail:mailbox=<lowercased-mailbox>:message_id=<gmail-message-id>

Example:
gmail:mailbox=support@example.com:message_id=18c3f1a2bcd9e01a

### Fallback (history-based)
Use only when `message_id` is missing but `history_id` exists.

Format:
gmail:mailbox=<lowercased-mailbox>:history_id=<gmail-history-id>

Example:
gmail:mailbox=support@example.com:history_id=987654321

### Rejection Rule
If BOTH `message_id` and `history_id` are missing:
- Reject ingest (HTTP 400)
- Emit an explicit failure event (no silent drop)

Reason:
Without at least one stable identifier, idempotency cannot be guaranteed across retries.

## Status Model (Idempotency Store)
The idempotency store supports explicit status transitions:

- claimed     → a worker owns the lease and may process
- completed  → terminal state; do not reprocess
- failed     → retryable; can be reclaimed by a new worker

A lease/TTL prevents permanent locks if a worker crashes.

## Notes
- `trace_id` is NOT an idempotency key. It changes per trigger/attempt.
- HistoryId ordering is mailbox-scoped and is used for cursor policy (ordering enforcement),
  not as a semantic message identifier unless message_id is missing.