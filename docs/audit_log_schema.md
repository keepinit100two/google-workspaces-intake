# Audit Log Row Schema (Minimum Viable)

This system must provide full auditability:
- every ingest decision is logged
- every action is logged
- failures are explicit (no silent drops)

This schema defines the minimum stable columns for a Google Sheets audit log.

## Row Identity
- audit_id (string): unique identifier for the log row (can be UUID)
- trace_id (string): correlation id for one ingest attempt (from GmailIngestRequest)
- idempotency_key (string): stable dedupe key (see docs/idempotency_keys.md)

## Message Identity
- source (string): "gmail"
- mailbox (string): monitored mailbox address
- message_id (string, nullable)
- thread_id (string, nullable)
- history_id (string, nullable)

## Ordering / Cursor Policy
- cursor_current_history_id (int, nullable): cursor store value at decision time
- cursor_incoming_history_id (int, nullable): parsed incoming history_id if present
- cursor_reason (string): OK | NO_CURSOR | NO_INCOMING_HISTORY_ID | INVALID_HISTORY_ID | BEHIND_CURSOR | AT_CURSOR
- is_late_event (bool)
- late_reason (string, nullable)

## Decision
- decision_id (string)
- decision_source (string): rule | ai | fallback
- category (string, nullable): operational category (5–7 categories)
- confidence (float, nullable): 0..1
- threshold_used (float, nullable)
- route (string): deterministic route string (includes NOOP_LATE_EVENT / SHADOW_NOOP)
- rule_id (string, nullable)
- allowlist_verdict (string, nullable): allowed | blocked | needs_review

## AI Attempts (when AI is used)
- ai_attempts_count (int)
- ai_final_status (string): accepted | rejected | not_used
- ai_reject_reason (string, nullable)

## Actions
- action_plan_summary (string): human-readable summary of intended actions
- action_status (string): executed | noop | skipped | failed
- action_error_code (string, nullable)
- action_error_detail (string, nullable)  (bounded)
- artifacts_path (string, nullable)

## Timing
- trigger_received_at (RFC3339)
- ingested_at (RFC3339)
- decided_at (RFC3339)
- acted_at (RFC3339, nullable)
- completed_at (RFC3339, nullable)

## Status / Outcome
- ingest_status (string): claimed | completed | failed
- failure_sink_notified (bool)