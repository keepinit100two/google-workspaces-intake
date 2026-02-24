import json
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from app.domain.schemas import Event

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "idempotency.sqlite3"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _dt_to_iso(dt: datetime) -> str:
    # Ensure deterministic serialization
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def _iso_to_dt(s: str) -> datetime:
    # Python 3.11 datetime.fromisoformat supports offsets
    return datetime.fromisoformat(s)


@dataclass(frozen=True)
class ClaimResult:
    """
    Deterministic idempotency claim result.

    claimed = True means caller owns the lease and may process.
    claimed = False means another processor owns it or it is already completed.

    status values:
      - "claimed"
      - "completed"
      - "failed"
    """

    claimed: bool
    status: str
    owner_id: Optional[str] = None
    lease_until: Optional[datetime] = None
    existing_event: Optional[Event] = None


class IdempotencyStore:
    """
    Interface for shared, multi-server-safe idempotency.

    This strengthens ingest safety by providing:
      - atomic first-writer-wins claiming
      - lease/TTL to recover from crashes
      - explicit status transitions (claimed -> completed/failed)
    """

    def try_claim(
        self,
        key: str,
        event: Event,
        lease_seconds: int = 120,
        owner_id: Optional[str] = None,
    ) -> ClaimResult:
        raise NotImplementedError

    def mark_completed(self, key: str, owner_id: str) -> None:
        raise NotImplementedError

    def mark_failed(self, key: str, owner_id: str, error: str) -> None:
        raise NotImplementedError

    def get(self, key: str) -> Optional[Event]:
        raise NotImplementedError

    # Backwards-compatible API (existing code may call set/get)
    def set(self, key: str, event: Event) -> None:
        raise NotImplementedError


class SQLiteIdempotencyStore(IdempotencyStore):
    """
    SQLite-backed idempotency store.

    NOTE:
    - Suitable for local dev / single instance.
    - NOT a shared multi-server store unless the DB file is on shared storage (not recommended).
    """

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS idempotency (
                    key TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    owner_id TEXT,
                    lease_until TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    event_json TEXT NOT NULL,
                    last_error TEXT
                )
                """
            )
            self._migrate_if_needed(conn)

    def _migrate_if_needed(self, conn: sqlite3.Connection) -> None:
        """
        Handle upgrade from the old schema:
          idempotency(key TEXT PRIMARY KEY, event_json TEXT NOT NULL)
        by adding columns if missing.
        """
        cols = conn.execute("PRAGMA table_info(idempotency)").fetchall()
        existing = {c[1] for c in cols}  # (cid, name, type, notnull, dflt_value, pk)

        required = {
            "status": "TEXT NOT NULL DEFAULT 'completed'",
            "owner_id": "TEXT",
            "lease_until": "TEXT",
            "created_at": "TEXT NOT NULL DEFAULT ''",
            "updated_at": "TEXT NOT NULL DEFAULT ''",
            "last_error": "TEXT",
        }

        # If we detect old table, it will be missing many columns.
        for name, ddl in required.items():
            if name not in existing:
                conn.execute(f"ALTER TABLE idempotency ADD COLUMN {name} {ddl}")

        # Backfill created_at/updated_at for legacy rows (best-effort)
        now = _dt_to_iso(_utc_now())
        if "created_at" in required or "updated_at" in required:
            conn.execute(
                """
                UPDATE idempotency
                SET created_at = CASE WHEN created_at = '' OR created_at IS NULL THEN ? ELSE created_at END,
                    updated_at = CASE WHEN updated_at = '' OR updated_at IS NULL THEN ? ELSE updated_at END,
                    status = CASE WHEN status IS NULL OR status = '' THEN 'completed' ELSE status END
                """,
                (now, now),
            )

    def get(self, key: str) -> Optional[Event]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT event_json FROM idempotency WHERE key = ?",
                (key,),
            ).fetchone()
        if not row:
            return None
        data = json.loads(row[0])
        return Event.model_validate(data)

    def set(self, key: str, event: Event) -> None:
        """
        Backwards-compatible setter: stores as completed.
        """
        now = _dt_to_iso(_utc_now())
        event_json = json.dumps(event.model_dump(), default=str)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO idempotency (key, status, owner_id, lease_until, created_at, updated_at, event_json, last_error)
                VALUES (?, 'completed', NULL, NULL, ?, ?, ?, NULL)
                ON CONFLICT(key) DO UPDATE SET
                    status='completed',
                    owner_id=NULL,
                    lease_until=NULL,
                    updated_at=excluded.updated_at,
                    event_json=excluded.event_json,
                    last_error=NULL
                """,
                (key, now, now, event_json),
            )

    def try_claim(
        self,
        key: str,
        event: Event,
        lease_seconds: int = 120,
        owner_id: Optional[str] = None,
    ) -> ClaimResult:
        owner_id = owner_id or f"worker-{uuid.uuid4()}"
        now = _utc_now()
        lease_until = now.replace(microsecond=0)  # stable-ish
        lease_until = lease_until + (datetime.utcfromtimestamp(0) - datetime.utcfromtimestamp(0))  # no-op for clarity
        lease_until = now + (datetime.fromtimestamp(lease_seconds, tz=timezone.utc) - datetime.fromtimestamp(0, tz=timezone.utc))
        # Above avoids importing timedelta; still deterministic as seconds delta.

        now_iso = _dt_to_iso(now)
        lease_iso = _dt_to_iso(lease_until)
        event_json = json.dumps(event.model_dump(), default=str)

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE;")  # lock for atomic claim

            row = conn.execute(
                "SELECT status, owner_id, lease_until, event_json FROM idempotency WHERE key = ?",
                (key,),
            ).fetchone()

            if row is None:
                # First writer wins
                conn.execute(
                    """
                    INSERT INTO idempotency (key, status, owner_id, lease_until, created_at, updated_at, event_json, last_error)
                    VALUES (?, 'claimed', ?, ?, ?, ?, ?, NULL)
                    """,
                    (key, owner_id, lease_iso, now_iso, now_iso, event_json),
                )
                conn.execute("COMMIT;")
                return ClaimResult(
                    claimed=True,
                    status="claimed",
                    owner_id=owner_id,
                    lease_until=lease_until,
                    existing_event=None,
                )

            status, existing_owner, existing_lease_iso, existing_event_json = row
            existing_event = Event.model_validate(json.loads(existing_event_json))

            # Completed means never process again (deterministic)
            if status == "completed":
                conn.execute("COMMIT;")
                return ClaimResult(
                    claimed=False,
                    status="completed",
                    owner_id=existing_owner,
                    lease_until=_iso_to_dt(existing_lease_iso) if existing_lease_iso else None,
                    existing_event=existing_event,
                )

            # If claimed but lease expired, allow takeover
            lease_expired = False
            if existing_lease_iso:
                try:
                    existing_lease = _iso_to_dt(existing_lease_iso)
                    lease_expired = existing_lease <= now
                except Exception:
                    # If unparsable, treat as expired (safe recovery)
                    lease_expired = True
            else:
                lease_expired = True

            if status == "claimed" and not lease_expired:
                # Another worker still owns it
                conn.execute("COMMIT;")
                return ClaimResult(
                    claimed=False,
                    status="claimed",
                    owner_id=existing_owner,
                    lease_until=_iso_to_dt(existing_lease_iso) if existing_lease_iso else None,
                    existing_event=existing_event,
                )

            # status == failed OR claimed-but-expired -> takeover with new lease
            conn.execute(
                """
                UPDATE idempotency
                SET status='claimed',
                    owner_id=?,
                    lease_until=?,
                    updated_at=?,
                    event_json=?,
                    last_error=NULL
                WHERE key=?
                """,
                (owner_id, lease_iso, now_iso, event_json, key),
            )
            conn.execute("COMMIT;")
            return ClaimResult(
                claimed=True,
                status="claimed",
                owner_id=owner_id,
                lease_until=lease_until,
                existing_event=existing_event,
            )

    def mark_completed(self, key: str, owner_id: str) -> None:
        now_iso = _dt_to_iso(_utc_now())
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE idempotency
                SET status='completed',
                    owner_id=?,
                    lease_until=NULL,
                    updated_at=?,
                    last_error=NULL
                WHERE key=?
                """,
                (owner_id, now_iso, key),
            )

    def mark_failed(self, key: str, owner_id: str, error: str) -> None:
        now_iso = _dt_to_iso(_utc_now())
        # Bound error length to keep DB clean
        error = (error or "")[:500]
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE idempotency
                SET status='failed',
                    owner_id=?,
                    lease_until=NULL,
                    updated_at=?,
                    last_error=?
                WHERE key=?
                """,
                (owner_id, now_iso, error, key),
            )


class FirestoreIdempotencyStore(IdempotencyStore):
    """
    Firestore-backed shared idempotency store (multi-server safe).

    This uses Firestore transactions for atomic claim semantics.
    Requirements:
      - google-cloud-firestore installed
      - service account / ADC configured in environment

    Environment:
      - FIRESTORE_PROJECT_ID (optional if ADC provides)
      - IDEMPOTENCY_COLLECTION (default: "idempotency")
    """

    def __init__(self, project_id: Optional[str] = None, collection: Optional[str] = None):
        try:
            from google.cloud import firestore  # type: ignore
        except Exception as e:
            raise RuntimeError(
                "google-cloud-firestore is required for FirestoreIdempotencyStore. "
                "Install with: pip install google-cloud-firestore"
            ) from e

        self._firestore = firestore
        self.project_id = project_id or os.getenv("FIRESTORE_PROJECT_ID")
        self.collection = collection or os.getenv("IDEMPOTENCY_COLLECTION", "idempotency")

        if self.project_id:
            self.client = firestore.Client(project=self.project_id)
        else:
            # Use default credentials/project
            self.client = firestore.Client()

    def _doc_ref(self, key: str):
        return self.client.collection(self.collection).document(key)

    def get(self, key: str) -> Optional[Event]:
        doc = self._doc_ref(key).get()
        if not doc.exists:
            return None
        data = doc.to_dict() or {}
        event_json = data.get("event_json")
        if not event_json:
            return None
        return Event.model_validate(json.loads(event_json))

    def set(self, key: str, event: Event) -> None:
        now = _utc_now()
        payload = {
            "status": "completed",
            "owner_id": None,
            "lease_until": None,
            "created_at": _dt_to_iso(now),
            "updated_at": _dt_to_iso(now),
            "event_json": json.dumps(event.model_dump(), default=str),
            "last_error": None,
        }
        self._doc_ref(key).set(payload, merge=True)

    def try_claim(
        self,
        key: str,
        event: Event,
        lease_seconds: int = 120,
        owner_id: Optional[str] = None,
    ) -> ClaimResult:
        owner_id = owner_id or f"worker-{uuid.uuid4()}"
        now = _utc_now()
        lease_until = now + (datetime.fromtimestamp(lease_seconds, tz=timezone.utc) - datetime.fromtimestamp(0, tz=timezone.utc))

        now_iso = _dt_to_iso(now)
        lease_iso = _dt_to_iso(lease_until)
        event_json = json.dumps(event.model_dump(), default=str)

        doc_ref = self._doc_ref(key)
        firestore = self._firestore

        @firestore.transactional
        def _txn_claim(txn):
            snap = doc_ref.get(transaction=txn)
            if not snap.exists:
                # First writer wins: create
                txn.create(
                    doc_ref,
                    {
                        "status": "claimed",
                        "owner_id": owner_id,
                        "lease_until": lease_iso,
                        "created_at": now_iso,
                        "updated_at": now_iso,
                        "event_json": event_json,
                        "last_error": None,
                    },
                )
                return ClaimResult(True, "claimed", owner_id, lease_until, None)

            data = snap.to_dict() or {}
            status = data.get("status", "failed")
            existing_owner = data.get("owner_id")
            existing_lease_iso = data.get("lease_until")
            existing_event_json = data.get("event_json")

            existing_event = None
            if existing_event_json:
                try:
                    existing_event = Event.model_validate(json.loads(existing_event_json))
                except Exception:
                    existing_event = None

            if status == "completed":
                return ClaimResult(
                    False,
                    "completed",
                    existing_owner,
                    _iso_to_dt(existing_lease_iso) if existing_lease_iso else None,
                    existing_event,
                )

            # Determine lease expiration
            lease_expired = True
            if existing_lease_iso:
                try:
                    lease_expired = _iso_to_dt(existing_lease_iso) <= now
                except Exception:
                    lease_expired = True

            if status == "claimed" and not lease_expired:
                return ClaimResult(
                    False,
                    "claimed",
                    existing_owner,
                    _iso_to_dt(existing_lease_iso) if existing_lease_iso else None,
                    existing_event,
                )

            # failed OR expired claim -> takeover
            txn.update(
                doc_ref,
                {
                    "status": "claimed",
                    "owner_id": owner_id,
                    "lease_until": lease_iso,
                    "updated_at": now_iso,
                    "event_json": event_json,
                    "last_error": None,
                },
            )
            return ClaimResult(True, "claimed", owner_id, lease_until, existing_event)

        txn = firestore.Transaction(self.client)
        return _txn_claim(txn)

    def mark_completed(self, key: str, owner_id: str) -> None:
        now_iso = _dt_to_iso(_utc_now())
        self._doc_ref(key).set(
            {
                "status": "completed",
                "owner_id": owner_id,
                "lease_until": None,
                "updated_at": now_iso,
                "last_error": None,
            },
            merge=True,
        )

    def mark_failed(self, key: str, owner_id: str, error: str) -> None:
        now_iso = _dt_to_iso(_utc_now())
        error = (error or "")[:500]
        self._doc_ref(key).set(
            {
                "status": "failed",
                "owner_id": owner_id,
                "lease_until": None,
                "updated_at": now_iso,
                "last_error": error,
            },
            merge=True,
        )


def get_idempotency_store() -> IdempotencyStore:
    """
    Factory for selecting idempotency backend.

    IDEMPOTENCY_BACKEND:
      - "sqlite" (default) : local dev
      - "firestore"        : shared multi-server safe
    """
    backend = os.getenv("IDEMPOTENCY_BACKEND", "sqlite").lower().strip()
    if backend == "firestore":
        return FirestoreIdempotencyStore()
    return SQLiteIdempotencyStore()