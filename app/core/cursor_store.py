import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple


DB_PATH = Path(__file__).resolve().parents[2] / "data" / "cursor.sqlite3"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_history_id(history_id: Optional[str]) -> Optional[int]:
    if history_id is None:
        return None
    s = str(history_id).strip()
    if s == "":
        return None
    # Gmail historyId is numeric-as-string
    try:
        return int(s)
    except ValueError:
        return None


@dataclass(frozen=True)
class CursorCheck:
    mailbox: str
    incoming_history_id: Optional[int]
    current_history_id: Optional[int]
    is_late: bool
    reason: str  # OK | NO_INCOMING_HISTORY_ID | NO_CURSOR | BEHIND_CURSOR | AT_CURSOR | INVALID_HISTORY_ID


class MailboxCursorStore:
    """
    Shared mailbox cursor policy store.

    Purpose:
      - Persist the last processed mailbox cursor (history_id)
      - Detect late/out-of-order events deterministically

    NOTE:
      - Cursor advance must occur only after successful pipeline completion.
      - Cursor check is read-only and safe early in ingest.
    """

    def get_cursor(self, mailbox: str) -> Optional[int]:
        raise NotImplementedError

    def check_late(self, mailbox: str, incoming_history_id: Optional[str]) -> CursorCheck:
        raise NotImplementedError

    def try_advance(self, mailbox: str, new_history_id: Optional[str]) -> bool:
        """
        Advance cursor only if new_history_id > current.
        Returns True if advanced, False otherwise.
        """
        raise NotImplementedError


class SQLiteMailboxCursorStore(MailboxCursorStore):
    """
    SQLite cursor store.
    - Suitable for local dev / single instance.
    - Not shared across servers unless DB is on shared storage (not recommended).
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
                CREATE TABLE IF NOT EXISTS mailbox_cursor (
                    mailbox TEXT PRIMARY KEY,
                    history_id INTEGER,
                    updated_at TEXT NOT NULL
                )
                """
            )

    def get_cursor(self, mailbox: str) -> Optional[int]:
        m = mailbox.strip().lower()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT history_id FROM mailbox_cursor WHERE mailbox = ?",
                (m,),
            ).fetchone()
        if not row:
            return None
        return row[0]

    def check_late(self, mailbox: str, incoming_history_id: Optional[str]) -> CursorCheck:
        m = mailbox.strip().lower()
        incoming = _parse_history_id(incoming_history_id)
        current = self.get_cursor(m)

        if incoming_history_id is not None and incoming is None:
            return CursorCheck(
                mailbox=m,
                incoming_history_id=None,
                current_history_id=current,
                is_late=False,
                reason="INVALID_HISTORY_ID",
            )

        if incoming is None:
            return CursorCheck(
                mailbox=m,
                incoming_history_id=None,
                current_history_id=current,
                is_late=False,
                reason="NO_INCOMING_HISTORY_ID",
            )

        if current is None:
            return CursorCheck(
                mailbox=m,
                incoming_history_id=incoming,
                current_history_id=None,
                is_late=False,
                reason="NO_CURSOR",
            )

        if incoming < current:
            return CursorCheck(
                mailbox=m,
                incoming_history_id=incoming,
                current_history_id=current,
                is_late=True,
                reason="BEHIND_CURSOR",
            )

        if incoming == current:
            return CursorCheck(
                mailbox=m,
                incoming_history_id=incoming,
                current_history_id=current,
                is_late=False,
                reason="AT_CURSOR",
            )

        return CursorCheck(
            mailbox=m,
            incoming_history_id=incoming,
            current_history_id=current,
            is_late=False,
            reason="OK",
        )

    def try_advance(self, mailbox: str, new_history_id: Optional[str]) -> bool:
        m = mailbox.strip().lower()
        new_val = _parse_history_id(new_history_id)
        if new_val is None:
            return False

        now = _utc_now_iso()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE;")
            row = conn.execute(
                "SELECT history_id FROM mailbox_cursor WHERE mailbox = ?",
                (m,),
            ).fetchone()

            if row is None:
                conn.execute(
                    "INSERT INTO mailbox_cursor (mailbox, history_id, updated_at) VALUES (?, ?, ?)",
                    (m, new_val, now),
                )
                conn.execute("COMMIT;")
                return True

            current = row[0]
            if current is None or new_val > current:
                conn.execute(
                    "UPDATE mailbox_cursor SET history_id = ?, updated_at = ? WHERE mailbox = ?",
                    (new_val, now, m),
                )
                conn.execute("COMMIT;")
                return True

            conn.execute("COMMIT;")
            return False


class FirestoreMailboxCursorStore(MailboxCursorStore):
    """
    Firestore shared cursor store.

    Environment:
      - FIRESTORE_PROJECT_ID (optional if ADC provides)
      - CURSOR_COLLECTION (default: "mailbox_cursor")
    """

    def __init__(self, project_id: Optional[str] = None, collection: Optional[str] = None):
        try:
            from google.cloud import firestore  # type: ignore
        except Exception as e:
            raise RuntimeError(
                "google-cloud-firestore is required for FirestoreMailboxCursorStore. "
                "Install with: pip install google-cloud-firestore"
            ) from e

        self._firestore = firestore
        self.project_id = project_id or os.getenv("FIRESTORE_PROJECT_ID")
        self.collection = collection or os.getenv("CURSOR_COLLECTION", "mailbox_cursor")

        if self.project_id:
            self.client = firestore.Client(project=self.project_id)
        else:
            self.client = firestore.Client()

    def _doc_ref(self, mailbox: str):
        m = mailbox.strip().lower()
        return self.client.collection(self.collection).document(m)

    def get_cursor(self, mailbox: str) -> Optional[int]:
        doc = self._doc_ref(mailbox).get()
        if not doc.exists:
            return None
        data = doc.to_dict() or {}
        val = data.get("history_id")
        return int(val) if val is not None else None

    def check_late(self, mailbox: str, incoming_history_id: Optional[str]) -> CursorCheck:
        incoming = _parse_history_id(incoming_history_id)
        current = self.get_cursor(mailbox)

        m = mailbox.strip().lower()
        if incoming_history_id is not None and incoming is None:
            return CursorCheck(m, None, current, False, "INVALID_HISTORY_ID")
        if incoming is None:
            return CursorCheck(m, None, current, False, "NO_INCOMING_HISTORY_ID")
        if current is None:
            return CursorCheck(m, incoming, None, False, "NO_CURSOR")
        if incoming < current:
            return CursorCheck(m, incoming, current, True, "BEHIND_CURSOR")
        if incoming == current:
            return CursorCheck(m, incoming, current, False, "AT_CURSOR")
        return CursorCheck(m, incoming, current, False, "OK")

    def try_advance(self, mailbox: str, new_history_id: Optional[str]) -> bool:
        new_val = _parse_history_id(new_history_id)
        if new_val is None:
            return False

        firestore = self._firestore
        doc_ref = self._doc_ref(mailbox)
        now = _utc_now_iso()

        @firestore.transactional
        def _txn(txn):
            snap = doc_ref.get(transaction=txn)
            if not snap.exists:
                txn.create(doc_ref, {"history_id": new_val, "updated_at": now})
                return True

            data = snap.to_dict() or {}
            cur = data.get("history_id")
            cur_val = int(cur) if cur is not None else None

            if cur_val is None or new_val > cur_val:
                txn.update(doc_ref, {"history_id": new_val, "updated_at": now})
                return True

            return False

        txn = firestore.Transaction(self.client)
        return _txn(txn)


def get_mailbox_cursor_store() -> MailboxCursorStore:
    """
    Factory for selecting cursor backend.

    CURSOR_BACKEND:
      - "sqlite" (default)
      - "firestore"
    """
    backend = os.getenv("CURSOR_BACKEND", "sqlite").lower().strip()
    if backend == "firestore":
        return FirestoreMailboxCursorStore()
    return SQLiteMailboxCursorStore()