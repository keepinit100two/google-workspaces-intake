from pathlib import Path

from app.core.cursor_store import SQLiteMailboxCursorStore


def test_cursor_no_cursor_not_late(tmp_path: Path):
    store = SQLiteMailboxCursorStore(db_path=tmp_path / "cursor.sqlite3")
    chk = store.check_late("support@example.com", "10")
    assert chk.is_late is False
    assert chk.reason == "NO_CURSOR"


def test_cursor_advance_and_detect_late(tmp_path: Path):
    store = SQLiteMailboxCursorStore(db_path=tmp_path / "cursor.sqlite3")

    # advance to 100
    advanced = store.try_advance("support@example.com", "100")
    assert advanced is True
    assert store.get_cursor("support@example.com") == 100

    # incoming behind cursor -> late
    chk = store.check_late("support@example.com", "99")
    assert chk.is_late is True
    assert chk.reason == "BEHIND_CURSOR"

    # incoming at cursor -> not late
    chk2 = store.check_late("support@example.com", "100")
    assert chk2.is_late is False
    assert chk2.reason == "AT_CURSOR"

    # incoming ahead -> OK
    chk3 = store.check_late("support@example.com", "101")
    assert chk3.is_late is False
    assert chk3.reason == "OK"


def test_cursor_does_not_regress(tmp_path: Path):
    store = SQLiteMailboxCursorStore(db_path=tmp_path / "cursor.sqlite3")

    assert store.try_advance("support@example.com", "50") is True
    assert store.try_advance("support@example.com", "49") is False
    assert store.get_cursor("support@example.com") == 50