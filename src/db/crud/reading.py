from typing import Optional, List, Dict, Any
from datetime import date
from src.db.d1_client import d1_query, d1_one, d1_exec

VALID_STATUSES = {"WANT_TO_READ", "READING", "READ", "DNF"}


def create_reading_log(
    owner_id: int,
    title: str,
    author: Optional[str] = None,
    book_id: Optional[int] = None,
    cover_url: Optional[str] = None,
    total_pages: Optional[int] = None,
    status: str = "WANT_TO_READ",
) -> Dict[str, Any]:
    if status not in VALID_STATUSES:
        raise ValueError(f"Invalid status: {status}")

    if book_id is not None:
        book = d1_one("SELECT title, author, cover_url FROM books WHERE book_id = ? AND owner_id = ?", [book_id, owner_id])
        if not book:
            raise ValueError(f"Book ID {book_id} does not exist.")
        title = title or book["title"]
        author = author or book["author"]
        cover_url = cover_url or book["cover_url"]

    started_at = date.today().isoformat() if status == "READING" else None

    meta = d1_exec(
        """
        INSERT INTO reading_log (owner_id, book_id, title, author, cover_url, status, total_pages, started_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [owner_id, book_id, title, author, cover_url, status, total_pages, started_at],
    )
    return get_reading_log(owner_id, meta["last_row_id"])


def list_reading_log(owner_id: int, status: Optional[str] = None) -> List[Dict[str, Any]]:
    if status:
        rows = d1_query(
            "SELECT * FROM reading_log WHERE owner_id = ? AND status = ? ORDER BY created_at DESC",
            [owner_id, status],
        )
    else:
        rows = d1_query("SELECT * FROM reading_log WHERE owner_id = ? ORDER BY created_at DESC", [owner_id])
    return [dict(r) for r in rows]


def get_reading_log(owner_id: int, reading_log_id: int) -> Optional[Dict[str, Any]]:
    row = d1_one("SELECT * FROM reading_log WHERE reading_log_id = ? AND owner_id = ?", [reading_log_id, owner_id])
    return dict(row) if row else None


def update_status(owner_id: int, reading_log_id: int, status: str, rating: Optional[int] = None) -> Dict[str, Any]:
    if status not in VALID_STATUSES:
        raise ValueError(f"Invalid status: {status}")

    current = get_reading_log(owner_id, reading_log_id)
    if not current:
        raise ValueError(f"Reading log {reading_log_id} does not exist.")

    today = date.today().isoformat()
    started_at = current["started_at"]
    finished_at = current["finished_at"]

    if status == "READING" and not started_at:
        started_at = today
    if status in ("READ", "DNF") and not finished_at:
        finished_at = today
    if status == "WANT_TO_READ":
        started_at = None
        finished_at = None

    d1_exec(
        """
        UPDATE reading_log
        SET status = ?, started_at = ?, finished_at = ?, rating = COALESCE(?, rating)
        WHERE reading_log_id = ? AND owner_id = ?
        """,
        [status, started_at, finished_at, rating, reading_log_id, owner_id],
    )
    return get_reading_log(owner_id, reading_log_id)


def update_progress(owner_id: int, reading_log_id: int, current_page: int, total_pages: Optional[int] = None) -> Dict[str, Any]:
    current = get_reading_log(owner_id, reading_log_id)
    if not current:
        raise ValueError(f"Reading log {reading_log_id} does not exist.")

    d1_exec(
        "UPDATE reading_log SET current_page = ?, total_pages = COALESCE(?, total_pages) WHERE reading_log_id = ? AND owner_id = ?",
        [current_page, total_pages, reading_log_id, owner_id],
    )
    return get_reading_log(owner_id, reading_log_id)


def delete_reading_log(owner_id: int, reading_log_id: int) -> None:
    current = get_reading_log(owner_id, reading_log_id)
    if not current:
        raise ValueError(f"Reading log {reading_log_id} does not exist.")
    d1_exec("DELETE FROM journal_entries WHERE reading_log_id = ? AND owner_id = ?", [reading_log_id, owner_id])
    d1_exec("DELETE FROM reading_log WHERE reading_log_id = ? AND owner_id = ?", [reading_log_id, owner_id])


def list_journal_entries(owner_id: int, reading_log_id: int) -> List[Dict[str, Any]]:
    rows = d1_query(
        "SELECT * FROM journal_entries WHERE reading_log_id = ? AND owner_id = ? ORDER BY entry_date DESC",
        [reading_log_id, owner_id],
    )
    return [dict(r) for r in rows]


def create_journal_entry(
    owner_id: int,
    reading_log_id: int,
    content: str,
    page_at_entry: Optional[int] = None,
    mood: Optional[str] = None,
    contains_spoilers: bool = False,
) -> Dict[str, Any]:
    log = get_reading_log(owner_id, reading_log_id)
    if not log:
        raise ValueError(f"Reading log {reading_log_id} does not exist.")

    meta = d1_exec(
        """
        INSERT INTO journal_entries (owner_id, reading_log_id, content, page_at_entry, mood, contains_spoilers)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [owner_id, reading_log_id, content, page_at_entry, mood, int(contains_spoilers)],
    )
    row = d1_one("SELECT * FROM journal_entries WHERE entry_id = ? AND owner_id = ?", [meta["last_row_id"], owner_id])
    return dict(row)
