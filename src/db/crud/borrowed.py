from typing import Optional, List, Dict, Any
from datetime import date
from src.db.d1_client import d1_query, d1_one, d1_exec


def create_borrow_record(
    owner_id: int,
    title: str,
    lender_name: str,
    author: Optional[str] = None,
    isbn: Optional[str] = None,
    cover_url: Optional[str] = None,
    borrowed_date: Optional[date] = None,
    due_date: Optional[date] = None,
    reminder_lead_days: int = 3,
    notes: Optional[str] = None,
) -> Dict[str, Any]:
    """Registra um livro que Liane pegou emprestado de terceiros."""
    borrowed_date = borrowed_date or date.today()

    meta = d1_exec(
        """
        INSERT INTO borrow_records
          (owner_id, title, author, isbn, cover_url, lender_name, borrowed_date, due_date, reminder_lead_days, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            owner_id, title, author, isbn, cover_url, lender_name,
            borrowed_date.isoformat(), due_date.isoformat() if due_date else None,
            reminder_lead_days, notes,
        ],
    )
    return get_borrow_record(owner_id, meta["last_row_id"])


def list_borrow_records(owner_id: int, status: Optional[str] = None) -> List[Dict[str, Any]]:
    """Lista os registros de livros pegos emprestado. status: 'active' | 'returned' | None (todos)."""
    where = "WHERE owner_id = ?"
    if status == "active":
        where += " AND returned_date IS NULL"
    elif status == "returned":
        where += " AND returned_date IS NOT NULL"

    rows = d1_query(
        f"""
        SELECT *,
               CASE WHEN due_date IS NOT NULL AND returned_date IS NULL
                    THEN CAST(julianday(due_date) - julianday('now') AS INTEGER)
                    ELSE NULL END as days_until_due
        FROM borrow_records
        {where}
        ORDER BY (returned_date IS NOT NULL), due_date ASC
        """,
        [owner_id],
    )
    return [dict(r) for r in rows]


def get_borrow_record(owner_id: int, borrow_id: int) -> Optional[Dict[str, Any]]:
    return d1_one(
        """
        SELECT *,
               CASE WHEN due_date IS NOT NULL AND returned_date IS NULL
                    THEN CAST(julianday(due_date) - julianday('now') AS INTEGER)
                    ELSE NULL END as days_until_due
        FROM borrow_records WHERE borrow_id = ? AND owner_id = ?
        """,
        [borrow_id, owner_id],
    )


def update_borrow_record(
    owner_id: int,
    borrow_id: int,
    due_date: Optional[date] = None,
    lender_name: Optional[str] = None,
    reminder_lead_days: Optional[int] = None,
    notes: Optional[str] = None,
) -> Dict[str, Any]:
    record = get_borrow_record(owner_id, borrow_id)
    if not record:
        raise ValueError(f"Borrow record {borrow_id} does not exist.")

    fields = {
        "due_date": due_date.isoformat() if due_date else None,
        "lender_name": lender_name,
        "reminder_lead_days": reminder_lead_days,
        "notes": notes,
    }
    updates = {k: v for k, v in fields.items() if v is not None}
    if not updates:
        return record

    set_clause = ", ".join(f"{k} = ?" for k in updates)
    d1_exec(
        f"UPDATE borrow_records SET {set_clause} WHERE borrow_id = ? AND owner_id = ?",
        [*updates.values(), borrow_id, owner_id],
    )
    return get_borrow_record(owner_id, borrow_id)


def return_borrow_record(owner_id: int, borrow_id: int, returned_date: Optional[date] = None) -> Dict[str, Any]:
    """Marca um livro emprestado como devolvido ao dono original."""
    returned_date = returned_date or date.today()

    record = get_borrow_record(owner_id, borrow_id)
    if not record:
        raise ValueError(f"Borrow record {borrow_id} does not exist.")
    if record["returned_date"] is not None:
        raise ValueError(f"Borrow record {borrow_id} was already returned on {record['returned_date']}.")

    d1_exec(
        "UPDATE borrow_records SET returned_date = ? WHERE borrow_id = ? AND owner_id = ?",
        [returned_date.isoformat(), borrow_id, owner_id],
    )
    return get_borrow_record(owner_id, borrow_id)


def delete_borrow_record(owner_id: int, borrow_id: int) -> None:
    record = get_borrow_record(owner_id, borrow_id)
    if not record:
        raise ValueError(f"Borrow record {borrow_id} does not exist.")
    d1_exec("DELETE FROM borrow_records WHERE borrow_id = ? AND owner_id = ?", [borrow_id, owner_id])
