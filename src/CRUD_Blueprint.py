"""
Lightweight compatibility shim that exposes the CRUD functions expected
by `front-end.py`.

This module tries to import real implementations from other modules
(`CRUD-Books.py`, `CRUD-Borrowers.py`, etc.). If a function is missing
it provides a clear NotImplementedError so the app fails with a
helpful message instead of ModuleNotFoundError.

Edit or replace this file with concrete implementations when you
complete the backend modules.
"""
from typing import Any
import pandas as pd
from sqlalchemy import text


def _not_implemented(name: str):
    def _fn(*args, **kwargs):
        raise NotImplementedError(
            f"Function '{name}' is not implemented.\n"
            "Please implement it in the appropriate module (e.g. CRUD-Books.py)"
        )

    return _fn


# ---- get_engine helper ----------------------------------------------------
def get_engine():
    """Return a SQLAlchemy Engine.

    Tries to import an `engine` or `get_engine()` from `src/sql_to_python.py`.
    If not found, raises RuntimeError with instructions to configure it.
    """
    # Prefer an explicit db_connection module (safer: doesn't execute heavy import-time code)
    try:
        from .db_connection import get_engine as _get_engine

        return _get_engine()
    except Exception:
        # Fallback to sql_to_python (legacy) which may define `get_engine` or `engine`.
        try:
            from .sql_to_python import get_engine as _get_engine

            return _get_engine()
        except Exception:
            try:
                from .sql_to_python import engine as _engine

                if _engine is None:
                    raise RuntimeError(
                        "Engine found in src/sql_to_python, but it's None. "
                        "Please configure and export an engine instance or implement get_engine()."
                    )
                return _engine
            except Exception:
                raise RuntimeError(
                    "Database engine is not configured.\n"
                    "Create `src/db_connection.py` or implement `get_engine()` in `src/sql_to_python.py`."
                )


# ---- Books CRUD implementations (adapted from notebook) ------------
def create_book(title, author, isbn=None, genre=None, cost=None):
    query = text(
        """
        INSERT INTO books (title, author, ISBN, cost_book, book_status)
        VALUES (:title, :author, :isbn, :cost, :status)
        """
    )
    with get_engine().connect() as conn:
        transaction = conn.begin()
        try:
            conn.execute(
                query,
                {"title": title, "author": author, "isbn": isbn, "cost": cost, "status": "available"},
            )
            transaction.commit()
            return f"Added book '{title}' by {author}."
        except Exception:
            transaction.rollback()
            raise


def get_books(title=None, author=None, genre=None, status=None, limit=100):
    query = "SELECT * FROM books WHERE 1=1"
    params = {}
    if title:
        query += " AND title LIKE :title"
        params["title"] = f"%{title}%"
    if author:
        query += " AND author LIKE :author"
        params["author"] = f"%{author}%"
    if genre:
        query += " AND genre = :genre"
        params["genre"] = genre
    if status:
        query += " AND book_status = :status"
        params["status"] = status
    query += " LIMIT :limit"
    params["limit"] = int(limit)

    with get_engine().connect() as conn:
        result = conn.execute(text(query), params)
        try:
            rows = result.mappings().all()
            df = pd.DataFrame(rows)
        except Exception:
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df


def get_book_by_id(book_id):
    query = text("SELECT * FROM books WHERE book_id = :book_id")
    with get_engine().connect() as conn:
        result = conn.execute(query, {"book_id": book_id})
        row = result.mappings().fetchone()
        return dict(row) if row else None


def update_book_details(book_id, title=None, author=None, isbn=None, genre=None, cost=None):
    set_clauses = []
    params = {"book_id": book_id}
    if title is not None:
        set_clauses.append("title = :title")
        params["title"] = title
    if author is not None:
        set_clauses.append("author = :author")
        params["author"] = author
    if isbn is not None:
        set_clauses.append("ISBN = :isbn")
        params["isbn"] = isbn
    if genre is not None:
        set_clauses.append("genre = :genre")
        params["genre"] = genre
    if cost is not None:
        set_clauses.append("cost_book = :cost")
        params["cost"] = cost
    if not set_clauses:
        raise ValueError("No fields to update.")
    set_clause = ", ".join(set_clauses)
    query = text(f"UPDATE books SET {set_clause} WHERE book_id = :book_id")
    with get_engine().connect() as conn:
        transaction = conn.begin()
        try:
            conn.execute(query, params)
            transaction.commit()
            return f"Updated book id {book_id}."
        except Exception:
            transaction.rollback()
            raise


def update_book_status(book_id, new_status):
    allowed = {"available", "borrowed", "overdue", "removed"}
    if new_status not in allowed:
        raise ValueError(f"Invalid status '{new_status}'. Allowed: {allowed}")
    query = text("UPDATE books SET book_status = :new_status WHERE book_id = :book_id")
    with get_engine().connect() as conn:
        transaction = conn.begin()
        try:
            conn.execute(query, {"new_status": new_status, "book_id": book_id})
            transaction.commit()
            return f"Updated status of book id {book_id} to '{new_status}'."
        except Exception:
            transaction.rollback()
            raise


def delete_book(book_id):
    query_status = text("SELECT book_status FROM books WHERE book_id = :book_id")
    query_update = text("UPDATE books SET book_status = 'removed' WHERE book_id = :book_id")
    with get_engine().connect() as conn:
        transaction = conn.begin()
        try:
            result = conn.execute(query_status, {"book_id": book_id})
            row = result.mappings().fetchone()
            if row is None:
                raise ValueError(f"Book id {book_id} not found.")
            current_status = row.get("book_status")
            if current_status == "borrowed":
                raise ValueError(f"Cannot delete book id {book_id} as it is currently BORROWED.")
            conn.execute(query_update, {"book_id": book_id})
            transaction.commit()
            return f"Book id {book_id} marked as REMOVED."
        except Exception:
            transaction.rollback()
            raise


# ---- Borrowers CRUD (basic implementations) ---------------------------
def create_borrower(name, email=None, phone=None, address=None):
    if not name:
        raise ValueError("Name is required for a borrower.")
    insert_sql = text(
        "INSERT INTO borrowers (name, email, phone, address) VALUES (:name, :email, :phone, :address)"
    )
    with get_engine().connect() as conn:
        with conn.begin() as trans:
            res = conn.execute(insert_sql, {"name": name, "email": email, "phone": phone, "address": address})
            try:
                borrower_id = res.inserted_primary_key[0]
            except Exception:
                borrower_id = getattr(res, "lastrowid", None)
    return borrower_id


def get_borrower_by_id(borrower_id):
    sql = text("SELECT * FROM borrowers WHERE borrower_id = :id")
    with get_engine().connect() as conn:
        row = conn.execute(sql, {"id": borrower_id}).mappings().fetchone()
        return dict(row) if row else None


def get_borrowers(name=None, status=None, limit=100):
    query = "SELECT * FROM borrowers WHERE 1=1"
    params = {}
    if name:
        query += " AND name LIKE :name"
        params["name"] = f"%{name}%"
    if status:
        query += " AND status = :status"
        params["status"] = status
    query += " LIMIT :limit"
    params["limit"] = int(limit)
    with get_engine().connect() as conn:
        rows = conn.execute(text(query), params).mappings().all()
        return [dict(r) for r in rows]


# ---- Simple dashboard stats -------------------------------------------
def get_dashboard_stats():
    stats = {
        "total_books": 0,
        "available_books": 0,
        "borrowed_books": 0,
        "active_loans": 0,
        "overdue_loans": 0,
        "total_borrowers": 0,
    }
    try:
        with get_engine().connect() as conn:
            books = conn.execute(text("SELECT COUNT(*) AS c FROM books")).fetchone()[0]
            borrowers = conn.execute(text("SELECT COUNT(*) AS c FROM borrowers")).fetchone()[0]
            borrowed = conn.execute(text("SELECT COUNT(*) AS c FROM books WHERE book_status = 'borrowed'")).fetchone()[0]
            overdue = conn.execute(text("SELECT COUNT(*) AS c FROM books WHERE book_status = 'overdue'")).fetchone()[0]
            stats.update(
                {
                    "total_books": int(books or 0),
                    "total_borrowers": int(borrowers or 0),
                    "borrowed_books": int(borrowed or 0),
                    "overdue_loans": int(overdue or 0),
                    "available_books": int((books or 0) - (borrowed or 0)),
                    "active_loans": int(borrowed or 0),
                }
            )
    except Exception:
        # return defaults
        pass
    return stats


# ---- Loans (empréstimos) -------------------------------------------------
def create_loan(book_id: int, borrower_id: int, loan_date: str, due_date: str):
    """Create a loan if the book is available and the borrower is active.

    loan_date and due_date should be strings in 'YYYY-MM-DD' format or
    any value accepted by your DB (we pass them as-is to the DB).
    Returns the new loan_id.
    """
    with get_engine().connect() as conn:
        trans = conn.begin()
        try:
            # Check book availability
            r = conn.execute(text("SELECT book_status FROM books WHERE book_id = :id"), {"id": book_id})
            row = r.mappings().fetchone()
            if not row:
                raise ValueError(f"Book id {book_id} not found")
            if row.get("book_status") != "available":
                raise ValueError(f"Book id {book_id} is not available")

            # Optionally check borrower status if column exists
            try:
                rb = conn.execute(text("SELECT status FROM borrowers WHERE borrower_id = :id"), {"id": borrower_id})
                brow = rb.mappings().fetchone()
                if brow and brow.get("status") and brow.get("status") != "ACTIVE":
                    raise ValueError(f"Borrower id {borrower_id} is not ACTIVE")
            except Exception:
                # If borrowers.status does not exist, ignore the check
                pass

            insert_sql = text(
                "INSERT INTO loans (book_id, borrower_id, loan_date, due_date, status) VALUES (:book_id, :borrower_id, :loan_date, :due_date, 'OPEN')"
            )
            res = conn.execute(insert_sql, {"book_id": book_id, "borrower_id": borrower_id, "loan_date": loan_date, "due_date": due_date})
            try:
                loan_id = res.inserted_primary_key[0]
            except Exception:
                loan_id = getattr(res, "lastrowid", None)

            # Update book status
            conn.execute(text("UPDATE books SET book_status = 'borrowed' WHERE book_id = :id"), {"id": book_id})

            trans.commit()
            return loan_id
        except Exception:
            trans.rollback()
            raise


def get_active_loans():
    """Return list of active loans (status = 'OPEN')."""
    with get_engine().connect() as conn:
        rows = conn.execute(text("SELECT * FROM loans WHERE status = 'OPEN'"))
        try:
            data = [dict(r) for r in rows.mappings().all()]
        except Exception:
            data = [dict(r) for r in rows.fetchall()]
    return data


def get_overdue_loans(reference_date: str = None):
    """Return loans where due_date < reference_date and status = 'OPEN'.

    If reference_date is None the DB's CURRENT_DATE will be used.
    """
    if reference_date:
        q = text("SELECT * FROM loans WHERE status = 'OPEN' AND due_date < :ref")
        params = {"ref": reference_date}
    else:
        q = text("SELECT * FROM loans WHERE status = 'OPEN' AND due_date < CURRENT_DATE()")
        params = {}

    with get_engine().connect() as conn:
        rows = conn.execute(q, params)
        try:
            data = [dict(r) for r in rows.mappings().all()]
        except Exception:
            data = [dict(r) for r in rows.fetchall()]
    return data


def get_loan_history_for_borrower(borrower_id: int):
    """Return all loans for a borrower as list of dicts."""
    q = text("SELECT * FROM loans WHERE borrower_id = :id ORDER BY loan_date DESC")
    with get_engine().connect() as conn:
        rows = conn.execute(q, {"id": borrower_id}).mappings().all()
        return [dict(r) for r in rows]


def get_loan_history_for_book(book_id: int):
    """Return all loans for a book as list of dicts."""
    q = text("SELECT * FROM loans WHERE book_id = :id ORDER BY loan_date DESC")
    with get_engine().connect() as conn:
        rows = conn.execute(q, {"id": book_id}).mappings().all()
        return [dict(r) for r in rows]


def process_return(loan_id: int, return_date: str = None):
    """Mark a loan as returned and set book back to available.

    If return_date is None the DB will use CURRENT_DATE().
    """
    with get_engine().connect() as conn:
        trans = conn.begin()
        try:
            # fetch loan
            r = conn.execute(text("SELECT * FROM loans WHERE loan_id = :id"), {"id": loan_id})
            loan = r.mappings().fetchone()
            if not loan:
                raise ValueError(f"Loan id {loan_id} not found")
            if loan.get("status") != "OPEN":
                raise ValueError(f"Loan id {loan_id} is not OPEN and cannot be returned")

            # update loan
            if return_date:
                conn.execute(text("UPDATE loans SET return_date = :rd, status = 'RETURNED' WHERE loan_id = :id"), {"rd": return_date, "id": loan_id})
            else:
                conn.execute(text("UPDATE loans SET return_date = CURRENT_DATE(), status = 'RETURNED' WHERE loan_id = :id"), {"id": loan_id})

            # set book available
            book_id = loan.get("book_id")
            conn.execute(text("UPDATE books SET book_status = 'available' WHERE book_id = :id"), {"id": book_id})

            trans.commit()
            return True
        except Exception:
            trans.rollback()
            raise


# --- Borrower helpers ----------------------------------------------------
def update_borrower_contact(borrower_id: int, contact_fields: dict):
    """Update borrower contact fields provided in `contact_fields` dict.

    Allowed keys: `name`, `email`, `phone`, `address`, `status`.
    Returns a confirmation string.
    """
    allowed = {"name", "email", "phone", "address", "status"}
    keys = [k for k in contact_fields.keys() if k in allowed]
    if not keys:
        raise ValueError("No valid contact fields provided to update.")
    set_clauses = []
    params = {"borrower_id": borrower_id}
    for k in keys:
        set_clauses.append(f"{k} = :{k}")
        params[k] = contact_fields[k]

    set_clause = ", ".join(set_clauses)
    query = text(f"UPDATE borrowers SET {set_clause} WHERE borrower_id = :borrower_id")
    with get_engine().connect() as conn:
        trans = conn.begin()
        try:
            conn.execute(query, params)
            trans.commit()
            return f"Updated borrower id {borrower_id}."
        except Exception:
            trans.rollback()
            raise


def set_borrower_status(borrower_id: int, new_status: str):
    """Set borrower's status (e.g., 'ACTIVE','INACTIVE','removed')."""
    query = text("UPDATE borrowers SET status = :status WHERE borrower_id = :id")
    with get_engine().connect() as conn:
        with conn.begin():
            conn.execute(query, {"status": new_status, "id": borrower_id})
    return f"Borrower id {borrower_id} status set to {new_status}."


def delete_borrower(borrower_id: int):
    """Soft-delete borrower by setting status to 'removed'.

    Prevent deletion if borrower has active OPEN loans.
    """
    with get_engine().connect() as conn:
        # Check active loans
        cnt = conn.execute(text("SELECT COUNT(*) FROM loans WHERE borrower_id = :id AND status = 'OPEN'"), {"id": borrower_id}).fetchone()[0]
        if cnt and int(cnt) > 0:
            raise ValueError(f"Borrower id {borrower_id} has active loans and cannot be removed.")
        with conn.begin():
            conn.execute(text("UPDATE borrowers SET status = 'removed' WHERE borrower_id = :id"), {"id": borrower_id})
    return f"Borrower id {borrower_id} marked as removed."


# --- Aliases + reports --------------------------------------------------
# Provide function names expected by `front-end.py` as thin aliases to implemented functions.
get_loan_history_by_book = get_loan_history_for_book
get_loan_history_by_borrower = get_loan_history_for_borrower


def get_most_borrowed_books(limit: int = 10):
    q = text(
        "SELECT b.book_id, b.title, COUNT(l.loan_id) AS times_borrowed "
        "FROM loans l JOIN books b ON l.book_id = b.book_id "
        "GROUP BY b.book_id, b.title ORDER BY times_borrowed DESC LIMIT :limit"
    )
    with get_engine().connect() as conn:
        rows = conn.execute(q, {"limit": int(limit)}).mappings().all()
        return [dict(r) for r in rows]


def get_most_active_borrowers(limit: int = 10):
    q = text(
        "SELECT br.borrower_id, br.name, COUNT(l.loan_id) AS loans_count "
        "FROM loans l JOIN borrowers br ON l.borrower_id = br.borrower_id "
        "GROUP BY br.borrower_id, br.name ORDER BY loans_count DESC LIMIT :limit"
    )
    with get_engine().connect() as conn:
        rows = conn.execute(q, {"limit": int(limit)}).mappings().all()
        return [dict(r) for r in rows]

