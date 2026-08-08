from typing import Optional, List, Dict, Any
from datetime import date, timedelta
from src.db.d1_client import d1_query, d1_one, d1_batch

def create_loan(book_id: int, person_id: int, loan_date: Optional[date] = None,
                loan_period_days: int = 14) -> Dict[str, Any]:
    """Cria um novo empréstimo. Valida se o livro está disponível e o usuário ativo."""
    loan_date = loan_date or date.today()
    due_date = loan_date + timedelta(days=loan_period_days)

    book = d1_one("SELECT book_id, title, book_status FROM books WHERE book_id = ?", [book_id])
    if not book:
        raise ValueError(f"Book ID {book_id} does not exist.")
    if book['book_status'].upper() != 'AVAILABLE':
        raise ValueError(f"Book '{book['title']}' is not available (status: {book['book_status']}).")

    borrower = d1_one(
        "SELECT person_id, first_name, last_name, status FROM borrowers WHERE person_id = ?",
        [person_id],
    )
    if not borrower:
        raise ValueError(f"Borrower ID {person_id} does not exist.")
    if borrower.get('status', 'ACTIVE') == 'INACTIVE':
        raise ValueError("Cannot loan to an inactive borrower.")

    metas = d1_batch([
        (
            """
            INSERT INTO transactions (book_id, person_id, loan_date, due_date)
            VALUES (?, ?, ?, ?)
            """,
            [book_id, person_id, loan_date.isoformat(), due_date.isoformat()],
        ),
        ("UPDATE books SET book_status = 'BORROWED' WHERE book_id = ?", [book_id]),
    ])

    return {
        "transaction_id": metas[0]["last_row_id"],
        "book_id": book_id,
        "book_title": book['title'],
        "person_id": person_id,
        "borrower_name": f"{borrower['first_name']} {borrower['last_name']}",
        "loan_date": loan_date,
        "due_date": due_date,
        "status": "active"
    }

def process_return(transaction_id: int, return_date: Optional[date] = None) -> Dict[str, Any]:
    """Registra a devolução de um livro e o marca como AVAILABLE novamente."""
    return_date = return_date or date.today()

    trans = d1_one(
        """
        SELECT t.*, b.title as book_title, br.first_name, br.last_name
        FROM transactions t
        JOIN books b ON t.book_id = b.book_id
        JOIN borrowers br ON t.person_id = br.person_id
        WHERE t.transaction_id = ?
        """,
        [transaction_id],
    )

    if not trans:
        raise ValueError(f"Transaction {transaction_id} does not exist.")
    if trans['actual_return_date'] is not None:
        raise ValueError(f"Transaction {transaction_id} already closed on {trans['actual_return_date']}.")

    d1_batch([
        (
            "UPDATE transactions SET actual_return_date = ? WHERE transaction_id = ?",
            [return_date.isoformat(), transaction_id],
        ),
        ("UPDATE books SET book_status = 'AVAILABLE' WHERE book_id = ?", [trans['book_id']]),
    ])

    due_date = date.fromisoformat(trans['due_date'])
    is_late = return_date > due_date
    days_late = (return_date - due_date).days if is_late else 0

    return {
        "transaction_id": transaction_id,
        "book_id": trans['book_id'],
        "book_title": trans['book_title'],
        "return_date": return_date,
        "is_late": is_late,
        "days_late": days_late,
        "status": "returned"
    }

def get_active_loans() -> List[Dict[str, Any]]:
    """Retorna todos os empréstimos ativos (em aberto)."""
    rows = d1_query(
        """
        SELECT t.transaction_id, t.book_id, b.title as book_title, t.person_id,
               (br.first_name || ' ' || br.last_name) as borrower_name,
               t.loan_date, t.due_date,
               CAST(julianday('now') - julianday(t.due_date) AS INTEGER) as days_overdue
        FROM transactions t
        JOIN books b ON t.book_id = b.book_id
        JOIN borrowers br ON t.person_id = br.person_id
        WHERE t.actual_return_date IS NULL
        ORDER BY t.due_date ASC
        """
    )
    return [dict(r) for r in rows]
