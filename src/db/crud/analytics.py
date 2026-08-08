from typing import Any, Dict, List
from src.db.d1_client import d1_query, d1_one

def get_totals(owner_id: int) -> Dict[str, Any]:
    """Contadores gerais: acervo, mutuários, empréstimos ativos e atrasados."""
    books = d1_one("SELECT COUNT(*) as c FROM books WHERE book_status != 'LOST' AND owner_id = ?", [owner_id])
    borrowers = d1_one("SELECT COUNT(*) as c FROM borrowers WHERE status = 'ACTIVE' AND owner_id = ?", [owner_id])
    active_loans = d1_one("SELECT COUNT(*) as c FROM transactions WHERE actual_return_date IS NULL AND owner_id = ?", [owner_id])
    overdue_now = d1_one(
        "SELECT COUNT(*) as c FROM transactions WHERE actual_return_date IS NULL AND date(due_date) < date('now') AND owner_id = ?",
        [owner_id],
    )
    return {
        "books": books["c"],
        "borrowers": borrowers["c"],
        "active_loans": active_loans["c"],
        "overdue_now": overdue_now["c"],
    }

def get_loans_per_month(owner_id: int, months: int = 12) -> List[Dict[str, Any]]:
    """Quantidade de empréstimos iniciados por mês, últimos N meses."""
    rows = d1_query(
        """
        SELECT strftime('%Y-%m', loan_date) as month, COUNT(*) as count
        FROM transactions
        WHERE date(loan_date) >= date('now', ?) AND owner_id = ?
        GROUP BY month
        ORDER BY month ASC
        """,
        [f"-{months} months", owner_id],
    )
    return [dict(r) for r in rows]

def get_top_books(owner_id: int, limit: int = 10) -> List[Dict[str, Any]]:
    """Livros mais emprestados (por número de transações)."""
    rows = d1_query(
        """
        SELECT b.book_id, b.title, b.author, COUNT(*) as loan_count
        FROM transactions t
        JOIN books b ON b.book_id = t.book_id
        WHERE t.owner_id = ?
        GROUP BY t.book_id
        ORDER BY loan_count DESC, b.title ASC
        LIMIT ?
        """,
        [owner_id, limit],
    )
    return [dict(r) for r in rows]

def get_late_return_rate_per_month(owner_id: int, months: int = 12) -> List[Dict[str, Any]]:
    """Para empréstimos já devolvidos, % que voltou atrasado, por mês do empréstimo."""
    rows = d1_query(
        """
        SELECT
            strftime('%Y-%m', loan_date) as month,
            COUNT(*) as total,
            SUM(CASE WHEN date(actual_return_date) > date(due_date) THEN 1 ELSE 0 END) as late
        FROM transactions
        WHERE actual_return_date IS NOT NULL
          AND date(loan_date) >= date('now', ?)
          AND owner_id = ?
        GROUP BY month
        ORDER BY month ASC
        """,
        [f"-{months} months", owner_id],
    )
    result = []
    for r in rows:
        total = r["total"] or 0
        late = r["late"] or 0
        result.append({
            "month": r["month"],
            "total": total,
            "late": late,
            "late_pct": round(100 * late / total, 1) if total else 0.0,
        })
    return result
