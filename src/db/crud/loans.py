from sqlalchemy import text
from typing import Optional, List, Dict, Any
from datetime import date, timedelta
from src.db.connection import get_engine

def create_loan(book_id: int, person_id: int, loan_date: Optional[date] = None, 
                loan_period_days: int = 14) -> Dict[str, Any]:
    """Cria um novo empréstimo. Valida se o livro está disponível e o usuário ativo."""
    engine = get_engine()
    
    loan_date = loan_date or date.today()
    due_date = loan_date + timedelta(days=loan_period_days)
    
    check_book_sql = text("SELECT book_id, title, book_status FROM books WHERE book_id = :book_id")
    check_borrower_sql = text("SELECT person_id, first_name, last_name, status FROM borrowers WHERE person_id = :person_id")
    insert_tx_sql = text("""
        INSERT INTO transactions (book_id, person_id, loan_date, due_date)
        VALUES (:book_id, :person_id, :loan_date, :due_date)
    """)
    update_book_sql = text("UPDATE books SET book_status = 'BORROWED' WHERE book_id = :book_id")
    
    with engine.begin() as conn:
        # 1. Validações
        book = conn.execute(check_book_sql, {"book_id": book_id}).mappings().one_or_none()
        if not book: raise ValueError(f"Book ID {book_id} does not exist.")
        if book['book_status'].upper() != 'AVAILABLE':
            raise ValueError(f"Book '{book['title']}' is not available (status: {book['book_status']}).")
            
        borrower = conn.execute(check_borrower_sql, {"person_id": person_id}).mappings().one_or_none()
        if not borrower: raise ValueError(f"Borrower ID {person_id} does not exist.")
        if borrower.get('status', 'ACTIVE') == 'INACTIVE':
            raise ValueError("Cannot loan to an inactive borrower.")
            
        # 2. Insere Transação
        result = conn.execute(insert_tx_sql, {
            "book_id": book_id, "person_id": person_id, 
            "loan_date": loan_date, "due_date": due_date
        })
        transaction_id = result.lastrowid
        
        # 3. Atualiza Livro
        conn.execute(update_book_sql, {"book_id": book_id})
    
    return {
        "transaction_id": transaction_id,
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
    engine = get_engine()
    return_date = return_date or date.today()
    
    get_tx_sql = text("""
        SELECT t.*, b.title as book_title, br.first_name, br.last_name
        FROM transactions t
        JOIN books b ON t.book_id = b.book_id
        JOIN borrowers br ON t.person_id = br.person_id
        WHERE t.transaction_id = :transaction_id
    """)
    
    with engine.begin() as conn:
        trans = conn.execute(get_tx_sql, {"transaction_id": transaction_id}).mappings().one_or_none()
        
        if not trans:
            raise ValueError(f"Transaction {transaction_id} does not exist.")
        if trans['actual_return_date'] is not None:
            raise ValueError(f"Transaction {transaction_id} already closed on {trans['actual_return_date']}.")
            
        conn.execute(
            text("UPDATE transactions SET actual_return_date = :return_date WHERE transaction_id = :transaction_id"),
            {"transaction_id": transaction_id, "return_date": return_date}
        )
        conn.execute(
            text("UPDATE books SET book_status = 'AVAILABLE' WHERE book_id = :book_id"),
            {"book_id": trans['book_id']}
        )
    
    is_late = return_date > trans['due_date']
    days_late = (return_date - trans['due_date']).days if is_late else 0
    
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
    engine = get_engine()
    query = text("""
        SELECT t.transaction_id, t.book_id, b.title as book_title, t.person_id,
               CONCAT(br.first_name, ' ', br.last_name) as borrower_name,
               t.loan_date, t.due_date, DATEDIFF(CURRENT_DATE, t.due_date) as days_overdue
        FROM transactions t
        JOIN books b ON t.book_id = b.book_id
        JOIN borrowers br ON t.person_id = br.person_id
        WHERE t.actual_return_date IS NULL
        ORDER BY t.due_date ASC
    """)
    
    with engine.connect() as conn:
        rows = conn.execute(query).mappings().all()
    return [dict(r) for r in rows]