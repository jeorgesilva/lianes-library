from sqlalchemy import text
from typing import Optional, List, Dict, Any
from src.db.connection import get_engine

def create_book(title: str, author: str, isbn: str = None, cost: float = None) -> Dict[str, Any]:
    """Insere um novo livro no catálogo."""
    query = text("""
        INSERT INTO books (title, author, ISBN, cost_book, book_status)
        VALUES (:title, :author, :isbn, :cost, :status)
    """)
    
    engine = get_engine()
    with engine.begin() as conn:
        result = conn.execute(query, {
            "title": title,
            "author": author,
            "isbn": isbn,
            "cost": cost,
            "status": "AVAILABLE"
        })
        book_id = result.lastrowid
        row = conn.execute(
            text("SELECT * FROM books WHERE book_id = :book_id"), 
            {"book_id": book_id}
        ).mappings().one()
        
    return dict(row)

def get_books(title: str = None, author: str = None, status: str = None, limit: int = 100) -> List[Dict[str, Any]]:
    """Busca livros usando filtros opcionais (agora retornando lista de dicts em vez de Pandas)."""
    query = "SELECT * FROM books WHERE 1=1"
    params = {}
    
    if title:
        query += " AND title LIKE :title"
        params["title"] = f"%{title}%"
    if author:
        query += " AND author LIKE :author"
        params["author"] = f"%{author}%"
    if status:
        query += " AND book_status = :status"
        params["status"] = status
        
    query += " LIMIT :limit"
    params["limit"] = limit
    
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(query), params).mappings().all()
        
    return [dict(r) for r in rows]

def get_book_by_id(book_id: int) -> Optional[Dict[str, Any]]:
    """Busca um único livro pelo seu ID."""
    query = text("SELECT * FROM books WHERE book_id = :book_id")
    engine = get_engine()
    
    with engine.connect() as conn:
        row = conn.execute(query, {"book_id": book_id}).mappings().one_or_none()
        
    return dict(row) if row else None

def update_book_status(book_id: int, new_status: str) -> Dict[str, Any]:
    """Atualiza o status de um livro, garantindo regras de negócio."""
    allowed_statuses = {'AVAILABLE', 'BORROWED', 'LOST', 'DAMAGED'}
    if new_status.upper() not in allowed_statuses:
        raise ValueError(f"Invalid status '{new_status}'. Allowed statuses: {allowed_statuses}")
        
    query = text("UPDATE books SET book_status = :new_status WHERE book_id = :book_id")
    engine = get_engine()
    
    with engine.begin() as conn:
        conn.execute(query, {"new_status": new_status.upper(), "book_id": book_id})
        row = conn.execute(
            text("SELECT * FROM books WHERE book_id = :book_id"), 
            {"book_id": book_id}
        ).mappings().one()
        
    return dict(row)

def delete_book(book_id: int) -> str:
    """Impede a exclusão (ou marca como LOST) se o livro estiver emprestado."""
    book = get_book_by_id(book_id)
    if not book:
        raise ValueError("Book not found.")
        
    if book['book_status'] == 'BORROWED':
        raise ValueError("Cannot delete or remove a book that is currently BORROWED.")
    
    # Ao invés de deletar do banco, marcamos como LOST ou DAMAGED para manter histórico
    update_book_status(book_id, 'LOST')
    return f"Book {book_id} marked as LOST (soft delete)."