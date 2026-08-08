from typing import Optional, List, Dict, Any
from src.db.d1_client import d1_query, d1_one

def create_book(owner_id: int, title: str, author: str, isbn: str = None, cost: float = None, cover_url: str = None) -> Dict[str, Any]:
    """Insere um novo livro no catálogo do usuário."""
    row = d1_one(
        """
        INSERT INTO books (title, author, ISBN, cost_book, book_status, cover_url, owner_id)
        VALUES (?, ?, ?, ?, 'AVAILABLE', ?, ?)
        RETURNING *
        """,
        [title, author, isbn, cost, cover_url, owner_id],
    )
    return dict(row)

def get_books(owner_id: int, title: str = None, author: str = None, status: str = None, limit: int = 100) -> List[Dict[str, Any]]:
    """Busca livros do usuário usando filtros opcionais."""
    query = "SELECT * FROM books WHERE owner_id = ?"
    params: List[Any] = [owner_id]

    if title:
        query += " AND title LIKE ?"
        params.append(f"%{title}%")
    if author:
        query += " AND author LIKE ?"
        params.append(f"%{author}%")
    if status:
        query += " AND book_status = ?"
        params.append(status)

    query += " LIMIT ?"
    params.append(limit)

    return [dict(r) for r in d1_query(query, params)]

def get_book_by_id(owner_id: int, book_id: int) -> Optional[Dict[str, Any]]:
    """Busca um único livro do usuário pelo seu ID."""
    row = d1_one("SELECT * FROM books WHERE book_id = ? AND owner_id = ?", [book_id, owner_id])
    return dict(row) if row else None

def update_book_status(owner_id: int, book_id: int, new_status: str) -> Dict[str, Any]:
    """Atualiza o status de um livro, garantindo regras de negócio."""
    allowed_statuses = {'AVAILABLE', 'BORROWED', 'LOST', 'DAMAGED'}
    if new_status.upper() not in allowed_statuses:
        raise ValueError(f"Invalid status '{new_status}'. Allowed statuses: {allowed_statuses}")

    row = d1_one(
        "UPDATE books SET book_status = ? WHERE book_id = ? AND owner_id = ? RETURNING *",
        [new_status.upper(), book_id, owner_id],
    )
    if row is None:
        raise ValueError(f"Book {book_id} not found.")
    return dict(row)

def delete_book(owner_id: int, book_id: int) -> str:
    """Impede a exclusão (ou marca como LOST) se o livro estiver emprestado."""
    book = get_book_by_id(owner_id, book_id)
    if not book:
        raise ValueError("Book not found.")

    if book['book_status'] == 'BORROWED':
        raise ValueError("Cannot delete or remove a book that is currently BORROWED.")

    # Ao invés de deletar do banco, marcamos como LOST ou DAMAGED para manter histórico
    update_book_status(owner_id, book_id, 'LOST')
    return f"Book {book_id} marked as LOST (soft delete)."
