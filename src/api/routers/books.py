from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from pydantic import BaseModel
from src.db.crud import books as crud_books

router = APIRouter(prefix="/books", tags=["Books"])

# --- Pydantic Schemas (Ideais para validação de entrada) ---
class BookCreate(BaseModel):
    title: str
    author: str
    isbn: Optional[str] = None
    cost: Optional[float] = None
    cover_url: Optional[str] = None

class BookStatusUpdate(BaseModel):
    status: str

# --- Endpoints ---

@router.post("/", status_code=201)
def create_book(book: BookCreate):
    """Adiciona um novo livro ao catálogo."""
    try:
        return crud_books.create_book(
            title=book.title, author=book.author,
            isbn=book.isbn, cost=book.cost, cover_url=book.cover_url
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/")
def list_books(
    title: Optional[str] = None, 
    author: Optional[str] = None, 
    status: Optional[str] = None, 
    limit: int = Query(100, le=500)
):
    """Lista livros com filtros opcionais."""
    return crud_books.get_books(title=title, author=author, status=status, limit=limit)

@router.get("/{book_id}")
def get_book(book_id: int):
    """Busca os detalhes de um livro específico."""
    book = crud_books.get_book_by_id(book_id)
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    return book

@router.patch("/{book_id}/status")
def update_status(book_id: int, payload: BookStatusUpdate):
    """Atualiza o status de um livro (ex: AVAILABLE, LOST)."""
    try:
        return crud_books.update_book_status(book_id, payload.status)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/{book_id}")
def delete_book(book_id: int):
    """Remove um livro (soft delete) se ele não estiver emprestado."""
    try:
        message = crud_books.delete_book(book_id)
        return {"detail": message}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))