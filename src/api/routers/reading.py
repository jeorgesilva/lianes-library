from fastapi import APIRouter, Depends, HTTPException
from typing import Optional
from pydantic import BaseModel
from src.db.crud import reading as crud_reading
from src.api.deps import get_current_user_id

router = APIRouter(prefix="/reading", tags=["Reading"])


# --- Pydantic Schemas ---
class ReadingLogCreate(BaseModel):
    title: Optional[str] = None
    author: Optional[str] = None
    book_id: Optional[int] = None
    cover_url: Optional[str] = None
    total_pages: Optional[int] = None
    status: str = "WANT_TO_READ"


class StatusUpdate(BaseModel):
    status: str
    rating: Optional[int] = None


class ProgressUpdate(BaseModel):
    current_page: int
    total_pages: Optional[int] = None


class JournalEntryCreate(BaseModel):
    content: str
    page_at_entry: Optional[int] = None
    mood: Optional[str] = None
    contains_spoilers: bool = False


# --- Endpoints ---
@router.get("/")
def list_reading_log(status: Optional[str] = None, owner_id: int = Depends(get_current_user_id)):
    """Lista os itens da fila de leitura, opcionalmente filtrando por status."""
    return crud_reading.list_reading_log(owner_id, status=status)


@router.post("/", status_code=201)
def create_reading_log(payload: ReadingLogCreate, owner_id: int = Depends(get_current_user_id)):
    """Adiciona um livro à fila de leitura (Quero Ler / Lendo / Lido)."""
    if not payload.book_id and not payload.title:
        raise HTTPException(status_code=400, detail="Either book_id or title is required.")
    try:
        return crud_reading.create_reading_log(
            owner_id=owner_id,
            title=payload.title or "",
            author=payload.author,
            book_id=payload.book_id,
            cover_url=payload.cover_url,
            total_pages=payload.total_pages,
            status=payload.status,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{reading_log_id}")
def get_reading_log(reading_log_id: int, owner_id: int = Depends(get_current_user_id)):
    """Busca um item específico da fila de leitura."""
    log = crud_reading.get_reading_log(owner_id, reading_log_id)
    if not log:
        raise HTTPException(status_code=404, detail="Reading log entry not found")
    return log


@router.patch("/{reading_log_id}/status")
def update_status(reading_log_id: int, payload: StatusUpdate, owner_id: int = Depends(get_current_user_id)):
    """Move o item entre Quero Ler / Lendo / Lido / Abandonei, ajustando datas automaticamente."""
    try:
        return crud_reading.update_status(owner_id, reading_log_id, payload.status, payload.rating)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.patch("/{reading_log_id}/progress")
def update_progress(reading_log_id: int, payload: ProgressUpdate, owner_id: int = Depends(get_current_user_id)):
    """Atualiza a página atual (e opcionalmente o total de páginas) de um livro em leitura."""
    try:
        return crud_reading.update_progress(owner_id, reading_log_id, payload.current_page, payload.total_pages)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/{reading_log_id}")
def delete_reading_log(reading_log_id: int, owner_id: int = Depends(get_current_user_id)):
    """Remove um item da fila de leitura (e seu diário)."""
    try:
        crud_reading.delete_reading_log(owner_id, reading_log_id)
        return {"detail": "Reading log entry removed."}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{reading_log_id}/entries")
def list_entries(reading_log_id: int, owner_id: int = Depends(get_current_user_id)):
    """Lista as entradas do diário de um livro, mais recentes primeiro."""
    return crud_reading.list_journal_entries(owner_id, reading_log_id)


@router.post("/{reading_log_id}/entries", status_code=201)
def create_entry(reading_log_id: int, payload: JournalEntryCreate, owner_id: int = Depends(get_current_user_id)):
    """Cria uma nova entrada de diário para um livro."""
    try:
        return crud_reading.create_journal_entry(
            owner_id=owner_id,
            reading_log_id=reading_log_id,
            content=payload.content,
            page_at_entry=payload.page_at_entry,
            mood=payload.mood,
            contains_spoilers=payload.contains_spoilers,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
