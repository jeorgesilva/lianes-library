from fastapi import APIRouter, Depends, HTTPException
from typing import Optional
from pydantic import BaseModel
from datetime import date
from src.db.crud import borrowed as crud_borrowed
from src.api.deps import get_current_user_id

router = APIRouter(prefix="/borrowed", tags=["Borrowed by Me"])


# --- Pydantic Schemas ---
class BorrowRecordCreate(BaseModel):
    title: str
    lender_name: str
    author: Optional[str] = None
    isbn: Optional[str] = None
    cover_url: Optional[str] = None
    borrowed_date: Optional[date] = None
    due_date: Optional[date] = None
    reminder_lead_days: int = 3
    notes: Optional[str] = None


class BorrowRecordUpdate(BaseModel):
    due_date: Optional[date] = None
    lender_name: Optional[str] = None
    reminder_lead_days: Optional[int] = None
    notes: Optional[str] = None


class BorrowRecordReturn(BaseModel):
    returned_date: Optional[date] = None


# --- Endpoints ---
@router.get("/")
def list_borrow_records(status: Optional[str] = None, owner_id: int = Depends(get_current_user_id)):
    """Lista os livros que Liane pegou emprestado de terceiros. status: active | returned."""
    return crud_borrowed.list_borrow_records(owner_id, status=status)


@router.post("/", status_code=201)
def create_borrow_record(payload: BorrowRecordCreate, owner_id: int = Depends(get_current_user_id)):
    """Registra um novo livro pego emprestado de terceiros (biblioteca, amigos)."""
    return crud_borrowed.create_borrow_record(
        owner_id=owner_id,
        title=payload.title,
        lender_name=payload.lender_name,
        author=payload.author,
        isbn=payload.isbn,
        cover_url=payload.cover_url,
        borrowed_date=payload.borrowed_date,
        due_date=payload.due_date,
        reminder_lead_days=payload.reminder_lead_days,
        notes=payload.notes,
    )


@router.get("/{borrow_id}")
def get_borrow_record(borrow_id: int, owner_id: int = Depends(get_current_user_id)):
    record = crud_borrowed.get_borrow_record(owner_id, borrow_id)
    if not record:
        raise HTTPException(status_code=404, detail="Borrow record not found")
    return record


@router.patch("/{borrow_id}")
def update_borrow_record(borrow_id: int, payload: BorrowRecordUpdate, owner_id: int = Depends(get_current_user_id)):
    try:
        return crud_borrowed.update_borrow_record(
            owner_id, borrow_id,
            due_date=payload.due_date,
            lender_name=payload.lender_name,
            reminder_lead_days=payload.reminder_lead_days,
            notes=payload.notes,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{borrow_id}/return")
def return_borrow_record(borrow_id: int, payload: BorrowRecordReturn, owner_id: int = Depends(get_current_user_id)):
    """Registra a devolução do livro para quem o emprestou a Liane."""
    try:
        return crud_borrowed.return_borrow_record(owner_id, borrow_id, payload.returned_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/{borrow_id}")
def delete_borrow_record(borrow_id: int, owner_id: int = Depends(get_current_user_id)):
    try:
        crud_borrowed.delete_borrow_record(owner_id, borrow_id)
        return {"detail": "Borrow record removed."}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
