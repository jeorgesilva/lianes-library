from fastapi import APIRouter, Depends, HTTPException
from typing import Optional
from pydantic import BaseModel
from datetime import date
from src.db.crud import loans as crud_loans
from src.api.deps import get_current_user_id

router = APIRouter(prefix="/loans", tags=["Loans"])

# --- Pydantic Schemas ---
class LoanCreate(BaseModel):
    book_id: int
    person_id: int
    loan_date: Optional[date] = None
    loan_period_days: int = 14

class LoanReturn(BaseModel):
    return_date: Optional[date] = None

# --- Endpoints ---

@router.post("/", status_code=201)
def checkout_book(loan: LoanCreate, owner_id: int = Depends(get_current_user_id)):
    """Cria um novo empréstimo de livro."""
    try:
        return crud_loans.create_loan(
            owner_id=owner_id,
            book_id=loan.book_id,
            person_id=loan.person_id,
            loan_date=loan.loan_date,
            loan_period_days=loan.loan_period_days
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/{transaction_id}/return")
def return_book(transaction_id: int, payload: LoanReturn, owner_id: int = Depends(get_current_user_id)):
    """Registra a devolução de um livro."""
    try:
        return crud_loans.process_return(
            owner_id=owner_id,
            transaction_id=transaction_id,
            return_date=payload.return_date
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/active")
def get_active_loans(owner_id: int = Depends(get_current_user_id)):
    """Lista todos os empréstimos que ainda não foram devolvidos."""
    return crud_loans.get_active_loans(owner_id)
