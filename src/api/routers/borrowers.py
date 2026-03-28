from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from pydantic import BaseModel
from src.db.crud import borrowers as crud_borrowers

router = APIRouter(prefix="/borrowers", tags=["Borrowers"])

# --- Pydantic Schemas ---
class BorrowerCreate(BaseModel):
    first_name: str
    last_name: str
    email: Optional[str] = None
    phone_number: Optional[str] = None
    relationship_type: Optional[str] = None
    address: Optional[str] = None

class BorrowerStatusUpdate(BaseModel):
    status: str

# --- Endpoints ---

@router.post("/", status_code=201)
def create_borrower(borrower: BorrowerCreate):
    """Cadastra um novo amigo/mutuário no sistema."""
    try:
        return crud_borrowers.create_borrower(
            first_name=borrower.first_name, 
            last_name=borrower.last_name, 
            email=borrower.email,
            phone_number=borrower.phone_number, 
            relationship_type=borrower.relationship_type, 
            address=borrower.address
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/")
def list_borrowers(
    first_name: Optional[str] = None, 
    status: Optional[str] = None, 
    limit: int = Query(100, le=500)
):
    """Lista todos os mutuários com filtros opcionais."""
    return crud_borrowers.get_borrowers(first_name=first_name, status=status, limit=limit)

@router.get("/{person_id}")
def get_borrower(person_id: int):
    """Busca os detalhes de um mutuário específico."""
    borrower = crud_borrowers.get_borrower_by_id(person_id)
    if not borrower:
        raise HTTPException(status_code=404, detail="Borrower not found")
    return borrower

@router.patch("/{person_id}/status")
def update_status(person_id: int, payload: BorrowerStatusUpdate):
    """Ativa ou inativa um mutuário."""
    try:
        return crud_borrowers.set_borrower_status(person_id, payload.status)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/{person_id}")
def delete_borrower(person_id: int):
    """Realiza o soft-delete (inativa) de um mutuário."""
    try:
        message = crud_borrowers.delete_borrower(person_id)
        return {"detail": message}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))