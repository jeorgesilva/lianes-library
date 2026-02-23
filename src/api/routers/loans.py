from fastapi import APIRouter

from ...schemas.loan import LoanCreate, LoanOut

router = APIRouter()


@router.get("/", response_model=list[LoanOut])
def list_loans():
    return []


@router.post("/", response_model=LoanOut, status_code=201)
def create_loan(payload: LoanCreate):
    return LoanOut(id=1, **payload.model_dump())
