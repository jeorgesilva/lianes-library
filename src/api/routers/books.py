from fastapi import APIRouter

from ...schemas.book import BookCreate, BookOut

router = APIRouter()


@router.get("/", response_model=list[BookOut])
def list_books():
    return []


@router.post("/", response_model=BookOut, status_code=201)
def create_book(payload: BookCreate):
    return BookOut(id=1, **payload.model_dump())
