from fastapi import APIRouter

from ...schemas.author import AuthorCreate, AuthorOut

router = APIRouter()


@router.get("/", response_model=list[AuthorOut])
def list_authors():
    return []


@router.post("/", response_model=AuthorOut, status_code=201)
def create_author(payload: AuthorCreate):
    return AuthorOut(id=1, **payload.model_dump())
