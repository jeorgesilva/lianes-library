from fastapi import APIRouter

from ...schemas.user import UserCreate, UserOut

router = APIRouter()


@router.get("/", response_model=list[UserOut])
def list_users():
    return []


@router.post("/", response_model=UserOut, status_code=201)
def create_user(payload: UserCreate):
    return UserOut(id=1, **payload.model_dump())
