from fastapi import APIRouter, Depends, HTTPException

from src.schemas.user import UserRegister, UserLogin, UserOut, TokenResponse
from src.db.crud import users as crud_users
from src.core.security import hash_password, verify_password, create_access_token
from src.api.deps import get_current_user_id

router = APIRouter(prefix="/auth", tags=["Auth"])


@router.post("/register", response_model=TokenResponse, status_code=201)
def register(payload: UserRegister):
    """Cria uma nova conta. Cada conta enxerga apenas seus próprios dados."""
    email = payload.email.lower()
    if crud_users.get_user_by_email(email):
        raise HTTPException(status_code=409, detail="Já existe uma conta com este e-mail.")

    user = crud_users.create_user(
        first_name=payload.first_name,
        last_name=payload.last_name,
        email=email,
        password_hash=hash_password(payload.password),
    )
    token = create_access_token(user["user_id"])
    return TokenResponse(access_token=token, user=UserOut(**user))


@router.post("/login", response_model=TokenResponse)
def login(payload: UserLogin):
    user = crud_users.get_user_by_email(payload.email.lower())
    if not user or not verify_password(payload.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="E-mail ou senha inválidos.")

    token = create_access_token(user["user_id"])
    return TokenResponse(access_token=token, user=UserOut(**user))


@router.get("/me", response_model=UserOut)
def me(user_id: int = Depends(get_current_user_id)):
    user = crud_users.get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return UserOut(**user)
