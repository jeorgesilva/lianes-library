from pydantic import BaseModel, EmailStr


class UserRegister(BaseModel):
    first_name: str
    last_name: str | None = None
    email: EmailStr
    password: str


class UserLogin(BaseModel):
    email: EmailStr
    password: str


class UserOut(BaseModel):
    user_id: int
    first_name: str
    last_name: str | None = None
    email: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserOut
