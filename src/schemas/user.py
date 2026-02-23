from pydantic import BaseModel


class UserCreate(BaseModel):
    first_name: str
    last_name: str | None = None
    email: str | None = None


class UserOut(UserCreate):
    id: int
