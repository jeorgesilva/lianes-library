from pydantic import BaseModel


class AuthorCreate(BaseModel):
    name: str


class AuthorOut(AuthorCreate):
    id: int
