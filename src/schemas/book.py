from pydantic import BaseModel


class BookCreate(BaseModel):
    title: str
    author: str | None = None
    isbn: str | None = None
    publisher: str | None = None


class BookOut(BookCreate):
    id: int
