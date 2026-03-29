from pydantic import BaseModel

class BookCreate(BaseModel):
    title: str
    author: str | None = None
    isbn: str | None = None
    publisher: str | None = None
    cover_url: str | None = None  # 👈 Adicione esta linha aqui

class BookOut(BookCreate):
    id: int
    # Como BookOut herda de BookCreate, ele já "ganha" o cover_url automaticamente!