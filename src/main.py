from fastapi import FastAPI

from .api.routers import authors, books, loans, users

app = FastAPI(
    title="Liane's Library API",
    version="0.1.0",
)


@app.get("/health")
def healthcheck():
    return {"status": "ok"}


app.include_router(books.router, prefix="/books", tags=["books"])
app.include_router(authors.router, prefix="/authors", tags=["authors"])
app.include_router(loans.router, prefix="/loans", tags=["loans"])
app.include_router(users.router, prefix="/users", tags=["users"])
