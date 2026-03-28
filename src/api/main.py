from fastapi import FastAPI
from src.api.routers import books, loans, borrowers, search 

app = FastAPI(
    title="Liane's Smart Library API",
    description="API for managing books, loans, and AI-powered semantic search.",
    version="2.0.0"
)

app.include_router(books.router)
app.include_router(loans.router)
app.include_router(borrowers.router)
app.include_router(search.router) 

@app.get("/")
def read_root():
    return {"message": "Welcome to Liane's Smart Library API! Access /docs for the Swagger UI."}