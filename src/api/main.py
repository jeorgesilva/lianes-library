from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.api.routers import books, loans, borrowers, search, chat, auth, analytics, reading, borrowed, notifications, wishlist

app = FastAPI(
    title="Liane's Smart Library API",
    description="API for managing books, loans, and AI-powered semantic search.",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(books.router)
app.include_router(loans.router)
app.include_router(borrowers.router)
app.include_router(search.router)
app.include_router(chat.router)
app.include_router(analytics.router)
app.include_router(reading.router)
app.include_router(borrowed.router)
app.include_router(notifications.router)
app.include_router(wishlist.router)

@app.get("/")
def read_root():
    return {"message": "Welcome to Liane's Smart Library API! Access /docs for the Swagger UI."}