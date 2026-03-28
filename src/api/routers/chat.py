from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from src.ai.agents.librarian import chat_with_librarian

router = APIRouter(prefix="/chat", tags=["AI Chatbot"])

class ChatRequest(BaseModel):
    message: str

@router.post("/")
def ask_the_librarian(request: ChatRequest):
    """
    Envia uma mensagem para o Agente RAG da Biblioteca da Liane.
    Ele buscará livros por semântica e responderá em linguagem natural.
    """
    if not request.message.strip():
        raise HTTPException(status_code=400, detail="A mensagem não pode estar vazia.")
        
    try:
        # Chama a função que faz o RAG
        reply = chat_with_librarian(request.message)
        return {"reply": reply}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno do LLM: {str(e)}")