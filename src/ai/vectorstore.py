# src/ai/vectorstore.py
import os
from typing import List, Dict, Any
from langchain_community.vectorstores import Chroma
from src.ai.embeddings import get_embeddings_model
from src.core.config import load_dotenv

load_dotenv()
CHROMA_DIR = os.getenv("CHROMA_PERSIST_DIR", "./chroma_data")

def get_chroma_db() -> Chroma:
    """Inicia e retorna a conexão com o banco de dados vetorial ChromaDB."""
    return Chroma(
        collection_name="lianes_books",
        embedding_function=get_embeddings_model(),
        persist_directory=CHROMA_DIR
    )

def vibe_search(query: str, limit: int = 5) -> List[Dict[str, Any]]:
    """
    A famosa 'Vibe Search'.
    Busca livros por similaridade semântica ao invés de palavras-chave exatas.
    """
    db = get_chroma_db()
    # Busca os K documentos mais similares ao texto digitado
    results = db.similarity_search_with_score(query, k=limit)
    
    formatted_results = []
    for doc, score in results:
        formatted_results.append({
            "book_id": doc.metadata.get("book_id"),
            "title": doc.metadata.get("title"),
            "author": doc.metadata.get("author"),
            "content_summary": doc.page_content,
            "relevance_score": round(1.0 - score, 4) # Converte a distância para % de relevância
        })
        
    return formatted_results