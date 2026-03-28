import os
from typing import List, Dict, Any
from dotenv import load_dotenv

# Usamos a integração oficial e mais moderna do LangChain para o Pinecone
from langchain_pinecone import PineconeVectorStore 
from src.ai.embeddings import get_embeddings_model

# Força a leitura do .env
load_dotenv(override=True)

PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
PINECONE_INDEX_NAME = os.getenv("PINECONE_INDEX_NAME", "lianes-library")

def get_pinecone_db() -> PineconeVectorStore:
    """Inicia e retorna a conexão com o banco de dados vetorial Pinecone."""
    return PineconeVectorStore(
        index_name=PINECONE_INDEX_NAME,
        embedding=get_embeddings_model(),
        pinecone_api_key=PINECONE_API_KEY
    )

def vibe_search(query: str, limit: int = 5) -> List[Dict[str, Any]]:
    """
    A famosa 'Vibe Search'.
    Busca livros por similaridade semântica ao invés de palavras-chave exatas.
    """
    db = get_pinecone_db()
    
    # Busca os K documentos mais similares ao texto digitado
    results = db.similarity_search_with_score(query, k=limit)
    
    formatted_results = []
    for doc, score in results:
        formatted_results.append({
            "book_id": doc.metadata.get("book_id"),
            "title": doc.metadata.get("title"),
            "author": doc.metadata.get("author"),
            "content_summary": doc.page_content,
            # O Pinecone (com Cosine) já retorna o score de 0 a 1, onde 1 é perfeito.
            "relevance_score": round(score, 4) 
        })
        
    return formatted_results