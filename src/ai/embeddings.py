# src/ai/embeddings.py
from langchain_community.embeddings import HuggingFaceEmbeddings

# Usamos um cache em memória para não carregar o modelo múltiplas vezes
_embeddings_model = None

def get_embeddings_model():
    """Retorna o modelo de embeddings (Singleton) para evitar recarregamento."""
    global _embeddings_model
    if _embeddings_model is None:
        # O modelo all-MiniLM-L6-v2 é rápido e excelente para buscas semânticas
        _embeddings_model = HuggingFaceEmbeddings(
            model_name="sentence-transformers/all-MiniLM-L6-v2"
        )
    return _embeddings_model