from .chroma_store import get_collection
from ..embeddings.sentence_transformer import get_embedding_model


def query_books(query: str, top_k: int = 5):
    model = get_embedding_model()
    query_embedding = model.encode([query]).tolist()
    collection = get_collection()
    return collection.query(query_embeddings=query_embedding, n_results=top_k)
