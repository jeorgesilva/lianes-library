from .chroma_store import get_collection
from ..embeddings.sentence_transformer import get_embedding_model


def index_books(texts: list[str], ids: list[str] | None = None):
    if ids is None:
        ids = [str(i) for i in range(len(texts))]
    model = get_embedding_model()
    embeddings = model.encode(texts).tolist()
    collection = get_collection()
    collection.add(documents=texts, embeddings=embeddings, ids=ids)
    return len(ids)
