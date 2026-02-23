from sentence_transformers import SentenceTransformer


DEFAULT_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"


def get_embedding_model(model_name: str | None = None) -> SentenceTransformer:
    return SentenceTransformer(model_name or DEFAULT_MODEL_NAME)
