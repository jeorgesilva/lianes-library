from pathlib import Path

import chromadb
from chromadb.config import Settings


DEFAULT_COLLECTION = "books"
DEFAULT_PERSIST_DIR = Path(".chroma")


def get_client(persist_dir: Path | None = None) -> chromadb.Client:
    path = str(persist_dir or DEFAULT_PERSIST_DIR)
    return chromadb.Client(Settings(persist_directory=path, anonymized_telemetry=False))


def get_collection(name: str = DEFAULT_COLLECTION, persist_dir: Path | None = None):
    client = get_client(persist_dir=persist_dir)
    return client.get_or_create_collection(name)
