"""
Trava o contrato de isolamento multi-tenant da Vibe Search:
vibe_search() sempre exige owner_id e nunca deve retornar livros de outro dono.

O Pinecone real é mockado: simulamos aqui o comportamento de filtragem por
metadata que o índice aplicaria, para garantir que src.ai.vectorstore.vibe_search
sempre repassa o filtro {"owner_id": owner_id} para similarity_search_with_score.
"""
from unittest.mock import patch, MagicMock

from langchain_core.documents import Document

from src.ai.vectorstore import vibe_search

USER_A = 1
USER_B = 2

# Simula o índice vetorial: um vetor por livro, cada um com owner_id nos metadados.
FAKE_INDEX = [
    (Document(page_content="Title: Dune. Author: Frank Herbert. Genre: Sci-fi.",
              metadata={"book_id": 101, "title": "Dune", "author": "Frank Herbert", "owner_id": USER_A}), 0.95),
    (Document(page_content="Title: Duna. Author: Frank Herbert. Genre: Ficção científica.",
              metadata={"book_id": 202, "title": "Duna", "author": "Frank Herbert", "owner_id": USER_B}), 0.93),
]


class FakePineconeDB:
    """Stub que reproduz o comportamento de filtragem por metadata do Pinecone."""

    def similarity_search_with_score(self, query, k=5, filter=None):
        owner_id = (filter or {}).get("owner_id")
        matches = [
            (doc, score) for doc, score in FAKE_INDEX
            if doc.metadata.get("owner_id") == owner_id
        ]
        return matches[:k]


@patch("src.ai.vectorstore.get_pinecone_db")
def test_vibe_search_never_leaks_other_owners_books(mock_get_db):
    mock_get_db.return_value = FakePineconeDB()

    results_a = vibe_search("uma aventura no deserto", owner_id=USER_A)
    results_b = vibe_search("uma aventura no deserto", owner_id=USER_B)

    assert len(results_a) == 1
    assert results_a[0]["book_id"] == 101

    assert len(results_b) == 1
    assert results_b[0]["book_id"] == 202

    # Nenhum resultado de A deve conter livros de B, e vice-versa.
    ids_a = {r["book_id"] for r in results_a}
    ids_b = {r["book_id"] for r in results_b}
    assert ids_a.isdisjoint(ids_b)


@patch("src.ai.vectorstore.get_pinecone_db")
def test_vibe_search_passes_owner_id_filter_to_pinecone(mock_get_db):
    fake_db = MagicMock()
    fake_db.similarity_search_with_score.return_value = []
    mock_get_db.return_value = fake_db

    vibe_search("qualquer busca", owner_id=USER_A, limit=7)

    fake_db.similarity_search_with_score.assert_called_once_with(
        "qualquer busca", k=7, filter={"owner_id": USER_A}
    )


def test_vibe_search_requires_owner_id_argument():
    # owner_id é posicional/obrigatório: chamar sem ele deve falhar em tempo de
    # chamada, travando o contrato de que nenhum caller pode esquecê-lo.
    try:
        vibe_search("busca sem dono")  # type: ignore[call-arg]
    except TypeError:
        pass
    else:
        raise AssertionError("vibe_search() deveria exigir owner_id")
