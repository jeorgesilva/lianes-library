from fastapi import APIRouter, HTTPException, Query
from src.ai.vectorstore import vibe_search

# Criamos um roteador focado em Busca e IA
router = APIRouter(prefix="/search", tags=["AI & Search"])

@router.get("/vibe")
def search_books_by_vibe(
    q: str = Query(..., description="A frase, sinopse ou 'vibe' que você quer buscar"),
    limit: int = Query(5, description="Número máximo de livros para retornar", le=20)
):
    """
    **Vibe Search (Busca Semântica)**
    
    Não procure por palavras exatas! Descreva o que você quer ler.
    Exemplo: "um livro engraçado sobre viagens espaciais" ou "uma história de detetive em Londres".
    """
    if not q.strip():
        raise HTTPException(status_code=400, detail="A query de busca não pode estar vazia.")
        
    try:
        results = vibe_search(query=q, limit=limit)
        return {
            "query": q,
            "count": len(results),
            "results": results
        }
    except Exception as e:
        # Captura erros caso o ChromaDB não esteja iniciado ou o modelo falhe
        raise HTTPException(status_code=500, detail=f"Erro na busca vetorial: {str(e)}")