# src/ai/indexer.py
from langchain_core.documents import Document
from src.db.crud.books import get_books
from src.ai.vectorstore import get_chroma_db

def sync_books_to_vectorstore():
    """
    Busca todos os livros no MySQL e atualiza o ChromaDB.
    Em um cenário real, você rodaria isso a cada novo livro cadastrado,
    ou como um 'Cron Job' (Job em background).
    """
    print("⏳ Buscando livros no banco de dados relacional...")
    all_books = get_books(limit=10000) # Puxa todo o catálogo
    
    if not all_books:
        print("❌ Nenhum livro encontrado no MySQL.")
        return

    documents = []
    for book in all_books:
        # Aqui criamos o "texto" que a IA vai ler para entender a vibe do livro
        # Se você tiver sinopses/descrições no banco, coloque aqui!
        text_content = f"Title: {book['title']}. Author: {book['author']}. Genre: {book.get('genre', 'Unknown')}."
        
        doc = Document(
            page_content=text_content,
            metadata={
                "book_id": book["book_id"],
                "title": book["title"],
                "author": book["author"]
            }
        )
        documents.append(doc)

    print(f"🧠 Gerando embeddings para {len(documents)} livros e salvando no ChromaDB...")
    db = get_chroma_db()
    
    # Opcional: limpar a coleção antes de recriar para evitar duplicatas em testes
    # db.delete_collection()
    # db = get_chroma_db()
    
    db.add_documents(documents)
    print("✅ Sincronização concluída com sucesso! A Vibe Search está pronta.")

# Se você rodar este arquivo diretamente (python -m src.ai.indexer), ele sincroniza os bancos.
if __name__ == "__main__":
    sync_books_to_vectorstore()