import os
from dotenv import load_dotenv

# 1º PASSO: LER AS SENHAS IMEDIATAMENTE (Antes de importar o resto)
load_dotenv(override=True)

# 2º PASSO: AGORA SIM, IMPORTAR O BANCO DE DADOS E A IA
from langchain_core.documents import Document
from src.db.crud.books import get_books
from src.ai.vectorstore import get_pinecone_db

def sync_books_to_vectorstore():
    """
    Busca todos os livros no MySQL (Aiven) e atualiza o Pinecone na nuvem.
    """
    print("⏳ A procurar livros na base de dados relacional (Aiven)...")
    all_books = get_books(limit=10000) # Puxa todo o catálogo
    
    if not all_books:
        print("❌ Nenhum livro encontrado no MySQL. Tem a certeza de que já importou o CSV?")
        return

    documents = []
    doc_ids = [] # Lista para guardar os IDs e evitar duplicados no Pinecone
    
    for book in all_books:
        # Aqui criamos o "texto" que a IA vai ler para entender a vibe do livro
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
        
        # O Pinecone exige que o ID seja sempre uma string (texto)
        doc_ids.append(str(book["book_id"])) 

    print(f"🧠 A gerar vetores para {len(documents)} livros e a enviar para o Pinecone...")
    
    try:
        db = get_pinecone_db()
        
        # Ao passarmos os 'ids', o Pinecone faz um "Upsert". 
        # Atualiza os que já existem e cria os novos. Zero duplicados!
        db.add_documents(documents, ids=doc_ids)
        
        print("✅ Sincronização concluída com sucesso! A Vibe Search já tem cérebro.")
    except Exception as e:
        print(f"❌ Erro ao enviar para o Pinecone: {e}")
        print("Verifique se o PINECONE_API_KEY está correto no ficheiro .env e se o Index de 384 dimensões foi criado.")

if __name__ == "__main__":
    sync_books_to_vectorstore()