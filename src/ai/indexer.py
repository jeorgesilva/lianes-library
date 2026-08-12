import os
from dotenv import load_dotenv

# 1º PASSO: LER AS SENHAS IMEDIATAMENTE (Antes de importar o resto)
load_dotenv(override=True)

# 2º PASSO: AGORA SIM, IMPORTAR O BANCO DE DADOS E A IA
from langchain_core.documents import Document
from src.db.crud.books import get_books
from src.db.crud.users import list_all_user_ids
from src.ai.vectorstore import get_pinecone_db

def sync_books_to_vectorstore():
    """
    Busca todos os livros de todos os usuários no D1 (via get_books) e atualiza
    o Pinecone na nuvem, gravando o owner_id nos metadados de cada vetor para
    que a busca semântica possa ser filtrada por dono.
    """
    print("⏳ A procurar usuários na base de dados relacional (D1)...")
    user_ids = list_all_user_ids()

    if not user_ids:
        print("❌ Nenhum usuário encontrado no D1.")
        return

    documents = []
    doc_ids = [] # Lista para guardar os IDs e evitar duplicados no Pinecone

    for owner_id in user_ids:
        print(f"⏳ A procurar livros do usuário {owner_id} na base de dados relacional (D1)...")
        user_books = get_books(owner_id=owner_id, limit=10000) # Puxa todo o catálogo do usuário

        for book in user_books:
            # Aqui criamos o "texto" que a IA vai ler para entender a vibe do livro
            text_content = f"Title: {book['title']}. Author: {book['author']}. Genre: {book.get('genre', 'Unknown')}."

            doc = Document(
                page_content=text_content,
                metadata={
                    "book_id": book["book_id"],
                    "title": book["title"],
                    "author": book["author"],
                    "owner_id": owner_id
                }
            )
            documents.append(doc)

            # O Pinecone exige que o ID seja sempre uma string (texto)
            doc_ids.append(str(book["book_id"]))

    if not documents:
        print("❌ Nenhum livro encontrado no D1. Tem a certeza de que já importou o CSV?")
        return

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