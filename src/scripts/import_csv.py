import os
import pandas as pd
from sqlalchemy import text
from dotenv import load_dotenv
from src.db.connection import get_engine

# 1. Força a leitura do .env na raiz
load_dotenv(override=True)

def importar_livros(caminho_csv):
    print(f"✅ .env carregado! A ligar ao Aiven...")
    print(f"📂 A ler o ficheiro: {caminho_csv}...")
    
    try:
        # Lemos o CSV ignorando linhas com erros (como a 2353 que vimos antes)
        df = pd.read_csv(
            caminho_csv, 
            on_bad_lines='warn', 
            engine='python',
            encoding='utf-8'
        )
        df = df.fillna('') 
    except Exception as e:
        print(f"❌ Erro fatal ao ler o CSV: {e}")
        return

    engine = get_engine()
    
    # Query SQL para inserir na nuvem (Aiven)
    insert_query = text("""
        INSERT INTO books (title, author, genre, ISBN, book_status, cover_url)
        VALUES (:title, :author, :genre, :isbn, :status, :cover_url)
    """)

    sucesso = 0
    print(f"⏳ A enviar {len(df)} linhas para a nuvem... Isto pode demorar um pouco.")
    
    with engine.begin() as conn:
        for index, row in df.iterrows():
            try:
                # ---------------------------------------------------------
                # MAPEAMENTO REAL (Baseado no seu head -n 1)
                # ---------------------------------------------------------
                conn.execute(insert_query, {
                    "title": str(row['title']),
                    "author": str(row['authors']),
                    "genre": str(row['language_code']),
                    "isbn": str(row['isbn']),
                    "status": "AVAILABLE",
                    "cover_url": None # Ou o link, se existir no seu CSV
                })
                sucesso += 1
                
                if sucesso % 500 == 0:
                    print(f"🚀 {sucesso} livros enviados...")
                    
            except Exception as e:
                # Se uma linha falhar, ele avisa e continua para a próxima
                print(f"⚠️ Erro na linha {index}: {e}")

    print(f"\n✅ IMPORTAÇÃO CONCLUÍDA!")
    print(f"📊 {sucesso} livros estão agora online no Aiven.")

if __name__ == "__main__":
    # Verifique se o nome do ficheiro está exatamente igual ao da sua pasta data/
    importar_livros("data/books_clean_debug2.csv")