import requests
import time
from sqlalchemy import text
from src.db.connection import get_engine
from dotenv import load_dotenv

load_dotenv(override=True)

def buscar_e_atualizar_capas():
    engine = get_engine()
    
    # 1. Busca os livros que têm ISBN mas NÃO têm capa
    query_select = text("SELECT book_id, title, ISBN FROM books WHERE cover_url IS NULL AND ISBN IS NOT NULL AND ISBN != '';")
    query_update = text("UPDATE books SET cover_url = :url WHERE book_id = :id")
    
    with engine.connect() as conn:
        livros_sem_capa = conn.execute(query_select).mappings().all()
        
    total = len(livros_sem_capa)
    print(f"🔍 Encontrados {total} livros sem capa para atualizar. A iniciar busca...")
    
    sucessos = 0
    
    # 2. Loop para buscar a capa de cada um
    for index, livro in enumerate(livros_sem_capa):
        book_id = livro['book_id']
        isbn = str(livro['ISBN']).strip()
        titulo = livro['title']
        
        print(f"[{index+1}/{total}] A buscar: {titulo[:30]}... (ISBN: {isbn})")
        
        url_api = f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data"
        
        try:
            res = requests.get(url_api, timeout=10)
            if res.status_code == 200:
                data = res.json()
                key = f"ISBN:{isbn}"
                
                if key in data:
                    # Tenta pegar a capa grande, se não tiver, tenta a média
                    capa = data[key].get("cover", {}).get("large") or data[key].get("cover", {}).get("medium")
                    
                    if capa:
                        # 3. Faz o UPDATE no banco de dados
                        with engine.begin() as conn:
                            conn.execute(query_update, {"url": capa, "id": book_id})
                        sucessos += 1
                        print("   ✅ Capa encontrada e guardada!")
                    else:
                        print("   ❌ Livro encontrado, mas sem imagem de capa na Open Library.")
                else:
                    print("   ❌ ISBN não encontrado na Open Library.")
            
            # Pequena pausa para não sobrecarregar a API da Open Library e ser bloqueado
            time.sleep(0.5)
            
        except Exception as e:
            print(f"   ⚠️ Erro ao buscar/salvar: {e}")

    print(f"\n🎉 ATUALIZAÇÃO CONCLUÍDA! {sucessos} novas capas foram adicionadas ao seu banco de dados.")

if __name__ == "__main__":
    buscar_e_atualizar_capas()