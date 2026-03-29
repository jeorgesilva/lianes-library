from sqlalchemy import text
from src.db.connection import get_engine
from dotenv import load_dotenv

load_dotenv(override=True)

def verificar_status_das_capas():
    engine = get_engine()
    
    query = text("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN cover_url IS NOT NULL THEN 1 ELSE 0 END) as com_capa,
            SUM(CASE WHEN cover_url IS NULL THEN 1 ELSE 0 END) as sem_capa
        FROM books;
    """)
    
    print("⏳ A consultar o banco de dados no Aiven...")
    with engine.connect() as conn:
        resultado = conn.execute(query).fetchone()
        
        print("\n📊 RELATÓRIO DE CAPAS:")
        print(f"Total de Livros: {resultado.total}")
        print(f"✅ Com Capa: {resultado.com_capa}")
        print(f"❌ Sem Capa: {resultado.sem_capa}")

if __name__ == "__main__":
    verificar_status_das_capas()