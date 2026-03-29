from sqlalchemy import text
from src.db.connection import get_engine
from dotenv import load_dotenv

load_dotenv(override=True)

def criar_coluna():
    engine = get_engine()
    query = text("ALTER TABLE books ADD COLUMN cover_url VARCHAR(500);")
    
    print("⏳ A conectar ao Aiven para criar a coluna...")
    try:
        with engine.begin() as conn:
            conn.execute(query)
        print("✅ SUCESSO ABSOLUTO! A coluna 'cover_url' foi criada no banco de dados.")
    except Exception as e:
        if "Duplicate column" in str(e):
            print("💡 A coluna já existia!")
        else:
            print(f"❌ Erro: {e}")

if __name__ == "__main__":
    criar_coluna()