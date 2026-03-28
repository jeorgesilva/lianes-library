import os
import urllib.parse
from dotenv import load_dotenv

# 1. Tenta carregar o .env da raiz
load_dotenv(override=True)

# 2. DEBUG: Vamos ver se ele está a ler (sem mostrar a senha toda por segurança)
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")

if host:
    print(f"✅ SUCESSO: .env carregado! A ligar ao host: {host[:10]}... na porta: {port}")
else:
    print("❌ ERRO: O ficheiro .env ainda não foi encontrado na raiz do projeto!")

# 3. Puxa os valores
DB_HOST = host
DB_PORT = port
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

# 4. Monta a URL (O int(DB_PORT) agora vai funcionar porque o valor existe!)
ENCODED_PASS = urllib.parse.quote_plus(DB_PASS) if DB_PASS else ""
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{ENCODED_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"