import os
from dotenv import load_dotenv
import urllib.parse

# Carrega as variáveis do arquivo .env na raiz do projeto
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "")
DB_NAME = os.getenv("DB_NAME", "lianes_library")

# Faz o parse da senha para evitar erros com caracteres especiais
ENCODED_PASS = urllib.parse.quote_plus(DB_PASS) if DB_PASS else ""

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{ENCODED_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
