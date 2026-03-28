from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from src.core.config import DATABASE_URL  # Aqui ele puxa a URL do outro arquivo

# pool_pre_ping ajuda com conexões instáveis na nuvem
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def get_engine() -> Engine:
    """Retorna a engine configurada do SQLAlchemy."""
    return engine