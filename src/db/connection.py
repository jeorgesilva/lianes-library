from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from src.core.config import DATABASE_URL

# pool_pre_ping=True ajuda a evitar erros se a conexão com o MySQL cair por inatividade
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def get_engine() -> Engine:
    """
    Retorna a engine configurada do SQLAlchemy.
    """
    return engine