from typing import Optional, Dict, Any
from src.db.d1_client import d1_one


def create_user(first_name: str, last_name: Optional[str], email: str, password_hash: str) -> Dict[str, Any]:
    """Cria um novo usuário. Assume que o e-mail já foi validado como único pelo chamador."""
    row = d1_one(
        """
        INSERT INTO users (first_name, last_name, email, password_hash)
        VALUES (?, ?, ?, ?)
        RETURNING user_id, first_name, last_name, email, created_at
        """,
        [first_name, last_name, email, password_hash],
    )
    return dict(row)


def get_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    row = d1_one("SELECT * FROM users WHERE email = ?", [email])
    return dict(row) if row else None


def get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
    row = d1_one(
        "SELECT user_id, first_name, last_name, email, created_at FROM users WHERE user_id = ?",
        [user_id],
    )
    return dict(row) if row else None
