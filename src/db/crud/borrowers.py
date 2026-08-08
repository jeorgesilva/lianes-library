from typing import Optional, List, Dict, Any
from src.db.d1_client import d1_query, d1_one

def create_borrower(owner_id: int, first_name: str, last_name: str, email: str = None,
                    phone_number: str = None, relationship_type: str = None,
                    address: str = None) -> Dict[str, Any]:
    """Insere um novo mutuário no banco de dados do usuário."""
    if not first_name and not last_name:
        raise ValueError('Name is required for a borrower.')

    row = d1_one(
        """
        INSERT INTO borrowers (first_name, last_name, email, phone_number, relationship_type, address, owner_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        RETURNING *
        """,
        [first_name, last_name, email, phone_number, relationship_type, address, owner_id],
    )
    return dict(row)

def get_borrower_by_id(owner_id: int, person_id: int) -> Optional[Dict[str, Any]]:
    """Busca um mutuário específico do usuário pelo ID."""
    row = d1_one("SELECT * FROM borrowers WHERE person_id = ? AND owner_id = ?", [person_id, owner_id])
    return dict(row) if row is not None else None

def get_borrowers(owner_id: int, first_name: str = None, status: str = None, limit: int = 100) -> List[Dict[str, Any]]:
    """Lista mutuários do usuário com filtros opcionais."""
    query = "SELECT * FROM borrowers WHERE owner_id = ?"
    params: List[Any] = [owner_id]

    if first_name:
        query += " AND first_name LIKE ?"
        params.append(f"%{first_name}%")

    if status:
        query += " AND status = ?"
        params.append(status)

    query += " ORDER BY first_name ASC LIMIT ?"
    params.append(limit)

    return [dict(r) for r in d1_query(query, params)]

def set_borrower_status(owner_id: int, person_id: int, new_status: str) -> Dict[str, Any]:
    """Atualiza o status do mutuário (ACTIVE / INACTIVE)."""
    allowed = {"ACTIVE", "INACTIVE"}
    if new_status.upper() not in allowed:
        raise ValueError(f"Invalid status '{new_status}'. Allowed: {allowed}")

    row = d1_one(
        "UPDATE borrowers SET status = ? WHERE person_id = ? AND owner_id = ? RETURNING *",
        [new_status.upper(), person_id, owner_id],
    )
    if row is None:
        raise ValueError(f"Borrower {person_id} not found.")
    return dict(row)

def delete_borrower(owner_id: int, person_id: int) -> str:
    """
    Realiza um SOFT DELETE.
    NUNCA apague a linha real, senão você perde o histórico de empréstimos e gera erros de Foreign Key.
    """
    # Em vez de DELETE FROM, usamos a função de status para inativar
    set_borrower_status(owner_id=owner_id, person_id=person_id, new_status="INACTIVE")
    return f"Borrower {person_id} has been deactivated (soft delete) successfully."
