from sqlalchemy import text
from typing import Optional, List, Dict, Any
from src.db.connection import get_engine

def create_borrower(first_name: str, last_name: str, email: str = None, 
                    phone_number: str = None, relationship_type: str = None, 
                    address: str = None) -> Dict[str, Any]:
    """Insere um novo mutuário no banco de dados."""
    if not first_name and not last_name:
        raise ValueError('Name is required for a borrower.')
    
    engine = get_engine()
    insert_sql = text("""
        INSERT INTO borrowers (first_name, last_name, email, phone_number, relationship_type, address)
        VALUES (:first_name, :last_name, :email, :phone_number, :relationship_type, :address)
    """)

    params = {
        "first_name": first_name, "last_name": last_name, "email": email,
        "phone_number": phone_number, "relationship_type": relationship_type, "address": address,
    }

    with engine.begin() as conn:
        result = conn.execute(insert_sql, params)
        person_id = result.lastrowid
        
        row = conn.execute(
            text('SELECT * FROM borrowers WHERE person_id = :person_id'),
            {"person_id": person_id}
        ).mappings().one()

    return dict(row)

def get_borrower_by_id(person_id: int) -> Optional[Dict[str, Any]]:
    """Busca um mutuário específico pelo ID."""
    engine = get_engine()
    sql = text("SELECT * FROM borrowers WHERE person_id = :person_id")

    with engine.connect() as conn:
        row = conn.execute(sql, {"person_id": person_id}).mappings().one_or_none()

    return dict(row) if row is not None else None

def get_borrowers(first_name: str = None, status: str = None, limit: int = 100) -> List[Dict[str, Any]]:
    """Lista mutuários com filtros opcionais."""
    engine = get_engine()
    query = "SELECT * FROM borrowers WHERE 1 = 1"
    params = {}

    if first_name: 
        query += " AND first_name LIKE :first_name"
        params["first_name"] = f"%{first_name}%"
        
    if status:
        query += " AND status = :status"
        params["status"] = status

    query += " ORDER BY first_name ASC LIMIT :limit"
    params["limit"] = limit

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).mappings().all()

    return [dict(r) for r in rows]

def set_borrower_status(person_id: int, new_status: str) -> Dict[str, Any]:
    """Atualiza o status do mutuário (ACTIVE / INACTIVE)."""
    allowed = {"ACTIVE", "INACTIVE"}
    if new_status.upper() not in allowed:
        raise ValueError(f"Invalid status '{new_status}'. Allowed: {allowed}")
    
    sql = text("UPDATE borrowers SET status = :status WHERE person_id = :person_id")
    engine = get_engine()
    
    with engine.begin() as conn:
        conn.execute(sql, {"status": new_status.upper(), "person_id": person_id})
        row = conn.execute(
            text("SELECT * FROM borrowers WHERE person_id = :person_id"),
            {"person_id": person_id}
        ).mappings().one()

    return dict(row)

def delete_borrower(person_id: int) -> str:
    """
    Realiza um SOFT DELETE. 
    NUNCA apague a linha real, senão você perde o histórico de empréstimos e gera erros de Foreign Key.
    """
    # Em vez de DELETE FROM, usamos a função de status para inativar
    set_borrower_status(person_id=person_id, new_status="INACTIVE")
    return f"Borrower {person_id} has been deactivated (soft delete) successfully."