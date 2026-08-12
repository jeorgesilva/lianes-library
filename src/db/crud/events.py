from typing import Any, Dict, List, Optional

from src.db.d1_client import d1_exec, d1_one, d1_query


def get_preferences(owner_id: int) -> Optional[Dict[str, Any]]:
    row = d1_one("SELECT * FROM user_event_preferences WHERE owner_id = ?", [owner_id])
    return dict(row) if row else None


def set_preferences(owner_id: int, city: Optional[str], radius_km: Optional[int]) -> Dict[str, Any]:
    existing = get_preferences(owner_id)
    if existing:
        d1_exec(
            "UPDATE user_event_preferences SET city = ?, radius_km = COALESCE(?, radius_km) WHERE owner_id = ?",
            [city, radius_km, owner_id],
        )
    else:
        d1_exec(
            "INSERT INTO user_event_preferences (owner_id, city, radius_km) VALUES (?, ?, COALESCE(?, 30))",
            [owner_id, city, radius_km],
        )
    return get_preferences(owner_id)


def list_events(city: Optional[str] = None, upcoming_only: bool = True) -> List[Dict[str, Any]]:
    where: List[str] = []
    params: List[Any] = []
    if city:
        where.append("city = ?")
        params.append(city)
    if upcoming_only:
        where.append("date(event_date) >= date('now')")

    clause = f"WHERE {' AND '.join(where)}" if where else ""
    rows = d1_query(f"SELECT * FROM literary_events {clause} ORDER BY event_date ASC", params)
    return [dict(r) for r in rows]


def get_event(event_id: int) -> Optional[Dict[str, Any]]:
    row = d1_one("SELECT * FROM literary_events WHERE event_id = ?", [event_id])
    return dict(row) if row else None


def create_manual_event(
    title: str,
    event_date: str,
    description: Optional[str] = None,
    venue_name: Optional[str] = None,
    city: Optional[str] = None,
    url: Optional[str] = None,
    image_url: Optional[str] = None,
) -> Dict[str, Any]:
    meta = d1_exec(
        """
        INSERT INTO literary_events (source, title, description, venue_name, city, event_date, url, image_url)
        VALUES ('manual', ?, ?, ?, ?, ?, ?, ?)
        """,
        [title, description, venue_name, city, event_date, url, image_url],
    )
    return get_event(meta["last_row_id"])


def delete_manual_event(event_id: int) -> None:
    """Only 'manual' rows are deletable — provider-sourced rows are refreshed by the weekly job, not user-managed."""
    d1_exec("DELETE FROM literary_events WHERE event_id = ? AND source = 'manual'", [event_id])
