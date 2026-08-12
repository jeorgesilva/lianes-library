from typing import List, Dict, Any
from src.db.d1_client import d1_query, d1_one, d1_exec


def list_notifications(owner_id: int, limit: int = 50) -> List[Dict[str, Any]]:
    """Lista as notificações mais recentes do usuário, não lidas primeiro."""
    rows = d1_query(
        """
        SELECT * FROM notifications
        WHERE owner_id = ?
        ORDER BY (read_at IS NOT NULL), created_at DESC
        LIMIT ?
        """,
        [owner_id, limit],
    )
    return [dict(r) for r in rows]


def mark_read(owner_id: int, notification_id: int) -> Dict[str, Any]:
    notification = d1_one(
        "SELECT notification_id, read_at FROM notifications WHERE notification_id = ? AND owner_id = ?",
        [notification_id, owner_id],
    )
    if not notification:
        raise ValueError(f"Notification {notification_id} does not exist.")
    if notification["read_at"] is not None:
        return {"detail": "Notification already marked as read."}

    d1_exec(
        "UPDATE notifications SET read_at = datetime('now') WHERE notification_id = ? AND owner_id = ?",
        [notification_id, owner_id],
    )
    return {"detail": "Notification marked as read."}
