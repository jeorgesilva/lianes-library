from typing import Any, Dict, List, Optional

from src.db.d1_client import d1_batch, d1_exec, d1_one, d1_query


def list_cached(owner_id: int) -> List[Dict[str, Any]]:
    """Non-expired recommendations for the owner, best match first."""
    rows = d1_query(
        """
        SELECT * FROM recommendation_cache
        WHERE owner_id = ? AND expires_at > datetime('now')
        ORDER BY match_score DESC
        """,
        [owner_id],
    )
    return [dict(r) for r in rows]


def replace_cache(owner_id: int, items: List[Dict[str, Any]], ttl_days: int = 7) -> List[Dict[str, Any]]:
    """Swaps the owner's cached recommendations for a freshly generated batch.

    Runs as one D1 batch (delete + inserts) so a reader never sees a
    half-replaced cache between the delete and the inserts.
    """
    statements: List[tuple[str, Optional[list[Any]]]] = [
        ("DELETE FROM recommendation_cache WHERE owner_id = ?", [owner_id])
    ]
    for item in items:
        statements.append(
            (
                """
                INSERT INTO recommendation_cache
                    (owner_id, title, author, isbn, cover_url, reason, source_genre,
                     match_score, best_price, best_price_source, best_price_url, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now', ?))
                """,
                [
                    owner_id,
                    item["title"],
                    item.get("author"),
                    item.get("isbn"),
                    item.get("cover_url"),
                    item.get("reason"),
                    item.get("source_genre"),
                    item.get("match_score"),
                    item.get("best_price"),
                    item.get("best_price_source"),
                    item.get("best_price_url"),
                    f"+{ttl_days} days",
                ],
            )
        )
    d1_batch(statements)
    return list_cached(owner_id)


def get_cache_item(owner_id: int, recommendation_id: int) -> Optional[Dict[str, Any]]:
    row = d1_one(
        "SELECT * FROM recommendation_cache WHERE recommendation_id = ? AND owner_id = ?",
        [recommendation_id, owner_id],
    )
    return dict(row) if row else None


def delete_cache_item(owner_id: int, recommendation_id: int) -> None:
    d1_exec(
        "DELETE FROM recommendation_cache WHERE recommendation_id = ? AND owner_id = ?",
        [recommendation_id, owner_id],
    )


def create_dismissal(owner_id: int, title: str, isbn: Optional[str] = None) -> None:
    d1_exec(
        "INSERT INTO recommendation_dismissals (owner_id, isbn, title) VALUES (?, ?, ?)",
        [owner_id, isbn, title],
    )


def list_dismissed(owner_id: int) -> List[Dict[str, Any]]:
    rows = d1_query(
        "SELECT isbn, title FROM recommendation_dismissals WHERE owner_id = ?",
        [owner_id],
    )
    return [dict(r) for r in rows]
