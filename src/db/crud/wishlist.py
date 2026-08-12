from typing import Optional, List, Dict, Any
from src.db.d1_client import d1_query, d1_one, d1_exec

VALID_STATUSES = {"ACTIVE", "PURCHASED", "ARCHIVED"}

# Correlated subqueries pull the latest and lowest snapshot alongside each
# item, so the frontend gets "current price" + "lowest ever" + "first ever
# seen" (for the % drop badge) in one round trip instead of N+1 queries.
_ITEM_WITH_PRICE_COLUMNS = """
    w.*,
    (SELECT price FROM wishlist_price_snapshots s WHERE s.wishlist_item_id = w.wishlist_item_id ORDER BY checked_at DESC LIMIT 1) as latest_price,
    (SELECT currency FROM wishlist_price_snapshots s WHERE s.wishlist_item_id = w.wishlist_item_id ORDER BY checked_at DESC LIMIT 1) as latest_currency,
    (SELECT url FROM wishlist_price_snapshots s WHERE s.wishlist_item_id = w.wishlist_item_id ORDER BY checked_at DESC LIMIT 1) as latest_price_url,
    (SELECT checked_at FROM wishlist_price_snapshots s WHERE s.wishlist_item_id = w.wishlist_item_id ORDER BY checked_at DESC LIMIT 1) as latest_checked_at,
    (SELECT MIN(price) FROM wishlist_price_snapshots s WHERE s.wishlist_item_id = w.wishlist_item_id) as lowest_price,
    (SELECT price FROM wishlist_price_snapshots s WHERE s.wishlist_item_id = w.wishlist_item_id ORDER BY checked_at ASC LIMIT 1) as first_price
"""


def _with_drop_pct(item: Dict[str, Any]) -> Dict[str, Any]:
    """Adds drop_pct: how far latest_price has fallen from the first price ever seen."""
    first = item.get("first_price")
    latest = item.get("latest_price")
    item["drop_pct"] = round((first - latest) / first * 100, 1) if first and latest and first > 0 else None
    return item


def create_wishlist_item(
    owner_id: int,
    title: str,
    author: Optional[str] = None,
    isbn: Optional[str] = None,
    cover_url: Optional[str] = None,
    target_price: Optional[float] = None,
    notes: Optional[str] = None,
) -> Dict[str, Any]:
    meta = d1_exec(
        """
        INSERT INTO wishlist_items (owner_id, title, author, isbn, cover_url, target_price, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [owner_id, title, author, isbn, cover_url, target_price, notes],
    )
    return get_wishlist_item(owner_id, meta["last_row_id"])


def list_wishlist_items(owner_id: int, status: Optional[str] = None) -> List[Dict[str, Any]]:
    where = "WHERE w.owner_id = ?"
    params: List[Any] = [owner_id]
    if status:
        where += " AND w.status = ?"
        params.append(status)

    rows = d1_query(
        f"SELECT {_ITEM_WITH_PRICE_COLUMNS} FROM wishlist_items w {where} ORDER BY w.added_at DESC",
        params,
    )
    return [_with_drop_pct(dict(r)) for r in rows]


def get_wishlist_item(owner_id: int, wishlist_item_id: int) -> Optional[Dict[str, Any]]:
    row = d1_one(
        f"SELECT {_ITEM_WITH_PRICE_COLUMNS} FROM wishlist_items w WHERE w.wishlist_item_id = ? AND w.owner_id = ?",
        [wishlist_item_id, owner_id],
    )
    return _with_drop_pct(dict(row)) if row else None


def update_wishlist_item(
    owner_id: int,
    wishlist_item_id: int,
    target_price: Optional[float] = None,
    notes: Optional[str] = None,
) -> Dict[str, Any]:
    item = get_wishlist_item(owner_id, wishlist_item_id)
    if not item:
        raise ValueError(f"Wishlist item {wishlist_item_id} does not exist.")

    fields = {"target_price": target_price, "notes": notes}
    updates = {k: v for k, v in fields.items() if v is not None}
    if not updates:
        return item

    set_clause = ", ".join(f"{k} = ?" for k in updates)
    d1_exec(
        f"UPDATE wishlist_items SET {set_clause} WHERE wishlist_item_id = ? AND owner_id = ?",
        [*updates.values(), wishlist_item_id, owner_id],
    )
    return get_wishlist_item(owner_id, wishlist_item_id)


def set_status(owner_id: int, wishlist_item_id: int, status: str) -> Dict[str, Any]:
    if status not in VALID_STATUSES:
        raise ValueError(f"Invalid status: {status}")

    item = get_wishlist_item(owner_id, wishlist_item_id)
    if not item:
        raise ValueError(f"Wishlist item {wishlist_item_id} does not exist.")

    d1_exec(
        "UPDATE wishlist_items SET status = ? WHERE wishlist_item_id = ? AND owner_id = ?",
        [status, wishlist_item_id, owner_id],
    )
    return get_wishlist_item(owner_id, wishlist_item_id)


def delete_wishlist_item(owner_id: int, wishlist_item_id: int) -> None:
    item = get_wishlist_item(owner_id, wishlist_item_id)
    if not item:
        raise ValueError(f"Wishlist item {wishlist_item_id} does not exist.")
    d1_exec("DELETE FROM wishlist_items WHERE wishlist_item_id = ? AND owner_id = ?", [wishlist_item_id, owner_id])


def list_price_snapshots(owner_id: int, wishlist_item_id: int) -> List[Dict[str, Any]]:
    """Price history for the sparkline. Ownership enforced via the parent item."""
    item = get_wishlist_item(owner_id, wishlist_item_id)
    if not item:
        raise ValueError(f"Wishlist item {wishlist_item_id} does not exist.")
    rows = d1_query(
        "SELECT * FROM wishlist_price_snapshots WHERE wishlist_item_id = ? ORDER BY checked_at ASC",
        [wishlist_item_id],
    )
    return [dict(r) for r in rows]
