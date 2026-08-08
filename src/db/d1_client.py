import os
from typing import Any, Optional

import requests

D1_PROXY_URL = os.getenv("D1_PROXY_URL")
INTERNAL_D1_TOKEN = os.getenv("INTERNAL_D1_TOKEN")


class D1Error(Exception):
    pass


def _post(body: Any) -> dict[str, Any]:
    response = requests.post(
        D1_PROXY_URL,
        json=body,
        headers={"X-Internal-Token": INTERNAL_D1_TOKEN},
        timeout=15,
    )
    response.raise_for_status()
    data = response.json()
    if not data.get("success"):
        raise D1Error(data.get("error", "D1 query failed"))
    return data


def d1_query(sql: str, params: Optional[list[Any]] = None) -> list[dict[str, Any]]:
    """Runs a SELECT (or an INSERT/UPDATE ... RETURNING) and returns the result rows."""
    return _post({"sql": sql, "params": params or []})["results"]


def d1_one(sql: str, params: Optional[list[Any]] = None) -> Optional[dict[str, Any]]:
    rows = d1_query(sql, params)
    return rows[0] if rows else None


def d1_exec(sql: str, params: Optional[list[Any]] = None) -> dict[str, Any]:
    """Runs an INSERT/UPDATE/DELETE without RETURNING and returns D1's meta (last_row_id, changes)."""
    return _post({"sql": sql, "params": params or []})["meta"]


def d1_batch(statements: list[tuple[str, Optional[list[Any]]]]) -> list[dict[str, Any]]:
    """Runs multiple statements atomically (env.DB.batch on the Worker side) and returns each one's meta."""
    body = [{"sql": sql, "params": params or []} for sql, params in statements]
    return _post(body)["meta"]
