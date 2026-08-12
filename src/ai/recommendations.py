import math
import os
import re
from collections import Counter
from typing import Any, Dict, List, Optional

import requests

from src.ai.embeddings import get_embeddings_model
from src.db.crud.books import get_books
from src.db.crud.recommendations import list_dismissed

GOOGLE_BOOKS_API_KEY = os.getenv("GOOGLE_BOOKS_API_KEY")
GOOGLE_BOOKS_URL = "https://www.googleapis.com/books/v1/volumes"

MAX_CANDIDATES_PER_QUERY = 8
MAX_RECOMMENDATIONS = 24
TOP_GENRES = 3
TOP_AUTHORS = 2
# No embedding signal available for a candidate (description missing, or the
# HF model failed to load) — a neutral score so it still surfaces via
# genre/author matching instead of being ranked to the bottom.
DEFAULT_MATCH_SCORE = 0.5


def _normalize(text: Optional[str]) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).strip()


def _cosine(a: List[float], b: List[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _centroid(vectors: List[List[float]]) -> Optional[List[float]]:
    if not vectors:
        return None
    dims = len(vectors[0])
    sums = [0.0] * dims
    for vec in vectors:
        for i, v in enumerate(vec):
            sums[i] += v
    return [s / len(vectors) for s in sums]


def build_taste_profile(owner_id: int) -> Dict[str, Any]:
    """Genre/author frequency plus an embedding centroid over the owner's shelf.

    The centroid is computed fresh from the shelf's own text (same
    sentence-transformers pipeline as vibe search), not read back from
    Pinecone: that index isn't scoped by owner, so querying it here could
    leak another owner's books into the taste profile.
    """
    books = get_books(owner_id, limit=10000)
    genre_counts = Counter(b["genre"] for b in books if b.get("genre"))
    author_counts = Counter(b["author"] for b in books if b.get("author"))

    centroid = None
    if books:
        texts = [
            f"Title: {b['title']}. Author: {b.get('author') or 'Unknown'}. Genre: {b.get('genre') or 'Unknown'}."
            for b in books
        ]
        try:
            vectors = get_embeddings_model().embed_documents(texts)
            centroid = _centroid(vectors)
        except Exception:
            centroid = None  # embedding model unavailable — degrade to genre/author matching only

    example_by_genre: Dict[str, str] = {}
    for b in books:
        genre = b.get("genre")
        if genre and genre not in example_by_genre:
            example_by_genre[genre] = b["title"]

    return {
        "books": books,
        "top_genres": [g for g, _ in genre_counts.most_common(TOP_GENRES)],
        "top_authors": [a for a, _ in author_counts.most_common(TOP_AUTHORS)],
        "example_by_genre": example_by_genre,
        "centroid": centroid,
    }


def _owned_keys(books: List[Dict[str, Any]]) -> tuple[set, set]:
    isbns = {b["ISBN"] for b in books if b.get("ISBN")}
    titles = {f"{_normalize(b['title'])}|{_normalize(b.get('author'))}" for b in books}
    return isbns, titles


def _dismissed_keys(owner_id: int) -> tuple[set, set]:
    dismissed = list_dismissed(owner_id)
    isbns = {d["isbn"] for d in dismissed if d.get("isbn")}
    titles = {_normalize(d["title"]) for d in dismissed}
    return isbns, titles


def _search_google_books(query: str, max_results: int = MAX_CANDIDATES_PER_QUERY) -> List[Dict[str, Any]]:
    """Free without a key (100 req/day); returns [] on any failure so a
    down/rate-limited API degrades the feature instead of breaking it."""
    params: Dict[str, Any] = {"q": query, "maxResults": max_results}
    if GOOGLE_BOOKS_API_KEY:
        params["key"] = GOOGLE_BOOKS_API_KEY

    try:
        res = requests.get(GOOGLE_BOOKS_URL, params=params, timeout=10)
        res.raise_for_status()
        data = res.json()
    except (requests.RequestException, ValueError):
        return []

    results = []
    for item in data.get("items", []):
        volume_info = item.get("volumeInfo", {})
        sale_info = item.get("saleInfo", {})
        if not volume_info.get("title"):
            continue

        isbn = next(
            (
                ident["identifier"]
                for ident in volume_info.get("industryIdentifiers", [])
                if ident.get("type") in ("ISBN_13", "ISBN_10")
            ),
            None,
        )

        price, source, buy_url = None, None, None
        if sale_info.get("saleability") == "FOR_SALE" and sale_info.get("retailPrice", {}).get("amount") is not None:
            price = sale_info["retailPrice"]["amount"]
            source = "google_books"
            buy_url = sale_info.get("buyLink")

        results.append(
            {
                "title": volume_info["title"],
                "author": ", ".join(volume_info.get("authors", [])) or None,
                "isbn": isbn,
                "cover_url": (volume_info.get("imageLinks") or {}).get("thumbnail"),
                "description": volume_info.get("description") or "",
                "best_price": price,
                "best_price_source": source,
                "best_price_url": buy_url or volume_info.get("infoLink"),
            }
        )
    return results


def generate_recommendations(owner_id: int) -> List[Dict[str, Any]]:
    """Builds a fresh, ranked recommendation list. Caller is responsible for caching."""
    profile = build_taste_profile(owner_id)
    books = profile["books"]
    if not books:
        return []

    owned_isbns, owned_titles = _owned_keys(books)
    dismissed_isbns, dismissed_titles = _dismissed_keys(owner_id)
    centroid = profile["centroid"]
    model = get_embeddings_model() if centroid else None

    # (query, source_genre, reason) — genres favor "because you have X",
    # authors favor "because you like Y" since there's no single source book.
    searches: List[tuple[str, Optional[str], str]] = []
    for genre in profile["top_genres"]:
        example = profile["example_by_genre"].get(genre)
        reason = f'Because you have "{example}" on your shelf' if example else f"Because you read a lot of {genre}"
        searches.append((f"subject:{genre}", genre, reason))
    for author in profile["top_authors"]:
        searches.append((f'inauthor:"{author}"', None, f"Because you like {author}"))

    seen_keys: set = set()
    candidates: List[Dict[str, Any]] = []

    for query, source_genre, reason in searches:
        for candidate in _search_google_books(query):
            title_author_key = f"{_normalize(candidate['title'])}|{_normalize(candidate['author'])}"
            dedupe_key = candidate["isbn"] or title_author_key
            if dedupe_key in seen_keys:
                continue
            if candidate["isbn"] and (candidate["isbn"] in owned_isbns or candidate["isbn"] in dismissed_isbns):
                continue
            if title_author_key in owned_titles or _normalize(candidate["title"]) in dismissed_titles:
                continue
            seen_keys.add(dedupe_key)

            match_score = DEFAULT_MATCH_SCORE
            if centroid and candidate["description"]:
                try:
                    match_score = _cosine(centroid, model.embed_query(candidate["description"]))
                except Exception:
                    pass

            candidates.append(
                {
                    "title": candidate["title"],
                    "author": candidate["author"],
                    "isbn": candidate["isbn"],
                    "cover_url": candidate["cover_url"],
                    "reason": reason,
                    "source_genre": source_genre,
                    "match_score": round(match_score, 4),
                    "best_price": candidate["best_price"],
                    "best_price_source": candidate["best_price_source"],
                    "best_price_url": candidate["best_price_url"],
                }
            )

    candidates.sort(key=lambda c: c["match_score"], reverse=True)
    return candidates[:MAX_RECOMMENDATIONS]
