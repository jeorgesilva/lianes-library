from fastapi import APIRouter, Depends, HTTPException

from src.ai.recommendations import generate_recommendations
from src.api.deps import get_current_user_id
from src.db.crud import recommendations as crud_recommendations

router = APIRouter(prefix="/recommendations", tags=["Recommendations"])


@router.get("/")
def list_recommendations(owner_id: int = Depends(get_current_user_id)):
    """Cached picks (7-day TTL); regenerates once automatically when the cache is empty or stale."""
    cached = crud_recommendations.list_cached(owner_id)
    if cached:
        return cached
    try:
        generated = generate_recommendations(owner_id)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Could not generate recommendations right now: {e}")
    return crud_recommendations.replace_cache(owner_id, generated)


@router.post("/refresh")
def refresh_recommendations(owner_id: int = Depends(get_current_user_id)):
    """Forces recalculation, bypassing the cache."""
    try:
        generated = generate_recommendations(owner_id)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Could not generate recommendations right now: {e}")
    return crud_recommendations.replace_cache(owner_id, generated)


@router.post("/{recommendation_id}/dismiss")
def dismiss_recommendation(recommendation_id: int, owner_id: int = Depends(get_current_user_id)):
    """Records 'not interested' so this title stops resurfacing, and drops it from the cache."""
    item = crud_recommendations.get_cache_item(owner_id, recommendation_id)
    if not item:
        raise HTTPException(status_code=404, detail="Recommendation not found")
    crud_recommendations.create_dismissal(owner_id, item["title"], item.get("isbn"))
    crud_recommendations.delete_cache_item(owner_id, recommendation_id)
    return {"detail": "Dismissed."}
