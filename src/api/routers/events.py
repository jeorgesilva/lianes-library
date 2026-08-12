from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from src.api.deps import get_current_user_id
from src.db.crud import events as crud_events

router = APIRouter(prefix="/events", tags=["Events"])


class EventPreferencesUpdate(BaseModel):
    city: Optional[str] = None
    radius_km: Optional[int] = None


class ManualEventCreate(BaseModel):
    title: str
    event_date: str
    description: Optional[str] = None
    venue_name: Optional[str] = None
    city: Optional[str] = None
    url: Optional[str] = None
    image_url: Optional[str] = None


@router.get("/")
def list_events(city: Optional[str] = None, owner_id: int = Depends(get_current_user_id)):
    """Upcoming literary events. Without a city filter, falls back to the owner's saved city preference."""
    if city is None:
        prefs = crud_events.get_preferences(owner_id)
        city = prefs["city"] if prefs else None
    return crud_events.list_events(city=city)


@router.get("/preferences")
def get_preferences(owner_id: int = Depends(get_current_user_id)):
    return crud_events.get_preferences(owner_id) or {"owner_id": owner_id, "city": None, "radius_km": 30}


@router.put("/preferences")
def set_preferences(payload: EventPreferencesUpdate, owner_id: int = Depends(get_current_user_id)):
    """Sets the city the weekly discovery job watches (section 4.5) and that this list falls back to."""
    return crud_events.set_preferences(owner_id, payload.city, payload.radius_km)


@router.post("/", status_code=201)
def create_manual_event(payload: ManualEventCreate, owner_id: int = Depends(get_current_user_id)):
    """Manual fallback (section 4.5) — keeps the feature usable even where the search API has no coverage."""
    return crud_events.create_manual_event(
        title=payload.title,
        event_date=payload.event_date,
        description=payload.description,
        venue_name=payload.venue_name,
        city=payload.city,
        url=payload.url,
        image_url=payload.image_url,
    )


@router.delete("/{event_id}")
def delete_event(event_id: int, owner_id: int = Depends(get_current_user_id)):
    event = crud_events.get_event(event_id)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    if event["source"] != "manual":
        raise HTTPException(status_code=400, detail="Only manually-added events can be removed.")
    crud_events.delete_manual_event(event_id)
    return {"detail": "Event removed."}
