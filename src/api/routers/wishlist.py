from fastapi import APIRouter, Depends, HTTPException
from typing import Optional
from pydantic import BaseModel
from src.db.crud import wishlist as crud_wishlist
from src.api.deps import get_current_user_id

router = APIRouter(prefix="/wishlist", tags=["Wishlist"])


# --- Pydantic Schemas ---
class WishlistItemCreate(BaseModel):
    title: str
    author: Optional[str] = None
    isbn: Optional[str] = None
    cover_url: Optional[str] = None
    target_price: Optional[float] = None
    notes: Optional[str] = None


class WishlistItemUpdate(BaseModel):
    target_price: Optional[float] = None
    notes: Optional[str] = None


# --- Endpoints ---
@router.get("/")
def list_wishlist_items(status: Optional[str] = None, owner_id: int = Depends(get_current_user_id)):
    """Lista os itens da wishlist, com o menor e o mais recente preço encontrado. status: ACTIVE | PURCHASED | ARCHIVED."""
    return crud_wishlist.list_wishlist_items(owner_id, status=status)


@router.post("/", status_code=201)
def create_wishlist_item(payload: WishlistItemCreate, owner_id: int = Depends(get_current_user_id)):
    """Adiciona um título à wishlist para monitoramento de preço."""
    return crud_wishlist.create_wishlist_item(
        owner_id=owner_id,
        title=payload.title,
        author=payload.author,
        isbn=payload.isbn,
        cover_url=payload.cover_url,
        target_price=payload.target_price,
        notes=payload.notes,
    )


@router.get("/{wishlist_item_id}")
def get_wishlist_item(wishlist_item_id: int, owner_id: int = Depends(get_current_user_id)):
    item = crud_wishlist.get_wishlist_item(owner_id, wishlist_item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Wishlist item not found")
    return item


@router.patch("/{wishlist_item_id}")
def update_wishlist_item(wishlist_item_id: int, payload: WishlistItemUpdate, owner_id: int = Depends(get_current_user_id)):
    try:
        return crud_wishlist.update_wishlist_item(
            owner_id, wishlist_item_id,
            target_price=payload.target_price,
            notes=payload.notes,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{wishlist_item_id}/purchased")
def mark_purchased(wishlist_item_id: int, owner_id: int = Depends(get_current_user_id)):
    """Marca o item como comprado — sai do monitoramento diário de preço."""
    try:
        return crud_wishlist.set_status(owner_id, wishlist_item_id, "PURCHASED")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{wishlist_item_id}/archive")
def archive_item(wishlist_item_id: int, owner_id: int = Depends(get_current_user_id)):
    """Arquiva o item sem marcá-lo como comprado — também sai do monitoramento."""
    try:
        return crud_wishlist.set_status(owner_id, wishlist_item_id, "ARCHIVED")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{wishlist_item_id}/reactivate")
def reactivate_item(wishlist_item_id: int, owner_id: int = Depends(get_current_user_id)):
    try:
        return crud_wishlist.set_status(owner_id, wishlist_item_id, "ACTIVE")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{wishlist_item_id}/snapshots")
def list_price_snapshots(wishlist_item_id: int, owner_id: int = Depends(get_current_user_id)):
    """Histórico de preço do item, usado para o sparkline no card."""
    try:
        return crud_wishlist.list_price_snapshots(owner_id, wishlist_item_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.delete("/{wishlist_item_id}")
def delete_wishlist_item(wishlist_item_id: int, owner_id: int = Depends(get_current_user_id)):
    try:
        crud_wishlist.delete_wishlist_item(owner_id, wishlist_item_id)
        return {"detail": "Wishlist item removed."}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
