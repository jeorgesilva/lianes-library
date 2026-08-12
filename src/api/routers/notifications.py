from fastapi import APIRouter, Depends, HTTPException
from src.db.crud import notifications as crud_notifications
from src.api.deps import get_current_user_id

router = APIRouter(prefix="/notifications", tags=["Notifications"])


@router.get("/")
def list_notifications(owner_id: int = Depends(get_current_user_id)):
    """Lista a central de notificações in-app do usuário (não lidas primeiro)."""
    return crud_notifications.list_notifications(owner_id)


@router.post("/{notification_id}/read")
def mark_read(notification_id: int, owner_id: int = Depends(get_current_user_id)):
    try:
        return crud_notifications.mark_read(owner_id, notification_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
