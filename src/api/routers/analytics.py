from fastapi import APIRouter, Depends, Query
from src.db.crud import analytics as crud_analytics
from src.api.deps import get_current_user_id

router = APIRouter(prefix="/analytics", tags=["Analytics"])

@router.get("/summary")
def get_summary(months: int = Query(12, ge=1, le=36), owner_id: int = Depends(get_current_user_id)):
    """Resumo para o dashboard: totais, empréstimos por mês, livros mais emprestados e taxa de atraso."""
    return {
        "totals": crud_analytics.get_totals(owner_id),
        "loans_per_month": crud_analytics.get_loans_per_month(owner_id, months=months),
        "top_books": crud_analytics.get_top_books(owner_id),
        "late_return_rate_per_month": crud_analytics.get_late_return_rate_per_month(owner_id, months=months),
    }
