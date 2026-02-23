from datetime import date

from pydantic import BaseModel


class LoanCreate(BaseModel):
    book_id: int
    user_id: int
    loan_date: date
    expected_return_date: date | None = None


class LoanOut(LoanCreate):
    id: int
    actual_return_date: date | None = None
