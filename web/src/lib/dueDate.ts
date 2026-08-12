import type { ActiveLoan, BorrowRecord } from "./api";

export type DueTone = "success" | "warning" | "danger" | "neutral";

/**
 * Tone + label for a book *I* borrowed from someone else (`borrowed_by_me`).
 * Moved out of BorrowedByMe.tsx (unchanged logic) so the Dashboard bento
 * cell can reuse the exact same due-date rules instead of re-implementing
 * them slightly differently.
 */
export function borrowTone(record: BorrowRecord): { tone: DueTone; label: string } {
  if (record.returned_date) return { tone: "neutral", label: "Returned" };
  if (record.days_until_due == null) return { tone: "neutral", label: "No due date" };
  if (record.days_until_due < 0) return { tone: "danger", label: `${Math.abs(record.days_until_due)}d overdue` };
  if (record.days_until_due <= record.reminder_lead_days) return { tone: "warning", label: `Due in ${record.days_until_due}d` };
  return { tone: "success", label: `Due in ${record.days_until_due}d` };
}

/**
 * Tone + label for a book *lent out* to someone else (loans/transactions).
 * `days_overdue` comes straight from the SQL
 * (`julianday('now') - julianday(due_date)`), so a *negative* value means
 * the loan isn't due yet — confirmed against src/db/crud/loans.py.
 */
export function loanTone(loan: ActiveLoan): { tone: DueTone; label: string } {
  if (loan.days_overdue > 0) return { tone: "danger", label: `${loan.days_overdue}d overdue` };
  if (loan.days_overdue === 0) return { tone: "warning", label: "Due today" };
  if (loan.days_overdue >= -2) return { tone: "warning", label: `Due in ${Math.abs(loan.days_overdue)}d` };
  return { tone: "success", label: `Due in ${Math.abs(loan.days_overdue)}d` };
}
