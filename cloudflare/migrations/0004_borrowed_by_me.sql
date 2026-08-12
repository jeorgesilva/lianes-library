CREATE TABLE borrow_records (
  borrow_id INTEGER PRIMARY KEY AUTOINCREMENT,
  owner_id INTEGER NOT NULL REFERENCES users(user_id),
  title TEXT NOT NULL,
  author TEXT,
  isbn TEXT,
  cover_url TEXT,
  lender_name TEXT NOT NULL,
  borrowed_date TEXT NOT NULL,
  due_date TEXT,
  returned_date TEXT,
  reminder_lead_days INTEGER NOT NULL DEFAULT 3,
  notes TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_borrow_records_owner ON borrow_records(owner_id);
CREATE INDEX idx_borrow_records_due_date ON borrow_records(due_date) WHERE returned_date IS NULL;
