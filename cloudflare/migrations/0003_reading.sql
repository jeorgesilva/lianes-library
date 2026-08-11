CREATE TABLE reading_log (
  reading_log_id INTEGER PRIMARY KEY AUTOINCREMENT,
  owner_id INTEGER NOT NULL REFERENCES users(user_id),
  book_id INTEGER REFERENCES books(book_id),
  title TEXT NOT NULL,
  author TEXT,
  cover_url TEXT,
  status TEXT NOT NULL DEFAULT 'WANT_TO_READ' CHECK(status IN ('WANT_TO_READ','READING','READ','DNF')),
  started_at TEXT,
  finished_at TEXT,
  rating INTEGER CHECK(rating BETWEEN 1 AND 5),
  current_page INTEGER,
  total_pages INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE journal_entries (
  entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
  owner_id INTEGER NOT NULL REFERENCES users(user_id),
  reading_log_id INTEGER NOT NULL REFERENCES reading_log(reading_log_id),
  entry_date TEXT NOT NULL DEFAULT (datetime('now')),
  content TEXT NOT NULL,
  page_at_entry INTEGER,
  mood TEXT,
  contains_spoilers INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_reading_log_owner_status ON reading_log(owner_id, status);
CREATE INDEX idx_journal_entries_reading_log ON journal_entries(reading_log_id, entry_date);
