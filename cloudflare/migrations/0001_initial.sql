CREATE TABLE books (
  book_id INTEGER PRIMARY KEY AUTOINCREMENT,
  ISBN TEXT,
  title TEXT NOT NULL,
  author TEXT,
  author_id INTEGER,
  genre TEXT,
  cost_book REAL,
  book_status TEXT NOT NULL DEFAULT 'AVAILABLE' CHECK(book_status IN ('AVAILABLE','BORROWED','LOST','DAMAGED')),
  date_added TEXT NOT NULL DEFAULT (datetime('now')),
  cover_url TEXT
);

CREATE TABLE borrowers (
  person_id INTEGER PRIMARY KEY AUTOINCREMENT,
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL,
  relationship_type TEXT,
  phone_number TEXT,
  email TEXT,
  address TEXT,
  status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK(status IN ('ACTIVE','INACTIVE')),
  date_joined TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE transactions (
  transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
  book_id INTEGER NOT NULL REFERENCES books(book_id),
  person_id INTEGER NOT NULL REFERENCES borrowers(person_id),
  loan_date TEXT NOT NULL,
  due_date TEXT NOT NULL,
  actual_return_date TEXT
);

CREATE INDEX idx_transactions_book_id ON transactions(book_id);
CREATE INDEX idx_transactions_person_id ON transactions(person_id);
CREATE INDEX idx_transactions_actual_return_date ON transactions(actual_return_date);
