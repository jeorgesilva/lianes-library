CREATE TABLE users (
  user_id INTEGER PRIMARY KEY AUTOINCREMENT,
  first_name TEXT NOT NULL,
  last_name TEXT,
  email TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

ALTER TABLE books ADD COLUMN owner_id INTEGER REFERENCES users(user_id);
ALTER TABLE borrowers ADD COLUMN owner_id INTEGER REFERENCES users(user_id);
ALTER TABLE transactions ADD COLUMN owner_id INTEGER REFERENCES users(user_id);

CREATE INDEX idx_books_owner_id ON books(owner_id);
CREATE INDEX idx_borrowers_owner_id ON borrowers(owner_id);
CREATE INDEX idx_transactions_owner_id ON transactions(owner_id);
