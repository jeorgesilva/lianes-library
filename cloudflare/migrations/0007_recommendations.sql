CREATE TABLE recommendation_cache (
  recommendation_id INTEGER PRIMARY KEY AUTOINCREMENT,
  owner_id INTEGER NOT NULL REFERENCES users(user_id),
  title TEXT NOT NULL,
  author TEXT,
  isbn TEXT,
  cover_url TEXT,
  reason TEXT,
  source_genre TEXT,
  match_score REAL,
  best_price REAL,
  best_price_source TEXT,
  best_price_url TEXT,
  generated_at TEXT NOT NULL DEFAULT (datetime('now')),
  expires_at TEXT NOT NULL
);

CREATE TABLE recommendation_dismissals (
  dismissal_id INTEGER PRIMARY KEY AUTOINCREMENT,
  owner_id INTEGER NOT NULL REFERENCES users(user_id),
  isbn TEXT,
  title TEXT NOT NULL,
  dismissed_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_recommendation_cache_owner ON recommendation_cache(owner_id);
CREATE INDEX idx_recommendation_dismissals_owner ON recommendation_dismissals(owner_id);
