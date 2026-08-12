CREATE TABLE wishlist_items (
  wishlist_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
  owner_id INTEGER NOT NULL REFERENCES users(user_id),
  title TEXT NOT NULL,
  author TEXT,
  isbn TEXT,
  cover_url TEXT,
  target_price REAL,
  notes TEXT,
  status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK(status IN ('ACTIVE','PURCHASED','ARCHIVED')),
  added_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE wishlist_price_snapshots (
  snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
  wishlist_item_id INTEGER NOT NULL REFERENCES wishlist_items(wishlist_item_id),
  source TEXT NOT NULL,
  price REAL NOT NULL,
  currency TEXT NOT NULL DEFAULT 'BRL',
  url TEXT,
  checked_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_wishlist_owner ON wishlist_items(owner_id);
CREATE INDEX idx_wishlist_snapshots_item ON wishlist_price_snapshots(wishlist_item_id, checked_at);
