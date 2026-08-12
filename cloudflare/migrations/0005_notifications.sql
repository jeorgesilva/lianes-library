CREATE TABLE notifications (
  notification_id INTEGER PRIMARY KEY AUTOINCREMENT,
  owner_id INTEGER NOT NULL REFERENCES users(user_id),
  type TEXT NOT NULL CHECK(type IN ('OVERDUE_LOAN','PRICE_DROP','BORROW_DUE_SOON','EVENT_NEARBY')),
  title TEXT NOT NULL,
  body TEXT,
  link TEXT,
  read_at TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_notifications_owner ON notifications(owner_id, read_at);
