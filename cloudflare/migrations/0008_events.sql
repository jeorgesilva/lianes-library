CREATE TABLE literary_events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL,           -- 'ticketmaster' | 'manual'
  external_id TEXT,
  title TEXT NOT NULL,
  description TEXT,
  venue_name TEXT,
  city TEXT,
  event_date TEXT NOT NULL,
  url TEXT,
  image_url TEXT,
  cached_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE user_event_preferences (
  owner_id INTEGER PRIMARY KEY REFERENCES users(user_id),
  city TEXT,
  radius_km INTEGER DEFAULT 30
);

CREATE INDEX idx_literary_events_date ON literary_events(event_date);
CREATE UNIQUE INDEX idx_literary_events_source_external ON literary_events(source, external_id);
