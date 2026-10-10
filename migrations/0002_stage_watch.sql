CREATE TABLE stage_watch (
  stage_key TEXT PRIMARY KEY,
  chain TEXT NOT NULL,
  slug TEXT NOT NULL,
  stage_uuid TEXT NOT NULL,
  first_seen_at INTEGER NOT NULL,
  first_free_at INTEGER,
  last_seen_at INTEGER NOT NULL,
  last_observed_scan_at TEXT NOT NULL,
  consecutive_free_observations INTEGER NOT NULL DEFAULT 0,
  last_price TEXT,
  last_start_time TEXT,
  last_end_time TEXT,
  last_max_per_wallet TEXT,
  last_stage_type TEXT,
  last_changed_at INTEGER NOT NULL,
  ever_seen_paid INTEGER NOT NULL DEFAULT 0,
  paid_warning_sent INTEGER NOT NULL DEFAULT 0,
  first_seen_live INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX stage_watch_drop ON stage_watch(chain, slug);
