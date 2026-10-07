CREATE TABLE IF NOT EXISTS alerts (
  alert_key TEXT PRIMARY KEY,
  chain TEXT NOT NULL,
  slug TEXT NOT NULL,
  stage_uuid TEXT NOT NULL,
  sent_at INTEGER NOT NULL,
  opensea_url TEXT
);

CREATE TABLE IF NOT EXISTS drop_cache (
  drop_key TEXT PRIMARY KEY,
  active_uuid TEXT,
  next_uuid TEXT,
  last_detail_at INTEGER NOT NULL DEFAULT 0,
  last_seen_at INTEGER NOT NULL DEFAULT 0,
  contract_address TEXT,
  opensea_url TEXT
);

CREATE TABLE IF NOT EXISTS misses (
  slug TEXT PRIMARY KEY,
  checked_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);
