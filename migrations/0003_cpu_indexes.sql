-- Non-destructive indexes for active watch filtering and alert joins.
CREATE INDEX IF NOT EXISTS stage_watch_free_start
  ON stage_watch(last_price, last_stage_type, CAST(last_start_time AS INTEGER), last_seen_at);
CREATE INDEX IF NOT EXISTS stage_watch_live
  ON stage_watch(first_seen_live, last_price, consecutive_free_observations);
