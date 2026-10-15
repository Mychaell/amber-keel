import { isMutedChain, isPracticeChain } from "./current.js";

function watchStatement(db, row) {
  const columns = Object.keys(row).filter(x => x !== "alerted");
  const sameConfig = ["last_price", "last_start_time", "last_end_time", "last_max_per_wallet", "last_stage_type"]
    .map(x => `stage_watch.${x} IS excluded.${x}`).join(" AND ");
  const update = columns.filter(x => x !== "stage_key").map(x => {
    if (["ever_seen_paid", "paid_warning_sent"].includes(x)) return `${x} = MAX(stage_watch.${x}, excluded.${x})`;
    if (["first_seen_at", "first_seen_live"].includes(x)) return `${x} = stage_watch.${x}`;
    if (x === "consecutive_free_observations") return `${x} = CASE
      WHEN excluded.last_price IS NOT '0' THEN 0
      WHEN ${sameConfig} AND stage_watch.first_free_at IS NOT NULL THEN
        stage_watch.${x} + CASE WHEN stage_watch.last_observed_scan_at = excluded.last_observed_scan_at THEN 0 ELSE 1 END
      ELSE 1 END`;
    if (x === "first_free_at") return `${x} = CASE WHEN excluded.last_price IS NOT '0' THEN NULL
      WHEN ${sameConfig} THEN COALESCE(stage_watch.${x}, excluded.${x}) ELSE excluded.last_seen_at END`;
    return `${x} = excluded.${x}`;
  });
  return db.prepare(`INSERT INTO stage_watch (${columns.join(",")}) VALUES (${columns.map(() => "?").join(",")})
    ON CONFLICT(stage_key) DO UPDATE SET ${update.join(",")}
    WHERE excluded.last_seen_at >= stage_watch.last_seen_at`).bind(...columns.map(x => row[x]));
}

export function createState(db) {
  return {
    async getStageWatches(keys) {
      const rows = new Map();
      for (let i = 0; i < keys.length; i += 80) {
        const part = keys.slice(i, i + 80);
        const result = await db.prepare(`SELECT w.*, a.alert_key AS alerted FROM stage_watch w
          LEFT JOIN alerts a ON a.alert_key = w.stage_key
          WHERE w.stage_key IN (${part.map(() => "?").join(",")})`).bind(...part).all();
        for (const row of result.results || []) rows.set(row.stage_key, { ...row, alerted: Boolean(row.alerted) });
      }
      return rows;
    },

    async saveStageWatches(rows) {
      for (let i = 0; i < rows.length; i += 40) {
        await db.batch(rows.slice(i, i + 40).map(row => watchStatement(db, row)));
      }
    },

    async getWatchCandidates(nowSec, config) {
      const chains = config.chains.filter(chain => !isMutedChain(config, chain));
      if (!chains.length) return [];
      const practiceChains = chains.filter(chain => isPracticeChain(config, chain));
      const practiceSql = practiceChains.length ? `w.chain IN (${practiceChains.map(() => "?").join(",")})` : "0";
      const types = config.alertFreeAllowlists
        ? "'public_sale','presale','allowlist','whitelist','signed_presale'" : "'public_sale'";
      const result = await db.prepare(`WITH relevant AS (
          SELECT alert_key AS stage_key FROM alerts
          UNION SELECT stage_key FROM stage_watch
            WHERE last_price = '0' AND last_stage_type IN (${types})
              AND CAST(last_start_time AS INTEGER) > ? AND CAST(last_start_time AS INTEGER) <= ?
          UNION SELECT stage_key FROM stage_watch
            WHERE first_seen_live = 1 AND last_price = '0' AND last_stage_type IN (${types})
        )
        SELECT w.*, a.alert_key AS alerted FROM relevant r
        JOIN stage_watch w ON w.stage_key = r.stage_key
        LEFT JOIN alerts a ON a.alert_key = w.stage_key
        WHERE (w.last_end_time IS NULL OR CAST(w.last_end_time AS INTEGER) > ?)
        AND w.chain IN (${chains.map(() => "?").join(",")})
        AND (${practiceSql} OR a.alert_key IS NOT NULL OR w.last_max_per_wallet IS NULL OR CAST(w.last_max_per_wallet AS INTEGER) <= ?)
        AND (a.alert_key IS NOT NULL OR w.last_stage_type = 'public_sale' OR NOT (${practiceSql}))
        AND w.last_seen_at <= ?
        ORDER BY (a.alert_key IS NOT NULL) DESC,
          (CAST(w.last_start_time AS INTEGER) <= ?) DESC, CAST(w.last_start_time AS INTEGER), w.last_seen_at`)
        .bind(nowSec, nowSec + config.watchWindowSeconds, nowSec, ...chains,
          ...practiceChains, config.maxAlertPerWallet, ...practiceChains, nowSec - config.watchRefreshSeconds, nowSec).all();
      return (result.results || []).map(row => ({ ...row, alerted: Boolean(row.alerted) }));
    },

    async getStageWatch(key) {
      return db.prepare("SELECT * FROM stage_watch WHERE stage_key = ?1").bind(key).first();
    },

    async saveStageWatch(row) {
      await watchStatement(db, row).run();
    },

    async closeStageWatch(key, nowSec) {
      // Retain observation history; a later observation restores the API end time.
      await db.prepare(`UPDATE stage_watch SET last_end_time = ?
        WHERE stage_key = ? AND last_seen_at <= ?`)
        .bind(String(nowSec), key, nowSec).run();
    },

    async closeCollectionWatches(drop, nowSec) {
      // Use the existing watch expiry mechanism without deleting history.
      await db.prepare(`UPDATE stage_watch SET last_end_time = ?
        WHERE chain = ? AND slug = ? AND last_seen_at <= ?
        AND (last_end_time IS NULL OR CAST(last_end_time AS INTEGER) > ?)`)
        .bind(String(nowSec), String(drop.chain).toLowerCase(), drop.collection_slug, nowSec, nowSec).run();
    },

    async getAlertedWatches() {
      const result = await db.prepare(`SELECT w.* FROM stage_watch w JOIN alerts a ON a.alert_key = w.stage_key`).all();
      return result.results || [];
    },

    async hasAlert(key) {
      const row = await db.prepare("SELECT 1 AS yes FROM alerts WHERE alert_key = ?1 LIMIT 1").bind(key).first();
      return Boolean(row);
    },

    async markAlert({ key, chain, slug, stageUuid, sentAt, openseaUrl }) {
      await db.prepare(
        "INSERT OR IGNORE INTO alerts (alert_key, chain, slug, stage_uuid, sent_at, opensea_url) VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
      ).bind(key, chain, slug, stageUuid, sentAt, openseaUrl || null).run();
    },

    async getCaches(keys) {
      if (!keys.length) return new Map();
      const map = new Map();
      for (let i = 0; i < keys.length; i += 80) {
        const part = keys.slice(i, i + 80);
        const placeholders = part.map((_, j) => `?${j + 1}`).join(",");
        const res = await db.prepare(`SELECT * FROM drop_cache WHERE drop_key IN (${placeholders})`).bind(...part).all();
        for (const row of res.results || []) map.set(row.drop_key, row);
      }
      return map;
    },

    async upsertCache(row) {
      await db.prepare(`
        INSERT INTO drop_cache (drop_key, active_uuid, next_uuid, last_detail_at, last_seen_at, contract_address, opensea_url)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        ON CONFLICT(drop_key) DO UPDATE SET
          active_uuid = excluded.active_uuid,
          next_uuid = excluded.next_uuid,
          last_detail_at = excluded.last_detail_at,
          last_seen_at = excluded.last_seen_at,
          contract_address = excluded.contract_address,
          opensea_url = excluded.opensea_url
      `).bind(
        row.dropKey,
        row.activeUuid || null,
        row.nextUuid || null,
        row.lastDetailAt || 0,
        row.lastSeenAt || 0,
        row.contractAddress || null,
        row.openseaUrl || null
      ).run();
    },

    async getMisses(slugs) {
      if (!slugs.length) return new Map();
      const map = new Map();
      for (let i = 0; i < slugs.length; i += 80) {
        const part = slugs.slice(i, i + 80);
        const placeholders = part.map((_, j) => `?${j + 1}`).join(",");
        const res = await db.prepare(`SELECT slug, checked_at FROM misses WHERE slug IN (${placeholders})`).bind(...part).all();
        for (const row of res.results || []) map.set(row.slug, Number(row.checked_at));
      }
      return map;
    },

    async markMiss(slug, checkedAt) {
      await db.prepare(`
        INSERT INTO misses (slug, checked_at) VALUES (?1, ?2)
        ON CONFLICT(slug) DO UPDATE SET checked_at = excluded.checked_at
      `).bind(slug, checkedAt).run();
    },

    async clearMiss(slug) {
      await db.prepare("DELETE FROM misses WHERE slug = ?1").bind(slug).run();
    },

    async getSetting(key) {
      const row = await db.prepare("SELECT value FROM settings WHERE key = ?1 LIMIT 1").bind(key).first();
      return row?.value ?? null;
    },

    async setSetting(key, value, updatedAt) {
      await db.prepare(`
        INSERT INTO settings (key, value, updated_at) VALUES (?1, ?2, ?3)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
      `).bind(key, String(value), updatedAt).run();
    }
  };
}
