export function createState(db) {
  return {
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
