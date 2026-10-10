import { detailRefreshSeconds, observeStage } from "./watch.js";
import { getConfig } from "./current.js";
import { notifyCandidate, sendTelegram } from "./signal.js";
import { createState } from "./state.js";
import { OpenSeaNotFound, Tide } from "./tide.js";
import { dropKey, mapLimit, parseWei } from "./util.js";

const DROP_TYPES = ["featured", "upcoming", "recently_minted"];

function mergeSummary(map, row) {
  if (!row?.collection_slug || !row?.chain) return;
  const key = dropKey(row);
  const prev = map.get(key);
  if (!prev) {
    map.set(key, row);
    return;
  }
  map.set(key, {
    ...prev,
    ...row,
    active_stage: row.active_stage || prev.active_stage || null,
    next_stage: row.next_stage || prev.next_stage || null,
  });
}

function cacheRow(drop, nowSec, lastDetailAt) {
  return {
    dropKey: dropKey(drop),
    activeUuid: drop.active_stage?.uuid || null,
    nextUuid: drop.next_stage?.uuid || null,
    lastDetailAt,
    lastSeenAt: nowSec,
    contractAddress: drop.contract_address || null,
    openseaUrl: drop.opensea_url || null,
  };
}

async function notifyStages(drop, stages, deps) {
  let sent = 0;
  const unique = new Map();
  for (const stage of stages || []) if (stage?.uuid) unique.set(stage.uuid, stage);
  for (const stage of unique.values()) {
    if (await notifyCandidate({ drop, stage, ...deps })) sent += 1;
  }
  return sent;
}

async function maybeValidateChains({ api, state, config, nowSec }) {
  const raw = await state.getSetting("chains_checked_at");
  const last = Number(raw || 0);
  if (nowSec - last < 86400) return;
  try {
    const payload = await api.getChains();
    const supported = new Set((payload.chains || []).map((x) => x.chain));
    const missing = config.chains.filter((x) => !supported.has(x));
    if (missing.length) throw new Error(`OpenSea does not report these target chains as supported: ${missing.join(", ")}`);
    await state.setSetting("chains_checked_at", nowSec, nowSec);
  } catch (err) {
    if (/does not report these target chains/.test(err.message)) throw err;
    console.warn(`chain validation skipped: ${err.message}`);
  }
}

async function calendarScan({ api, state, config, env, nowSec, scanId }) {
  let feedSuccesses = 0;
  const feedErrors = [];
  const pages = await Promise.all(DROP_TYPES.map(async (type) => {
    try {
      const rows = await api.listDrops(type, config.chains);
      feedSuccesses += 1;
      return rows;
    } catch (err) {
      feedErrors.push(`${type}: ${err.message}`);
      console.warn(`drops ${type}: ${err.message}`);
      return [];
    }
  }));
  if (feedSuccesses === 0) {
    throw new Error(`all OpenSea drop feeds failed: ${feedErrors.join(" | ")}`);
  }
  const summaries = new Map();
  for (const rows of pages) for (const row of rows) mergeSummary(summaries, row);
  const drops = [...summaries.values()];
  console.log(`calendar ${drops.length} unique drops`);

  let sent = 0;
  // Record summaries before details, but defer Telegram decisions until details finish.
  // Conflicting sources reset stability without adding a second scan observation.
  for (const drop of drops) {
    const stages = new Map([drop.active_stage, drop.next_stage].filter(stage => stage?.uuid).map(stage => [stage.uuid, stage]));
    for (const stage of stages.values()) {
      const price = parseWei(stage.price);
      if (price != null && price !== 0n) {
        sent += await notifyStages(drop, [stage], { state, env, nowSec, scanId, config });
      } else {
        await observeStage({ drop, stage, state, nowSec, scanId });
      }
    }
  }
  const alertedWatches = await state.getAlertedWatches();
  for (const watch of alertedWatches) {
    if (watch.last_end_time != null && Number(watch.last_end_time) <= nowSec) continue;
    const key = `${watch.chain}:${watch.slug}`;
    if (!summaries.has(key)) {
      const drop = { chain: watch.chain, collection_slug: watch.slug };
      summaries.set(key, drop);
      drops.push(drop);
    }
  }
  const observations = new Map(drops.map(drop => [dropKey(drop), { drop, stages: [drop.active_stage, drop.next_stage] }]));

  const keys = drops.map(dropKey);
  const caches = await state.getCaches(keys);
  const due = drops
    .map((drop) => ({ drop, cached: caches.get(dropKey(drop)) || null }))
    .filter(({ drop, cached }) => {
      if (!cached) return true;
      const changed = (cached.active_uuid || null) !== (drop.active_stage?.uuid || null)
        || (cached.next_uuid || null) !== (drop.next_stage?.uuid || null);
      const stale = nowSec - Number(cached.last_detail_at || 0) >= detailRefreshSeconds(drop, alertedWatches, nowSec, config);
      return changed || stale;
    })
    .sort((a, b) => Number(a.cached?.last_detail_at || 0) - Number(b.cached?.last_detail_at || 0))
    .slice(0, config.maxDetailsPerScan);

  await mapLimit(due, 4, async ({ drop }) => {
    try {
      const detail = await api.getDrop(drop.collection_slug);
      observations.set(dropKey(detail), { drop: detail, stages: detail.stages || [] });
      await state.upsertCache(cacheRow(detail, nowSec, nowSec));
      await state.clearMiss(detail.collection_slug);
    } catch (err) {
      if (err instanceof OpenSeaNotFound) {
        await state.markMiss(drop.collection_slug, nowSec);
      } else {
        console.warn(`detail ${drop.collection_slug}: ${err.message}`);
      }
    }
  });

  const detailedKeys = new Set(due.map(({ drop }) => dropKey(drop)));
  await mapLimit(drops.filter((drop) => !detailedKeys.has(dropKey(drop))), 4, async (drop) => {
    const cached = caches.get(dropKey(drop));
    const changed = !cached
      || (cached.active_uuid || null) !== (drop.active_stage?.uuid || null)
      || (cached.next_uuid || null) !== (drop.next_stage?.uuid || null);
    if (changed) {
      await state.upsertCache(cacheRow(drop, nowSec, Number(cached?.last_detail_at || 0)));
    }
  });

  for (const { drop, stages } of observations.values()) {
    sent += await notifyStages(drop, stages, {
      state, env, nowSec, scanId, config, allowFreeAllowlists: config.alertFreeAllowlists,
      maxAlertPerWallet: config.maxAlertPerWallet, minAlertSupply: config.minAlertSupply,
    });
  }
  return { sent, summaries };
}

async function collectionSweep({ api, state, config, env, nowSec, summaries, scanId }) {
  const raw = await state.getSetting("collection_sweep_at");
  const last = Number(raw || 0);
  if (nowSec - last < config.collectionSweepSeconds || config.maxFallbackDetails === 0) return 0;

  const pages = await Promise.all(config.chains.map(async (chain) => {
    try {
      const payload = await api.listCollections(chain, config.collectionLimit);
      return (payload.collections || []).map((row) => ({ ...row, _chain: chain }));
    } catch (err) {
      console.warn(`collections ${chain}: ${err.message}`);
      return [];
    }
  }));

  const candidates = new Map();
  for (const rows of pages) {
    for (const row of rows) {
      const slug = row.collection || row.collection_slug;
      if (!slug) continue;
      const chain = row.contracts?.find((x) => config.chains.includes(x.chain))?.chain || row._chain;
      const key = `${chain}:${slug}`;
      if (summaries.has(key)) continue;
      if (!candidates.has(slug)) candidates.set(slug, { slug, chain });
    }
  }

  const slugs = [...candidates.keys()];
  const misses = await state.getMisses(slugs);
  const filtered = slugs
    .filter((slug) => nowSec - Number(misses.get(slug) || 0) >= config.missTtlSeconds)
    .slice(0, config.maxFallbackDetails);

  let sent = 0;
  await mapLimit(filtered, 4, async (slug) => {
    try {
      const detail = await api.getDrop(slug);
      if (!config.chains.includes(detail.chain)) return;
      sent += await notifyStages(detail, detail.stages || [], {
        state, env, nowSec, scanId, config, allowFreeAllowlists: config.alertFreeAllowlists, maxAlertPerWallet: config.maxAlertPerWallet, minAlertSupply: config.minAlertSupply,
      });
      await state.upsertCache(cacheRow(detail, nowSec, nowSec));
      await state.clearMiss(slug);
    } catch (err) {
      if (err instanceof OpenSeaNotFound) await state.markMiss(slug, nowSec);
      else console.warn(`fallback ${slug}: ${err.message}`);
    }
  });

  await state.setSetting("collection_sweep_at", nowSec, nowSec);
  console.log(`collection sweep ${filtered.length} detail checks`);
  return sent;
}

export async function runScan(env, deps = {}) {
  const config = getConfig(env);
  const nowSec = deps.nowSec ?? Math.floor(Date.now() / 1000);
  const scanId = deps.scanId ?? crypto.randomUUID();
  const state = deps.state || createState(env.DB);
  const api = deps.api || new Tide({ apiKey: env.OPENSEA_API_KEY });

  await maybeValidateChains({ api, state, config, nowSec });
  const first = await calendarScan({ api, state, config, env, nowSec, scanId });
  const extra = await collectionSweep({ api, state, config, env, nowSec, summaries: first.summaries, scanId });
  console.log(`done alerts=${first.sent + extra}`);
  return { alerts: first.sent + extra, drops: first.summaries.size };
}

function jsonResponse(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

function isAuthorized(request, env) {
  if (!env.CONTROL_TOKEN) return false;
  return request.headers.get("authorization") === `Bearer ${env.CONTROL_TOKEN}`;
}

export default {
  async scheduled(_controller, env, ctx) {
    ctx.waitUntil(runScan(env).catch((err) => console.error("scan failed:", err.message)));
  },

  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/" && request.method === "GET") {
      return new Response("amber-keel", {
        status: 200,
        headers: { "content-type": "text/plain; charset=utf-8", "cache-control": "no-store" },
      });
    }

    if (url.pathname !== "/run" && url.pathname !== "/test-telegram" && url.pathname !== "/diag") {
      return jsonResponse({ ok: false, error: "not found" }, 404);
    }

    if (request.method !== "POST") {
      return jsonResponse({ ok: false, error: "method not allowed" }, 405);
    }

    if (!env.CONTROL_TOKEN) {
      return jsonResponse({ ok: false, error: "manual control is not configured" }, 503);
    }

    if (!isAuthorized(request, env)) {
      return jsonResponse({ ok: false, error: "unauthorized" }, 401);
    }

    try {
      if (url.pathname === "/test-telegram") {
        await sendTelegram(env, "amber-keel test ✅");
        return jsonResponse({ ok: true, telegram: "sent" });
      }

      if (url.pathname === "/diag") {
        const config = getConfig(env);
        const api = new Tide({ apiKey: env.OPENSEA_API_KEY });
        const checks = {};
        try {
          const chains = await api.getChains();
          checks.chains = {
            ok: true,
            count: (chains.chains || []).length,
            targets: config.chains.map((chain) => ({
              chain,
              supported: (chains.chains || []).some((row) => row.chain === chain),
            })),
          };
        } catch (err) {
          checks.chains = { ok: false, error: err.message };
        }

        for (const type of DROP_TYPES) {
          try {
            const rows = await api.listDrops(type, config.chains);
            checks[type] = { ok: true, count: rows.length };
          } catch (err) {
            checks[type] = { ok: false, error: err.message };
          }
        }

        return jsonResponse({ ok: true, checks });
      }

      const result = await runScan(env);
      return jsonResponse({ ok: true, ...result });
    } catch (err) {
      console.error(`manual ${url.pathname} failed:`, err.message);
      return jsonResponse({ ok: false, error: err.message }, 500);
    }
  },
};
