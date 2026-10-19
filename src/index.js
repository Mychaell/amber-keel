import { detailRefreshSeconds } from "./watch.js";
import { getConfig, isPracticeChain, isMutedChain, chainPolicy } from "./current.js";
import { notifyCandidate, sendTelegram } from "./signal.js";
import { createState } from "./state.js";
import { OpenSeaNotFound, Tide } from "./tide.js";
import { alertKey, dropKey, isSoldOut, isFreePublicStage, mapLimit, parseWei } from "./util.js";

const DROP_TYPES = ["featured", "upcoming", "recently_minted"];

function scanState(env, deps, metrics) {
  const state = deps.state || createState(env.DB, metrics);
  const writes = new Set(['saveStageWatch', 'saveStageWatches', 'closeStageWatch', 'closeCollectionWatches',
    'markAlert', 'reserveAlert', 'confirmAlert', 'releaseAlert', 'upsertCache', 'markMiss', 'clearMiss', 'setSetting']);
  return new Proxy(state, { get(target, name) {
    const fn = target[name];
    if (typeof fn !== 'function' || !writes.has(name)) return fn;
    return async (...args) => {
      try {
        const result = await fn(...args);
        if (deps.state) metrics.writes += name === 'saveStageWatches' ? args[0].length : 1;
        return result;
      } catch (err) {
        metrics.writeFailures++;
        console.error(`D1 quota/write failure (${name}): ${err.message}`);
        err.d1WriteFailure = true;
        throw err;
      }
    };
  } });
}

function scanMetrics() {
  return { writes: 0, rowsWritten: 0, writeFailures: 0, normalCandidates: 0, practiceCandidates: 0,
    mutedSkipped: 0, unstableSkipped: 0, walletLimitSkipped: 0, supplySkipped: 0,
    soldOutSkipped: 0, paidSkipped: 0, allowlistSkipped: 0 };
}

function countCandidate(drop, stage, config, metrics) {
  if (!metrics || !stage?.uuid) return;
  if (isMutedChain(config, drop.chain)) { metrics.mutedSkipped++; return; }
  if (parseWei(stage.price) !== 0n) { metrics.paidSkipped++; return; }
  if (stage.stage_type !== 'public_sale') { metrics.allowlistSkipped++; return; }
  const practice = isPracticeChain(config, drop.chain);
  metrics[practice ? 'practiceCandidates' : 'normalCandidates']++;
  if (isSoldOut(drop)) metrics.soldOutSkipped++;
  else if (!practice && Number(stage.max_per_wallet) > config.maxAlertPerWallet) metrics.walletLimitSkipped++;
  else if (!practice && drop.max_supply != null && drop.max_supply !== '' && Number(drop.max_supply) < config.minAlertSupply) metrics.supplySkipped++;
}

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

function eligible(drop, stage, config, nowSec) {
  if (isMutedChain(config, drop.chain) || isSoldOut(drop) || !config.chains.includes(String(drop.chain).toLowerCase())
    || !isFreePublicStage(stage, nowSec, !isPracticeChain(config, drop.chain) && config.alertFreeAllowlists)) return false;
  if (isPracticeChain(config, drop.chain)) return true;
  return !(stage.max_per_wallet != null && stage.max_per_wallet !== "" && Number(stage.max_per_wallet) > config.maxAlertPerWallet)
    && !(drop.max_supply != null && drop.max_supply !== "" && Number(drop.max_supply) < config.minAlertSupply);
}

// One bulk read and batched writes per group, including existing watches that no longer qualify.
async function processStages(entries, deps) {
  const { state, config, nowSec } = deps;
  const unique = new Map();
  for (const { drop, stages } of entries) for (const stage of stages || []) {
    if (!isMutedChain(config, drop.chain) && stage?.uuid) unique.set(alertKey(drop, stage), { drop, stage });
  }
  const keys = [...unique.keys()];
  const watches = state.getStageWatches ? await state.getStageWatches(keys)
    : new Map(await Promise.all(keys.map(async key => [key, await state.getStageWatch(key)])));
  const pending = new Map();
  const flush = async () => {
    const rows = [...pending.values()];
    if (state.saveStageWatches) await state.saveStageWatches(rows);
    else for (const row of rows) await state.saveStageWatch(row);
    pending.clear();
  };
  const buffered = {
    ...state,
    getStageWatch: async key => pending.get(key) || watches.get(key) || null,
    saveStageWatch: async row => { pending.set(row.stage_key, row); },
    reserveAlert: async row => { await flush(); return state.reserveAlert(row); },
    hasAlert: async key => state.getStageWatches ? Boolean(watches.get(key)?.alerted) : state.hasAlert(key),
  };
  let alerts = 0, warnings = 0, watched = 0;
  for (const [key, { drop, stage }] of unique) {
    countCandidate(drop, stage, config, deps.metrics);
    // Keep closed sold-out watches closed on rediscovery; paid warnings still run.
    if (isSoldOut(drop) && !(parseWei(stage.price) != null && parseWei(stage.price) !== 0n
      && await buffered.hasAlert(key) && !watches.get(key)?.paid_warning_sent)) continue;
    if (!watches.get(key) && !eligible(drop, stage, config, nowSec)) continue;
    watched++;
    const warning = await buffered.hasAlert(key) && parseWei(stage.price) != null && parseWei(stage.price) !== 0n;
    if (await notifyCandidate({ drop, stage, ...deps, state: buffered,
      allowFreeAllowlists: config.alertFreeAllowlists, maxAlertPerWallet: config.maxAlertPerWallet,
      minAlertSupply: config.minAlertSupply })) {
      if (warning) warnings++; else alerts++;
    }
  }
  await flush();
  for (const { drop, authoritative } of entries) {
    if (authoritative && !isMutedChain(config, drop.chain) && isSoldOut(drop)) await state.closeCollectionWatches(drop, nowSec);
  }
  return { alerts, warnings, watched };
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
    if (err.d1WriteFailure) throw err;
    if (/does not report these target chains/.test(err.message)) throw err;
    console.warn(`chain validation skipped: ${err.message}`);
  }
}

async function calendarScan({ api, state, config, env, nowSec, scanId, telegram, metrics }) {
  let feedSuccesses = 0;
  const feedErrors = [];
  const pages = await Promise.all(DROP_TYPES.map(async (type) => {
    try {
      const rows = await api.listDrops(type, config.chains, config.maxDropPagesPerFeed);
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
  for (const drop of summaries.values()) if (isMutedChain(config, drop.chain)) {
    for (const stage of [drop.active_stage, drop.next_stage]) countCandidate(drop, stage, config, metrics);
  }
  const drops = [...summaries.values()].filter(drop => !isMutedChain(config, drop.chain));

  let sent = 0;
  const summaryKeys = drops.flatMap(drop => [drop.active_stage, drop.next_stage].filter(s => s?.uuid).map(s => alertKey(drop, s)));
  const existing = state.getStageWatches ? await state.getStageWatches(summaryKeys)
    : new Map(await Promise.all(summaryKeys.map(async key => [key, await state.getStageWatch(key)])));
  const observations = new Map(drops.map(drop => [dropKey(drop), { drop, stages: [drop.active_stage, drop.next_stage] }]));

  const relevantDrops = drops.filter(drop => [drop.active_stage, drop.next_stage].some(stage => stage?.uuid && (existing.get(alertKey(drop, stage)) || eligible(drop, stage, config, nowSec))));
  const keys = relevantDrops.map(dropKey);
  const caches = await state.getCaches(keys);
  const due = relevantDrops
    .map((drop) => ({ drop, cached: caches.get(dropKey(drop)) || null }))
    .filter(({ drop, cached }) => {
      if (!cached) return true;
      const changed = (cached.active_uuid || null) !== (drop.active_stage?.uuid || null)
        || (cached.next_uuid || null) !== (drop.next_stage?.uuid || null);
      const stale = nowSec - Number(cached.last_detail_at || 0) >= detailRefreshSeconds(drop, [], nowSec, config);
      return changed || stale;
    })
    .sort((a, b) => Number(a.cached?.last_detail_at || 0) - Number(b.cached?.last_detail_at || 0))
    .slice(0, config.maxDetailsPerScan);

  await mapLimit(due, 4, async ({ drop }) => {
    try {
      const detail = await api.getDrop(drop.collection_slug);
      observations.set(dropKey(detail), { drop: detail, stages: detail.stages || [], authoritative: true });
      await state.upsertCache(cacheRow(detail, nowSec, nowSec));
      await state.clearMiss(detail.collection_slug);
    } catch (err) {
      if (err.d1WriteFailure) throw err;
      if (err instanceof OpenSeaNotFound) {
        await state.markMiss(drop.collection_slug, nowSec);
      } else {
        console.warn(`detail ${drop.collection_slug}: ${err.message}`);
      }
    }
  });

  const detailedKeys = new Set(due.map(({ drop }) => dropKey(drop)));
  await mapLimit(relevantDrops.filter((drop) => !detailedKeys.has(dropKey(drop))), 4, async (drop) => {
    const cached = caches.get(dropKey(drop));
    const changed = !cached
      || (cached.active_uuid || null) !== (drop.active_stage?.uuid || null)
      || (cached.next_uuid || null) !== (drop.next_stage?.uuid || null);
    if (changed) {
      await state.upsertCache(cacheRow(drop, nowSec, Number(cached?.last_detail_at || 0)));
    }
  });

  for (const { drop, stages, authoritative } of observations.values()) {
    if (!authoritative || isSoldOut(drop)) continue;
    const present = new Set(stages.map(stage => stage.uuid));
    for (const row of existing.values()) if (row && row.chain === String(drop.chain).toLowerCase()
      && row.slug === drop.collection_slug && !present.has(row.stage_uuid)) await state.closeStageWatch(row.stage_key, nowSec);
  }
  const results = await processStages([...observations.values()], { state, env, nowSec, scanId, config, telegram, metrics });
  sent += results.alerts;
  return { sent, warnings: results.warnings, watched: results.watched, details: due.length,
    pages: pages.reduce((sum, rows) => sum + (rows.pages || 0), 0),
    truncated: pages.some(rows => rows.truncated), summaries };
}

async function collectionSweep({ api, state, config, env, nowSec, summaries, scanId, telegram, metrics }) {
  const raw = await state.getSetting("collection_sweep_at");
  const last = Number(raw || 0);
  if (nowSec - last < config.collectionSweepSeconds || config.maxFallbackDetails === 0) return { alerts: 0, warnings: 0, details: 0, watched: 0 };

  const pages = await Promise.all(config.chains.filter(chain => !isMutedChain(config, chain)).map(async (chain) => {
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
      if (isMutedChain(config, chain)) continue;
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

  const observations = [];
  await mapLimit(filtered, 4, async (slug) => {
    try {
      const detail = await api.getDrop(slug);
      if (!config.chains.includes(detail.chain) || isMutedChain(config, detail.chain)) return;
      observations.push({ drop: detail, stages: detail.stages || [], authoritative: true });
      await state.upsertCache(cacheRow(detail, nowSec, nowSec));
      await state.clearMiss(slug);
    } catch (err) {
      if (err.d1WriteFailure) throw err;
      if (err instanceof OpenSeaNotFound) await state.markMiss(slug, nowSec);
      else console.warn(`fallback ${slug}: ${err.message}`);
    }
  });

  await state.setSetting("collection_sweep_at", nowSec, nowSec);

  const result = await processStages(observations, { state, env, nowSec, scanId, config, telegram, metrics });
  return { ...result, details: filtered.length };
}

export async function runDiscoveryScan(env, deps = {}) {
  const config = getConfig(env);
  const nowSec = deps.nowSec ?? Math.floor(Date.now() / 1000);
  const scanId = String(Math.floor(nowSec / 60));
  const metrics = scanMetrics();
  const state = scanState(env, deps, metrics);
  const api = deps.api || new Tide({ apiKey: env.OPENSEA_API_KEY });

  await maybeValidateChains({ api, state, config, nowSec });
  const first = await calendarScan({ api, state, config, env, nowSec, scanId, telegram: deps.telegram, metrics });
  const extra = await collectionSweep({ api, state, config, env, nowSec, summaries: first.summaries, scanId, telegram: deps.telegram, metrics });
  console.log(`discovery: ${Object.entries(metrics).map(([key, value]) => `${key}=${value}`).join(" ")} drops=${first.summaries.size} watched=${first.watched + extra.watched} details=${first.details + extra.details} alerts=${first.sent + extra.alerts} warnings=${first.warnings + extra.warnings} pages=${first.pages} truncated=${first.truncated}`);
  return { alerts: first.sent + extra.alerts, warnings: first.warnings + extra.warnings, drops: first.summaries.size, pages: first.pages, truncated: first.truncated, ...metrics };
}

// Backwards-compatible programmatic discovery entry point.
export const runScan = runDiscoveryScan;

export async function runWatchScan(env, deps = {}) {
  const config = getConfig(env);
  const nowSec = deps.nowSec ?? Math.floor(Date.now() / 1000);
  const scanId = String(Math.floor(nowSec / 60));
  const metrics = scanMetrics();
  const state = scanState(env, deps, metrics);
  const api = deps.api || new Tide({ apiKey: env.OPENSEA_API_KEY });
  const candidates = await state.getWatchCandidates(nowSec, config);
  const groups = new Map();
  for (const row of candidates) {
    if (isMutedChain(config, row.chain)) continue;
    const key = `${row.chain}:${row.slug}`;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }
  let alerts = 0, warnings = 0;
  const selected = [...groups.values()].slice(0, config.maxWatchDetailsPerScan);
  await mapLimit(selected, 4, async rows => {
    try {
      state.notePolled?.(rows, nowSec);
      const detail = await api.getDrop(rows[0].slug);
      if (String(detail.chain).toLowerCase() !== rows[0].chain) return;
      const ids = new Set(rows.map(row => row.stage_uuid));
      const present = new Set((detail.stages || []).map(stage => stage.uuid));
      for (const row of rows) {
        if (!present.has(row.stage_uuid)) await state.closeStageWatch(row.stage_key, nowSec);
      }
      const result = await processStages([{ drop: detail, stages: (detail.stages || []).filter(stage => ids.has(stage.uuid)), authoritative: true }],
        { state, env, nowSec, scanId, config, telegram: deps.telegram, metrics });
      alerts += result.alerts; warnings += result.warnings;
    } catch (err) {
      if (err.d1WriteFailure) throw err;
      if (err instanceof OpenSeaNotFound) {
        await state.closeCollectionWatches({ chain: rows[0].chain, collection_slug: rows[0].slug }, nowSec);
        console.log(`watch drop removed: ${rows[0].slug}`);
      } else {
        console.warn(`watch detail ${rows[0].slug}: ${err.message}`);
      }
    }
  });
  console.log(`watch: ${Object.entries(metrics).map(([key, value]) => `${key}=${value}`).join(" ")} candidates=${candidates.length} details=${selected.length} alerts=${alerts} warnings=${warnings}`);
  return { candidates: candidates.length, details: selected.length, alerts, warnings, ...metrics };
}

export function dispatchScheduled(controller, env, deps) {
  const minute = new Date(controller.scheduledTime).getUTCMinutes();
  return minute % 5 === 0 ? runDiscoveryScan(env, deps) : runWatchScan(env, deps);
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
  async scheduled(controller, env, ctx) {
    ctx.waitUntil(dispatchScheduled(controller, env).catch((err) => console.error("scan failed:", err.message)));
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
            const rows = await api.listDrops(type, config.chains, config.maxDropPagesPerFeed);
            checks[type] = { ok: true, count: rows.length };
          } catch (err) {
            checks[type] = { ok: false, error: err.message };
          }
        }

        return jsonResponse({ ok: true, checks, chainPolicy: Object.fromEntries(config.chains.map(chain => [chain, chainPolicy(config, chain)])) });
      }

      const mode = url.searchParams.get("mode") || "discovery";
      if (!["watch", "discovery"].includes(mode)) return jsonResponse({ ok: false, error: "unknown mode" }, 400);
      const result = await (mode === "watch" ? runWatchScan : runDiscoveryScan)(env);
      return jsonResponse({ ok: true, ...result });
    } catch (err) {
      console.error(`manual ${url.pathname} failed:`, err.message);
      return jsonResponse({ ok: false, error: err.message }, 500);
    }
  },
};
