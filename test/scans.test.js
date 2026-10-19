import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { readFileSync } from 'node:fs';
import { createState } from '../src/state.js';
import { getConfig } from '../src/current.js';
import { runDiscoveryScan, runWatchScan, dispatchScheduled } from '../src/index.js';
import { OpenSeaNotFound, Tide } from '../src/tide.js';
const now = 1800000000;
const iso = n => new Date(n * 1000).toISOString();
const env = { DB: {}, OPENSEA_API_KEY: 'x', TELEGRAM_BOT_TOKEN: 'x', TELEGRAM_CHAT_ID: 'x', TARGET_CHAINS: 'base', PRACTICE_MODE: 'false', MAX_FALLBACK_DETAILS: '0' };
const stage = { uuid: 'a', price: '0', stage_type: 'public_sale', max_per_wallet: '10', start_time: iso(now + 1200), end_time: iso(now + 7200) };
const drop = { chain: 'base', collection_slug: 'test', max_supply: 150, next_stage: stage, stages: [stage] };
function fixture() {
  const sql = new DatabaseSync(':memory:');
  for (const name of ['0001_init.sql', '0002_stage_watch.sql', '0003_cpu_indexes.sql']) sql.exec(readFileSync(new URL(`../migrations/${name}`, import.meta.url), 'utf8'));
  const db = { prepare(query) {
    // node:sqlite versions differ in support for D1's numbered placeholders.
    const positions = [...query.matchAll(/\?(\d+)/g)].map(match => Number(match[1]) - 1);
    const stmt = sql.prepare(query.replace(/\?\d+/g, '?')); let args = [];
    return { bind(...values) { args = positions.length ? positions.map(index => values[index]) : values; return this; }, async first() { return stmt.get(...args) || null; },
      async all() { return { results: stmt.all(...args) }; }, async run() { return stmt.run(...args); } };
  }, async batch(statements) { return Promise.all(statements.map(stmt => stmt.run())); } };
  const state = createState(db);
  const calls = { feeds: 0, collections: 0, chains: 0, details: [] };
  let current = drop;
  const api = { listDrops: async () => { calls.feeds++; return [current]; },
    getDrop: async slug => { calls.details.push(slug); return { ...current, collection_slug: slug }; },
    listCollections: async () => { calls.collections++; return { collections: [] }; },
    getChains: async () => { calls.chains++; return { chains: [{ chain: 'base' }] }; } };
  return { sql, db, state, api, calls, set: d => { current = d; },
    discovery: async (seconds = 0, extra = {}) => runDiscoveryScan({ ...env, ...extra }, { state, api, nowSec: now + seconds, telegram: async () => {} }),
    watch: async (seconds = 60, extra = {}, telegram = async () => {}) => runWatchScan({ ...env, ...extra }, { state, api, nowSec: now + seconds, telegram }) };
}
test('migrations apply and watch cron calls only direct watched details', async () => {
  const f = fixture(); await f.discovery();
  f.calls.feeds = f.calls.chains = f.calls.collections = 0;
  await dispatchScheduled({ cron: '* * * * *', scheduledTime: Date.UTC(2026, 9, 15, 12, 1) }, env, { state: f.state, api: f.api, nowSec: now + 60 });
  assert.equal(f.calls.feeds, 0); assert.equal(f.calls.collections, 0); assert.equal(f.calls.chains, 0);
  assert.equal(f.calls.details.length, 2);
});
test('discovery cron runs feeds and never queries watch candidates', async () => {
  const f = fixture(); f.state.getWatchCandidates = () => { throw new Error('watch query called'); };
  await dispatchScheduled({ cron: '* * * * *', scheduledTime: Date.UTC(2026, 9, 15, 12, 0) }, env, { state: f.state, api: f.api, nowSec: now });
  assert.equal(f.calls.feeds, 3);
});
for (const endTime of [stage.end_time, null]) {
  for (const alerted of [false, true]) {
    test(`missing watched stage closes and discovery reactivates it (end=${endTime}, alerted=${alerted})`, async () => {
      const f = fixture();
      const watched = { ...stage, end_time: endTime };
      f.set({ ...drop, next_stage: watched, stages: [watched] });
      await f.discovery();
      const key = 'base:test:a';
      if (alerted) await f.state.markAlert({ key, chain: 'base', slug: 'test', stageUuid: 'a', sentAt: now });
      const before = await f.state.getStageWatch(key);
      assert.equal((await f.state.getWatchCandidates(now + 60, getConfig(env))).length, 1);

      f.set({ ...drop, next_stage: null, stages: [] });
      f.calls.details = [];
      await f.watch(60);
      assert.deepEqual(f.calls.details, ['test']);
      assert.deepEqual({ ...await f.state.getStageWatch(key) }, { ...before, last_end_time: String(now + 60) });
      assert.equal((await f.state.getWatchCandidates(now + 120, getConfig(env))).length, 0);
      await f.watch(120);
      assert.deepEqual(f.calls.details, ['test']);
      assert.equal(await f.state.hasAlert(key), alerted);

      f.set({ ...drop, next_stage: watched, stages: [watched] });
      await f.discovery(300);
      const restored = await f.state.getStageWatch(key);
      assert.equal(restored.first_seen_at, before.first_seen_at);
      assert.equal(restored.last_end_time, endTime == null ? null : String(now + 7200));
      assert.equal((await f.state.getWatchCandidates(now + 360, getConfig(env))).length, 1);
      f.calls.details = [];
      await f.watch(360);
      assert.deepEqual(f.calls.details, ['test']);
    });
  }
}
test('watch and discovery share a UTC minute; next minute adds one observation', async () => {
  const f = fixture(); await f.discovery(); await f.watch(1, { WATCH_REFRESH_SECONDS: '1' });
  assert.equal((await f.state.getStageWatch('base:test:a')).consecutive_free_observations, 1);
  await f.watch(61);
  assert.equal((await f.state.getStageWatch('base:test:a')).consecutive_free_observations, 2);
});
for (const [name, changes] of Object.entries({ paid: { stages: [{ ...stage, price: '1' }], next_stage: { ...stage, price: '1' } }, wallet: { stages: [{ ...stage, max_per_wallet: '11' }], next_stage: { ...stage, max_per_wallet: '11' } }, supply: { max_supply: 149 } })) {
  test(`new ${name} filtered stage creates no watch row`, async () => {
    const f = fixture(); f.set({ ...drop, ...changes }); await f.discovery();
    assert.equal(await f.state.getStageWatch('base:test:a'), null);
    assert.equal(f.calls.details.length, 0);
  });
}
test('rediscovered watched free to paid preserves history', async () => {
  const f = fixture(); await f.discovery(); const paid = { ...stage, price: '1' };
  f.set({ ...drop, next_stage: paid, stages: [paid] }); await f.discovery(300);
  const row = await f.state.getStageWatch('base:test:a');
  assert.equal(row.ever_seen_paid, 1); assert.equal(row.first_free_at, null); assert.equal(row.consecutive_free_observations, 0);
});
test('alerted stages remain watched until expiry and paid warning is one-time', async () => {
  const f = fixture(); await f.discovery();
  await f.state.markAlert({ key: 'base:test:a', chain: 'base', slug: 'test', stageUuid: 'a', sentAt: now });
  const paid = { ...stage, price: '1' }; f.set({ ...drop, next_stage: paid, stages: [paid] });
  let sent = 0; assert.equal((await f.watch(60, {}, async () => { sent++; })).warnings, 1);
  await f.watch(120, {}, async () => { sent++; }); assert.equal(sent, 1);
  assert.equal((await f.state.getWatchCandidates(now + 180, getConfig(env))).length, 1);
  assert.equal((await f.state.getWatchCandidates(now + 7200, getConfig(env))).length, 0);
});
test('first discovered live alerts after two minute observations', async () => {
  const f = fixture(); const live = { ...stage, start_time: iso(now - 60) };
  f.set({ ...drop, next_stage: live, stages: [live] }); await f.discovery();
  assert.equal((await f.watch()).alerts, 1);
});
test('watch cap prioritizes alerted, live, then nearest upcoming and retains overflow', async () => {
  const f = fixture(); await f.discovery(); const original = await f.state.getStageWatch('base:test:a');
  for (const [slug, start, alerted] of [['far', now + 1700, false], ['near', now + 700, false], ['live', now - 60, false], ['alerted', now + 1600, true]]) {
    await f.state.saveStageWatch({ ...original, stage_key: `base:${slug}:a`, slug, last_start_time: String(start), first_seen_live: start < now ? 1 : 0 });
    if (alerted) await f.state.markAlert({ key: `base:${slug}:a`, chain: 'base', slug, stageUuid: 'a', sentAt: now });
  }
  f.calls.details = []; await f.watch(60, { MAX_WATCH_DETAILS_PER_SCAN: '3' });
  assert.deepEqual(f.calls.details, ['alerted', 'live', 'near']);
  const remaining = await f.state.getWatchCandidates(now + 120, getConfig(env));
  assert.ok(remaining.some(row => row.slug === 'far'));
});
test('pagination cap warns and tracks truncation without throwing', async () => {
  let requests = 0; const warnings = []; const warn = console.warn;
  console.warn = msg => warnings.push(msg);
  try {
    const api = new Tide({ apiKey: 'x', fetchImpl: async () => { requests++; return Response.json({ drops: [drop], next: 'more' }); } });
    const rows = await api.listDrops('featured', ['base'], 2);
    assert.equal(requests, 2); assert.equal(rows.pages, 2); assert.equal(rows.truncated, true);
    assert.match(warnings[0], /truncated at 2 pages/);
    const f = fixture(); f.api.listDrops = (...args) => api.listDrops(...args);
    const result = await f.discovery(0, { MAX_DROP_PAGES_PER_FEED: '2' });
    assert.equal(result.truncated, true); assert.equal(result.pages, 6);
  } finally { console.warn = warn; }
});
test('collection fallback defaults to 900 seconds and never runs during watch', async () => {
  const f = fixture(); assert.equal(getConfig(env).collectionSweepSeconds, 900);
  await f.discovery(0, { MAX_FALLBACK_DETAILS: '1' });
  assert.equal(f.calls.collections, 1);
  await f.discovery(899, { MAX_FALLBACK_DETAILS: '1' }); assert.equal(f.calls.collections, 1);
  await f.discovery(900, { MAX_FALLBACK_DETAILS: '1' }); assert.equal(f.calls.collections, 2);
  await f.watch(960, { MAX_FALLBACK_DETAILS: '1' }); assert.equal(f.calls.collections, 2);
});
test('SQL upcoming stability still waits ten minutes and suppresses duplicates', async () => {
  const f = fixture(); await f.discovery(); await f.watch(60); await f.watch(120);
  assert.equal((await f.watch(599)).alerts, 0);
  assert.equal((await f.watch(600, { WATCH_REFRESH_SECONDS: '1' })).alerts, 1);
  assert.equal((await f.watch(660)).alerts, 0);
});
test('watch refresh waits sixty seconds and groups stages of one collection', async () => {
  const f = fixture(); await f.discovery();
  const row = await f.state.getStageWatch('base:test:a');
  await f.state.saveStageWatch({ ...row, stage_key: 'base:test:b', stage_uuid: 'b' });
  f.set({ ...drop, stages: [stage, { ...stage, uuid: 'b' }] });
  f.calls.details = [];
  await f.watch(59); assert.equal(f.calls.details.length, 0);
  await f.watch(60); assert.equal(f.calls.details.length, 1);
  assert.equal((await f.state.getStageWatch('base:test:b')).consecutive_free_observations, 2);
});
test('atomic SQL upserts deduplicate stale concurrent same-minute observations', async () => {
  const f = fixture(); await f.discovery(); const row = await f.state.getStageWatch('base:test:a');
  const next = { ...row, last_seen_at: now + 60, last_observed_scan_at: String(Math.floor((now + 60) / 60)), consecutive_free_observations: 2 };
  await f.state.saveStageWatches([next]); await f.state.saveStageWatches([next]);
  assert.equal((await f.state.getStageWatch(row.stage_key)).consecutive_free_observations, 2);
  await f.state.saveStageWatch({ ...next, last_price: '1', first_free_at: null, ever_seen_paid: 1, paid_warning_sent: 1 });
  await f.state.saveStageWatch(next);
  const final = await f.state.getStageWatch(row.stage_key);
  assert.equal(final.ever_seen_paid, 1); assert.equal(final.paid_warning_sent, 1);
  assert.equal(final.consecutive_free_observations, 1); assert.equal(final.first_free_at, now + 60);
});
test('authenticated manual run rejects unknown modes; other controls retain authentication', async () => {
  const { default: worker } = await import('../src/index.js');
  const control = { ...env, CONTROL_TOKEN: 'local-test' };
  const request = path => new Request(`https://example.test${path}`, { method: 'POST', headers: { authorization: 'Bearer local-test' } });
  assert.equal((await worker.fetch(request('/run?mode=invalid'), control)).status, 400);
  for (const path of ['/run?mode=watch', '/run?mode=discovery', '/diag', '/test-telegram']) {
    assert.equal((await worker.fetch(new Request(`https://example.test${path}`, { method: 'POST' }), control)).status, 401);
  }
});

const practice = { PRACTICE_MODE: 'true' };
test('Arbitrum is configured by default and in wrangler; practice is a boolean', () => {
  const vars = JSON.parse(readFileSync(new URL('../wrangler.jsonc', import.meta.url), 'utf8')).vars;
  const secrets = { ...env, TARGET_CHAINS: undefined };
  assert.ok(getConfig(secrets).chains.includes('arbitrum'));
  assert.ok(getConfig({ ...secrets, ...vars }).chains.includes('arbitrum'));
  assert.deepEqual(getConfig({ ...secrets, ...vars }).practiceChains, ['base', 'arbitrum']);
  assert.equal(getConfig({ ...secrets, PRACTICE_MODE: 'false' }).practiceMode, false);
  assert.equal(getConfig(secrets).practiceMode, false);
  assert.equal(vars.ALERT_FREE_ALLOWLISTS, 'false');
  assert.equal(getConfig(secrets).upcomingAlertWindowSeconds, 600);
});
for (const [name, changes] of Object.entries({
  'upcoming at T-10': { start_time: iso(now + 600) },
  'new live': { start_time: iso(now - 60) },
  'wallet above 10': { start_time: iso(now + 600), max_per_wallet: '11' },
  'supply below 150': { start_time: iso(now + 600) },
})) {
  test(`practice alerts ${name} on first observation and suppresses duplicates`, async () => {
    const f = fixture(); const candidate = { ...stage, ...changes };
    f.set({ ...drop, max_supply: name === 'supply below 150' ? 149 : 150, next_stage: candidate, stages: [candidate] });
    assert.equal((await f.discovery(0, practice)).alerts, 1);
    assert.equal((await f.state.getStageWatch('base:test:a')).consecutive_free_observations, 1);
    assert.equal((await f.watch(60, practice)).alerts, 0);
    assert.equal((await f.discovery(300, practice)).alerts, 0);
  });
}
for (const [name, changes] of Object.entries({
  paid: { price: '1' },
  'unknown price': { price: null },
  allowlist: { stage_type: 'allowlist' },
  presale: { stage_type: 'presale' },
  expired: { start_time: iso(now - 120), end_time: iso(now) },
  'invalid start': { start_time: 'invalid' },
})) {
  test(`practice rejects ${name}`, async () => {
    const f = fixture(); const candidate = { ...stage, start_time: iso(now + 600), ...changes };
    f.set({ ...drop, next_stage: candidate, stages: [candidate] });
    assert.equal((await f.discovery(0, { ...practice, ALERT_FREE_ALLOWLISTS: 'true' })).alerts, 0);
    assert.equal(await f.state.getStageWatch('base:test:a'), null);
  });
}
test('practice rejects chains outside configured targets', async () => {
  const f = fixture(); f.set({ ...drop, chain: 'ethereum' });
  assert.equal((await f.discovery(0, practice)).alerts, 0);
  assert.equal(await f.state.getStageWatch('ethereum:test:a'), null);
});
test('practice upcoming alert and fast watch windows remain bounded', async () => {
  const f = fixture(); const candidate = { ...stage, start_time: iso(now + 86400), end_time: iso(now + 90000), max_per_wallet: '50' };
  f.set({ ...drop, next_stage: candidate, stages: [candidate] });
  assert.equal((await f.discovery(0, practice)).alerts, 0);
  f.calls.details = [];
  assert.equal((await f.watch(60, practice)).candidates, 0);
  assert.deepEqual(f.calls.details, []);
  assert.equal((await f.state.getWatchCandidates(now + 86400 - 1801, getConfig({ ...env, ...practice }))).length, 0);
  assert.equal((await f.state.getWatchCandidates(now + 86400 - 1800, getConfig({ ...env, ...practice }))).length, 1);
  assert.equal((await f.watch(86400 - 601, practice)).alerts, 0);
  assert.equal((await f.watch(86400 - 600, { ...practice, WATCH_REFRESH_SECONDS: '1' })).alerts, 1);
});
test('practice warning remains one-time and retains history across free/paid switches', async () => {
  const f = fixture(); const live = { ...stage, start_time: iso(now - 60), max_per_wallet: '50' };
  f.set({ ...drop, max_supply: 100, next_stage: live, stages: [live] });
  assert.equal((await f.discovery(0, practice)).alerts, 1);
  const before = await f.state.getStageWatch('base:test:a');
  const paid = { ...live, price: '1' }; f.set({ ...drop, next_stage: paid, stages: [paid] });
  const messages = [];
  assert.equal((await f.watch(60, practice, async (_, msg) => messages.push(msg))).warnings, 1);
  await f.watch(120, practice, async (_, msg) => messages.push(msg));
  f.set({ ...drop, next_stage: live, stages: [live] }); await f.watch(180, practice);
  f.set({ ...drop, next_stage: paid, stages: [paid] }); await f.watch(240, practice, async (_, msg) => messages.push(msg));
  assert.equal(messages.length, 1); assert.match(messages[0], /MINT CHANGED/);
  const after = await f.state.getStageWatch('base:test:a');
  assert.equal(after.first_seen_at, before.first_seen_at);
  assert.equal(after.paid_warning_sent, 1);
  assert.equal(await f.state.hasAlert('base:test:a'), true);
});
test('practice watch retains detail cap and never scans catalogs or chains', async () => {
  const f = fixture(); await f.discovery(0, practice);
  const original = await f.state.getStageWatch('base:test:a');
  for (let i = 0; i < 5; i++) await f.state.saveStageWatch({ ...original, stage_key: `base:extra-${i}:a`, slug: `extra-${i}`, last_max_per_wallet: '50' });
  f.calls.details = []; f.calls.feeds = f.calls.collections = f.calls.chains = 0;
  const result = await f.watch(60, { ...practice, MAX_WATCH_DETAILS_PER_SCAN: '2' });
  assert.equal(result.candidates, 6); assert.equal(result.details, 2);
  assert.equal(f.calls.details.length, 2);
  assert.equal(f.calls.feeds, 0); assert.equal(f.calls.collections, 0); assert.equal(f.calls.chains, 0);
  assert.equal((await f.state.getWatchCandidates(now + 120, getConfig({ ...env, ...practice }))).length, 6);
});

test('explicit practice=false restores normal stability for upcoming and live stages', async () => {
  const f = fixture(); await f.discovery(0, { PRACTICE_MODE: 'false' });
  assert.equal((await f.watch(60, { PRACTICE_MODE: 'false' })).alerts, 0);
  assert.equal((await f.watch(120, { PRACTICE_MODE: 'false' })).alerts, 0);
  assert.equal((await f.watch(600, { PRACTICE_MODE: 'false' })).alerts, 1);
  const g = fixture(); const live = { ...stage, start_time: iso(now - 60) };
  g.set({ ...drop, next_stage: live, stages: [live] });
  assert.equal((await g.discovery(0, { PRACTICE_MODE: 'false' })).alerts, 0);
  assert.equal((await g.watch(60, { PRACTICE_MODE: 'false' })).alerts, 1);
});
for (const [name, changes] of Object.entries({ wallet: { max_per_wallet: '11' }, supply: {} })) {
  test(`explicit practice=false restores ${name} quality filter`, async () => {
    const f = fixture(); const live = { ...stage, start_time: iso(now - 60), ...changes };
    f.set({ ...drop, max_supply: name === 'supply' ? 149 : 150, next_stage: live, stages: [live] });
    assert.equal((await f.discovery(0, { PRACTICE_MODE: 'false' })).alerts, 0);
    assert.equal(await f.state.getStageWatch('base:test:a'), null);
    assert.equal((await f.discovery(300, practice)).alerts, 1);
  });
}

for (const practiceMode of ['true', 'false']) {
  for (const alerted of [false, true]) {
    test(`authoritative sold-out watch closes with history intact, practice=${practiceMode}, alerted=${alerted}`, async () => {
      const f = fixture();
      const mode = { PRACTICE_MODE: practiceMode };
      const live = { ...stage, start_time: iso(now - 7200) };
      // Seed a watch without alerting, then exercise both alert states.
      f.set({ ...drop, next_stage: live, stages: [live] });
      await f.discovery();
      const key = 'base:test:a';
      if (alerted) await f.state.markAlert({ key, chain: 'base', slug: 'test', stageUuid: 'a', sentAt: now });
      const before = await f.state.getStageWatch(key);
      f.set({ ...drop, total_supply: 150, next_stage: live, stages: [live] });
      f.calls.details = [];
      assert.equal((await f.watch(60, mode)).alerts, 0);
      assert.deepEqual({ ...await f.state.getStageWatch(key) }, { ...before, last_end_time: String(now + 60) });
      assert.equal(await f.state.hasAlert(key), alerted);
      assert.equal((await f.watch(120, mode)).candidates, 0);
      assert.deepEqual(f.calls.details, ['test']);
      await f.discovery(300, mode);
      assert.equal((await f.watch(360, mode)).candidates, 0);
      assert.equal((await f.state.getStageWatch(key)).first_seen_at, before.first_seen_at);
    });
  }
  for (const total_supply of [150, 151]) {
    test(`sold-out discovery rejects without extra requests, total=${total_supply}, practice=${practiceMode}`, async () => {
      const f = fixture(); const live = { ...stage, start_time: iso(now - 7200) };
      f.set({ ...drop, total_supply, next_stage: live, stages: [live] });
      assert.equal((await f.discovery(0, { PRACTICE_MODE: practiceMode })).alerts, 0);
      assert.equal(await f.state.getStageWatch('base:test:a'), null);
      assert.deepEqual(f.calls.details, []);
    });
  }
}

test('sold-out authoritative discovery closes existing watches even when detail omits the UUID', async () => {
  const f = fixture(); await f.discovery();
  const before = await f.state.getStageWatch('base:test:a');
  // Discovery summary still exposes the stage, while getDrop is authoritative.
  f.api.getDrop = async () => ({ ...drop, total_supply: 150, stages: [] });
  await f.discovery(300, practice);
  const after = await f.state.getStageWatch('base:test:a');
  assert.equal(after.last_end_time, String(now + 300));
  assert.equal(after.first_seen_at, before.first_seen_at);
  assert.equal((await f.watch(360, practice)).candidates, 0);
});

for (const minute of [0, 5, 10, 1, 4, 6]) {
  test(`scheduled UTC minute ${minute} runs only ${minute % 5 === 0 ? 'discovery' : 'watch'} using scheduledTime`, async t => {
    const f = fixture();
    await f.discovery();
    f.calls.feeds = f.calls.chains = f.calls.collections = 0;
    f.calls.details = [];
    let watchQueries = 0;
    const getCandidates = f.state.getWatchCandidates;
    f.state.getWatchCandidates = async (...args) => { watchQueries++; return getCandidates(...args); };
    // Delayed delivery in the opposite mode must not affect routing.
    t.mock.method(Date, 'now', () => Date.UTC(2026, 9, 15, 12, minute % 5 === 0 ? 1 : 0));
    await dispatchScheduled({ cron: '* * * * *', scheduledTime: Date.UTC(2026, 9, 15, 12, minute) }, env,
      { state: f.state, api: f.api, nowSec: now + 300, telegram: async () => {} });
    assert.equal(f.calls.feeds, minute % 5 === 0 ? 3 : 0);
    assert.equal(watchQueries, minute % 5 === 0 ? 0 : 1);
    if (minute % 5 !== 0) assert.deepEqual(f.calls.details, ['test']);
  });
}

test('wrangler config uses only the one-minute cron', () => {
  const config = JSON.parse(readFileSync(new URL('../wrangler.jsonc', import.meta.url), 'utf8'));
  assert.deepEqual(config.triggers.crons, ['* * * * *']);
});

test('watch 404 expires all rows for selected chain and slug, retaining history and alerts', async () => {
  const f = fixture(); await f.discovery();
  const original = await f.state.getStageWatch('base:test:a');
  // Include a stage outside the watch window: cleanup must cover more than selected rows.
  await f.state.saveStageWatch({ ...original, stage_key: 'base:test:later', stage_uuid: 'later', last_start_time: String(now + 86400) });
  await f.state.saveStageWatch({ ...original, stage_key: 'arbitrum:test:a', chain: 'arbitrum' });
  await f.state.saveStageWatch({ ...original, stage_key: 'base:other:a', slug: 'other' });
  await f.state.markAlert({ key: original.stage_key, chain: 'base', slug: 'test', stageUuid: 'a', sentAt: now });
  const before = await f.state.getStageWatch(original.stage_key);
  f.calls.details = [];
  f.api.getDrop = async slug => {
    f.calls.details.push(slug);
    if (slug === 'test') throw new OpenSeaNotFound('404');
    return { ...drop, collection_slug: slug };
  };
  await f.watch(60);
  assert.deepEqual({ ...await f.state.getStageWatch(original.stage_key) }, { ...before, last_end_time: String(now + 60) });
  assert.equal((await f.state.getStageWatch('base:test:later')).last_end_time, String(now + 60));
  assert.equal((await f.state.getStageWatch('arbitrum:test:a')).last_end_time, original.last_end_time);
  assert.equal((await f.state.getStageWatch('base:other:a')).last_end_time, original.last_end_time);
  assert.equal(await f.state.hasAlert(original.stage_key), true);
  assert.ok(!(await f.state.getWatchCandidates(now + 120, getConfig(env))).some(row => row.slug === 'test'));
  f.calls.details = [];
  await f.watch(120);
  assert.deepEqual(f.calls.details, ['other']);
});

for (const message of ['OpenSea 500: internal server error', 'network fetch failed', 'OpenSea 429: rate limited', 'request timeout']) {
  test(`watch ${message} retains watches for retry`, async () => {
    const f = fixture(); await f.discovery();
    const before = await f.state.getStageWatch('base:test:a');
    let calls = 0;
    f.api.getDrop = async () => { calls++; throw new Error(message); };
    await f.watch(60);
    assert.deepEqual(await f.state.getStageWatch('base:test:a'), before);
    await f.watch(120);
    assert.equal(calls, 2);
  });
}

for (const mode of ['watch', 'discovery']) {
  test(`authenticated manual /run?mode=${mode} still runs only ${mode}`, async t => {
    const { default: worker } = await import('../src/index.js');
    const f = fixture();
    const paths = [];
    t.mock.method(globalThis, 'fetch', async input => {
      const url = new URL(input);
      paths.push(url.pathname);
      if (url.pathname.endsWith('/chains')) return Response.json({ chains: [{ chain: 'base' }] });
      if (url.pathname.includes('/drops')) return Response.json({ drops: [] });
      throw new Error(`unexpected API request ${url}`);
    });
    const response = await worker.fetch(new Request(`https://example.test/run?mode=${mode}`, {
      method: 'POST', headers: { authorization: 'Bearer local-test' },
    }), { ...env, DB: f.db, CONTROL_TOKEN: 'local-test' });
    assert.equal(response.status, 200);
    const result = await response.json();
    assert.equal(result.ok, true);
    if (mode === 'watch') { assert.equal(result.candidates, 0); assert.deepEqual(paths, []); }
    else { assert.equal(result.drops, 0); assert.equal(paths.filter(path => path.includes('/drops')).length, 3); }
  });
}

const chainSettings = { PRACTICE_CHAINS: 'base,arbitrum', MUTED_CHAINS: 'robinhood' };
for (const [chain, policy] of Object.entries({
  robinhood: 'muted', base: 'practice', arbitrum: 'practice', ethereum: 'normal',
  optimism: 'normal', ink: 'normal', arc: 'normal', hyperevm: 'normal',
})) {
  test(`${chain} effective policy is ${policy}; live observation follows policy`, async () => {
    const { chainPolicy } = await import('../src/current.js');
    const f = fixture();
    const settings = { ...chainSettings, TARGET_CHAINS: chain };
    const config = getConfig({ ...env, ...settings });
    assert.equal(chainPolicy(config, chain), policy);
    f.api.getChains = async () => ({ chains: [{ chain }] });
    const live = { ...stage, start_time: iso(now - 60) };
    f.set({ ...drop, chain, active_stage: live, next_stage: null, stages: [live] });
    assert.equal((await f.discovery(0, settings)).alerts, policy === 'practice' ? 1 : 0);
    assert.equal(f.calls.details.length, policy === 'muted' ? 0 : 1);
    if (policy === 'normal') assert.equal((await f.watch(60, settings)).alerts, 1);
    if (policy === 'muted') {
      assert.equal(await f.state.getStageWatch(`${chain}:test:a`), null);
      assert.equal((await f.watch(60, settings)).candidates, 0);
    }
  });
  for (const [name, change, dropChange] of [
    ['wallet ceiling', { max_per_wallet: '11' }, {}],
    ['supply floor', {}, { max_supply: 149 }],
    ['paid', { price: '1' }, {}],
    ['allowlist', { stage_type: 'presale' }, {}],
    ['sold out', {}, { total_supply: 150 }],
  ]) {
    test(`${chain} applies ${name} under ${policy} policy`, async () => {
      const f = fixture(), settings = { ...chainSettings, TARGET_CHAINS: chain };
      f.api.getChains = async () => ({ chains: [{ chain }] });
      const live = { ...stage, start_time: iso(now - 60), ...change };
      f.set({ ...drop, chain, ...dropChange, next_stage: live, stages: [live] });
      const bypass = policy === 'practice' && ['wallet ceiling', 'supply floor'].includes(name);
      assert.equal((await f.discovery(0, settings)).alerts, bypass ? 1 : 0);
      assert.equal(Boolean(await f.state.getStageWatch(`${chain}:test:a`)), bypass);
    });
  }
}
test('policy lists normalize, deduplicate, override legacy mode, and mute takes precedence', async () => {
  const { chainPolicy } = await import('../src/current.js');
  const config = getConfig({ ...env, PRACTICE_MODE: 'true',
    PRACTICE_CHAINS: ' Base,ARBITRUM,base, Robinhood, ', MUTED_CHAINS: ' Robinhood,ROBINHOOD ' });
  assert.deepEqual(config.practiceChains, ['base', 'arbitrum', 'robinhood']);
  assert.deepEqual(config.mutedChains, ['robinhood']);
  assert.equal(chainPolicy(config, 'ROBINHOOD'), 'muted');
  assert.equal(chainPolicy(config, 'ethereum'), 'normal');
});
test('muting historical alerted and unalerted watches preserves all history without detail calls', async () => {
  const f = fixture();
  await f.discovery();
  const original = await f.state.getStageWatch('base:test:a');
  for (const slug of ['old-alert', 'old-candidate']) {
    await f.state.saveStageWatch({ ...original, chain: 'robinhood', slug, stage_key: `robinhood:${slug}:a`, last_max_per_wallet: '100' });
  }
  await f.state.markAlert({ key: 'robinhood:old-alert:a', chain: 'robinhood', slug: 'old-alert', stageUuid: 'a', sentAt: now });
  const before = f.sql.prepare("SELECT * FROM stage_watch WHERE chain = 'robinhood'").all();
  const alerts = f.sql.prepare('SELECT * FROM alerts').all();
  const settings = { ...chainSettings, TARGET_CHAINS: 'robinhood', MAX_FALLBACK_DETAILS: '12' };
  f.api.getChains = async () => ({ chains: [{ chain: 'robinhood' }] });
  f.set({ ...drop, chain: 'robinhood', collection_slug: 'old-alert', total_supply: 150 });
  f.calls.details = [];
  assert.equal((await f.discovery(300, settings)).alerts, 0);
  assert.equal((await f.watch(360, settings)).candidates, 0);
  assert.deepEqual(f.calls.details, []);
  assert.deepEqual(f.sql.prepare("SELECT * FROM stage_watch WHERE chain = 'robinhood'").all(), before);
  assert.deepEqual(f.sql.prepare('SELECT * FROM alerts').all(), alerts);
});
test('watch SQL bypasses wallet ceiling only for practice chains, preserving alerted normal monitoring', async () => {
  const f = fixture(); await f.discovery();
  const original = await f.state.getStageWatch('base:test:a');
  for (const chain of ['base', 'arbitrum', 'ethereum', 'optimism', 'robinhood']) {
    await f.state.saveStageWatch({ ...original, chain, stage_key: `${chain}:test:a`, last_max_per_wallet: '99' });
  }
  const settings = getConfig({ ...env, ...chainSettings, TARGET_CHAINS: 'base,arbitrum,ethereum,optimism,robinhood' });
  assert.deepEqual((await f.state.getWatchCandidates(now + 60, settings)).map(r => r.chain).sort(), ['arbitrum', 'base']);
  await f.state.markAlert({ key: 'ethereum:test:a', chain: 'ethereum', slug: 'test', stageUuid: 'a', sentAt: now });
  assert.deepEqual((await f.state.getWatchCandidates(now + 60, settings)).map(r => r.chain).sort(), ['arbitrum', 'base', 'ethereum']);
});
test('/diag exposes effective policy for every target chain', async t => {
  const { default: worker } = await import('../src/index.js');
  t.mock.method(globalThis, 'fetch', async input => Response.json(String(input).endsWith('/chains') ? { chains: [] } : { drops: [] }));
  const response = await worker.fetch(new Request('https://example.test/diag', {
    method: 'POST', headers: { authorization: 'Bearer local-test' },
  }), { ...env, ...chainSettings, TARGET_CHAINS: undefined, CONTROL_TOKEN: 'local-test' });
  assert.equal(response.status, 200);
  assert.deepEqual((await response.json()).chainPolicy, {
    robinhood: 'muted', base: 'practice', optimism: 'normal', ethereum: 'normal',
    ink: 'normal', arc: 'normal', hyperevm: 'normal', arbitrum: 'practice',
  });
});
for (const chain of ['base', 'arbitrum', 'ethereum', 'optimism']) {
  test(`${chain} keeps upcoming stability and one-time FREE to PAID warning under chain lists`, async () => {
    const f = fixture(), settings = { ...chainSettings, TARGET_CHAINS: chain };
    f.api.getChains = async () => ({ chains: [{ chain }] });
    const practice = ['base', 'arbitrum'].includes(chain);
    f.set({ ...drop, chain });
    assert.equal((await f.discovery(0, settings)).alerts, 0);
    assert.equal((await f.watch(60, settings)).alerts, 0);
    assert.equal((await f.watch(120, settings)).alerts, 0);
    assert.equal((await f.watch(600, settings)).alerts, 1);
    const paid = { ...stage, price: '1' };
    f.set({ ...drop, chain, next_stage: paid, stages: [paid] });
    const messages = [];
    assert.equal((await f.watch(660, settings, async (_, msg) => messages.push(msg))).warnings, 1);
    assert.equal((await f.watch(720, settings, async (_, msg) => messages.push(msg))).warnings, 0);
    assert.equal(messages.length, 1);
    assert.match(messages[0], /FREE → PAID/);
    if (practice) {
      const g = fixture();
      g.api.getChains = f.api.getChains;
      const upcoming = { ...stage, start_time: iso(now + 600) };
      g.set({ ...drop, chain, next_stage: upcoming, stages: [upcoming] });
      assert.equal((await g.discovery(0, settings)).alerts, 1);
    }
  });
}

for (const alerted of [false, true]) {
  test(`unchanged practice watch performs zero stage writes, alerted=${alerted}`, async () => {
    const f = fixture(); await f.discovery();
    if (alerted) await f.state.markAlert({ key: 'base:test:a', chain: 'base', slug: 'test', stageUuid: 'a', sentAt: now });
    const before = await f.state.getStageWatch('base:test:a');
    let writes = 0;
    const save = f.state.saveStageWatches;
    f.state.saveStageWatches = async rows => { writes += rows.length; return save(rows); };
    await f.watch(60, { PRACTICE_MODE: 'true' });
    assert.equal(writes, 0);
    assert.deepEqual(await f.state.getStageWatch('base:test:a'), before);
    assert.equal(f.calls.details.length, 2);
  });
}
for (const [name, changes] of Object.entries({ paid: { price: '1' }, start: { start_time: iso(now + 1300) }, wallet: { max_per_wallet: '9' } })) {
  test(`practice ${name} change persists once`, async () => {
    const f = fixture(); await f.discovery();
    let writes = 0;
    const save = f.state.saveStageWatches;
    f.state.saveStageWatches = async rows => { writes += rows.length; return save(rows); };
    const changed = { ...stage, ...changes };
    f.set({ ...drop, next_stage: changed, stages: [changed] });
    await f.watch(60, { PRACTICE_MODE: 'true' });
    assert.equal(writes, 1);
    if (name === 'paid') {
      f.set(drop); await f.discovery(120, { PRACTICE_MODE: 'true' });
      assert.equal(writes, 2);
      assert.equal((await f.state.getStageWatch('base:test:a')).last_price, '0');
    }
  });
}
test('normal observation writes once per UTC minute then stops at minimum count', async () => {
  const f = fixture(); await f.discovery();
  let writes = 0;
  const save = f.state.saveStageWatches;
  f.state.saveStageWatches = async rows => { writes += rows.length; return save(rows); };
  await f.watch(60); assert.equal(writes, 1);
  // Force another poll in the same minute, with a one second refresh.
  await f.watch(61, { WATCH_REFRESH_SECONDS: '1' }); assert.equal(writes, 1);
  await f.watch(121); assert.equal(writes, 2);
  await f.watch(600); await f.watch(660); assert.equal(writes, 2);
  assert.equal(await f.state.hasAlert('base:test:a'), true);
});
for (const differs of [false, true]) {
  test(`discovery persists only final authoritative stage, differs=${differs}`, async () => {
    const f = fixture(); const saved = [];
    const save = f.state.saveStageWatches;
    f.state.saveStageWatches = async rows => { saved.push(...rows); return save(rows); };
    const authoritative = { ...stage, max_per_wallet: differs ? '9' : '10' };
    f.api.getDrop = async () => ({ ...drop, stages: [authoritative] });
    await f.discovery();
    assert.equal(saved.length, 1);
    assert.equal(saved[0].last_max_per_wallet, authoritative.max_per_wallet);
  });
}
for (const method of ['saveStageWatches', 'reserveAlert']) {
  test(`D1 quota failure in ${method} aborts scan before Telegram`, async t => {
    const f = fixture(); let sends = 0; const logs = [];
    t.mock.method(console, 'error', msg => logs.push(msg));
    f.state[method] = async () => { throw new Error('D1 daily row-write limit exceeded'); };
    await assert.rejects(runDiscoveryScan({ ...env, PRACTICE_MODE: 'true' }, {
      state: f.state, api: f.api, nowSec: now + 600, telegram: async () => { sends++; },
    }), /row-write limit exceeded/);
    assert.equal(sends, 0);
    assert.equal(await f.state.hasAlert('base:test:a'), false);
    assert.ok(logs.some(msg => msg.includes('D1 quota/write failure')));
  });
}
test('identical cache, settings, miss writes and absent clearMiss change no rows', async () => {
  const f = fixture();
  const row = { dropKey: 'base:test', activeUuid: 'a', lastDetailAt: now, lastSeenAt: now };
  await f.state.upsertCache(row); await f.state.setSetting('example', '1', now); await f.state.markMiss('absent', now);
  const before = f.sql.prepare('SELECT total_changes() AS n').get().n;
  await f.state.upsertCache({ ...row, lastSeenAt: now + 60 });
  await f.state.setSetting('example', '1', now + 60); await f.state.markMiss('absent', now);
  await f.state.clearMiss('never-existed');
  assert.equal(f.sql.prepare('SELECT total_changes() AS n').get().n, before);
});
test('scan logs include aggregate write and filter diagnostics', async t => {
  const logs = []; t.mock.method(console, 'log', msg => logs.push(msg));
  const f = fixture(); await f.discovery(); await f.watch();
  for (const mode of ['discovery', 'watch']) {
    const line = logs.find(msg => msg.startsWith(`${mode}:`));
    for (const field of ['writes', 'rowsWritten', 'writeFailures', 'normalCandidates', 'practiceCandidates', 'mutedSkipped',
      'unstableSkipped', 'walletLimitSkipped', 'supplySkipped', 'soldOutSkipped', 'paidSkipped', 'allowlistSkipped']) assert.match(line, new RegExp(`${field}=\\d+`));
  }
});
test('production D1 metrics count statements and affected rows', async () => {
  const f = fixture();
  const result = await runWatchScan(env, { state: f.state, api: f.api, nowSec: now });
  assert.equal(result.writes, 0);
  const metrics = { writes: 0, rowsWritten: 0 };
  const state = createState(f.db, metrics);
  await state.setSetting('metric', '1', now);
  await state.setSetting('metric', '1', now + 60);
  assert.equal(metrics.writes, 2); assert.equal(metrics.rowsWritten, 1);
});

// Durable alert claims use the existing sent_at column; exercise real SQLite SQL.
const claimKey = 'base:test:a';
const claimRow = timestamp => ({ key: claimKey, chain: 'base', slug: 'test',
  stageUuid: 'a', claimTimestamp: timestamp, ttlSeconds: 300 });
const claimValue = f => f.sql.prepare('SELECT sent_at FROM alerts WHERE alert_key = ?').get(claimKey)?.sent_at;
async function claimFixture() {
  const f = fixture();
  await f.discovery(0, { PRACTICE_MODE: 'true' });
  const { notifyCandidate } = await import('../src/signal.js');
  f.notify = (seconds = 0, telegram = async () => {}, changes = {}) => notifyCandidate({
    drop, stage: { ...stage, start_time: iso(now - 60), ...changes }, state: f.state,
    env, nowSec: now + seconds, config: getConfig({ ...env, PRACTICE_MODE: 'true' }), telegram,
  });
  return f;
}

test('claim: reservation precedes Telegram and successful delivery is confirmed', async () => {
  const f = await claimFixture();
  assert.equal(await f.notify(0, async () => assert.equal(claimValue(f), -now)), true);
  assert.ok(claimValue(f) > 0);
  assert.equal(await f.state.hasAlert(claimKey), true);
});
test('claim: historical positive alert suppresses duplicate and remains unchanged', async () => {
  const f = await claimFixture();
  await f.state.markAlert({ ...claimRow(now), sentAt: now - 86400 });
  assert.equal(await f.notify(0, async () => assert.fail('duplicate Telegram')), false);
  assert.equal(claimValue(f), now - 86400);
});
test('claim: pending reservation inside TTL suppresses duplicate', async () => {
  const f = await claimFixture();
  assert.equal(await f.state.reserveAlert(claimRow(now)), true);
  assert.equal(await f.notify(299, async () => assert.fail('duplicate Telegram')), false);
  assert.equal(claimValue(f), -now);
});
test('claim: stale reservation is reclaimed at TTL boundary', async () => {
  const f = await claimFixture();
  await f.state.reserveAlert(claimRow(now));
  assert.equal(await f.state.reserveAlert(claimRow(now + 300)), true);
  assert.equal(claimValue(f), -(now + 300));
});
test('claim: Telegram failure releases pending row and permits immediate retry', async () => {
  const f = await claimFixture();
  assert.equal(await f.notify(0, async () => { throw new Error('Telegram unavailable'); }), false);
  assert.equal(claimValue(f), undefined);
  assert.equal(await f.notify(), true);
});
test('claim: Telegram failure and failed release leave a negative unconfirmed claim', async () => {
  const f = await claimFixture();
  f.state.releaseAlert = async () => { throw new Error('D1 quota blocked'); };
  assert.equal(await f.notify(0, async () => { throw new Error('Telegram unavailable'); }), false);
  assert.equal(claimValue(f), -now);
  assert.equal(await f.state.hasAlert(claimKey), false);
});
test('claim: failed release becomes retryable after TTL', async () => {
  const f = await claimFixture();
  f.state.releaseAlert = async () => { throw new Error('D1 quota blocked'); };
  await f.notify(0, async () => { throw new Error('Telegram unavailable'); });
  assert.equal(await f.notify(299), false);
  assert.equal(await f.notify(300), true);
  assert.ok(claimValue(f) > 0);
});
test('claim: interruption after durable reservation recovers after TTL', async () => {
  const f = await claimFixture();
  await f.state.reserveAlert(claimRow(now)); // Worker terminates here, without cleanup.
  assert.equal(await f.notify(299), false);
  assert.equal(await f.notify(300), true);
  assert.ok(claimValue(f) > 0);
});
test('claim: Telegram success and confirm failure leave recoverable pending state', async () => {
  const f = await claimFixture();
  const confirm = f.state.confirmAlert;
  f.state.confirmAlert = async () => { throw new Error('D1 quota blocked'); };
  let messages = 0;
  assert.equal(await f.notify(0, async () => messages++), true);
  assert.equal(claimValue(f), -now);
  assert.equal(await f.state.hasAlert(claimKey), false);
  assert.equal(await f.notify(299, async () => messages++), false);
  f.state.confirmAlert = confirm;
  assert.equal(await f.notify(300, async () => messages++), true);
  assert.equal(messages, 2);
  assert.ok(claimValue(f) > 0);
});
test('claim: hasAlert returns false for pending rows', async () => {
  const f = await claimFixture();
  await f.state.reserveAlert(claimRow(now));
  assert.equal(await f.state.hasAlert(claimKey), false);
});
test('claim: hasAlert returns true for confirmed positive rows', async () => {
  const f = await claimFixture();
  await f.state.reserveAlert(claimRow(now));
  assert.equal(await f.state.confirmAlert(claimKey, now, now + 1), true);
  assert.equal(await f.state.hasAlert(claimKey), true);
});
test('claim: stage watch joins exclude pending alerts', async () => {
  const f = await claimFixture();
  await f.state.reserveAlert(claimRow(now));
  assert.equal((await f.state.getStageWatches([claimKey])).get(claimKey).alerted, false);
  assert.deepEqual(await f.state.getAlertedWatches(), []);
});
test('claim: watch candidates treat pending as unconfirmed and preserve eligibility filters', async () => {
  const f = await claimFixture();
  await f.state.reserveAlert(claimRow(now));
  const settings = getConfig({ ...env, PRACTICE_MODE: 'true' });
  const candidates = await f.state.getWatchCandidates(now + 60, settings);
  assert.equal(candidates.length, 1);
  assert.equal(candidates[0].alerted, false);
  f.sql.prepare("UPDATE stage_watch SET last_max_per_wallet = '11'").run();
  assert.deepEqual(await f.state.getWatchCandidates(now + 120, getConfig(env)), []);
});
test('claim: historical positive rows still join and receive FREE to PAID warning', async () => {
  const f = await claimFixture();
  await f.state.markAlert({ ...claimRow(now), sentAt: now - 86400 });
  assert.equal((await f.state.getStageWatches([claimKey])).get(claimKey).alerted, true);
  assert.equal((await f.state.getAlertedWatches()).length, 1);
  assert.equal((await f.state.getWatchCandidates(now + 60, getConfig(env)))[0].alerted, true);
  let messages = 0;
  await f.notify(120, async (_, message) => { assert.match(message, /FREE → PAID/); messages++; }, { price: '1' });
  assert.equal(messages, 1);
  assert.equal(claimValue(f), now - 86400);
});
test('claim: initial reservation D1 failure never sends Telegram', async () => {
  const f = await claimFixture();
  f.state.reserveAlert = async () => { throw new Error('D1 quota blocked'); };
  await assert.rejects(f.notify(0, async () => assert.fail('unsafe Telegram')), /D1 quota blocked/);
  assert.equal(claimValue(f), undefined);
});
test('claim: concurrent notification attempts have only one sender', async () => {
  const f = await claimFixture();
  let messages = 0;
  const results = await Promise.all(Array.from({ length: 8 }, () => f.notify(0, async () => { messages++; })));
  assert.equal(messages, 1);
  assert.equal(results.filter(Boolean).length, 1);
  assert.ok(claimValue(f) > 0);
});
test('claim: stale owners cannot release or confirm a newer claim or confirmed history', async () => {
  const f = await claimFixture();
  await f.state.reserveAlert(claimRow(now));
  await f.state.reserveAlert(claimRow(now + 300));
  assert.equal(await f.state.confirmAlert(claimKey, now, now + 301), false);
  await f.state.releaseAlert(claimKey, now);
  assert.equal(claimValue(f), -(now + 300));
  assert.equal(await f.state.confirmAlert(claimKey, now + 300, now + 301), true);
  await f.state.releaseAlert(claimKey, now + 300);
  assert.equal(claimValue(f), now + 301);
  assert.equal(await f.state.reserveAlert(claimRow(now + 900)), false);
});
test('claim: pending FREE alert never triggers a paid-switch warning', async () => {
  const f = await claimFixture();
  await f.state.reserveAlert(claimRow(now));
  assert.equal(await f.notify(60, async () => assert.fail('unconfirmed warning'), { price: '1' }), false);
  assert.equal(claimValue(f), -now);
});
test('claim: configured TTL governs reservation lifetime', async () => {
  const f = await claimFixture();
  assert.equal(getConfig(env).alertClaimTtlSeconds, 300);
  const settings = getConfig({ ...env, ALERT_CLAIM_TTL_SECONDS: '600' });
  assert.equal(settings.alertClaimTtlSeconds, 600);
  await f.state.reserveAlert({ ...claimRow(now), ttlSeconds: settings.alertClaimTtlSeconds });
  assert.equal(await f.state.reserveAlert({ ...claimRow(now + 599), ttlSeconds: settings.alertClaimTtlSeconds }), false);
  assert.equal(await f.state.reserveAlert({ ...claimRow(now + 600), ttlSeconds: settings.alertClaimTtlSeconds }), true);
  assert.throws(() => getConfig({ ...env, ALERT_CLAIM_TTL_SECONDS: '0' }), /ALERT_CLAIM_TTL_SECONDS/);
});


test('claim: pending upcoming watch remains retryable after it goes live', async () => {
  const f = await claimFixture();
  await f.state.reserveAlert(claimRow(now));
  const settings = getConfig(env);
  // This stage was first seen upcoming, so first_seen_live cannot select it.
  const candidates = await f.state.getWatchCandidates(now + 1201, settings);
  assert.equal(candidates.length, 1);
  assert.equal(candidates[0].alerted, false);
  assert.equal(await f.notify(1201), true);
});
