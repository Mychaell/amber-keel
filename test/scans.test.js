import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { readFileSync } from 'node:fs';
import { createState } from '../src/state.js';
import { getConfig } from '../src/current.js';
import { runDiscoveryScan, runWatchScan, dispatchScheduled } from '../src/index.js';
import { Tide } from '../src/tide.js';
const now = 1800000000;
const iso = n => new Date(n * 1000).toISOString();
const env = { DB: {}, OPENSEA_API_KEY: 'x', TELEGRAM_BOT_TOKEN: 'x', TELEGRAM_CHAT_ID: 'x', TARGET_CHAINS: 'base', MAX_FALLBACK_DETAILS: '0' };
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
  return { sql, state, api, calls, set: d => { current = d; },
    discovery: async (seconds = 0, extra = {}) => runDiscoveryScan({ ...env, ...extra }, { state, api, nowSec: now + seconds, telegram: async () => {} }),
    watch: async (seconds = 60, extra = {}, telegram = async () => {}) => runWatchScan({ ...env, ...extra }, { state, api, nowSec: now + seconds, telegram }) };
}
test('migrations apply and watch cron calls only direct watched details', async () => {
  const f = fixture(); await f.discovery();
  f.calls.feeds = f.calls.chains = f.calls.collections = 0;
  await dispatchScheduled({ cron: '* * * * *' }, env, { state: f.state, api: f.api, nowSec: now + 60 });
  assert.equal(f.calls.feeds, 0); assert.equal(f.calls.collections, 0); assert.equal(f.calls.chains, 0);
  assert.equal(f.calls.details.length, 2);
});
test('discovery cron runs feeds and never queries watch candidates', async () => {
  const f = fixture(); f.state.getWatchCandidates = () => { throw new Error('watch query called'); };
  await dispatchScheduled({ cron: '*/5 * * * *' }, env, { state: f.state, api: f.api, nowSec: now });
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
  assert.equal(getConfig({ ...secrets, ...vars }).practiceMode, true);
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
