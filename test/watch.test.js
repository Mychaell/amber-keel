import test from 'node:test';
import assert from 'node:assert/strict';
import { notifyCandidate } from '../src/signal.js';
import { detailRefreshSeconds } from '../src/watch.js';
import { getConfig } from '../src/current.js';
import { runScan } from '../src/index.js';
const now = 1800000000;
const iso = seconds => new Date(seconds * 1000).toISOString();
const config = { stabilityMinSeconds: 600, stabilityMinObservations: 3, upcomingAlertWindowSeconds: 600, liveMinObservations: 2, watchRefreshSeconds: 60, watchWindowSeconds: 1800, detailRefreshSeconds: 300 };
const drop = { chain: 'base', collection_slug: 'test', collection_name: 'Test', max_supply: 150 };
const stage = { uuid: 'a', price: '0', stage_type: 'public_sale', max_per_wallet: '10', start_time: iso(now + 1200), end_time: iso(now + 7200) };
function fixture(overrides = {}) {
  const watches = new Map(), alerts = new Set(), messages = [];
  const state = {
    getStageWatch: async key => watches.get(key) || null,
    saveStageWatch: async row => watches.set(row.stage_key, { ...row }),
    hasAlert: async key => alerts.has(key), markAlert: async ({ key }) => alerts.add(key),
    getAlertedWatches: async () => [...watches.values()].filter(w => alerts.has(w.stage_key)),
  };
  const observe = (seconds, changes = {}, scanId = String(seconds)) => notifyCandidate({ drop, stage: { ...stage, ...overrides, ...changes }, nowSec: now + seconds, scanId, state, env: {}, config, telegram: async (_, msg) => messages.push(msg) });
  return { observe, state, watches, messages };
}
test('first sighting and three early observations do not alert; stable T-10 does', async () => {
  const f = fixture();
  assert.equal(await f.observe(0), false);
  assert.equal(await f.observe(60), false);
  assert.equal(await f.observe(120), false);
  assert.equal(await f.observe(599), false);
  assert.equal(await f.observe(600), true);
  assert.equal(await f.observe(660), false);
  assert.equal(f.messages.length, 1);
});
test('stability outside the upcoming alert window does not alert', async () => {
  const f = fixture({ start_time: iso(now + 3600) });
  await f.observe(0); await f.observe(300);
  assert.equal(await f.observe(600), false);
});
test('summary and detail within a scan count once, including changed fields', async () => {
  const f = fixture();
  await f.observe(0, {}, 'scan'); await f.observe(1, {}, 'scan');
  assert.equal([...f.watches.values()][0].consecutive_free_observations, 1);
  await f.observe(2, { max_per_wallet: '9' }, 'scan');
  assert.equal([...f.watches.values()][0].consecutive_free_observations, 1);
});
for (const [name, change] of Object.entries({ start: { start_time: iso(now + 1500) }, wallet: { max_per_wallet: '9' }, end: { end_time: iso(now + 8000) }, type: { stage_type: 'presale' } })) {
  test(`${name} change resets stability`, async () => {
    const f = fixture(); await f.observe(0); await f.observe(300);
    assert.equal(await f.observe(600, change), false);
    const watch = [...f.watches.values()][0];
    assert.equal(watch.first_free_at, now + 600);
    assert.equal(watch.consecutive_free_observations, 1);
  });
}
test('free to paid resets; paid to free must earn a fresh period', async () => {
  const f = fixture({ start_time: iso(now + 2400) });
  await f.observe(0); await f.observe(300); await f.observe(600, { price: '1' });
  let w = [...f.watches.values()][0];
  assert.equal(w.first_free_at, null); assert.equal(w.consecutive_free_observations, 0); assert.equal(w.ever_seen_paid, 1);
  await f.observe(1200); await f.observe(1500);
  assert.equal(await f.observe(1799), false);
  assert.equal(await f.observe(1800), true);
});
test('late upcoming discovery cannot bypass stability when it becomes live', async () => {
  const f = fixture({ start_time: iso(now + 300) });
  await f.observe(0); await f.observe(60);
  assert.equal(await f.observe(299), false);
  assert.equal(await f.observe(600), false);
});
test('first discovered live needs two separate observations', async () => {
  const f = fixture({ start_time: iso(now - 60) });
  assert.equal(await f.observe(0), false);
  assert.equal(await f.observe(1, {}, '0'), false);
  assert.equal(await f.observe(60), true);
});
test('alerted stage switching paid sends exactly one warning across scans and switches', async () => {
  const f = fixture(); await f.observe(0); await f.observe(300); await f.observe(600);
  assert.equal(await f.observe(660, { price: '1', max_per_wallet: '50' }), true);
  assert.equal(await f.observe(720, { price: '1' }), false);
  await f.observe(780); await f.observe(840, { price: '2' });
  assert.equal(f.messages.length, 2);
  assert.equal(f.messages[1], '⚠️ MINT CHANGED\nTest\nChain: Base\nPrice changed: FREE → PAID\nDo not mint until rechecked.');
});
test('60 second refresh only for nearby free candidates or unended alerted stages', () => {
  const refresh = (s, watches = []) => detailRefreshSeconds({ ...drop, next_stage: s }, watches, now, config);
  assert.equal(refresh(stage), 60);
  assert.equal(refresh({ ...stage, start_time: iso(now) }), 60);
  assert.equal(refresh({ ...stage, start_time: iso(now + 1800) }), 60);
  assert.equal(refresh({ ...stage, start_time: iso(now + 1801) }), 300);
  assert.equal(refresh({ ...stage, price: '1' }), 300);
  assert.equal(refresh(null, [{ chain: 'base', slug: 'test', last_end_time: String(now + 1) }]), 60);
  assert.equal(refresh(null, [{ chain: 'base', slug: 'test', last_end_time: String(now) }]), 300);
  assert.equal(refresh(null, [{ chain: 'base', slug: 'other', last_end_time: null }]), 300);
});
test('unalerted free mint that started two hours ago uses normal refresh', () => {
  const liveStage = { ...stage, start_time: iso(now - 7200) };
  const liveDrop = { ...drop, active_stage: liveStage, stages: [liveStage] };
  assert.equal(detailRefreshSeconds(liveDrop, [], now, config), 300);
  assert.equal(detailRefreshSeconds(liveDrop, [{ chain: 'base', slug: 'test', last_end_time: String(now + 1) }], now, config), 60);
});
test('config defaults and integer validation', () => {
  const env = { DB: {}, OPENSEA_API_KEY: 'x', TELEGRAM_BOT_TOKEN: 'x', TELEGRAM_CHAT_ID: 'x' };
  const actual = getConfig(env);
  for (const [key, value] of Object.entries(config)) assert.equal(actual[key], value);
  for (const key of ['STABILITY_MIN_SECONDS', 'STABILITY_MIN_OBSERVATIONS', 'UPCOMING_ALERT_WINDOW_SECONDS', 'LIVE_MIN_OBSERVATIONS', 'WATCH_REFRESH_SECONDS', 'WATCH_WINDOW_SECONDS']) {
    assert.throws(() => getConfig({ ...env, [key]: '0' }), /must be an integer/);
  }
});
test('Worker merges summary and detail into one observation and polls selectively', async () => {
  const f = fixture(); let detailCalls = 0; let cache = null;
  Object.assign(f.state, { getSetting: async () => String(now), getCaches: async () => new Map(cache ? [['base:test', cache]] : []),
    upsertCache: async row => { cache = { active_uuid: row.activeUuid, next_uuid: row.nextUuid, last_detail_at: row.lastDetailAt }; }, clearMiss: async () => {}, markMiss: async () => {} });
  const api = { listDrops: async () => [{ ...drop, next_stage: stage }], getDrop: async () => { detailCalls++; return { ...drop, next_stage: stage, stages: [stage] }; } };
  const env = { DB: {}, OPENSEA_API_KEY: 'x', TELEGRAM_BOT_TOKEN: 'x', TELEGRAM_CHAT_ID: 'x', MAX_FALLBACK_DETAILS: '0' };
  await runScan(env, { api, state: f.state, nowSec: now, scanId: 'a' });
  assert.equal([...f.watches.values()][0].consecutive_free_observations, 1);
  await runScan(env, { api, state: f.state, nowSec: now + 59, scanId: 'b' });
  assert.equal(detailCalls, 1);
  await runScan(env, { api, state: f.state, nowSec: now + 60, scanId: 'c' });
  assert.equal(detailCalls, 2);
  assert.equal([...f.watches.values()][0].consecutive_free_observations, 3);
});
test('live paid-to-free recovery also requires a fresh ten minute period', async () => {
  const f = fixture({ start_time: iso(now - 60) });
  await f.observe(0); await f.observe(60, { price: '1' });
  await f.observe(120);
  assert.equal(await f.observe(180), false);
  assert.equal(await f.observe(719), false);
  assert.equal(await f.observe(720), true);
});
