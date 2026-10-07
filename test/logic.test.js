import test from "node:test";
import assert from "node:assert/strict";
import { isFreePublicStage, parseIsoSeconds, stageStatus } from "../src/util.js";
import { buildMessage, notifyCandidate } from "../src/signal.js";
import { Tide } from "../src/tide.js";

const NOW = Math.floor(Date.parse("2026-10-07T12:00:00Z") / 1000);

function stage(overrides = {}) {
  return {
    uuid: "stage-1",
    stage_type: "public_sale",
    price: "0",
    start_time: "2026-10-07T13:00:00Z",
    end_time: "2026-10-07T14:00:00Z",
    max_per_wallet: "2",
    ...overrides,
  };
}

const drop = {
  chain: "robinhood",
  collection_slug: "hood-apes",
  collection_name: "Hood Apes",
  contract_address: "0x1234",
  opensea_url: "https://opensea.io/collection/hood-apes/overview",
  total_supply: "10",
  max_supply: "2222",
};

test("zero-price public stage qualifies", () => {
  assert.equal(isFreePublicStage(stage(), NOW), true);
});

test("paid public stage does not qualify", () => {
  assert.equal(isFreePublicStage(stage({ price: "1" }), NOW), false);
});

test("zero-price allowlist does not qualify by default", () => {
  assert.equal(isFreePublicStage(stage({ stage_type: "presale" }), NOW), false);
});

test("expired stage does not qualify", () => {
  assert.equal(isFreePublicStage(stage({ start_time: "2026-10-07T10:00:00Z", end_time: "2026-10-07T11:00:00Z" }), NOW), false);
});

test("future free public stage is upcoming", () => {
  assert.equal(stageStatus(stage(), NOW), "UPCOMING");
});

test("active free public stage is live", () => {
  assert.equal(stageStatus(stage({ start_time: "2026-10-07T11:00:00Z" }), NOW), "LIVE");
});

test("ISO dates parse correctly", () => {
  assert.equal(parseIsoSeconds("2026-10-07T12:00:00Z"), NOW);
});

test("identical stage UUID does not generate a second alert", async () => {
  const sent = new Set();
  let telegramCalls = 0;
  const state = {
    hasAlert: async (key) => sent.has(key),
    markAlert: async ({ key }) => sent.add(key),
  };
  const telegram = async () => { telegramCalls += 1; };
  const args = { drop, stage: stage(), state, env: {}, nowSec: NOW, telegram };
  assert.equal(await notifyCandidate(args), true);
  assert.equal(await notifyCandidate(args), false);
  assert.equal(telegramCalls, 1);
});

test("wallet limit of 10 still qualifies for alerting", async () => {
  let telegramCalls = 0;
  const state = {
    hasAlert: async () => false,
    markAlert: async () => {},
  };
  const telegram = async () => { telegramCalls += 1; };
  const sent = await notifyCandidate({
    drop,
    stage: stage({ max_per_wallet: "10" }),
    state,
    env: {},
    nowSec: NOW,
    maxAlertPerWallet: 10,
    telegram,
  });
  assert.equal(sent, true);
  assert.equal(telegramCalls, 1);
});

test("wallet limit above 10 is filtered from Telegram", async () => {
  let telegramCalls = 0;
  let marked = false;
  const state = {
    hasAlert: async () => false,
    markAlert: async () => { marked = true; },
  };
  const telegram = async () => { telegramCalls += 1; };
  const sent = await notifyCandidate({
    drop,
    stage: stage({ max_per_wallet: "11" }),
    state,
    env: {},
    nowSec: NOW,
    maxAlertPerWallet: 10,
    telegram,
  });
  assert.equal(sent, false);
  assert.equal(telegramCalls, 0);
  assert.equal(marked, false);
});

test("different stage UUID on the same collection generates another alert", async () => {
  const sent = new Set();
  let telegramCalls = 0;
  const state = {
    hasAlert: async (key) => sent.has(key),
    markAlert: async ({ key }) => sent.add(key),
  };
  const telegram = async () => { telegramCalls += 1; };
  await notifyCandidate({ drop, stage: stage({ uuid: "a" }), state, env: {}, nowSec: NOW, telegram });
  await notifyCandidate({ drop, stage: stage({ uuid: "b" }), state, env: {}, nowSec: NOW, telegram });
  assert.equal(telegramCalls, 2);
});

test("pagination follows next until null", async () => {
  const urls = [];
  const fakeFetch = async (url) => {
    urls.push(url);
    const u = new URL(url);
    const cursor = u.searchParams.get("cursor");
    const body = cursor
      ? { drops: [{ collection_slug: "b" }], next: null }
      : { drops: [{ collection_slug: "a" }], next: "next-page" };
    return new Response(JSON.stringify(body), { status: 200, headers: { "content-type": "application/json" } });
  };
  const api = new Tide({ apiKey: "x", fetchImpl: fakeFetch });
  const rows = await api.listDrops("upcoming", ["base", "ink"]);
  assert.deepEqual(rows.map((x) => x.collection_slug), ["a", "b"]);
  assert.equal(urls.length, 2);
  assert.equal(new URL(urls[1]).searchParams.get("cursor"), "next-page");
});

test("Telegram failure does not mark the stage sent", async () => {
  let marked = false;
  const state = {
    hasAlert: async () => false,
    markAlert: async () => { marked = true; },
  };
  const telegram = async () => { throw new Error("nope"); };
  assert.equal(await notifyCandidate({ drop, stage: stage(), state, env: {}, nowSec: NOW, telegram }), false);
  assert.equal(marked, false);
});

test("message uses exact OpenSea URL", () => {
  const msg = buildMessage(drop, stage(), NOW);
  assert.match(msg, /https:\/\/opensea\.io\/collection\/hood-apes\/overview/);
});
