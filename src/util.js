export function parseIsoSeconds(value) {
  if (!value || typeof value !== "string") return null;
  const ms = Date.parse(value);
  return Number.isFinite(ms) ? Math.floor(ms / 1000) : null;
}

export function parseWei(value) {
  if (value == null || value === "") return null;
  try {
    return BigInt(String(value));
  } catch {
    return null;
  }
}

export function stageStatus(stage, nowSec = Math.floor(Date.now() / 1000)) {
  const start = parseIsoSeconds(stage?.start_time);
  const end = parseIsoSeconds(stage?.end_time);
  if (start == null) return null;
  if (end != null && end <= nowSec) return null;
  return start > nowSec ? "UPCOMING" : "LIVE";
}

const ALLOWLIST_TYPES = new Set(["presale", "allowlist", "whitelist", "signed_presale"]);

export function isFreePublicStage(stage, nowSec = Math.floor(Date.now() / 1000), allowFreeAllowlists = false) {
  if (!stage || !stage.uuid) return false;
  const price = parseWei(stage.price);
  if (price !== 0n) return false;
  if (!stageStatus(stage, nowSec)) return false;
  if (stage.stage_type === "public_sale") return true;
  return allowFreeAllowlists && ALLOWLIST_TYPES.has(stage.stage_type);
}

export function alertKey(drop, stage) {
  return `${String(drop.chain || "unknown").toLowerCase()}:${drop.collection_slug}:${stage.uuid}`;
}

export function dropKey(drop) {
  return `${String(drop.chain || "unknown").toLowerCase()}:${drop.collection_slug}`;
}

export function fmtInteger(value) {
  if (value == null || value === "") return null;
  try {
    return BigInt(String(value)).toLocaleString("en-US");
  } catch {
    return String(value);
  }
}

export function fmtUtc(value) {
  const sec = parseIsoSeconds(value);
  if (sec == null) return null;
  return new Date(sec * 1000).toISOString().replace("T", " ").replace(".000Z", " UTC");
}

export async function mapLimit(items, limit, fn) {
  const out = new Array(items.length);
  let next = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (true) {
      const i = next++;
      if (i >= items.length) return;
      out[i] = await fn(items[i], i);
    }
  });
  await Promise.all(workers);
  return out;
}
