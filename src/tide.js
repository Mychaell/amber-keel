const BASE = "https://api.opensea.io/api/v2";

export class OpenSeaNotFound extends Error {}

function retryDelaySeconds(response, attempt) {
  const retryAfter = response.headers.get("retry-after");
  if (retryAfter && /^\d+$/.test(retryAfter)) return Math.min(Number(retryAfter), 30);
  return Math.min(2 ** attempt * 2, 20);
}

export class Tide {
  constructor({ apiKey, fetchImpl = (input, init) => globalThis.fetch(input, init), sleepImpl = (ms) => new Promise((r) => setTimeout(r, ms)), baseUrl = BASE }) {
    this.apiKey = apiKey;
    this.fetchImpl = fetchImpl;
    this.sleepImpl = sleepImpl;
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  async get(path, params = {}, attempt = 0) {
    const url = new URL(`${this.baseUrl}${path}`);
    for (const [key, value] of Object.entries(params)) {
      if (value != null && value !== "") url.searchParams.set(key, String(value));
    }
    const res = await this.fetchImpl(url.toString(), {
      headers: { accept: "application/json", "x-api-key": this.apiKey },
    });

    const remaining = res.headers.get("x-ratelimit-remaining");
    const limit = res.headers.get("x-ratelimit-limit");
    if (remaining != null && Number(remaining) < 20) {
      console.warn(`OpenSea rate remaining ${remaining}${limit ? `/${limit}` : ""}`);
    }

    if (res.status === 404) throw new OpenSeaNotFound(path);
    if (res.status === 429 && attempt < 2) {
      const wait = retryDelaySeconds(res, attempt);
      console.warn(`OpenSea 429; retrying in ${wait}s`);
      await this.sleepImpl(wait * 1000);
      return this.get(path, params, attempt + 1);
    }
    if (!res.ok) {
      const body = await res.text().catch(() => "");
      throw new Error(`OpenSea ${res.status} ${path}${body ? `: ${body.slice(0, 160)}` : ""}`);
    }
    return res.json();
  }

  async listDrops(type, chains) {
    const rows = [];
    let cursor = null;
    let pages = 0;
    do {
      const payload = await this.get("/drops", {
        type,
        limit: 100,
        chains: chains.join(","),
        cursor,
      });
      rows.push(...(payload.drops || []));
      cursor = payload.next || null;
      pages += 1;
      if (pages > 100) throw new Error(`OpenSea pagination exceeded 100 pages for ${type}`);
    } while (cursor);
    return rows;
  }

  getDrop(slug) {
    return this.get(`/drops/${encodeURIComponent(slug)}`);
  }

  listCollections(chain, limit) {
    return this.get("/collections", { chain, order_by: "created_date", limit });
  }

  getChains() {
    return this.get("/chains");
  }
}
