const DEFAULT_CHAINS = ["robinhood", "base", "optimism", "ethereum", "ink", "arc", "hyperevm"];

function intVar(env, name, fallback, min, max) {
  const raw = env[name];
  const n = raw == null || raw === "" ? fallback : Number(raw);
  if (!Number.isInteger(n) || n < min || n > max) {
    throw new Error(`${name} must be an integer between ${min} and ${max}`);
  }
  return n;
}

function boolVar(env, name, fallback = false) {
  const raw = env[name];
  if (raw == null || raw === "") return fallback;
  return /^(1|true|yes|on)$/i.test(String(raw));
}

export function getConfig(env) {
  const chains = String(env.TARGET_CHAINS || DEFAULT_CHAINS.join(","))
    .split(",")
    .map((x) => x.trim().toLowerCase())
    .filter(Boolean);

  if (!chains.length) throw new Error("TARGET_CHAINS is empty");

  for (const secret of ["OPENSEA_API_KEY", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]) {
    if (!env[secret]) throw new Error(`${secret} is missing`);
  }
  if (!env.DB) throw new Error("D1 binding DB is missing");

  return {
    chains: [...new Set(chains)],
    detailRefreshSeconds: intVar(env, "DETAIL_REFRESH_SECONDS", 300, 60, 86400),
    maxDetailsPerScan: intVar(env, "MAX_DETAILS_PER_SCAN", 8, 0, 100),
    collectionSweepSeconds: intVar(env, "COLLECTION_SWEEP_SECONDS", 300, 60, 86400),
    collectionLimit: intVar(env, "COLLECTION_LIMIT", 12, 1, 100),
    maxFallbackDetails: intVar(env, "MAX_FALLBACK_DETAILS", 12, 0, 100),
    missTtlSeconds: intVar(env, "MISS_TTL_SECONDS", 21600, 300, 604800),
    alertFreeAllowlists: boolVar(env, "ALERT_FREE_ALLOWLISTS", false),
    maxAlertPerWallet: intVar(env, "MAX_ALERT_PER_WALLET", 10, 1, 1000000),
  };
}
