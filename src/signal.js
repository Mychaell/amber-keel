import { isPracticeChain, isMutedChain } from "./current.js";
import { observeStage, isStable } from "./watch.js";
import { alertKey, isSoldOut, fmtInteger, fmtUtc, isFreePublicStage, stageStatus, parseWei, parseIsoSeconds } from "./util.js";

function chainLabel(chain) {
  const names = {
    robinhood: "Robinhood",
    base: "Base",
    optimism: "Optimism",
    ethereum: "Ethereum",
    ink: "Ink",
    arc: "Arc",
    hyperevm: "HyperEVM",
    arbitrum: "Arbitrum",
  };
  return names[String(chain || "").toLowerCase()] || String(chain || "Unknown");
}

export function buildMessage(drop, stage, nowSec = Math.floor(Date.now() / 1000)) {
  const status = stageStatus(stage, nowSec) || "UNKNOWN";
  const isPublic = stage.stage_type === "public_sale";
  const lines = [
    isPublic
      ? (status === "LIVE" ? "🔥 FREE PUBLIC MINT" : "🟢 FREE PUBLIC MINT")
      : "🟡 FREE ALLOWLIST",
    "",
    drop.collection_name || drop.collection_slug || "OpenSea drop",
    `Chain: ${chainLabel(drop.chain)}`,
    "Price: FREE",
  ];

  const minted = fmtInteger(drop.total_supply);
  const max = fmtInteger(drop.max_supply);
  if (minted != null && max != null) lines.push(`Supply: ${minted} / ${max}`);

  if (stage.max_per_wallet != null && stage.max_per_wallet !== "") {
    lines.push(`Limit: ${stage.max_per_wallet} / wallet`);
  }

  const start = fmtUtc(stage.start_time);
  if (start) lines.push("", `Starts: ${start}`);
  lines.push(`Status: ${status}`);

  if (drop.contract_address) lines.push("", "Contract:", drop.contract_address);
  if (drop.opensea_url) lines.push("", "🔗 OpenSea:", drop.opensea_url);

  return lines.join("\n");
}

export async function sendTelegram(env, text, fetchImpl = fetch) {
  const url = `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`;
  const res = await fetchImpl(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      chat_id: env.TELEGRAM_CHAT_ID,
      text,
      disable_web_page_preview: true,
    }),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Telegram ${res.status}${body ? `: ${body.slice(0, 160)}` : ""}`);
  }
}

export async function notifyCandidate({
  drop,
  stage,
  state,
  env,
  nowSec,
  allowFreeAllowlists = false,
  maxAlertPerWallet = 10,
  minAlertSupply = 150,
  scanId,
  config = { stabilityMinSeconds: 600, stabilityMinObservations: 3, upcomingAlertWindowSeconds: 600, liveMinObservations: 2 },
  telegram = sendTelegram,
  metrics,
}) {
  if (!stage?.uuid || isMutedChain(config, drop.chain)) return false;
  const practice = isPracticeChain(config, drop.chain);
  const key = alertKey(drop, stage);
  const watch = await observeStage({ drop, stage, state, nowSec, scanId, config });
  const alerted = await state.hasAlert(key);
  const price = parseWei(stage.price);
  if (alerted && price != null && price !== 0n && !watch.paid_warning_sent) {
    try {
      await telegram(env, ["⚠️ MINT CHANGED", drop.collection_name || drop.collection_slug,
        `Chain: ${chainLabel(drop.chain)}`, "Price changed: FREE → PAID", "Do not mint until rechecked."].join("\n"));
    } catch (err) {
      console.error(`telegram warning failed ${key}: ${err.message}`);
      return false;
    }
    await state.saveStageWatch({ ...watch, paid_warning_sent: 1 });
    return true;
  }
  if (isSoldOut(drop) || alerted || !isFreePublicStage(stage, nowSec, !practice && allowFreeAllowlists)) return false;
  if (practice) {
    if (config.chains && !config.chains.includes(String(drop.chain).toLowerCase())) return false;
    if (stageStatus(stage, nowSec) === "UPCOMING"
      && parseIsoSeconds(stage.start_time) - nowSec > config.upcomingAlertWindowSeconds) return false;
  } else if (!isStable(watch, stage, nowSec, config)) {
    if (metrics) metrics.unstableSkipped++;
    return false;
  }

  const rawLimit = stage?.max_per_wallet;
  if (!practice && rawLimit != null && rawLimit !== "") {
    const perWallet = Number(rawLimit);
    if (Number.isFinite(perWallet) && perWallet > maxAlertPerWallet) return false;
  }

  const rawSupply = drop?.max_supply;
  if (!practice && rawSupply != null && rawSupply !== "") {
    const maxSupply = Number(rawSupply);
    if (Number.isFinite(maxSupply) && maxSupply < minAlertSupply) return false;
  }

  const message = buildMessage(drop, stage, nowSec);
  // Reserve durably before sending; a failed D1 write must never send a mint alert.
  const claimed = await state.reserveAlert({
    key, chain: drop.chain || "unknown", slug: drop.collection_slug,
    stageUuid: stage.uuid, claimTimestamp: nowSec, ttlSeconds: config.alertClaimTtlSeconds ?? 300, openseaUrl: drop.opensea_url || null,
  });
  if (!claimed) return false;
  try {
    await telegram(env, message);
  } catch (err) {
    try {
      await state.releaseAlert(key, nowSec);
    } catch (releaseError) {
      console.error(`alert release failed ${key}; pending claim will expire: ${releaseError.message}`);
    }
    console.error(`telegram failed ${key}: ${err.message}`);
    return false;
  }
  try {
    if (!await state.confirmAlert(key, nowSec, Math.floor(Date.now() / 1000))) {
      throw new Error("claim no longer owned");
    }
  } catch (err) {
    console.error(`Telegram delivery succeeded but alert confirmation failed ${key}; pending claim remains recoverable: ${err.message}`);
  }
  return true;
}
