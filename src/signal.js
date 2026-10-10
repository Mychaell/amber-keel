import { observeStage, isStable } from "./watch.js";
import { alertKey, fmtInteger, fmtUtc, isFreePublicStage, stageStatus, parseWei } from "./util.js";

function chainLabel(chain) {
  const names = {
    robinhood: "Robinhood",
    base: "Base",
    optimism: "Optimism",
    ethereum: "Ethereum",
    ink: "Ink",
    arc: "Arc",
    hyperevm: "HyperEVM",
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
}) {
  if (!stage?.uuid) return false;
  const key = alertKey(drop, stage);
  const watch = await observeStage({ drop, stage, state, nowSec, scanId });
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
  if (alerted || !isFreePublicStage(stage, nowSec, allowFreeAllowlists) || !isStable(watch, stage, nowSec, config)) return false;

  const rawLimit = stage?.max_per_wallet;
  if (rawLimit != null && rawLimit !== "") {
    const perWallet = Number(rawLimit);
    if (Number.isFinite(perWallet) && perWallet > maxAlertPerWallet) return false;
  }

  const rawSupply = drop?.max_supply;
  if (rawSupply != null && rawSupply !== "") {
    const maxSupply = Number(rawSupply);
    if (Number.isFinite(maxSupply) && maxSupply < minAlertSupply) return false;
  }

  const message = buildMessage(drop, stage, nowSec);
  try {
    await telegram(env, message);
  } catch (err) {
    console.error(`telegram failed ${key}: ${err.message}`);
    return false;
  }

  await state.markAlert({
    key,
    chain: drop.chain || "unknown",
    slug: drop.collection_slug,
    stageUuid: stage.uuid,
    sentAt: nowSec,
    openseaUrl: drop.opensea_url || null,
  });
  console.log(`alerted ${key}`);
  return true;
}
