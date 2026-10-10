import { alertKey, isFreePublicStage, parseIsoSeconds, parseWei, stageStatus } from "./util.js";

const fields = {
  last_price: stage => parseWei(stage.price)?.toString() ?? null,
  last_start_time: stage => parseIsoSeconds(stage.start_time)?.toString() ?? null,
  last_end_time: stage => parseIsoSeconds(stage.end_time)?.toString() ?? null,
  last_max_per_wallet: stage => stage.max_per_wallet == null || stage.max_per_wallet === "" ? null : String(Number(stage.max_per_wallet)),
  last_stage_type: stage => stage.stage_type ?? null,
};

export async function observeStage({ drop, stage, state, nowSec, scanId = String(nowSec) }) {
  const key = alertKey(drop, stage);
  const previous = await state.getStageWatch(key);
  const values = Object.fromEntries(Object.entries(fields).map(([name, read]) => [name, read(stage)]));
  const changed = previous && Object.keys(fields).some(name => previous[name] !== values[name]);
  const free = parseWei(stage.price) === 0n;
  const paid = parseWei(stage.price) != null && parseWei(stage.price) !== 0n;
  const sameScan = previous?.last_observed_scan_at === String(scanId);
  const row = {
    stage_key: key, chain: String(drop.chain || "unknown").toLowerCase(), slug: drop.collection_slug, stage_uuid: stage.uuid,
    first_seen_at: previous?.first_seen_at ?? nowSec,
    first_free_at: free ? (changed ? nowSec : previous?.first_free_at ?? nowSec) : null,
    last_seen_at: nowSec, last_observed_scan_at: String(scanId),
    consecutive_free_observations: free ? (changed || previous?.first_free_at == null ? 1 : Number(previous.consecutive_free_observations) + (sameScan ? 0 : 1)) : 0,
    ...values,
    last_changed_at: changed ? nowSec : previous?.last_changed_at ?? nowSec,
    ever_seen_paid: paid || previous?.ever_seen_paid ? 1 : 0,
    paid_warning_sent: previous?.paid_warning_sent ?? 0,
    first_seen_live: previous?.first_seen_live ?? (stageStatus(stage, nowSec) === "LIVE" ? 1 : 0),
  };
  await state.saveStageWatch(row);
  return row;
}

export function isStable(watch, stage, nowSec, config) {
  if (watch.first_seen_live && !watch.ever_seen_paid) return watch.consecutive_free_observations >= config.liveMinObservations;
  const start = parseIsoSeconds(stage.start_time);
  const stable = watch.first_free_at != null && nowSec - watch.first_free_at >= config.stabilityMinSeconds
    && watch.consecutive_free_observations >= config.stabilityMinObservations;
  if (!stable || start == null) return false;
  if (watch.first_seen_live) return true;
  // Upcoming discoveries must earn stability before launch, even if evaluated later.
  if (watch.first_free_at + config.stabilityMinSeconds >= start) return false;
  return start - nowSec <= config.upcomingAlertWindowSeconds;
}

export function detailRefreshSeconds(drop, alertedWatches, nowSec, config) {
  const nearLaunch = [drop.active_stage, drop.next_stage, ...(drop.stages || [])].some(stage => {
    const start = parseIsoSeconds(stage?.start_time);
    return isFreePublicStage(stage, nowSec, config.alertFreeAllowlists)
      && start != null && start - nowSec >= 0 && start - nowSec <= config.watchWindowSeconds;
  });
  const alerted = alertedWatches.some(row => row.chain === String(drop.chain).toLowerCase() && row.slug === drop.collection_slug
    && (row.last_end_time == null || Number(row.last_end_time) > nowSec));
  return nearLaunch || alerted ? config.watchRefreshSeconds : config.detailRefreshSeconds;
}
