#!/usr/bin/env python3
"""Robinhood free-mint watcher. One Telegram message per new free public stage."""

import json
import os
import sys
import time
from pathlib import Path

import requests

API = "https://api.opensea.io"
CHAINS = (
    "robinhood",
    "base",
    "ethereum",
    "hyperevm",
    "polygon",
    "ink",
    "arbitrum",
    "optimism",
)
DROP_TYPES = ("featured", "upcoming", "recently_minted")
PUBLIC_HINTS = ("public", "fcfs", "open sale", "open mint", "general")
PRIVATE_HINTS = (
    "allowlist",
    "whitelist",
    "presale",
    "pre-sale",
    "pre sale",
    "gtd",
    "guaranteed",
    "team",
    "holder",
    "vip",
    "reserved",
)
PUBLIC_TYPES = {"public", "public_sale", "publicsale"}
PRIVATE_TYPES = {"presale", "signed_presale", "allowlist", "whitelist", "private"}

API_KEY = os.getenv("OPENSEA_API_KEY", "").strip()
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
COLLECTION_LIMIT = int(os.getenv("COLLECTION_LIMIT", "12"))
REQUEST_GAP = float(os.getenv("REQUEST_GAP", "0.8"))
STATE_PATH = Path(os.getenv("STATE_PATH", "seen.json"))
LAST_CALL = 0.0


class NotADrop(Exception):
    pass


def load_state():
    if not STATE_PATH.exists():
        return {"sent": {}, "misses": {}}
    try:
        data = json.loads(STATE_PATH.read_text())
    except json.JSONDecodeError:
        return {"sent": {}, "misses": {}}
    data.setdefault("sent", {})
    data.setdefault("misses", {})
    return data


def save_state(state):
    STATE_PATH.write_text(json.dumps(state))


def get_json(path, params=None):
    global LAST_CALL
    for attempt in range(4):
        wait = REQUEST_GAP - (time.time() - LAST_CALL)
        if wait > 0:
            time.sleep(wait)
        LAST_CALL = time.time()
        response = requests.get(
            f"{API}{path}",
            headers={"accept": "application/json", "x-api-key": API_KEY},
            params=params,
            timeout=30,
        )
        if response.status_code == 404:
            raise NotADrop()
        if response.status_code == 429:
            retry = response.headers.get("Retry-After", "")
            time.sleep(int(retry) if retry.isdigit() else min(30, 2 ** attempt * 2))
            continue
        response.raise_for_status()
        return response.json()
    raise requests.HTTPError(f"rate limited: {path}")


def slug_of(drop):
    return (
        drop.get("slug")
        or drop.get("collection_slug")
        or (drop.get("collection") or {}).get("slug")
        or ""
    )


def name_of(drop):
    return (
        drop.get("name")
        or drop.get("collection_name")
        or drop.get("collectionName")
        or (drop.get("collection") or {}).get("name")
        or slug_of(drop)
        or "Free mint"
    )


def as_int(value):
    if value is None or value == "":
        return None
    if isinstance(value, dict):
        value = value.get("value") or value.get("wei") or value.get("raw")
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def price_wei(stage):
    raw = stage.get("price")
    if raw is None:
        raw = stage.get("price_wei") or stage.get("priceWei") or 0
    if isinstance(raw, dict):
        raw = raw.get("value") or raw.get("wei") or raw.get("raw") or 0
    try:
        return int(raw or 0)
    except (TypeError, ValueError):
        return -1


def is_public(stage):
    kind = str(stage.get("type") or stage.get("stage_type") or stage.get("stageType") or "").lower()
    if kind in PUBLIC_TYPES:
        return True
    if kind in PRIVATE_TYPES:
        return False
    label = str(stage.get("label") or stage.get("name") or "").lower()
    if any(hint in label for hint in PRIVATE_HINTS) and not any(hint in label for hint in PUBLIC_HINTS):
        return False
    return any(hint in label for hint in PUBLIC_HINTS)


def supply_left(drop):
    minted = as_int(drop.get("total_supply") or drop.get("totalSupply") or drop.get("minted"))
    maximum = as_int(drop.get("max_supply") or drop.get("maxSupply"))
    if minted is None or maximum is None:
        return None
    return max(maximum - minted, 0)


def rows_from(payload):
    if isinstance(payload, list):
        return payload
    return payload.get("drops") or payload.get("collections") or payload.get("results") or payload.get("data") or []


def list_drops():
    found = []
    seen = set()
    collections = []
    for chain in CHAINS:
        for drop_type in DROP_TYPES:
            try:
                payload = get_json(
                    "/api/v2/drops",
                    {"type": drop_type, "limit": 50, "chains": chain},
                )
            except requests.HTTPError as exc:
                print(f"{chain} {drop_type}: {exc}")
                continue
            for row in rows_from(payload):
                slug = slug_of(row)
                if not slug or slug in seen:
                    continue
                seen.add(slug)
                detail = row if row.get("stages") else get_json(f"/api/v2/drops/{slug}")
                if detail:
                    detail["chain"] = detail.get("chain") or chain
                    found.append(detail)
        try:
            payload = get_json(
                "/api/v2/collections",
                {"chain": chain, "order_by": "created_date", "limit": COLLECTION_LIMIT},
            )
        except requests.HTTPError as exc:
            print(f"{chain} collections: {exc}")
            continue
        for row in rows_from(payload):
            row["chain"] = chain
            collections.append(row)
    return found, seen, collections


def send(text):
    response = requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        json={"chat_id": CHAT_ID, "text": text, "disable_web_page_preview": True},
        timeout=30,
    )
    response.raise_for_status()


def consider(drop, state):
    slug = slug_of(drop)
    if not slug or supply_left(drop) == 0:
        return 0
    now = int(time.time())
    sent = 0
    for index, stage in enumerate(drop.get("stages") or []):
        if price_wei(stage) != 0 or not is_public(stage):
            continue
        end = as_int(stage.get("end_time") or stage.get("endTime"))
        if end is not None and end < now:
            continue
        start = as_int(stage.get("start_time") or stage.get("startTime")) or 0
        key = f"{slug}:{index}:{start}"
        if key in state["sent"]:
            continue
        label = str(stage.get("label") or stage.get("name") or "Public")
        chain = drop.get("chain") or "unknown"
        send(f"{name_of(drop)}\n{chain} · {label} · free\nhttps://opensea.io/collection/{slug}")
        state["sent"][key] = now
        sent += 1
        print(f"sent {slug} {label}")
    return sent


def scan(state):
    drops, seen, collections = list_drops()
    count = 0
    for drop in drops:
        if drop:
            count += consider(drop, state)
    misses = state["misses"]
    now = int(time.time())
    for row in collections:
        slug = row.get("collection") or row.get("slug")
        if not slug or slug in seen:
            continue
        if misses.get(slug) and now - misses[slug] < 6 * 60 * 60:
            continue
        seen.add(slug)
        try:
            detail = get_json(f"/api/v2/drops/{slug}")
        except NotADrop:
            misses[slug] = now
            continue
        if detail:
            detail["chain"] = detail.get("chain") or row.get("chain")
            count += consider(detail, state)
    save_state(state)
    print(f"done, new messages: {count}")


def main():
    missing = [name for name, value in (
        ("OPENSEA_API_KEY", API_KEY),
        ("TELEGRAM_BOT_TOKEN", BOT_TOKEN),
        ("TELEGRAM_CHAT_ID", CHAT_ID),
    ) if not value]
    if missing:
        raise SystemExit("Missing " + ", ".join(missing))
    state = load_state()
    once = "--once" in sys.argv
    while True:
        try:
            scan(state)
        except requests.RequestException as exc:
            print(f"scan failed: {exc}")
            if once:
                raise
        if once:
            return
        time.sleep(int(os.getenv("POLL_SECONDS", "45")))


if __name__ == "__main__":
    main()
