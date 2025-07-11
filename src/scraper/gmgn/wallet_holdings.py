#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Скоринг Solana-кошельков (gmgn.ai).
Вычисляет 14 ключевых метрик и печатает только их.
"""

from __future__ import annotations
import json, sys, time
from datetime import datetime, timezone, timedelta
from statistics import mean, median, StatisticsError
from typing import Any, Dict, List

from curl_cffi import requests as curl
from curl_cffi.requests.errors import CurlError

GMGN_ENDPOINT = "https://gmgn.ai/api/v1/wallet_holdings/sol/{address}"
headers = {
    "sec-ch-ua-full-version-list": "\"Google Chrome\";v=\"137.0.7151.104\", \"Chromium\";v=\"137.0.7151.104\", \"Not/A)Brand\";v=\"24.0.0.0\"",
    "sec-ch-ua-platform": "\"Windows\"",
    "Referer": "https://gmgn.ai/sol/address/43DbKtri_9vooJ8FPhM4k7n39JC9NtW2942K1NUtotNjbeCbbC3jF",
    "sec-ch-ua": "\"Google Chrome\";v=\"137\", \"Chromium\";v=\"137\", \"Not/A)Brand\";v=\"24\"",
    "sec-ch-ua-bitness": "\"64\"",
    "sec-ch-ua-model": "\"\"",
    "sec-ch-ua-mobile": "?0",
    "baggage": "sentry-environment=production,sentry-release=20250617-62-c04b007,sentry-public_key=93c25bab7246077dc3eb85b59d6e7d40,sentry-trace_id=4d83c3ec170d4d598ecb8d9293b3647e,sentry-sample_rate=0.005,sentry-sampled=false",
    "sentry-trace": "461f75434c7d4bb3b80adcb56f4244fe-8e0d7e42f7995574-0",
    "sec-ch-ua-arch": "\"x86\"",
    "sec-ch-ua-full-version": "\"137.0.7151.104\"",
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
    "sec-ch-ua-platform-version": "\"15.0.0\"",
}
params = {
    "device_id": "c45e37f7-53ff-4d68-813b-fd0f7b736979",
    "client_id": "gmgn_web_20250617-62-c04b007",
    "from_app": "gmgn",
    "app_ver": "20250617-62-c04b007",
    "tz_name": "Europe/Moscow",
    "tz_offset": "10800",
    "app_lang": "ru",
    "fp_did": "77abb27885cffbec63c7f9fbd35b4116",
    "os": "web",
    "limit": "50",
    "orderby": "last_active_timestamp",
    "direction": "desc",
    "showsmall": "true",
    "sellout": "true",
    "tx30d": "true",
}

# ─────────────────────────── utils ────────────────────────────────────
def read_lines(path: str) -> List[str]:
    with open(path, encoding="utf-8") as fp:
        return [l.strip() for l in fp if l.strip()]

# ──────────────────── HTTP fetch with retries ─────────────────────────
def fetch_holdings(address: str, max_retry: int = 5) -> List[Dict[str, Any]]:
    attempt = 0
    while True:
        try:
            rsp = curl.get(
                GMGN_ENDPOINT.format(address=address),
                headers=headers | {"Referer": f"https://gmgn.ai/sol/address/{address}"},
                params=params,
                impersonate="chrome120",
                timeout=15,
            )
            rsp.raise_for_status()
            return rsp.json()["data"]["holdings"]
        except (CurlError, Exception) as exc:
            attempt += 1
            if attempt >= max_retry:
                raise RuntimeError(f"all retries failed ({max_retry}) – {exc}") from exc
            time.sleep(1)

# ─────────────────── helpers for metrics ──────────────────────────────
def humanize(sec: int | float | None) -> str | None:
    if sec is None:
        return None
    td = timedelta(seconds=sec)
    d, rem = divmod(td.total_seconds(), 86_400)
    h, rem = divmod(rem, 3_600)
    m, s = divmod(rem, 60)
    return " ".join(f"{int(v)}{u}" for v, u in ((d,"d"),(h,"h"),(m,"m"),(s,"s")) if v or u=="s")

# ───────────────────────── basic block ────────────────────────────────
def calc_basic(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    pnl       = [float(it["total_profit_pnl"]) for it in data]
    wins      = sum(p >= 0 for p in pnl)
    losses    = len(pnl) - wins
    winrate   = wins / len(pnl) * 100 if pnl else 0

    durations = [it["end_holding_at"] - it["start_holding_at"]
                 for it in data if it["start_holding_at"] and it["end_holding_at"]]
    avg_hold  = mean(durations) if durations else None

    starts    = sorted(int(it["start_holding_at"]) for it in data if it.get("start_holding_at"))
    intervals = [b - a for a, b in zip(starts, starts[1:])]
    avg_gap   = mean(intervals) if intervals else None

    return {
        "total_trades"     : len(pnl),
        "winrate_pct"      : round(winrate, 2),
        "wins"             : wins,
        "losses"           : losses,
        "avg_hold_human"   : humanize(avg_hold),
        "avg_interval_human": humanize(avg_gap),
    }

# ───────────────────────── quality block ──────────────────────────────
def calc_quality(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    pnl = [float(it["total_profit_pnl"]) for it in data]
    gross_p = sum(p for p in pnl if p > 0)
    gross_l = -sum(p for p in pnl if p < 0)
    profit_factor = gross_p / gross_l if gross_l else None
    expectancy    = mean(pnl) if pnl else 0

    wins  = [p for p in pnl if p > 0]
    losses = [p for p in pnl if p < 0]
    rr = abs(mean(wins) / mean(losses)) if wins and losses else None

    liqs     = [float(it["liquidity"]) for it in data if it.get("liquidity")]
    med_liq  = median(liqs) if liqs else None

    usd_vals = [float(it["usd_value"]) for it in data if float(it["usd_value"]) > 0]
    hhi = None
    if usd_vals:
        total = sum(usd_vals)
        hhi   = sum((v / total) ** 2 for v in usd_vals) if total else None

    honeypot_share = (sum(bool(it["token"].get("is_honeypot")) for it in data)
                      / len(data) * 100) if data else 0

    net_pnl_30d = sum(float(it.get("realized_profit_30d", 0))
                      + float(it.get("unrealized_profit", 0))
                      for it in data)

    turnover = sum(float(it.get("history_bought_cost", 0))
                   + float(it.get("history_sold_income", 0))
                   for it in data)
    pnl_per_turn = net_pnl_30d / turnover if turnover else None

    last_ts   = max((int(it["last_active_timestamp"]) for it in data), default=None)
    days_idle = (time.time() - last_ts) / 86_400 if last_ts else None

    return {
        "profit_factor"        : round(profit_factor, 4) if profit_factor else None,
        "expectancy"           : round(expectancy, 4),
        "risk_reward"          : round(rr, 4) if rr else None,
        "median_liquidity_usd" : round(med_liq, 2) if med_liq else None,
        "hhi"                  : round(hhi, 4) if hhi else None,
        "honeypot_share_pct"   : round(honeypot_share, 2),
        "net_pnl_30d_usd"      : round(net_pnl_30d, 2),
        "turnover_usd"         : round(turnover, 2),
        "pnl_per_turnover"     : round(pnl_per_turn, 6) if pnl_per_turn else None,
        "days_idle"            : round(days_idle, 2) if days_idle else None,
    }

# ───────────────────────── orchestrator ───────────────────────────────
def analyse_wallet(addr: str) -> Dict[str, Any]:
    data   = fetch_holdings(addr)
    basic  = calc_basic(data)
    qual   = calc_quality(data)
    return {
        "address"      : addr,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        **basic,
        **qual,
    }

# ───────────────────────────── main ────────────────────────────────────
if __name__ == "__main__":
    import argparse
    prs = argparse.ArgumentParser("Solana wallet scorer")
    prs.add_argument("-w", "--wallets", default="src/scraper/gmgn/wallets.txt")
    prs.add_argument("-d", "--delay", type=float, default=0.5)
    args = prs.parse_args()

    wallets = read_lines(args.wallets)
    if not wallets:
        sys.exit("wallet list is empty")

    # какие поля выводить
    KEYS = [
        "address","profit_factor","expectancy","risk_reward","winrate_pct",
        "net_pnl_30d_usd","median_liquidity_usd","hhi","honeypot_share_pct",
        "days_idle","total_trades","avg_hold_human","avg_interval_human",
        "turnover_usd","pnl_per_turnover"
    ]

    try:
        for addr in wallets:
            try:
                full = analyse_wallet(addr)
                out  = {k: full.get(k) for k in KEYS}
                print(json.dumps(out, ensure_ascii=False), flush=True)
            except Exception as exc:
                print(f"⚠️  {addr}: {exc}", flush=True)
            time.sleep(args.delay)
    except KeyboardInterrupt:
        pass
