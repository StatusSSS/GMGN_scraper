#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Параллельный скоринг Solana-кошельков (gmgn.ai) — с ротацией прокси,
корректным Ctrl-C и сохранением результатов в единый JSON-файл.

Файлы входа
-----------
wallets.txt   — один address в строке
proxies.txt   — ip:port  •  ip:port:user:pass  •  http://user:pass@ip:port  •  socks5://ip:port
"""

from __future__ import annotations

import json
import random
import re
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from math import ceil
from statistics import mean, median
from typing import Any, Dict, List, Sequence
from statistics import StatisticsError
from curl_cffi import requests as curl
from curl_cffi.requests.errors import CurlError

# ╔═════════════════════  API & headers  ═════════════════════════════════╗
GMGN_ENDPOINT = 'https://gmgn.ai/api/v1/wallet_holdings/sol/{address}'
headers = {
    'sec-ch-ua-full-version-list': '"Google Chrome";v="137.0.7151.104", "Chromium";v="137.0.7151.104", "Not/A)Brand";v="24.0.0.0"',
    'sec-ch-ua-platform': '"Windows"',
    'Referer': 'https://gmgn.ai/sol/address/43DbKtri_9vooJ8FPhM4k7n39JC9NtW2942K1NUtotNjbeCbbC3jF',
    'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
    'sec-ch-ua-bitness': '"64"',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-mobile': '?0',
    'baggage': 'sentry-environment=production,sentry-release=20250617-62-c04b007,sentry-public_key=93c25bab7246077dc3eb85b59d6e7d40,sentry-trace_id=4d83c3ec170d4d598ecb8d9293b3647e,sentry-sample_rate=0.005,sentry-sampled=false',
    'sentry-trace': '461f75434c7d4bb3b80adcb56f4244fe-8e0d7e42f7995574-0',
    'sec-ch-ua-arch': '"x86"',
    'sec-ch-ua-full-version': '"137.0.7151.104"',
    'Accept': 'application/json, text/plain, */*',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
    'sec-ch-ua-platform-version': '"15.0.0"',
}
params = {
    'device_id': 'c45e37f7-53ff-4d68-813b-fd0f7b736979',
    'client_id': 'gmgn_web_20250617-62-c04b007',
    'from_app': 'gmgn',
    'app_ver': '20250617-62-c04b007',
    'tz_name': 'Europe/Moscow',
    'tz_offset': '10800',
    'app_lang': 'ru',
    'fp_did': '77abb27885cffbec63c7f9fbd35b4116',
    'os': 'web',
    'limit': '50',
    'orderby': 'last_active_timestamp',
    'direction': 'desc',
    'showsmall': 'true',
    'sellout': 'true',
    'tx30d': 'true',
}

# ╔═════════════════════  utils: чтение файлов  ══════════════════════════╗
PROXY_DEFAULT_SCHEME = "http"      # можно заменить на socks5


def normalize_proxy(line: str) -> str | None:
    """ ip:port[:user:pass] | URL → proxy-URL """
    ln = line.strip()
    if not ln:
        return None
    if re.match(r"^\w+://", ln):          # уже полноформатный URL
        return ln
    parts = ln.split(":")
    if len(parts) == 2:                   # ip:port
        ip, port = parts
        return f"{PROXY_DEFAULT_SCHEME}://{ip}:{port}"
    if len(parts) == 4:                   # ip:port:user:pass
        ip, port, user, pwd = parts
        return f"{PROXY_DEFAULT_SCHEME}://{user}:{pwd}@{ip}:{port}"
    return None


def read_lines(path: str, *, proxy_mode: bool = False) -> List[str]:
    out: list[str] = []
    with open(path, encoding="utf-8") as fp:
        for raw in fp:
            raw = raw.strip()
            if not raw:
                continue
            item = normalize_proxy(raw) if proxy_mode else raw
            if item:
                out.append(item)
    return out


# ╔═════════════════════  HTTP-запрос с ретраями  ════════════════════════╗
def fetch_holdings(address: str, proxies: Sequence[str], max_retry: int = 5) -> List[Dict[str, Any]]:
    attempt = 0
    while True:
        proxy_url = random.choice(proxies)
        prox_cfg = {"http": proxy_url, "https": proxy_url}
        try:
            rsp = curl.get(
                GMGN_ENDPOINT.format(address=address),
                headers=headers | {"Referer": f"https://gmgn.ai/sol/address/{address}"},
                params=params,
                # proxies=prox_cfg,
                impersonate="chrome120",
                timeout=15,
            )
            rsp.raise_for_status()
            return rsp.json()["data"]["holdings"]
        except (CurlError, Exception) as exc:
            attempt += 1
            if attempt >= max_retry:
                raise RuntimeError(f"all retries failed ({max_retry}) – {exc}") from exc
            continue


# ╔═════════════════════  расчёты метрик  ═══════════════════════════════╗
def humanize(sec: int | float | None) -> str | None:
    if sec is None:
        return None
    td = timedelta(seconds=sec)
    d, rem = divmod(td.total_seconds(), 86_400)
    h, rem = divmod(rem, 3_600)
    m, s = divmod(rem, 60)
    return " ".join(
        f"{v}{u}" for v, u in ((int(d), "d"), (int(h), "h"), (int(m), "m"), (int(s), "s")) if v or u == "s"
    )


def calc_basic(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    pnl = [float(it["total_profit_pnl"]) for it in data]
    wins, losses = sum(p >= 0 for p in pnl), sum(p < 0 for p in pnl)
    winrate = wins / len(pnl) * 100 if pnl else 0
    big_wins = sum(p >= 1 for p in pnl)

    durations = [
        it["end_holding_at"] - it["start_holding_at"]
        for it in data
        if it["start_holding_at"] and it["end_holding_at"]
    ]
    avg_hold_sec = mean(durations) if durations else None

    starts = sorted(int(it["start_holding_at"]) for it in data if it.get("start_holding_at"))
    intervals = [b - a for a, b in zip(starts, starts[1:])]
    avg_int_sec = mean(intervals) if intervals else None

    return {
        "total_trades": len(pnl),
        "winrate_pct": round(winrate, 2),
        "wins": wins, "losses": losses,
        "pnl_ge_100pct": big_wins,
        "avg_hold_sec": avg_hold_sec,
        "avg_interval_sec": avg_int_sec,
    }


def calc_quality(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    pnl = [float(it["total_profit_pnl"]) for it in data]
    gross_p, gross_l = sum(p for p in pnl if p > 0), -sum(p for p in pnl if p < 0)
    profit_factor = gross_p / gross_l if gross_l else float("inf")
    expectancy = mean(pnl) if pnl else 0

    wins  = [p for p in pnl if p > 0]
    losses = [p for p in pnl if p < 0]
    try:
        rr = abs(mean(wins) / mean(losses)) if losses and wins else float("inf")
    except StatisticsError:
        rr = float("inf")

    liqs = [float(it["liquidity"]) for it in data if it.get("liquidity")]
    try:
        med_liq = median(liqs) if liqs else 0.0
    except StatisticsError:
        med_liq = 0.0

    usd_vals = [float(it["usd_value"]) for it in data if float(it["usd_value"]) > 0]
    hhi = sum((v / sum(usd_vals)) ** 2 for v in usd_vals) if usd_vals else 0

    honeypot_share = (sum(bool(it["token"].get("is_honeypot")) for it in data)
                      / len(data) * 100) if data else 0
    net_pnl_30d = sum(float(it.get("realized_profit_30d", 0)) +
                      float(it.get("unrealized_profit", 0)) for it in data)

    # days_idle
    last_ts = max((int(it["last_active_timestamp"]) for it in data), default=None)
    days_idle = (time.time() - last_ts) / 86_400 if last_ts else None

    return {
        "profit_factor": round(profit_factor, 4),
        "expectancy": round(expectancy, 4),
        "risk_reward": round(rr, 4),
        "hhi": round(hhi, 4),
        "median_liquidity_usd": round(med_liq, 2),
        "honeypot_share_pct": round(honeypot_share, 2),
        "net_pnl_30d_usd": round(net_pnl_30d, 2),
        "days_idle": round(days_idle, 2) if days_idle is not None else None,
    }


def analyse_wallet(addr: str, proxies: Sequence[str]) -> Dict[str, Any]:
    data = fetch_holdings(addr, proxies)
    basic = calc_basic(data)
    qual = calc_quality(data)
    basic["avg_hold_human"] = humanize(basic.pop("avg_hold_sec"))
    basic["avg_interval_human"] = humanize(basic.pop("avg_interval_sec"))
    return {"address": addr, "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"), **basic, **qual}

# ╔═════════════════════  многопоточность  ═══════════════════════════════╗
results: list[Dict[str, Any]] = []
results_lock = threading.Lock()
print_lock = threading.Lock()
stop_event = threading.Event()


def worker(batch: List[str], proxies: Sequence[str], delay: float):
    for addr in batch:
        if stop_event.is_set():
            break
        try:
            met = analyse_wallet(addr, proxies)
            with results_lock:
                results.append(met)
            with print_lock:
                print(json.dumps(met, ensure_ascii=False, indent=2), flush=True)
        except Exception as exc:
            with print_lock:
                print(f"⚠️  {addr}: {exc}", flush=True)
        time.sleep(delay)


def join_with_interrupt(threads: List[threading.Thread]):
    """Ожидаем завершение потоков, реагируем на Ctrl-C."""
    try:
        while any(t.is_alive() for t in threads):
            for t in threads:
                t.join(timeout=0.5)
    except KeyboardInterrupt:
        stop_event.set()
        print("\n⏹  Ctrl-C pressed — waiting threads to finish…", file=sys.stderr)
        for t in threads:
            t.join()


# ╔═════════════════════  main  ══════════════════════════════════════════╗
if __name__ == "__main__":
    import argparse

    prs = argparse.ArgumentParser("Solana wallet scorer (parallel, proxy, Ctrl-C safe)")
    prs.add_argument("-w", "--wallets", default="src/scraper/gmgn/wallets.txt", help="wallet list")
    prs.add_argument("-p", "--proxies", default="src/scraper/gmgn/proxies.txt", help="proxy list")
    prs.add_argument("-o", "--outfile", default="scores.json", help="output JSON file")
    prs.add_argument("-n", "--threads", type=int, default=1, help="number of threads")
    prs.add_argument("-d", "--delay", type=float, default=0.5, help="delay between requests in each thread")
    args = prs.parse_args()

    wallets = read_lines(args.wallets)
    proxies = read_lines(args.proxies, proxy_mode=True)
    if not proxies:
        raise RuntimeError("proxy list empty/invalid")

    chunk = ceil(len(wallets) / args.threads) or 1
    parts = [wallets[i:i + chunk] for i in range(0, len(wallets), chunk)]

    threads: list[threading.Thread] = [
        threading.Thread(target=worker, args=(part, proxies, args.delay), daemon=True) for part in parts
    ]
    for t in threads:
        t.start()

    join_with_interrupt(threads)

    # сохраняем даже частичные результаты
    with open(args.outfile, "w", encoding="utf-8") as fp:
        json.dump(results, fp, ensure_ascii=False, indent=2)
    print(f"\n✓ Saved {len(results)} records to {args.outfile}", flush=True)
