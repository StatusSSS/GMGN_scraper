#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Скоринг Solana‑кошельков (gmgn.ai) с сохранением результата в таблицу **wallet_snapshot**.

Что делает скрипт:
1. Берёт адреса из справочника `wallets` (модель `Wallet`).
2. Для каждого адреса запрашивает историю холдингов на gmgn.ai.
3. Считает 14+ метрик + суммарный PnL.
4. Печатает короткий JSON в stdout (как раньше) **и** вставляет запись
   в `wallet_snapshot` (модель `WalletSnapshot`).

Запуск:
    python score_solana_wallets.py --delay 0.5 [--limit 100]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import datetime, timezone, timedelta
from statistics import mean, median
from typing import Any, Dict, List, Sequence

from curl_cffi import requests as curl
from curl_cffi.requests.errors import CurlError
from sqlalchemy import select

# ────────────────────────── DB imports ────────────────────────────
# ⚠️  Замените пути импорта на те, что используются у вас в проекте
from src.sdk.databases.postgres.dependency import AsyncSessionLocal  # пул соединений
from src.sdk.databases.postgres.models import Wallet, WalletSnapshot                     # модели

# ────────────────────────── HTTP consts ───────────────────────────
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

# ──────────────────── HTTP fetch with retries ─────────────────────

def fetch_holdings(address: str, max_retry: int = 5) -> List[Dict[str, Any]]:
    """Запрашивает историю холдингов адреса на gmgn.ai с ретраями."""
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

# ─────────────────────────── utils ────────────────────────────────

def humanize(sec: int | float | None) -> str | None:
    if sec is None:
        return None
    td = timedelta(seconds=sec)
    d, rem = divmod(td.total_seconds(), 86_400)
    h, rem = divmod(rem, 3_600)
    m, s = divmod(rem, 60)
    return " ".join(
        f"{int(v)}{u}" for v, u in ((d, "d"), (h, "h"), (m, "m"), (s, "s")) if v or u == "s"
    )

# ───────────────────────── basic block ────────────────────────────

def calc_basic(data: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    pnl = [float(it["total_profit_pnl"]) for it in data]
    wins = sum(p >= 0 for p in pnl)
    losses = len(pnl) - wins
    winrate = wins / len(pnl) * 100 if pnl else 0

    durations = [
        it["end_holding_at"] - it["start_holding_at"]
        for it in data
        if it["start_holding_at"] and it["end_holding_at"]
    ]
    avg_hold = mean(durations) if durations else None

    starts = sorted(int(it["start_holding_at"]) for it in data if it.get("start_holding_at"))
    intervals = [b - a for a, b in zip(starts, starts[1:])]
    avg_gap = mean(intervals) if intervals else None

    return {
        "total_trades": len(pnl),
        "winrate_pct": round(winrate, 2),
        "wins": wins,
        "losses": losses,
        "avg_hold_human": humanize(avg_hold),
        "avg_interval_human": humanize(avg_gap),
    }

# ───────────────────────── quality block ──────────────────────────

def calc_quality(data: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    pnl = [float(it["total_profit_pnl"]) for it in data]
    gross_p = sum(p for p in pnl if p > 0)
    gross_l = -sum(p for p in pnl if p < 0)
    profit_factor = gross_p / gross_l if gross_l else None
    expectancy = mean(pnl) if pnl else 0
    total_pnl = sum(pnl)

    wins = [p for p in pnl if p > 0]
    losses = [p for p in pnl if p < 0]
    rr = abs(mean(wins) / mean(losses)) if wins and losses else None

    liqs = [float(it["liquidity"]) for it in data if it.get("liquidity")]
    med_liq = median(liqs) if liqs else None

    usd_vals = [float(it["usd_value"]) for it in data if float(it["usd_value"]) > 0]
    hhi = None
    if usd_vals:
        total = sum(usd_vals)
        hhi = sum((v / total) ** 2 for v in usd_vals) if total else None

    honeypot_share = (
        sum(bool(it["token"].get("is_honeypot")) for it in data) / len(data) * 100 if data else 0
    )

    net_pnl_30d = sum(
        float(it.get("realized_profit_30d", 0)) + float(it.get("unrealized_profit", 0))
        for it in data
    )

    turnover = sum(
        float(it.get("history_bought_cost", 0)) + float(it.get("history_sold_income", 0))
        for it in data
    )
    pnl_per_turn = net_pnl_30d / turnover if turnover else None

    last_ts = max((int(it["last_active_timestamp"]) for it in data), default=None)
    days_idle = (time.time() - last_ts) / 86_400 if last_ts else None

    return {
        "profit_factor": round(profit_factor, 8) if profit_factor else 0,
        "expectancy": round(expectancy, 8),
        "risk_reward": round(rr, 8) if rr else 0,
        "median_liquidity_usd": round(med_liq, 2) if med_liq else 0,
        "hhi": round(hhi, 6) if hhi else 0,
        "honeypot_share_pct": round(honeypot_share, 2),
        "net_pnl_30d_usd": round(net_pnl_30d, 2),
        "turnover_usd": round(turnover, 2),
        "pnl_per_turnover": round(pnl_per_turn, 6) if pnl_per_turn else 0,
        "days_idle": round(days_idle, 2) if days_idle else 0,
        "pnl": round(total_pnl, 2),
    }

# ───────────────────────── orchestrator ──────────────────────────

def analyse_wallet(addr: str) -> Dict[str, Any]:
    data = fetch_holdings(addr)
    basic = calc_basic(data)
    qual = calc_quality(data)
    return {
        "address": addr,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        **basic,
        **qual,
    }

# ────────────────── addresses loader from DB ─────────────────────

async def load_wallet_addresses(limit: int | None = None) -> List[str]:
    """Возвращает адреса из таблицы `wallets` (ordered by id)."""
    async with AsyncSessionLocal() as session:
        result = await session.scalars(select(Wallet.address).order_by(Wallet.id))
        addresses = result.all()
        if limit is not None:
            addresses = addresses[:limit]
        return addresses

# ───────────────────── snapshot persister ────────────────────────

def build_snapshot(row: Dict[str, Any]) -> WalletSnapshot:
    """Преобразует словарь метрик в объект WalletSnapshot."""
    return WalletSnapshot(
        address              = row["address"],
        ts_utc               = datetime.fromisoformat(row["timestamp_utc"]),
        profit_factor        = row["profit_factor"],
        expectancy           = row["expectancy"],
        risk_reward          = row["risk_reward"],
        winrate_pct          = row["winrate_pct"],
        net_pnl_30d_usd      = row["net_pnl_30d_usd"],
        median_liquidity_usd = row["median_liquidity_usd"],
        hhi                  = row["hhi"],
        honeypot_share_pct   = row["honeypot_share_pct"],
        days_idle            = row["days_idle"],
        total_trades         = row["total_trades"],
        avg_hold_human       = row["avg_hold_human"] or "",
        avg_interval_human   = row["avg_interval_human"] or "",
        turnover_usd         = row["turnover_usd"],
        pnl_per_turnover     = row["pnl_per_turnover"],
        pnl                  = row["pnl"],
    )

# ───────────────────────────── main ───────────────────────────────

async def main_async(limit: int | None, delay: float) -> None:
    try:
        wallets = await load_wallet_addresses(limit)
    except Exception as exc:
        sys.exit(f"Не удалось загрузить адреса из БД: {exc}")

    if not wallets:
        sys.exit("Список кошельков пуст — нечего анализировать")

    # какие поля выводить в stdout
    KEYS = [
        "address",
        "profit_factor",
        "expectancy",
        "risk_reward",
        "winrate_pct",
        "net_pnl_30d_usd",
        "median_liquidity_usd",
        "hhi",
        "honeypot_share_pct",
        "days_idle",
        "total_trades",
        "avg_hold_human",
        "avg_interval_human",
        "turnover_usd",
        "pnl_per_turnover",
        "pnl",
    ]

    async with AsyncSessionLocal() as session:
        for addr in wallets:
            try:
                full: Dict[str, Any] = await asyncio.to_thread(analyse_wallet, addr)

                # печать в stdout остаётся для совместимости
                out = {k: full.get(k) for k in KEYS}
                print(json.dumps(out, ensure_ascii=False), flush=True)

                # сохранение в БД
                snapshot = build_snapshot(full)
                session.add(snapshot)
                await session.commit()
            except Exception as exc:
                await session.rollback()
                print(f"⚠️  {addr}: {exc}", flush=True)
            await asyncio.sleep(delay)

if __name__ == "__main__":
    prs = argparse.ArgumentParser("Solana wallet scorer + DB writer")
    prs.add_argument("--limit", "-l", type=int, help="Максимум адресов для обработки")
    prs.add_argument("--delay", "-d", type=float, default=0.5, help="Пауза между запросами, сек")
    args = prs.parse_args()

    try:
        asyncio.run(main_async(args.limit, args.delay))
    except KeyboardInterrupt:
        pass
