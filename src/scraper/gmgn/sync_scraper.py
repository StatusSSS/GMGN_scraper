#!/usr/bin/env python3
"""
Enhanced sync_scraper.py
========================
Robust logging, progress‑tracking, graceful recovery. Uses shared Loguru
config (``src.sdk.infrastructure.logger``).
"""
from __future__ import annotations

import asyncio
import json
import os
import random
import threading
import time
from typing import Dict, List, Optional

from curl_cffi import requests as curl
from curl_cffi.requests.exceptions import HTTPError
from dotenv import load_dotenv
from sqlalchemy.exc import IntegrityError

# ─── Shared logger ────────────────────────────────────────────────────
from src.sdk.infrastructure.logger import logger

# ─── GMGN / Postgres SDK ──────────────────────────────────────────────
from src.sdk.databases.postgres.dependency import with_db_session
from src.sdk.databases.postgres.models import Wallet
from src.sdk.queues.redis_connect import get_redis
import redis.asyncio as aioredis

load_dotenv()

# ───────────────────────────── ENV─────────────────────────────────────
QUEUE_NAME   = os.getenv("REDIS_QUEUE", "wallet_queue")
API_PERIOD   = os.getenv("GMGN_PERIOD", "7d")
API_TIMEOUT  = int(os.getenv("GMGN_TIMEOUT", "30"))
REQ_DELAY    = float(os.getenv("GMGN_DELAY", "2"))
MAX_RETRIES  = int(os.getenv("GMGN_RETRIES", "20"))
MAX_WORKERS  = int(os.getenv("GMGN_WORKERS", "20"))
SHOW_BODY    = int(os.getenv("GMGN_SHOW_BODY", "300"))
AVG_DELAY = float(os.getenv("AVG_DELAY", "3.0"))
PROGRESS_EVERY = int(os.getenv("PROGRESS_EVERY", "100"))
# ──────────────────────────── Constants ───────────────────────────────
HEADERS_BASE = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'baggage': 'sentry-environment=production,sentry-release=20250718-1289-8c7e54b,sentry-public_key=93c25bab7246077dc3eb85b59d6e7d40,sentry-trace_id=4f22e16c017245a580fe4fc7539071d8,sentry-sample_rate=0.01,sentry-sampled=false',
    'priority': 'u=1, i',
    'referer': 'https://gmgn.ai/sol/address/43DbKtri_9NQn3TLma9sKd6pbBuXy5we6PMs6QLWAz8awhZyGNgmo',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-arch': '"x86"',
    'sec-ch-ua-bitness': '"64"',
    'sec-ch-ua-full-version': '"138.0.7204.157"',
    'sec-ch-ua-full-version-list': '"Not)A;Brand";v="8.0.0.0", "Chromium";v="138.0.7204.157", "Google Chrome";v="138.0.7204.157"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-platform': '"Windows"',
    'sec-ch-ua-platform-version': '"15.0.0"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'sentry-trace': 'eaa883c4174b43cf8a91b7c19f48a2f9-8ca244c0816a50e4-0',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
}

PARAMS_BASE = {
    'device_id': 'c45e37f7-53ff-4d68-813b-fd0f7b736979',
    'client_id': 'gmgn_web_20250718-1289-8c7e54b',
    'from_app': 'gmgn',
    'app_ver': '20250718-1289-8c7e54b',
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
    'hide_airdrop': 'false',
    'tx30d': 'true',
}

# ───────────────────────── Proxy rotation ─────────────────────────────
PROXY_POOL: List[str] = []        # свободные прокси (round‑robin)
PROXY_LOCK = threading.Lock()
WORKER_PROXIES: Dict[int, str] = {}

FAIL_WALLETS_FILE = "fail_wallets.txt"

async def human_pause():
    # экспоненциальная задержка
    delay = random.expovariate(1 / AVG_DELAY)
    # каждые 50 запросов – большой кофе-брейк
    if random.random() < 0.02:
        delay += random.uniform(30, 90)
    await asyncio.sleep(delay)

def load_lines(path: str) -> List[str]:
    try:
        with open(path, encoding="utf-8") as f:
            return [ln.strip() for ln in f if ln.strip()]
    except FileNotFoundError:
        logger.warning("File '{}' not found", path)
        return []

def log_http_error(e: HTTPError, wallet: str, attempt: int, proxy: str) -> None:
    resp = e.response
    snippet = (resp.text or "").strip()[:SHOW_BODY]
    if len(resp.text or "") > SHOW_BODY:
        snippet += "…"
    logger.error(
        "[{}] attempt {} via {} → HTTP {} {} | {}",
        wallet,
        attempt,
        proxy,
        resp.status_code,
        resp.reason,
        snippet,
    )

def mark_failed_wallet(wallet: str) -> None:
    try:
        if os.path.exists(FAIL_WALLETS_FILE):
            with open(FAIL_WALLETS_FILE, "r", encoding="utf-8") as f:
                if wallet in {ln.strip() for ln in f}:
                    return
        with open(FAIL_WALLETS_FILE, "a", encoding="utf-8") as f:
            f.write(wallet + "\n")
    except Exception as e:
        logger.error("Cannot write to {}: {}", FAIL_WALLETS_FILE, e)

# ───────────────────────── Proxy helpers ──────────────────────────────

def rotate_proxy(worker_id: int) -> None:
    """Circularly assign next proxy to worker."""
    with PROXY_LOCK:
        if not PROXY_POOL:
            return
        current = WORKER_PROXIES.get(worker_id)
        new = PROXY_POOL.pop(0)
        WORKER_PROXIES[worker_id] = new
        if current:
            PROXY_POOL.append(current)
        logger.info("[worker {}] proxy switch: {} → {}", worker_id, current, new)

# ───────────────────────────── DB save ────────────────────────────────

@with_db_session
async def save_snapshot_if_positive(wallet: str, pnl_value: float, *, db_session) -> None:
    if pnl_value <= 0.6:
        return
    snapshot = Wallet(address=wallet, pnl=round(pnl_value, 3))
    db_session.add(snapshot)
    try:
        await db_session.flush()
        logger.success("Snapshot saved for {} (PnL {:.3f})", wallet, pnl_value)
    except IntegrityError:
        await db_session.rollback()
        logger.info("Duplicate snapshot skipped for {}", wallet)

# ─────────────────── HTTP request (sync, executor) ────────────────────

def fetch_wallet_stat(worker_id: int, wallet: str, token: str) -> Optional[dict]:
    proxy_str = WORKER_PROXIES[worker_id]
    host, port, user, pwd = proxy_str.split(":", 3)
    proxy_url = f"http://{user}:{pwd}@{host}:{port}"
    proxies = {"http": proxy_url, "https": proxy_url}

    url = f"https://gmgn.ai/api/v1/wallet_stat/sol/{wallet}/{API_PERIOD}"
    headers = {**HEADERS_BASE, "referer": f"https://gmgn.ai/sol/address/{wallet}"}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = curl.get(
                url=url,
                params=PARAMS_BASE,
                headers=headers,
                impersonate="chrome120",
                timeout=API_TIMEOUT,
                proxies=proxies,
            )
            resp.raise_for_status()
            return resp.json()
        except HTTPError as e:
            log_http_error(e, wallet, attempt, proxy_str)
        except Exception as e:
            logger.error("[{}|{}] attempt {} via {} → {}: {}", token, wallet[:6], attempt, proxy_str, type(e).__name__, e)
        time.sleep(1.5)
    return None

# ───────────────────── per‑wallet async task ──────────────────────────

async def process_wallet(worker_id: int, wallet: str, *, token: str) -> None:
    loop = asyncio.get_running_loop()
    data = await loop.run_in_executor(None, fetch_wallet_stat, worker_id, wallet, token)

    if data is None:
        mark_failed_wallet(wallet)
        logger.warning("{}… failed after {} retries", wallet[:6], MAX_RETRIES)
        rotate_proxy(worker_id)
        return

    try:
        pnl_value = data["data"]["pnl"]
    except (KeyError, TypeError):
        logger.error("[{}|{}] Unexpected API payload: {}", token, wallet[:6], data)
        return

    logger.info("[{}] {}… PnL {:.3f}", token, wallet[:6], pnl_value)
    await save_snapshot_if_positive(wallet, pnl_value)

async def worker_chunk(worker_id: int,
                       wallets: List[str],
                       *,
                       token: str,
                       total: int,
                       processed: List[int]) -> None:
    """Ворк-корутина: суммарный счётчик processed[0] показывает общий прогресс."""
    logger.info(
        "[worker {}] start token {} wallets={} proxy {}",
        worker_id, token, len(wallets), WORKER_PROXIES[worker_id]
    )

    for w in wallets:
        try:
            await process_wallet(worker_id, w, token=token)
        except Exception as e:
            logger.exception("[worker {}] unhandled on wallet {}: {}", worker_id, w, e)

        # ---------- общий счётчик ----------
        processed[0] += 1
        done = processed[0]
        if done % PROGRESS_EVERY == 0 or done == total:
            logger.info("[{}] progress {} / {}", token, done, total)
        # -----------------------------------

        await human_pause()

    logger.info("[worker {}] finished token {} ({} wallets)",
                worker_id, token, len(wallets))


async def handle_batch(wallets: List[str], proxies: List[str], *, token: str, src: str) -> None:
    global PROXY_POOL, WORKER_PROXIES

    total = len(wallets)
    logger.success("➡️  batch start token={} src={} wallets={}", token, src, total)

    n = min(MAX_WORKERS, len(proxies), total)
    if n == 0:
        logger.warning("No proxies or wallets (token={})", token)
        return

    # распределяем прокси
    initial = proxies[:n]
    PROXY_POOL = proxies[n:]
    WORKER_PROXIES = {i: initial[i] for i in range(n)}

    processed = [0]  # общий mutable-счётчик для прогресса

    chunks = [wallets[i::n] for i in range(n)]
    tasks = [
        asyncio.create_task(
            worker_chunk(i, chunks[i], token=token, total=total, processed=processed)
        )
        for i in range(n)
    ]
    await asyncio.gather(*tasks, return_exceptions=True)

    logger.success("✅ batch finished token={} wallets={}", token, total)


# ───────────────────────── Main redis loop ────────────────────────────

async def redis_loop(proxies: List[str]) -> None:
    """
    Слушает Redis-очередь и передаёт пачки кошельков в handle_batch().
    Благодаря async-клиенту event-loop больше не блокируется.
    """
    rds = get_redis()                                    # асинхронный клиент
    logger.success("Worker started, queue '%s'", QUEUE_NAME)

    while True:
        try:
            # ↓↓↓ БЫЛО  _, payload = rds.blpop(QUEUE_NAME, 0)
            _key, payload = await rds.blpop(QUEUE_NAME)   # timeout=None ⇒ ждать вечно

            raw = json.loads(payload)

            # --- распаковка сообщения ---
            if isinstance(raw, dict):
                wallets = raw.get("wallets", [])
                token   = raw.get("token", "unknown")
                src     = raw.get("src",   "unknown")
            else:
                wallets = raw
                token = src = "legacy"
            # -----------------------------

            if not isinstance(wallets, list):
                logger.error("Malformed message: %s", raw)
                continue

            await handle_batch(wallets, proxies, token=token, src=src)

        except Exception as exc:
            logger.exception("redis_loop error: %s", exc)
            await asyncio.sleep(3)

# ─────────────────────────── Entrypoint ───────────────────────────────

def main() -> None:
    here = os.path.dirname(__file__)
    proxies_file = os.path.join(here, "proxies.txt")

    proxies = load_lines(proxies_file)
    if not proxies:
        logger.warning("proxies.txt missing or empty")
        return

    while True:
        try:
            asyncio.run(redis_loop(proxies))
        except KeyboardInterrupt:
            logger.info("Stopped by Ctrl-C")
            break
        except Exception as exc:
            logger.exception("sync_scraper crash: {} — restart in 5s", exc)
            time.sleep(5)


if __name__ == "__main__":
    main()