#!/usr/bin/env python3
"""
Enhanced sync_scraper.py
========================
• Asynchronous batch scraper for gmgn.ai with Redis queue.
• Generates **unique, randomized** request headers & params **per HTTP request**
  so every worker, every call looks like a brand-new client.
• Keeps the original logging, proxy rotation and DB-snapshot logic.

How header/param generation works
---------------------------------
A lightweight factory creates realistic UA strings and UA-Client-Hints that match
one another plus Sentry baggage IDs, language prefs, tz offsets, etc.  The
space of combinations is astronomically large, guaranteeing uniqueness across
20+ workers and thousands of requests.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import json
import os
import random
import threading
import time
import uuid
from typing import Dict, List, Optional

import redis.asyncio as aioredis
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

load_dotenv()

# ───────────────────────────── ENV─────────────────────────────────────
QUEUE_NAME: str = os.getenv("REDIS_QUEUE", "wallet_queue")
API_PERIOD: str = os.getenv("GMGN_PERIOD", "7d")
API_TIMEOUT: int = int(os.getenv("GMGN_TIMEOUT", "30"))
REQ_DELAY: float = float(os.getenv("GMGN_DELAY", "2"))
MAX_RETRIES: int = int(os.getenv("GMGN_RETRIES", "20"))
MAX_WORKERS: int = int(os.getenv("GMGN_WORKERS", "20"))
SHOW_BODY: int = int(os.getenv("GMGN_SHOW_BODY", "300"))
AVG_DELAY: float = float(os.getenv("AVG_DELAY", "3.0"))
PROGRESS_EVERY: int = int(os.getenv("PROGRESS_EVERY", "100"))

# ─────────────────────────── Random header/param factory ─────────────
# =====================================================================
# 1) Constants with real-world options
# ---------------------------------------------------------------------
_BROWSERS = [
    # (brand, UA-template)
    (
        "Google Chrome",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{major}.0.0.0 Safari/537.36",
    ),
    (
        "Opera GX",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{major}.0.0.0 Safari/537.36 OPR/{major}.0.0.0",
    ),
    (
        "Chromium",
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{major}.0.0.0 Safari/537.36",
    ),
]
_PLATFORMS = [
    ("Windows", "15.0.0", "x86", "64"),
    ("macOS", "14.5.0", "x86", "64"),
    ("Linux", "6.9", "x86", "64"),
]
_LANGS = ["ru", "en", "de", "es"]
_TZS = [
    ("Europe/Moscow", 10800),
    ("Europe/Berlin", 7200),
    ("Asia/Tokyo", 32400),
]


# 2) Helper utilities
# ---------------------------------------------------------------------
_def_rng = random.SystemRandom()  # cryptographically better randomness


def _uid() -> str:
    """Return random 32-hex UUID4."""
    return uuid.uuid4().hex


def _build_stamp() -> str:
    today = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d")
    return f"{today}-{_def_rng.randint(50, 1500)}-{_uid()[:7]}"


# 3) Factory that can be used per worker or per request
# ---------------------------------------------------------------------
class HeaderFactory:
    """Generate matching headers + params; thread-safe."""

    def __init__(self, rnd: Optional[random.Random] = None) -> None:
        self.rnd = rnd or _def_rng

    def headers(self, addr: str) -> Dict[str, str]:
        brand, ua_tpl = self.rnd.choice(_BROWSERS)
        major = self.rnd.randint(133, 140)
        ua = ua_tpl.format(major=major)

        platform, platform_ver, arch, bits = self.rnd.choice(_PLATFORMS)

        # полная версия типа 137.0.7151.104
        full_ver = f"{major}.0.{self.rnd.randint(4000, 8000)}.{self.rnd.randint(50, 200)}"

        # **строки**, а не случайные кортежи
        full_list = ", ".join([
            f'"{brand}";v="{full_ver}"',
            f'"Chromium";v="{full_ver}"',
            '"Not/A)Brand";v="24.0.0.0"',
        ])
        sec_ch = ", ".join([
            f'"{brand}";v="{major}"',
            f'"Chromium";v="{major}"',
            '"Not/A)Brand";v="24"',
        ])

        return {
            "Accept": "application/json, text/plain, */*",
            "accept-language": f"ru-RU,ru;q=0.9,{self.rnd.choice(_LANGS)}-US;q=0.8,en;q=0.7",
            "User-Agent": ua,

            # UA-Client-Hints
            "sec-ch-ua-full-version-list": full_list,
            "sec-ch-ua-full-version": f'"{full_ver}"',
            "sec-ch-ua": sec_ch,
            "sec-ch-ua-platform": f'"{platform}"',
            "sec-ch-ua-platform-version": f'"{platform_ver}"',
            "sec-ch-ua-arch": f'"{arch}"',
            "sec-ch-ua-bitness": f'"{bits}"',
            "sec-ch-ua-model": '""',
            "sec-ch-ua-mobile": "?0",

            # Sentry / baggage
            "baggage": (
                "sentry-environment=production,"
                f"sentry-release={_build_stamp()},"
                "sentry-public_key=93c25bab7246077dc3eb85b59d6e7d40,"
                f"sentry-trace_id={_uid()},"
                "sentry-sample_rate=0.005,sentry-sampled=false"
            ),
            "sentry-trace": f"{_uid()}-{_uid()[:16]}-0",

            # Referer
            "Referer": f"https://gmgn.ai/sol/address/{addr}",
        }

    def params(self) -> Dict[str, str]:
        tz_name, tz_offset = self.rnd.choice(_TZS)
        stamp = _build_stamp()
        return {
            "device_id": _uid(),
            "client_id": f"gmgn_web_{stamp}",
            "from_app": "gmgn",
            "app_ver": stamp,
            "tz_name": tz_name,
            "tz_offset": str(tz_offset),
            "app_lang": self.rnd.choice(_LANGS),
            "fp_did": hashlib.md5(_uid().encode()).hexdigest(),
            "os": "web",
            # Static filters
            "limit": "50",
            "orderby": "last_active_timestamp",
            "direction": "desc",
            "showsmall": "true",
            "sellout": "true",
            "tx30d": "true",
        }


# ──────────────────────────── Proxy globals ───────────────────────────
PROXY_POOL: List[str] = []
PROXY_LOCK = threading.Lock()
WORKER_PROXIES: Dict[int, str] = {}

FAIL_WALLETS_FILE = "fail_wallets.txt"

# ───────────────────────────── Helpers ────────────────────────────────

def human_pause() -> float:
    """Return delay seconds based on exponential distribution."""
    delay = random.expovariate(1 / AVG_DELAY)
    if random.random() < 0.02:
        delay += random.uniform(30, 90)
    return delay


async def async_sleep_random():
    await asyncio.sleep(human_pause())


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


# ───────────────────────────── DB save ───────────────────────────────

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


# ─────────────────── HTTP request (sync, in thread) ───────────────────

def fetch_wallet_stat(worker_id: int, wallet: str, factory: HeaderFactory) -> Optional[dict]:
    proxy_str = WORKER_PROXIES[worker_id]
    host, port, user, pwd = proxy_str.split(":", 3)
    proxy_url = f"http://{user}:{pwd}@{host}:{port}"
    proxies = {"http": proxy_url, "https": proxy_url}

    url = f"https://gmgn.ai/api/v1/wallet_stat/sol/{wallet}/{API_PERIOD}"

    for attempt in range(1, MAX_RETRIES + 1):
        hdrs = factory.headers(wallet)
        prms = factory.params()
        try:
            resp = curl.get(
                url=url,
                params=prms,
                headers=hdrs,
                impersonate="chrome120",
                timeout=API_TIMEOUT,
                proxies=proxies,
            )
            resp.raise_for_status()
            return resp.json()
        except HTTPError as e:
            log_http_error(e, wallet, attempt, proxy_str)
        except Exception as e:
            logger.error(
                "[{}|{}] attempt {} via {} → {}: {}",
                worker_id,
                wallet[:6],
                attempt,
                proxy_str,
                type(e).__name__,
                e,
            )
        time.sleep(1.5)
    return None


# ───────────────────── per-wallet async task ──────────────────────────
async def process_wallet(worker_id: int, wallet: str, *, factory: HeaderFactory) -> None:
    loop = asyncio.get_running_loop()
    data = await loop.run_in_executor(None, fetch_wallet_stat, worker_id, wallet, factory)

    if data is None:
        mark_failed_wallet(wallet)
        logger.warning("{}… failed after {} retries", wallet[:6], MAX_RETRIES)
        rotate_proxy(worker_id)
        return

    try:
        pnl_value = data["data"]["pnl"]
    except (KeyError, TypeError):
        logger.error("[{}] Unexpected API payload: {}", wallet[:6], data)
        return

    logger.info("[worker {}] {}… PnL {:.3f}", worker_id, wallet[:6], pnl_value)
    await save_snapshot_if_positive(wallet, pnl_value)


async def worker_chunk(
    worker_id: int,
    wallets: List[str],
    *,
    token: str,
    total: int,
    processed: List[int],
) -> None:
    """Worker coroutine: processes its wallet slice, updates shared counter."""
    factory = HeaderFactory(random.Random(uuid.uuid4().int ^ worker_id))
    logger.info(
        "[worker {}] start token {} wallets={} proxy {}",
        worker_id,
        token,
        len(wallets),
        WORKER_PROXIES[worker_id],
    )

    for w in wallets:
        try:
            await process_wallet(worker_id, w, factory=factory)
        except Exception as e:
            logger.exception("[worker {}] unhandled on wallet {}: {}", worker_id, w, e)

        processed[0] += 1
        done = processed[0]
        if done % PROGRESS_EVERY == 0 or done == total:
            logger.info("[{}] progress {} / {}", token, done, total)

        await async_sleep_random()

    logger.info("[worker {}] finished token {} ({} wallets)", worker_id, token, len(wallets))


async def handle_batch(wallets: List[str], proxies: List[str], *, token: str, src: str) -> None:
    global PROXY_POOL, WORKER_PROXIES

    total = len(wallets)
    logger.success("➡️  batch start token={} src={} wallets={}", token, src, total)

    n = min(MAX_WORKERS, len(proxies), total)
    if n == 0:
        logger.warning("No proxies or wallets (token={})", token)
        return

    # distribute proxies round-robin
    initial = proxies[:n]
    PROXY_POOL = proxies[n:]
    WORKER_PROXIES = {i: initial[i] for i in range(n)}

    processed = [0]  # shared mutable counter

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
    """Listen Redis queue endlessly and dispatch batches."""
    rds: aioredis.Redis = get_redis()
    logger.success("Worker started, queue '%s'", QUEUE_NAME)

    while True:
        try:
            _key, payload = await rds.blpop(QUEUE_NAME)  # block indefinitely
            raw = json.loads(payload)

            if isinstance(raw, dict):
                wallets = raw.get("wallets", [])
                token = raw.get("token", "unknown")
                src = raw.get("src", "unknown")
            else:
                wallets = raw
                token = src = "legacy"

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
    proxies_file = os.path.join(here, "proxies_cap.txt")

    proxies = load_lines(proxies_file)
    if not proxies:
        logger.warning("proxies_cap.txt missing or empty")
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
