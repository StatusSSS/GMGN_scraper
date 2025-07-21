
from __future__ import annotations

import asyncio
import json
import os
import random
import threading
import time
import uuid
from collections import defaultdict
from typing import DefaultDict, Dict, List, Optional

import redis.asyncio as aioredis
from curl_cffi import requests as curl
from curl_cffi.requests.exceptions import HTTPError
from dotenv import load_dotenv
from sqlalchemy.exc import IntegrityError

# ─── internal SDK ─────────────────────────────────────────────────────
from src.sdk.infrastructure.logger import logger
from src.sdk.databases.postgres.dependency import with_db_session
from src.sdk.databases.postgres.models import Wallet
from src.sdk.queues.redis_connect import get_redis

# ─── gmgn helpers ─────────────────────────────────────────────────────
from src.scraper.gmgn import fingerprints as fp
from src.scraper.gmgn.proxy_manager import ProxyManager  # ← новый модуль

load_dotenv()

# ─────────────────────────── ENV ──────────────────────────────────────
QUEUE_NAME = os.getenv("REDIS_QUEUE", "wallet_queue")
API_PERIOD = os.getenv("GMGN_PERIOD", "7d")
API_TIMEOUT = int(os.getenv("GMGN_TIMEOUT", "30"))
MAX_RETRIES = int(os.getenv("GMGN_RETRIES", "20"))
MAX_WORKERS = int(os.getenv("GMGN_WORKERS", "20"))
SHOW_BODY = int(os.getenv("GMGN_SHOW_BODY", "300"))
AVG_DELAY = float(os.getenv("AVG_DELAY", "3.0"))
PROGRESS_EVERY = int(os.getenv("PROGRESS_EVERY", "100"))
COOL_DOWN_SEC = int(os.getenv("PROXY_COOLDOWN", "7200"))
ERR_THRESHOLD = int(os.getenv("PROXY_ERR_THRESHOLD", "10"))
MAX_IP_LIFETIME = int(os.getenv("PROXY_MAX_LIFETIME", "3600"))

# ───────────────────────── Proxy globals ──────────────────────────────
PM: ProxyManager  # инициализируется в main()
WORKER_PROXIES: Dict[int, str] = {}
ERROR_STREAK: DefaultDict[str, int] = defaultdict(int)
FIRST_USE_TS: Dict[str, float] = {}

# счётчик запросов на каждый IP для триггера ротации fingerprint’а
PROXY_COUNTERS: Dict[str, int] = {}
PROXY_COUNTERS_LOCK = threading.Lock()

FAIL_WALLETS_FILE = "fail_wallets.txt"

# ────────────────────────── helpers ───────────────────────────────────

def human_pause() -> float:
    """Экспоненциальная задержка + редкие длинные паузы."""
    delay = random.expovariate(1 / AVG_DELAY)
    if random.random() < 0.02:
        delay += random.uniform(30, 90)
    return delay


async def async_sleep_random():  # non‑blocking sleep
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
        with open(FAIL_WALLETS_FILE, "a+", encoding="utf-8") as f:
            f.seek(0)
            if wallet in {ln.strip() for ln in f}:
                return
            f.write(wallet + "\n")
    except Exception as exc:
        logger.error("Cannot write to {}: {}", FAIL_WALLETS_FILE, exc)


# ───────────────────────── Proxy rotation ────────────────────────────

def rotate_proxy(worker_id: int) -> None:
    """Снять IP с воркера, отправить его на охлаждение и выдать новый."""
    current = WORKER_PROXIES[worker_id]
    PM.release(current)  # → cooldown

    new_proxy = PM.acquire()  # ← ready / recycled
    WORKER_PROXIES[worker_id] = new_proxy

    # сбрасываем счётчики для нового IP
    ERROR_STREAK[new_proxy] = 0
    FIRST_USE_TS[new_proxy] = time.time()

    logger.info(
        "[worker {}] proxy switch: {} → {}", worker_id, current, new_proxy
    )


# ───────────────────────── DB save ────────────────────────────────────


@with_db_session
async def save_snapshot_if_positive(wallet: str, pnl_value: float, *, db_session):
    if pnl_value <= 0.6:
        return
    snapshot = Wallet(address=wallet, pnl=round(pnl_value, 3))
    db_session.add(snapshot)
    try:
        await db_session.flush()
        logger.success("Snapshot saved for {} (PnL {:.3f})", wallet, pnl_value)
    except IntegrityError:  # duplicate
        await db_session.rollback()


# ────────────────── HTTP request (sync, thread) ───────────────────────

def fetch_wallet_stat(worker_id: int, wallet: str) -> Optional[dict]:
    proxy_str = WORKER_PROXIES[worker_id]
    host, port, user, pwd = proxy_str.split(":", 3)
    proxy_url = f"http://{user}:{pwd}@{host}:{port}"
    proxies = {"http": proxy_url, "https": proxy_url}

    url = f"https://gmgn.ai/api/v1/wallet_stat/sol/{wallet}/{API_PERIOD}"

    for attempt in range(1, MAX_RETRIES + 1):
        # ≡ выбор fingerprint для текущего IP ≡
        with PROXY_COUNTERS_LOCK:
            cnt = PROXY_COUNTERS[proxy_str]
            PROXY_COUNTERS[proxy_str] += 1
        headers, params = fp.pick_headers_params(proxy_str, cnt)
        headers["Referer"] = f"https://gmgn.ai/sol/address/{wallet}"

        try:
            resp = curl.get(
                url,
                params=params,
                headers=headers,
                impersonate="chrome120",
                timeout=API_TIMEOUT,
                proxies=proxies,
            )
            resp.raise_for_status()
            # success -------------------------------------------
            ERROR_STREAK[proxy_str] = 0
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

        # ошибка: инкремент счётчика
        ERROR_STREAK[proxy_str] += 1
        time.sleep(1.5)

        too_many = ERROR_STREAK[proxy_str] >= ERR_THRESHOLD
        too_long = time.time() - FIRST_USE_TS.get(proxy_str, 0) >= MAX_IP_LIFETIME
        if too_many or too_long:
            logger.warning(
                "[worker {}] proxy {} overheated (errors={}, lived={} s)",
                worker_id,
                proxy_str,
                ERROR_STREAK[proxy_str],
                int(time.time() - FIRST_USE_TS.get(proxy_str, 0)),
            )
            rotate_proxy(worker_id)
            return None

    return None





async def process_wallet(worker_id: int, wallet: str) -> None:
    loop = asyncio.get_running_loop()
    data = await loop.run_in_executor(None, fetch_wallet_stat, worker_id, wallet)

    if data is None:
        mark_failed_wallet(wallet)
        logger.warning("{}… failed after {} retries", wallet[:6], MAX_RETRIES)
        return

    try:
        pnl_value = data["data"]["pnl"]
    except (KeyError, TypeError):
        logger.error("[{}] Unexpected API payload: {}", wallet[:6], data)
        return

    logger.info("[worker {}] {}… PnL {:.3f}", worker_id, wallet[:6], pnl_value)
    await save_snapshot_if_positive(wallet, pnl_value)


# ───────────────────────── worker coroutine ───────────────────────────


async def worker_chunk(
    worker_id: int,
    wallets: List[str],
    *,
    token: str,
    total: int,
    processed: List[int],
) -> None:

    logger.info(
        "[worker {}] start token {} wallets={} proxy {}",
        worker_id,
        token,
        len(wallets),
        WORKER_PROXIES[worker_id],
    )

    for w in wallets:
        try:
            await process_wallet(worker_id, w)
        except Exception as e:
            logger.exception("[worker {}] unhandled on wallet {}: {}", worker_id, w, e)

        processed[0] += 1
        done = processed[0]
        if done % PROGRESS_EVERY == 0 or done == total:
            logger.info("[{}] progress {} / {}", token, done, total)

        await async_sleep_random()

    logger.info(
        "[worker {}] finished token {} ({} wallets)",
        worker_id,
        token,
        len(wallets),
    )


# ────────────────────────── batch handler ─────────────────────────────


async def handle_batch(wallets: List[str], *, token: str, src: str):
    total = len(wallets)
    logger.success("➡️  batch start token={} src={} wallets={}", token, src, total)

    # инициализируем fingerprints при первом запуске
    if not fp.PROXY_IDENTITIES:
        fp.init_proxies(PM.ready)
        for p in PM.ready:
            PROXY_COUNTERS[p] = 0

    n = min(MAX_WORKERS, len(PM.ready) + len(PM.cooling), total)
    if n == 0:
        logger.warning("No proxies or wallets (token={})", token)
        return

    # выдаём воркерам IP из ProxyManager’а
    initial = [PM.acquire() for _ in range(n)]
    for proxy in initial:
        FIRST_USE_TS[proxy] = time.time()
        ERROR_STREAK[proxy] = 0

    global WORKER_PROXIES
    WORKER_PROXIES = {i: initial[i] for i in range(n)}

    processed = [0]  # shared counter
    chunks = [wallets[i::n] for i in range(n)]
    tasks = [
        asyncio.create_task(
            worker_chunk(i, chunks[i], token=token, total=total, processed=processed)
        )
        for i in range(n)
    ]
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.success("✅ batch finished token={} wallets={}", token, total)


# ───────────────────────── Redis loop ─────────────────────────────────


async def redis_loop() -> None:
    rds: aioredis.Redis = get_redis()
    logger.success("Worker started, queue '%s'", QUEUE_NAME)

    while True:
        try:
            _key, payload = await rds.blpop(QUEUE_NAME)  # block
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

            await handle_batch(wallets, token=token, src=src)

        except Exception as exc:
            logger.exception("redis_loop error: %s", exc)
            await asyncio.sleep(3)


# ─────────────────────────── Entrypoint ───────────────────────────────


def main() -> None:
    global PM

    here = os.path.dirname(__file__)
    proxies_file = os.path.join(here, "proxies.txt")
    proxies = load_lines(proxies_file)
    if not proxies:
        logger.warning("proxies.txt missing or empty")
        return

    PM = ProxyManager(proxies, cool_down_sec=COOL_DOWN_SEC)

    while True:
        try:
            asyncio.run(redis_loop())
        except KeyboardInterrupt:
            logger.info("Stopped by Ctrl-C")
            break
        except Exception as exc:
            logger.exception("sync_scraper crash: {} — restart in 5s", exc)
            time.sleep(5)


if __name__ == "__main__":
    main()
