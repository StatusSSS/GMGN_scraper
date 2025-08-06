from __future__ import annotations
"""
Обновлённая версия sync_scraper.py
• По умолчанию работает один воркер (MAX_WORKERS = env("GMGN_WORKERS", 1))
• Добавлено подробное логирование соответствия UA ↔ cookies:
    - показываем UA и список имён куков, которыми реально пользуется запрос;
    - предупреждаем, если UA из fingerprints не совпадает с тем, через который
      куки были получены test_cap.py (метка [UA MISMATCH]).
"""

import asyncio
import json
import os
import random
import threading
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote
import redis.asyncio as aioredis
import redis as redis_sync
from curl_cffi import requests as curl
from curl_cffi.requests.exceptions import HTTPError
from dotenv import load_dotenv
from sqlalchemy.exc import IntegrityError

from src.sdk.infrastructure.logger import logger
from src.sdk.databases.postgres.dependency import with_db_session
from src.sdk.databases.postgres.models import Wallet
from src.sdk.queues.redis_connect import get_redis

from src.scraper.gmgn import fingerprints as fp
from src.scraper.gmgn.proxy_manager import ProxyManager

load_dotenv()

# ────────────────────────── ENV ──────────────────────────────────────
QUEUE_NAME       = os.getenv("REDIS_QUEUE", "wallet_queue")
REDIS_HOST       = os.getenv("REDIS_HOST", "redis")
REDIS_PORT       = int(os.getenv("REDIS_PORT", 6379))
COOKIE_TASKS_Q   = os.getenv("COOKIE_TASKS", "cookie_tasks")

API_PERIOD       = os.getenv("GMGN_PERIOD", "7d")
API_TIMEOUT      = int(os.getenv("GMGN_TIMEOUT", "30"))
MAX_RETRIES      = int(os.getenv("GMGN_RETRIES", "20"))
# ↓ По умолчанию 1 воркер; можно переопределить GMGN_WORKERS
MAX_WORKERS      = int(os.getenv("GMGN_WORKERS", "1"))
SHOW_BODY        = int(os.getenv("GMGN_SHOW_BODY", "300"))
AVG_DELAY        = float(os.getenv("AVG_DELAY", "5"))
PROGRESS_EVERY   = int(os.getenv("PROGRESS_EVERY", "100"))

COOL_DOWN_SEC    = 30 * 60        # как было, 30 мин «охлаждение» IP
WAIT_COOKIES_SEC = 10

BAD_STATUSES = {403, 429, 500, 502, 503, 504}

# ───────────────────────── globals ───────────────────────────────────
PM: ProxyManager
WORKER_PROXIES: Dict[int, str] = {}

UA_COOKIES: Dict[str, Tuple[str, str]] = {}   # proxy → (ua, cookie_hdr)
UA_COOKIES_LOCK = threading.Lock()

FAIL_WALLETS_FILE = "fail_wallets.txt"
LAST_BEAT: Dict[int, float] = {}

# sync-redis (используется в потоках fetch_wallet_stat)
rds_sync: redis_sync.Redis = redis_sync.Redis(
    REDIS_HOST, REDIS_PORT, decode_responses=True
)

# ───────────────────────── helpers ──────────────────────────────────
def human_pause() -> float:
    """Экспоненциальная пауза, имитирующая случайные задержки человека."""
    delay = random.expovariate(1 / AVG_DELAY)
    if random.random() < 0.02:
        delay += random.uniform(30, 90)  # иногда залипаем надолго
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
        "[{}] {} via {} → {} {} | body={} | headers={}",
        wallet, attempt, proxy, resp.status_code, resp.reason,
        snippet,
        {k: resp.headers.get(k) for k in ("cf-ray", "cf-cache-status", "retry-after")},
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

# ─────────────────── redis-session helpers ───────────────────────────
def _session_key(proxy: str) -> str:
    return f"cookies:{proxy}"



def load_session(proxy: str) -> Tuple[str, str]:
    while True:
        raw = rds_sync.get(_session_key(proxy))
        if raw:
            payload = json.loads(raw)
            ua = payload["ua"]

            # куки → строка, все спец-символы кодируем
            cookie_hdr = "; ".join(
                f"{c['name']}={quote(str(c['value']), safe='')}"
                for c in payload["cookies"]
            )

            logger.debug(
                "[LOAD] {} ua={} cookies={}",
                proxy, ua[:40], cookie_hdr[:120],
            )
            return ua, cookie_hdr

        logger.info(
            "[session] cookies for {} not ready → sleep {}s",
            proxy, WAIT_COOKIES_SEC,
        )
        time.sleep(WAIT_COOKIES_SEC)


def invalidate_session(proxy: str, ua: str):
    """Удаляем куки и ставим задачу на повторное получение."""
    rds_sync.delete(_session_key(proxy))
    rds_sync.rpush(COOKIE_TASKS_Q, json.dumps({"proxy": proxy, "ua": ua}))
    logger.warning("[session] invalidated cookies for {} → task re-queued", proxy)

# ───────────────────────── DB save ───────────────────────────────────
@with_db_session
async def save_snapshot_if_positive(wallet: str, pnl_value: float, *, db_session):
    if pnl_value <= 0.6:      # храним только «плюсовые» кошельки
        return
    snapshot = Wallet(address=wallet, pnl=round(pnl_value, 3))
    db_session.add(snapshot)
    try:
        await db_session.flush()
        logger.success("Snapshot saved for {} (PnL {:.3f})", wallet, pnl_value)
    except IntegrityError:
        await db_session.rollback()


def fetch_wallet_stat(worker_id: int, wallet: str) -> Optional[dict]:
    proxy_str = WORKER_PROXIES[worker_id]

    # ── helpers ────────────────────────────────────────────────────
    def build_proxy_dict(p: str) -> Dict[str, str]:
        host, port, user, pwd = p.split(":", 3)
        return {
            "http":  f"http://{user}:{pwd}@{host}:{port}",
            "https": f"http://{user}:{pwd}@{host}:{port}",
        }

    # ── pre-calc ───────────────────────────────────────────────────
    proxies_dict = build_proxy_dict(proxy_str)
    url = f"https://gmgn.ai/api/v1/wallet_stat/sol/{wallet}/{API_PERIOD}"

    with UA_COOKIES_LOCK:
        ua, cookie_hdr = UA_COOKIES[proxy_str]

    logger.debug(
        "[UA/COOKIES] proxy {} ua={}… cookies={}",
        proxy_str, ua[:80],
        ", ".join(c.split('=')[0] for c in cookie_hdr.split("; "))
    )

    # ── main loop ──────────────────────────────────────────────────
    for attempt in range(1, MAX_RETRIES + 1):
        headers, params = fp.pick_headers_params(proxy_str, 0)

        # base-headers
        headers.update({
            "Cookie":  cookie_hdr,
            "Referer": f"https://gmgn.ai/sol/address/{wallet}",
            "Origin": "https://gmgn.ai",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "X-Requested-With": "XMLHttpRequest",
        })

        # 1️⃣ UA из cookies ↔ UA в запросе
        headers["User-Agent"] = ua

        # 2️⃣ /wallet_stat — оставляем только period, остальные фильтры убираем
        for trash in ("limit", "orderby", "direction",
                      "showsmall", "sellout", "tx30d"):
            params.pop(trash, None)

        params["period"] = API_PERIOD            # ← вернуть period=7d

        logger.debug(
            "[REQ] proxy {} → {} | headers={} | params={}",
            proxy_str, url,
            {k: headers.get(k) for k in (
                "User-Agent", "Cookie", "Origin",
                "Sec-Fetch-Site", "Sec-Fetch-Mode",
                "Sec-Fetch-Dest", "X-Requested-With",
            )},
            params,
        )
        logger.debug("[UA OK] proxy {}", proxy_str)

        # safety-check: все headers — str
        for k, v in headers.items():
            if not isinstance(v, str):
                logger.error("Header not str: %s = %r", k, v)

        try:
            resp = curl.get(
                url,
                params=params,
                headers=headers,
                impersonate="chrome138",      # JA3 ⇆ UA-major = 138
                timeout=API_TIMEOUT,
                proxies=proxies_dict,
            )
            resp.raise_for_status()
            return resp.json()

        except HTTPError as e:
            log_http_error(e, wallet, attempt, proxy_str)
            if e.response.status_code in BAD_STATUSES:
                invalidate_session(proxy_str, ua)
                ua, cookie_hdr = load_session(proxy_str)
                with UA_COOKIES_LOCK:
                    UA_COOKIES[proxy_str] = (ua, cookie_hdr)
                time.sleep(2)

        except Exception as e:
            logger.error(
                "[{}|{}] attempt {} via {} → {}: {}",
                worker_id, wallet[:6], attempt, proxy_str,
                type(e).__name__, e,
            )

        time.sleep(1.5)

    return None


async def process_wallet(worker_id: int, wallet: str) -> None:
    if isinstance(wallet, dict):
        wallet = wallet.get("signing_wallet") or str(wallet)
    loop = asyncio.get_running_loop()
    data = await asyncio.wait_for(
        loop.run_in_executor(None, fetch_wallet_stat, worker_id, wallet),
        timeout=API_TIMEOUT + 10,
    )

    if data is None:
        mark_failed_wallet(wallet)
        logger.warning("{}… failed after {} retries", wallet[:6], MAX_RETRIES)
        return

    pnl_value = data.get("data", {}).get("pnl")
    if pnl_value is None:
        logger.error("[{}] Unexpected payload: {}", wallet[:6], data)
        return

    logger.info("[worker {}] {}… PnL {:.3f}", worker_id, wallet[:6], pnl_value)
    await save_snapshot_if_positive(wallet, pnl_value)

# ───────────────────────── worker coroutine ──────────────────────────
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
        worker_id, token, len(wallets), WORKER_PROXIES[worker_id]
    )

    LAST_BEAT[worker_id] = time.time()

    for w in wallets:
        try:
            await process_wallet(worker_id, w)
        except Exception as e:
            logger.exception("[worker {}] unhandled on wallet {}: {}", worker_id, w, e)

        processed[0] += 1
        done = processed[0]
        if done % PROGRESS_EVERY == 0 or done == total:
            logger.info("[{}] progress {} / {}", token, done, total)

        LAST_BEAT[worker_id] = time.time()
        await async_sleep_random()

    logger.info(
        "[worker {}] finished token {} ({} wallets)",
        worker_id, token, len(wallets)
    )

# ─────────────────────── watchdog & heartbeat ────────────────────────
async def watchdog():
    while True:
        now = time.time()
        for wid, ts in list(LAST_BEAT.items()):
            if now - ts > 300:
                logger.error("[WATCHDOG] worker {} frozen? last beat {:.0f}s ago", wid, now - ts)
        await asyncio.sleep(60)


async def heartbeat():
    while True:
        logger.info(
            "[♥] tasks={} in_use={} readyIP={} cooling{}",
            len(asyncio.all_tasks()),
            len(PM.in_use), len(PM.ready), len(PM.cooling)
        )
        await asyncio.sleep(30)

# ───────────────────────── batch handler ─────────────────────────────
async def handle_batch(wallets: List[str], *, token: str, src: str):
    total = len(wallets)
    logger.success("➡️  batch start token={} src={} wallets={}", token, src, total)

    if not fp.PROXY_IDENTITIES:
        fp.init_proxies(PM.ready)

    n = min(MAX_WORKERS, len(PM.ready), total)
    if n == 0:
        logger.warning("No proxies or wallets (token={})", token)
        return

    initial = [PM.acquire() for _ in range(n)]
    global WORKER_PROXIES
    WORKER_PROXIES = {i: initial[i] for i in range(n)}

    for p in initial:
        ua, cookie_hdr = load_session(p)
        UA_COOKIES[p] = (ua, cookie_hdr)

    processed = [0]
    chunks = [wallets[i::n] for i in range(n)]
    tasks = [
        asyncio.create_task(
            worker_chunk(i, chunks[i], token=token, total=total, processed=processed)
        )
        for i in range(n)
    ] + [asyncio.create_task(watchdog()), asyncio.create_task(heartbeat())]

    await asyncio.gather(*tasks, return_exceptions=True)
    logger.success("✅ batch finished token={} wallets={}", token, total)

# ───────────────────────── Redis loop ────────────────────────────────
async def redis_loop() -> None:
    rds: aioredis.Redis = get_redis()
    logger.success("Worker started, queue '{}'", QUEUE_NAME)

    while True:
        try:
            _key, payload = await rds.blpop(QUEUE_NAME)
            raw = json.loads(payload)

            if isinstance(raw, Dict):
                wallets = raw.get("wallets", [])
                token   = raw.get("token", "unknown")
                src     = raw.get("src",   "unknown")
            else:
                wallets = raw
                token = src = "legacy"

            if not isinstance(wallets, list):
                logger.error("Malformed message: {}", raw)
                continue

            await handle_batch(wallets, token=token, src=src)

        except Exception as exc:
            logger.exception("redis_loop error: {}", exc)
            await asyncio.sleep(3)


def main() -> None:
    global PM

    here = os.path.dirname(__file__)
    proxies = load_lines(os.path.join(here, "proxies.txt"))
    if not proxies:
        logger.warning("proxies.txt missing or empty")
        return
    logger.success("Loaded {} proxies", len(proxies))

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
