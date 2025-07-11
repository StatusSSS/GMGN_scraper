#!/usr/bin/env python3
"""
Async-воркер для gmgn.ai: опрашивает кошельки через HTTP-прокси
и сохраняет PnL-снимки (pnl > 0.6) в PostgreSQL.

 • Кошельки — src/wallets.txt
 • Прокси   — src/proxies.txt
 • Параллельность — до GMGN_WORKERS (по умолчанию 20)
 • Ротация прокси: у воркера есть «свой» прокси; при MAX_RETRIES ошибок
   он исключается и берётся следующий из пула.
"""

from __future__ import annotations

import asyncio
import os
import random
import sys
import time
from typing import Dict, List, Optional

import httpx
from dotenv import load_dotenv
from sqlalchemy.exc import IntegrityError

# ─── PostgreSQL SDK ────────────────────────────────────────────
from src.sdk.databases.postgres.dependency import AsyncSessionLocal, engine
from src.sdk.databases.postgres.models import WalletSnapshot

# ─── env / константы ───────────────────────────────────────────
load_dotenv()

API_PERIOD   = os.getenv("GMGN_PERIOD",  "7d")
API_TIMEOUT  = int(os.getenv("GMGN_TIMEOUT",  "30"))
REQ_DELAY    = float(os.getenv("GMGN_DELAY",  "2"))
MAX_RETRIES  = int(os.getenv("GMGN_RETRIES", "20"))
MAX_WORKERS  = int(os.getenv("GMGN_WORKERS", "20"))
SHOW_BODY    = int(os.getenv("GMGN_SHOW_BODY", "300"))

HEADERS_BASE = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
    ),
}

PARAMS_BASE = {
    "device_id": "c45e37f7-53ff-4d68-813b-fd0f7b736979",
    "client_id": "gmgn_web_20250617-62-c04b007",
    "from_app":  "gmgn",
    "app_ver":   "20250617-62-c04b007",
    "tz_name":   "Europe/Moscow",
    "tz_offset": "10800",
    "app_lang":  "ru",
    "fp_did":    "77abb27885cffbec63c7f9fbd35b4116",
    "os":        "web",
}

# ─── структуры для прокси-ротации ─────────────────────────────
PROXY_POOL: List[str]           = []
WORKER_PROXIES: Dict[int, str]  = {}
PROXY_LOCK  = asyncio.Lock()

FAIL_WALLETS_FILE = "fail_wallets.txt"

# ---------------------------------------------------------------------------
# База данных
# ---------------------------------------------------------------------------

async def init_db() -> None:
    """Создаёт таблицы, если их ещё нет (idempotent)."""
    from src.sdk.databases.postgres.base import Base

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

# ---------------------------------------------------------------------------
# Утилиты
# ---------------------------------------------------------------------------

def load_lines(path: str) -> List[str]:
    try:
        with open(path, encoding="utf-8") as fp:
            return [ln.strip() for ln in fp if ln.strip()]
    except FileNotFoundError:
        return []

def log_http_error(e: httpx.HTTPStatusError, wallet: str, attempt: int, proxy: str) -> None:
    body = (e.response.text or "").strip()[:SHOW_BODY]
    if len(e.response.text or "") > SHOW_BODY:
        body += "…"
    print(f"[{wallet}] попытка {attempt} через {proxy}: "
          f"HTTP {e.response.status_code} {e.response.reason_phrase}\n{body}\n")

def mark_failed_wallet(wallet: str) -> None:
    """Записывает кошелёк в fail_wallets.txt (игнорирует дубли)."""
    try:
        if os.path.exists(FAIL_WALLETS_FILE):
            with open(FAIL_WALLETS_FILE, "r", encoding="utf-8") as fp:
                if wallet in {ln.strip() for ln in fp}:
                    return
        with open(FAIL_WALLETS_FILE, "a", encoding="utf-8") as fp:
            fp.write(wallet + "\n")
    except Exception as exc:  # noqa: BLE001
        print(f"⚠️  Не удалось записать в {FAIL_WALLETS_FILE}: {exc}")

# ---------------------------------------------------------------------------
# HTTP-запрос
# ---------------------------------------------------------------------------

async def fetch_wallet_stat(worker_id: int, wallet: str) -> Optional[dict]:
    """
    Пытается получить статистику кошелька, перебирая прокси.
    Возвращает JSON при успехе или None, если все прокси исчерпаны.
    """
    async with PROXY_LOCK:
        proxies_to_try = [WORKER_PROXIES[worker_id]] + PROXY_POOL.copy()

    for proxy_str in proxies_to_try:
        async with PROXY_LOCK:
            WORKER_PROXIES[worker_id] = proxy_str

        host, port, user, pwd = proxy_str.split(":", 3)
        proxy_url = f"http://{user}:{pwd}@{host}:{port}"
        url       = f"https://gmgn.ai/api/v1/wallet_stat/sol/{wallet}/{API_PERIOD}"
        headers   = {**HEADERS_BASE, "referer": f"https://gmgn.ai/sol/address/{wallet}"}

        async with httpx.AsyncClient(proxies=proxy_url,
                                     timeout=API_TIMEOUT,
                                     http2=True) as client:
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    r = await client.get(url, headers=headers, params=PARAMS_BASE)
                    r.raise_for_status()
                    return r.json()

                except httpx.HTTPStatusError as e:
                    log_http_error(e, wallet, attempt, proxy_str)
                except Exception as exc:  # noqa: BLE001
                    print(f"[{wallet}] попытка {attempt} через {proxy_str}: "
                          f"{type(exc).__name__}: {exc}")

                await asyncio.sleep(1.5)  # back-off

        # proxy исчерпана
        mark_failed_wallet(wallet)
        async with PROXY_LOCK:
            if proxy_str in PROXY_POOL:
                PROXY_POOL.remove(proxy_str)
            print(f"[worker {worker_id}] прокси {proxy_str} исключена "
                  f"после {MAX_RETRIES} ошибочных попыток\n")

    return None

# ---------------------------------------------------------------------------
# Бизнес-логика
# ---------------------------------------------------------------------------

async def save_snapshot(address: str, pnl: float) -> None:
    """INSERT INTO wallet_snapshot (address, pnl, ts_utc) VALUES (…)"""
    async with AsyncSessionLocal() as session:
        session.add(WalletSnapshot(address=address, pnl=pnl))
        try:
            await session.commit()
        except IntegrityError:
            await session.rollback()   # дубликат? спокойно игнорируем

async def process_wallet(worker_id: int, wallet: str) -> None:
    data = await fetch_wallet_stat(worker_id, wallet)
    if not data:
        print(f"{wallet[:6]}… ❌")
        return

    pnl_value = round(float(data["data"]["pnl"]), 3)
    print(f"{pnl_value:.3f}")

    if pnl_value > 0.6:
        await save_snapshot(wallet, pnl_value)

    print(f"{wallet[:6]}… ✔")

async def worker_chunk(worker_id: int, wallets: List[str]) -> None:
    print(f"[worker {worker_id}] старт, {len(wallets)} кошельков, "
          f"прокси {WORKER_PROXIES[worker_id]}")
    for w in wallets:
        await process_wallet(worker_id, w)
        await asyncio.sleep(REQ_DELAY + random.uniform(0, 1))
    print(f"[worker {worker_id}] завершил свою часть")

async def parallel_mode(wallets: List[str], proxies: List[str]) -> None:
    global PROXY_POOL, WORKER_PROXIES

    n = min(MAX_WORKERS, len(proxies), len(wallets))
    initial = proxies[:n]
    PROXY_POOL      = proxies[n:]
    WORKER_PROXIES  = {i: initial[i] for i in range(n)}

    chunks = [wallets[i::n] for i in range(n)]
    await asyncio.gather(*(worker_chunk(i, chunks[i]) for i in range(n)))

# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------

async def run() -> None:
    here = os.path.dirname(__file__)
    wallets = load_lines(os.path.join(here, "wallets.txt"))
    proxies = load_lines(os.path.join(here, "proxies.txt"))

    if not wallets:
        print("⚠️  wallets.txt пуст или не найден"); sys.exit(1)
    if not proxies:
        print("⚠️  proxies.txt пуст или не найден");  sys.exit(1)

    await init_db()

    start = time.perf_counter()
    try:
        await parallel_mode(wallets, proxies)
    except KeyboardInterrupt:
        print("Остановлено по Ctrl-C")
    finally:
        print(f"⏱ Скрипт завершён за {time.perf_counter() - start:.1f} сек.")

if __name__ == "__main__":
    asyncio.run(run())
