#!/usr/bin/env python3
"""
Async‑воркер для gmgn.ai, который:
  • Читает пакеты кошельков из Redis‑очереди (по‑умолчанию `wallet_queue`).
  • Параллельно (до N воркеров = кол‑ва «живых» прокси) запрашивает статистику
    через HTTP‑прокси.
  • Если PnL > 0.6 — сохраняет снимок в PostgreSQL (таблица `wallet_snapshot`).

Изменения по сравнению с прошлой версией:
  1. **Кошельки берутся из Redis.**  Файла `wallets.txt` больше не нужно.
  2. **Ротация прокси после неудачного кошелька.**  Если за `MAX_RETRIES`
     попыток на текущей прокси не получен ответ, кошелёк заносится в
     `fail_wallets.txt`, прокси меняется, и воркер переходит к *следующему*
     кошельку (а не пробует тот же адрес на новой прокси).
  3. Вызов `mark_failed_wallet()` теперь происходит *один раз* — когда адрес
     окончательно признан «плохим».
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

# ─── GMGN / Postgres SDK ────────────────────────────────────────────────
from src.sdk.databases.postgres.dependency import with_db_session
from src.sdk.databases.postgres.models import Wallet
from src.sdk.queues.redis_connect import get_redis

load_dotenv()

# ─── ENV ────────────────────────────────────────────────────────────────
QUEUE_NAME  = os.getenv("REDIS_QUEUE",    "wallet_queue")
API_PERIOD  = os.getenv("GMGN_PERIOD",    "7d")
API_TIMEOUT = int(os.getenv("GMGN_TIMEOUT", "30"))
REQ_DELAY   = float(os.getenv("GMGN_DELAY",   "2"))
MAX_RETRIES = int(os.getenv("GMGN_RETRIES",  "20"))
MAX_WORKERS = int(os.getenv("GMGN_WORKERS",  "20"))
SHOW_BODY   = int(os.getenv("GMGN_SHOW_BODY", "300"))

# ─── CONSTANT HEADERS / PARAMS ──────────────────────────────────────────
HEADERS_BASE = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
    ),
}

PARAMS_BASE = {
    "device_id":  "c45e37f7-53ff-4d68-813b-fd0f7b736979",
    "client_id":  "gmgn_web_20250617-62-c04b007",
    "from_app":   "gmgn",
    "app_ver":    "20250617-62-c04b007",
    "tz_name":    "Europe/Moscow",
    "tz_offset":  "10800",
    "app_lang":   "ru",
    "fp_did":     "77abb27885cffbec63c7f9fbd35b4116",
    "os":         "web",
}

# ─── Proxy‑pool globals ────────────────────────────────────────────────
PROXY_POOL:   List[str]      = []  # свободные прокси (круговая очередь)
PROXY_LOCK                  = threading.Lock()
WORKER_PROXIES: Dict[int,str] = {}  # proxy в работе у каждого воркера

FAIL_WALLETS_FILE = "fail_wallets.txt"

# ----------------------------------------------------------------------
# Utils
# ----------------------------------------------------------------------

def load_lines(path: str) -> List[str]:
    try:
        with open(path, encoding="utf-8") as f:
            return [ln.strip() for ln in f if ln.strip()]
    except FileNotFoundError:
        return []


def log_http_error(e: HTTPError, wallet: str, attempt: int, proxy: str) -> None:
    resp = e.response
    snippet = (resp.text or "").strip()[:SHOW_BODY]
    if len(resp.text or "") > SHOW_BODY:
        snippet += "…"
    print(
        f"[{wallet}] попытка {attempt} через {proxy}: "
        f"HTTP {resp.status_code} {resp.reason}\n{snippet}\n"
    )


def mark_failed_wallet(wallet: str) -> None:
    """Однократно добавляет адрес в fail_wallets.txt."""
    try:
        if os.path.exists(FAIL_WALLETS_FILE):
            with open(FAIL_WALLETS_FILE, "r", encoding="utf-8") as f:
                if wallet in {ln.strip() for ln in f}:
                    return
        with open(FAIL_WALLETS_FILE, "a", encoding="utf-8") as f:
            f.write(wallet + "\n")
    except Exception as e:
        print(f"⚠️  Не удалось записать в {FAIL_WALLETS_FILE}: {e}")


# ----------------------------------------------------------------------
# Proxy rotation helpers
# ----------------------------------------------------------------------

def rotate_proxy(worker_id: int) -> None:
    """Выдаёт воркеру следующую прокси из пула (round‑robin)."""
    with PROXY_LOCK:
        if not PROXY_POOL:
            return  # пул пуст — остаёмся на старой прокси
        current = WORKER_PROXIES.get(worker_id)
        new     = PROXY_POOL.pop(0)
        WORKER_PROXIES[worker_id] = new
        if current:
            PROXY_POOL.append(current)  # отправляем старую в конец
        print(f"[worker {worker_id}] proxy switch: {current} → {new}")


# ----------------------------------------------------------------------
# DB: save positive PnL
# ----------------------------------------------------------------------

@with_db_session
async def save_snapshot_if_positive(wallet: str, pnl_value: float, *, db_session) -> None:
    if pnl_value <= 0.6:
        return
    snapshot = Wallet(address=wallet, pnl=round(pnl_value, 3))
    db_session.add(snapshot)
    try:
        await db_session.commit()
    except IntegrityError:
        await db_session.rollback()  # запись уже есть — игнор
    except Exception:
        await db_session.rollback()
        raise


# ----------------------------------------------------------------------
# Single HTTP request (sync, via executor)
# ----------------------------------------------------------------------

def fetch_wallet_stat(worker_id: int, wallet: str) -> Optional[dict]:
    """Пробует получить статистику *максимум MAX_RETRIES раз* на текущей прокси."""
    proxy_str = WORKER_PROXIES[worker_id]
    host, port, user, pwd = proxy_str.split(":", 3)
    proxy_url = f"http://{user}:{pwd}@{host}:{port}"
    proxies   = {"http": proxy_url, "https": proxy_url}
    url       = f"https://gmgn.ai/api/v1/wallet_stat/sol/{wallet}/{API_PERIOD}"
    headers   = {**HEADERS_BASE, "referer": f"https://gmgn.ai/sol/address/{wallet}"}
    params    = PARAMS_BASE.copy()

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = curl.get(
                url=url,
                params=params,
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
            print(f"[{wallet}] попытка {attempt} через {proxy_str}: {type(e).__name__}: {e}")
        time.sleep(1.5)  # back‑off

    # вышли из цикла — все MAX_RETRIES исчерпаны
    return None


# ----------------------------------------------------------------------
# Async per‑wallet coroutine
# ----------------------------------------------------------------------

async def process_wallet(worker_id: int, wallet: str) -> None:
    loop = asyncio.get_running_loop()
    data = await loop.run_in_executor(None, fetch_wallet_stat, worker_id, wallet)

    if data is None:
        mark_failed_wallet(wallet)
        print(f"{wallet[:6]}… ❌ (fail)")
        rotate_proxy(worker_id)             # меняем прокси и идём к следующему addr
        return

    # успех — обрабатываем PnL
    pnl_value = data["data"]["pnl"]
    print(f"{pnl_value:.3f}")
    await save_snapshot_if_positive(wallet, pnl_value)
    print(f"{wallet[:6]}… ✔")


# ----------------------------------------------------------------------
# Worker that handles its chunk sequentially
# ----------------------------------------------------------------------

async def worker_chunk(worker_id: int, wallets: List[str]) -> None:
    print(
        f"[worker {worker_id}] старт, {len(wallets)} кошельков, "
        f"прокси {WORKER_PROXIES[worker_id]}"
    )
    for w in wallets:
        await process_wallet(worker_id, w)
        await asyncio.sleep(REQ_DELAY + random.uniform(0, 1))
    print(f"[worker {worker_id}] завершил свою часть")


# ----------------------------------------------------------------------
# Redis → parallel processing helper
# ----------------------------------------------------------------------

async def handle_batch(wallets: List[str], proxies: List[str]) -> None:
    """Делит список на чанки, запускает воркеры, ждёт их завершения."""
    global PROXY_POOL, WORKER_PROXIES
    n = min(MAX_WORKERS, len(proxies), len(wallets))
    if n == 0:
        print("⚠️  Нет рабочих прокси или кошельков")
        return

    # первичная раздача прокси
    initial      = proxies[:n]
    PROXY_POOL   = proxies[n:]
    WORKER_PROXIES = {i: initial[i] for i in range(n)}

    chunks = [wallets[i::n] for i in range(n)]
    tasks  = [asyncio.create_task(worker_chunk(i, chunks[i])) for i in range(n)]
    await asyncio.gather(*tasks)


# ----------------------------------------------------------------------
# Main infinite loop: BLPOP
# ----------------------------------------------------------------------

async def redis_loop(proxies: List[str]) -> None:
    rds = get_redis()
    print("✅ worker запущен, очередь:", QUEUE_NAME)

    while True:
        _, payload = rds.blpop(QUEUE_NAME, 0)
        raw = json.loads(payload)
        wallets = raw.get("wallets") if isinstance(raw, dict) else raw
        if not isinstance(wallets, list):
            print("⚠️  неизвестный формат сообщения:", raw)
            continue

        print(f"→ пакет из {len(wallets)} кошельков")
        await handle_batch(wallets, proxies)


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------

def main() -> None:
    here         = os.path.dirname(__file__)
    proxies_file = os.path.join(here, "proxies.txt")

    proxies = load_lines(proxies_file)
    if not proxies:
        print("⚠️  Файл proxies.txt пуст или не найден")
        return

    try:
        asyncio.run(redis_loop(proxies))
    except KeyboardInterrupt:
        print("Остановлено по Ctrl-C")


if __name__ == "__main__":
    main()
