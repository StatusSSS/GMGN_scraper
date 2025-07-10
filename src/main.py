#!/usr/bin/env python3
"""
Параллельный BLPOP-воркер для gmgn.ai с ротацией прокси после MAX_RETRIES.
  • Читает кошельки из src/wallets.txt
  • Читает прокси из src/proxies.txt
  • Запускает до 10 асинхронных воркеров, у каждого свой прокси
  • Прокси меняется только если для неё исчерпаны MAX_RETRIES
"""

from __future__ import annotations
import os
import sys
import time
import asyncio
import random
import threading
from typing import List, Optional, Dict
from curl_cffi import requests as curl
from curl_cffi.requests.exceptions import HTTPError
from dotenv import load_dotenv

load_dotenv()

# ────────── ENV ────────────────────────────────────────────────────────
API_PERIOD  = os.getenv("GMGN_PERIOD", "7d")
API_TIMEOUT = int(os.getenv("GMGN_TIMEOUT", "30"))
REQ_DELAY   = float(os.getenv("GMGN_DELAY", "2"))
MAX_RETRIES = int(os.getenv("GMGN_RETRIES", "20"))
SHOW_BODY   = int(os.getenv("GMGN_SHOW_BODY", "300"))
# ───────────────────────────────────────────────────────────────────────

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

# глобальные структуры для ротации прокси
PROXY_POOL: List[str] = []
PROXY_LOCK = threading.Lock()
WORKER_PROXIES: Dict[int, str] = {}


def load_wallets(path: str) -> List[str]:
    with open(path, encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def load_proxies(path: str) -> List[str]:
    with open(path, encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def log_http_error(e: HTTPError, wallet: str, attempt: int, proxy: str) -> None:
    resp = e.response
    snippet = (resp.text or "").strip()[:SHOW_BODY]
    if len(resp.text or "") > SHOW_BODY:
        snippet += "…"
    print(f"[{wallet}] попытка {attempt} через {proxy}: HTTP {resp.status_code} {resp.reason}\n{snippet}\n")


def fetch_wallet_stat(worker_id: int, wallet: str) -> Optional[dict]:
    """Синхронный запрос: для каждой прокси внутренняя логика MAX_RETRIES."""
    # копия списка всех прокси для этой задачи
    with PROXY_LOCK:
        proxies_to_try = [WORKER_PROXIES[worker_id]] + PROXY_POOL.copy()

    for proxy_str in proxies_to_try:
        # установим текущую прокси воркера
        with PROXY_LOCK:
            WORKER_PROXIES[worker_id] = proxy_str

        host, port, user, pwd = proxy_str.split(":", 3)
        proxy_url = f"http://{user}:{pwd}@{host}:{port}"
        proxies = {"http": proxy_url, "https": proxy_url}
        url     = f"https://gmgn.ai/api/v1/wallet_stat/sol/{wallet}/{API_PERIOD}"
        headers = HEADERS_BASE | {"referer": f"https://gmgn.ai/sol/address/{wallet}"}
        params  = PARAMS_BASE.copy()

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

            time.sleep(1.5)  # back-off

        # если дошли сюда — для этой прокси все попытки исчерпаны
        with PROXY_LOCK:
            # уберём "упавшую" прокси из пула
            if proxy_str in PROXY_POOL:
                PROXY_POOL.remove(proxy_str)
            print(f"[worker {worker_id}] прокси {proxy_str} исчерпала {MAX_RETRIES} попыток, меняю\n")

    # никаких прокси не справились
    return None


async def process_wallet(worker_id: int, wallet: str) -> None:
    loop = asyncio.get_running_loop()
    data = await loop.run_in_executor(None, fetch_wallet_stat, worker_id, wallet)
    if data:
        print(f"{wallet[:6]}… ✔")
    else:
        print(f"{wallet[:6]}… ❌")


async def worker_chunk(worker_id: int, wallets: List[str]) -> None:
    print(f"[worker {worker_id}] запущен, {len(wallets)} кошельков, прокси {WORKER_PROXIES[worker_id]}")
    for w in wallets:
        await process_wallet(worker_id, w)
        await asyncio.sleep(REQ_DELAY + random.uniform(0, 1))
    print(f"[worker {worker_id}] закончил свою часть")


async def parallel_mode(wallets: List[str], proxies: List[str]) -> None:
    global PROXY_POOL, WORKER_PROXIES

    n = min(10, len(proxies), len(wallets))
    initial = proxies[:n]
    PROXY_POOL = proxies[n:]
    WORKER_PROXIES = {i: initial[i] for i in range(n)}

    chunks = [wallets[i::n] for i in range(n)]
    tasks = [asyncio.create_task(worker_chunk(i, chunks[i])) for i in range(n)]
    await asyncio.gather(*tasks)


def main() -> None:
    here = os.path.dirname(__file__)
    wallets_file = os.path.join(here, "wallets.txt")
    proxies_file = os.path.join(here, "proxies.txt")

    wallets = load_wallets(wallets_file)
    proxies = load_proxies(proxies_file)

    if not wallets:
        print("⚠️  Файл wallets.txt пуст или не найден")
        sys.exit(1)
    if not proxies:
        print("⚠️  Файл proxies.txt пуст или не найден")
        sys.exit(1)

    try:
        asyncio.run(parallel_mode(wallets, proxies))
    except KeyboardInterrupt:
        print("Остановлено по Ctrl-C")

if __name__ == "__main__":
    main()
