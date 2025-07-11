#!/usr/bin/env python3
"""
Async-воркер для gmgn.ai, который параллельно пробивает статистику кошельков
через список HTTP-прокси и при PnL > 0.6 сохраняет данные в PostgreSQL.

Главное:
  • Кошельки берутся из src/wallets.txt
  • Прокси      из src/proxies.txt
  • До 20 параллельных воркеров (значение по умолчанию можно переопределить
    переменной окружения GMGN_WORKERS)
  • Для каждой прокси максимум MAX_RETRIES запросов; если лимит исчерпан
    – прокси выключается и берётся следующая из пула
  • Если конкретный кошелёк подряд «падает» MAX_RETRIES раз, он заносится в
    fail_wallets.txt
  • Если полученный PnL > 0.6 → запись в таблицу wallet_snapshot (PostgreSQL)
  • По окончании работы выводится, сколько времени занял запуск целиком
"""

from __future__ import annotations

import asyncio
import os
import random
import sys
import threading
import time
from typing import Dict, List, Optional

from curl_cffi import requests as curl
from curl_cffi.requests.exceptions import HTTPError
from dotenv import load_dotenv
from sqlalchemy.exc import IntegrityError

# ─── PostgreSQL SDK ───────────────────────────────────────────────────────
from src.sdk.databases.postgres.dependency import with_db_session
from src.sdk.databases.postgres.models import Wallet

load_dotenv()

API_PERIOD = os.getenv("GMGN_PERIOD", "7d")
API_TIMEOUT = int(os.getenv("GMGN_TIMEOUT", "30"))
REQ_DELAY = float(os.getenv("GMGN_DELAY", "2"))
MAX_RETRIES = int(os.getenv("GMGN_RETRIES", "20"))
MAX_WORKERS = int(os.getenv("GMGN_WORKERS", "20"))
SHOW_BODY = int(os.getenv("GMGN_SHOW_BODY", "300"))

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
    "from_app": "gmgn",
    "app_ver": "20250617-62-c04b007",
    "tz_name": "Europe/Moscow",
    "tz_offset": "10800",
    "app_lang": "ru",
    "fp_did": "77abb27885cffbec63c7f9fbd35b4116",
    "os": "web",
}

# ─── глобальные структуры для ротации прокси ──────────────────────────────
PROXY_POOL: List[str] = []
PROXY_LOCK = threading.Lock()
WORKER_PROXIES: Dict[int, str] = {}

FAIL_WALLETS_FILE = "fail_wallets.txt"

# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------


def load_lines(path: str) -> List[str]:
    try:
        with open(path, encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
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
    """Добавляет кошелёк в fail_wallets.txt, если его там ещё нет."""
    try:
        if os.path.exists(FAIL_WALLETS_FILE):
            with open(FAIL_WALLETS_FILE, "r", encoding="utf-8") as f:
                if wallet in {line.strip() for line in f}:
                    return
        with open(FAIL_WALLETS_FILE, "a", encoding="utf-8") as f:
            f.write(wallet + "\n")
    except Exception as e:
        print(f"⚠️  Не удалось записать в {FAIL_WALLETS_FILE}: {e}")


# ---------------------------------------------------------------------------
# Сохранение снапшота в PostgreSQL
# ---------------------------------------------------------------------------


@with_db_session
async def save_snapshot_if_positive(wallet: str, pnl_value: float, *, db_session) -> None:
    """
    Записывает адрес и PnL в wallet_snapshot, если pnl_value > 0.6.
    PnL округляется до трёх знаков после точки.
    """
    if pnl_value <= 0.6:
        return

    snapshot = Wallet(
        address=wallet,
        pnl=round(pnl_value, 3),  # 12.345678 → 12.346
        # ts_utc берётся из default=datetime.utcnow в модели
    )
    db_session.add(snapshot)
    try:
        await db_session.commit()
    except IntegrityError:
        # Запись на этот адрес/день уже существует — игнорируем
        await db_session.rollback()
    except Exception:
        await db_session.rollback()
        raise


# ---------------------------------------------------------------------------
# Сетевой запрос (синхронный, вызывается в executor)
# ---------------------------------------------------------------------------


def fetch_wallet_stat(worker_id: int, wallet: str) -> Optional[dict]:
    """Однократная попытка получить статистику по wallet с ротацией прокси."""
    # локальная копия текущей + всех остальных доступных прокси
    with PROXY_LOCK:
        proxies_to_try = [WORKER_PROXIES[worker_id]] + PROXY_POOL.copy()

    for proxy_str in proxies_to_try:
        # назначаем текущую прокси воркеру
        with PROXY_LOCK:
            WORKER_PROXIES[worker_id] = proxy_str

        host, port, user, pwd = proxy_str.split(":", 3)
        proxy_url = f"http://{user}:{pwd}@{host}:{port}"
        proxies = {"http": proxy_url, "https": proxy_url}
        url = f"https://gmgn.ai/api/v1/wallet_stat/sol/{wallet}/{API_PERIOD}"
        headers = {**HEADERS_BASE, "referer": f"https://gmgn.ai/sol/address/{wallet}"}
        params = PARAMS_BASE.copy()

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
                print(
                    f"[{wallet}] попытка {attempt} через {proxy_str}: "
                    f"{type(e).__name__}: {e}"
                )

            time.sleep(1.5)  # back-off

        # если дошли сюда — на этой прокси полностью исчерпали лимит MAX_RETRIES
        mark_failed_wallet(wallet)
        with PROXY_LOCK:
            if proxy_str in PROXY_POOL:
                PROXY_POOL.remove(proxy_str)
            print(
                f"[worker {worker_id}] прокси {proxy_str} исключена "
                f"после {MAX_RETRIES} ошибочных попыток\n"
            )

    # все прокси перебраны, не удалось получить данные
    return None


# ---------------------------------------------------------------------------
# Асинхронные воркер-корутины
# ---------------------------------------------------------------------------


async def process_wallet(worker_id: int, wallet: str) -> None:
    loop = asyncio.get_running_loop()
    data = await loop.run_in_executor(None, fetch_wallet_stat, worker_id, wallet)
    if data:
        pnl_value = data["data"]["pnl"]
        print(f"{pnl_value:.3f}")

        # если PnL положительный — сохраняем в БД
        await save_snapshot_if_positive(wallet, pnl_value)

        print(f"{wallet[:6]}… ✔")
    else:
        print(f"{wallet[:6]}… ❌")


async def worker_chunk(worker_id: int, wallets: List[str]) -> None:
    print(
        f"[worker {worker_id}] старт, {len(wallets)} кошельков, "
        f"прокси {WORKER_PROXIES[worker_id]}"
    )
    for w in wallets:
        await process_wallet(worker_id, w)
        await asyncio.sleep(REQ_DELAY + random.uniform(0, 1))
    print(f"[worker {worker_id}] завершил свою часть")


async def parallel_mode(wallets: List[str], proxies: List[str]) -> None:
    global PROXY_POOL, WORKER_PROXIES

    n = min(MAX_WORKERS, len(proxies), len(wallets))
    initial = proxies[:n]
    PROXY_POOL = proxies[n:]
    WORKER_PROXIES = {i: initial[i] for i in range(n)}

    chunks = [wallets[i::n] for i in range(n)]
    tasks = [asyncio.create_task(worker_chunk(i, chunks[i])) for i in range(n)]
    await asyncio.gather(*tasks)


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------


def main() -> None:
    here = os.path.dirname(__file__)
    wallets_file = os.path.join(here, "wallets.txt")
    proxies_file = os.path.join(here, "proxies.txt")

    wallets = load_lines(wallets_file)
    proxies = load_lines(proxies_file)

    if not wallets:
        print("⚠️  Файл wallets.txt пуст или не найден")
        sys.exit(1)
    if not proxies:
        print("⚠️  Файл proxies.txt пуст или не найден")
        sys.exit(1)

    start_ts = time.perf_counter()

    try:
        asyncio.run(parallel_mode(wallets, proxies))
    except KeyboardInterrupt:
        print("Остановлено по Ctrl-C")
    finally:
        duration = time.perf_counter() - start_ts
        print(f"⏱ Скрипт завершён за {duration:.1f} сек.")


if __name__ == "__main__":
    main()
