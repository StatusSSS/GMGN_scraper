#!/usr/bin/env python3
"""
BLPOP-воркер для gmgn.ai

  • Без аргументов — слушает Redis-очередь `wallet_queue`.
  • С аргументами — проверяет указанные кошельки локально.

Примеры:
  python gmgn_worker.py                       # рабочий режим
  python gmgn_worker.py 4Qmk… HhKg…           # отладка без Redis
"""

from __future__ import annotations

import os, sys, json, time, asyncio
from typing import List, Optional
import random
from curl_cffi import requests as curl
from curl_cffi.requests.exceptions import HTTPError
from dotenv import load_dotenv

load_dotenv()

# ────────── ENV ────────────────────────────────────────────────────────
QUEUE_NAME  = os.getenv("REDIS_QUEUE", "wallet_queue")
API_PERIOD  = os.getenv("GMGN_PERIOD", "7d")
API_TIMEOUT = int(os.getenv("GMGN_TIMEOUT", "30"))
REQ_DELAY   = float(os.getenv("GMGN_DELAY", "2"))
MAX_RETRIES = int(os.getenv("GMGN_RETRIES", "10"))
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

# импорт Redis-коннектора нужен только в «боевом» режиме
if len(sys.argv) == 1:
    from src.sdk.queues.redis_connect import get_redis


# ────────── helpers ────────────────────────────────────────────────────
def log_http_error(e: HTTPError, wallet: str, attempt: int) -> None:
    resp = e.response
    snippet = (resp.text or "").strip()[:SHOW_BODY]
    if len(resp.text or "") > SHOW_BODY:
        snippet += "…"
    print(f"[{wallet}] попытка {attempt}: HTTP {resp.status_code} {resp.reason}\n{snippet}\n")


def fetch_wallet_stat(wallet: str) -> Optional[dict]:
    """Синхронно запрашивает gmgn.ai, делает ретраи, возвращает JSON либо None."""
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
            )
            resp.raise_for_status()
            print(resp.text)
            return resp.json()

        except HTTPError as e:
            log_http_error(e, wallet, attempt)
        except Exception as e:
            print(f"[{wallet}] попытка {attempt}: {type(e).__name__}: {e}")

        if attempt < MAX_RETRIES:
            time.sleep(1.5)        # back-off

    return None


# ────────── async layer ────────────────────────────────────────────────
async def process_wallet(wallet: str) -> None:
    loop = asyncio.get_running_loop()
    data = await loop.run_in_executor(None, fetch_wallet_stat, wallet)
    if data:
        print(f"{wallet[:6]}… ✔")
    else:
        print(f"{wallet[:6]}… ❌")


async def worker_loop() -> None:
    rds = get_redis()
    print("✅ worker запущен, очередь:", QUEUE_NAME)

    while True:
        _, payload = rds.blpop(QUEUE_NAME, 0)
        raw = json.loads(payload)

        # поддерживаем два формата сообщения
        if isinstance(raw, dict):
            wallets = raw.get("wallets") or []
        else:
            wallets = raw

        if not isinstance(wallets, list):
            print("⚠️  неизвестный формат сообщения:", raw)
            continue

        print(f"→ пакет из {len(wallets)} кошельков")
        for w in wallets:
            await process_wallet(w)
            await asyncio.sleep(REQ_DELAY + random.uniform(0, 1))


async def direct_mode(wallets: List[str]) -> None:
    await asyncio.gather(*(process_wallet(w) for w in wallets))


# ────────── entrypoint ────────────────────────────────────────────────
def main() -> None:
    if len(sys.argv) > 1:          # режим отладки без Redis
        asyncio.run(direct_mode(sys.argv[1:]))
    else:                          # боевой режим
        try:
            asyncio.run(worker_loop())
        except KeyboardInterrupt:
            print("Остановлено по Ctrl-C")


if __name__ == "__main__":
    main()
