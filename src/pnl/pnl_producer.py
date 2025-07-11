"""
src/pnl/pnl_producer.py

Консюмер трёх очередей (PumpSwap / Raydium / Meteora):
— ждёт токен в Redis
— выбирает нужный ClickHouse-fetcher по источнику
— кладёт список найденных кошельков в wallet_queue
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Dict, List, Callable

from dotenv import load_dotenv

from src.clickhouse_pnl.wallet_fetchers import (   # ✨ новые fetch-функции
    fetch_pumpswap_pnl,
    fetch_raydium_wallets,
    fetch_meteora_wallets,
)
from src.sdk.queues.redis_connect import get_redis

load_dotenv()

# ───────────────────────────── Config ──────────────────────────────
TOKEN_QUEUES_ENV = os.getenv(
    "TOKEN_QUEUES",
    "pump_queue,raydium_queue,meteora_queue",
)
TOKEN_QUEUES: List[str] = [q.strip() for q in TOKEN_QUEUES_ENV.split(",") if q.strip()]

QUEUE_FLAGS_ENV = os.getenv(
    "QUEUE_FLAGS",
    "pump_queue=PumpSwap,raydium_queue=Raydium,meteora_queue=Meteora",
)
QUEUE_FLAGS: Dict[str, str] = {
    k.strip(): v.strip()
    for k, v in (
        pair.split("=", 1) for pair in QUEUE_FLAGS_ENV.split(",") if "=" in pair
    )
}

WALLETS_QUEUE       = os.getenv("WALLETS_QUEUE", "wallet_queue")
RESET_WALLETS_QUEUE = os.getenv("RESET_WALLETS_QUEUE", "1") == "1"
DELAY_SEC           = int(os.getenv("DELAY_SEC", "0"))
OUT_DIR             = os.getenv("OUT_DIR", "reports")

# соответствие «флаг источника → функция-fetcher»
FETCHERS: Dict[str, Callable[[str], List[Dict]]] = {
    "PumpSwap": fetch_pumpswap_pnl,
    "Raydium":  fetch_raydium_wallets,
    "Meteora":  fetch_meteora_wallets,
}

# ───────────────────────────── Helpers ─────────────────────────────
def push_wallets_to_redis(
    wallets: List[str],
    *,
    token: str,
    src_flag: str,
    queue: str = WALLETS_QUEUE,
) -> None:
    """Кладёт результат одним JSON-объектом в Redis-лист `queue`."""
    if not wallets:
        return
    payload = {"src": src_flag, "token": token, "wallets": wallets}
    get_redis().rpush(queue, json.dumps(payload))

# ─────────────────────────── Consumer loop ─────────────────────────
def consume_tokens() -> None:
    """Слушает TOKEN_QUEUES, обрабатывает токены и пушит кошельки в WALLETS_QUEUE."""
    rds = get_redis()

    if RESET_WALLETS_QUEUE:
        rds.delete(WALLETS_QUEUE)
        for q in TOKEN_QUEUES:
            rds.delete(q)
        print(f"🧹  Очистили {WALLETS_QUEUE} и все queues из TOKEN_QUEUES")

    Path(OUT_DIR).mkdir(parents=True, exist_ok=True)

    processed = 0
    while True:
        # blpop(block) → (queue_name, value)
        queue_name_raw, raw_token = rds.blpop(TOKEN_QUEUES)
        queue_name = (
            queue_name_raw.decode()
            if isinstance(queue_name_raw, (bytes, bytearray))
            else str(queue_name_raw)
        )
        token = (
            raw_token.decode()
            if isinstance(raw_token, (bytes, bytearray))
            else str(raw_token)
        )
        src_flag = QUEUE_FLAGS.get(queue_name, queue_name)

        processed += 1
        print(f"🛠️  [{processed}] {src_flag}: обрабатываем токен {token}")

        fetcher = FETCHERS.get(src_flag)
        if fetcher is None:
            print(f"⚠️  Неизвестный src_flag={src_flag} — пропускаем токен {token}")
            continue

        try:
            rows = fetcher(token)
        except Exception as e:
            print(f"⚠️  Ошибка при обработке токена {token} ({src_flag}): {e}")
            continue

        wallets = [row["signing_wallet"] for row in rows]
        push_wallets_to_redis(wallets, token=token, src_flag=src_flag)
        print(
            f"📬  {src_flag} token={token} → {len(wallets)} кошельков отправлено в {WALLETS_QUEUE}"
        )

        if DELAY_SEC:
            time.sleep(DELAY_SEC)

# ─────────────────────────── Entrypoint ────────────────────────────
if __name__ == "__main__":
    try:
        consume_tokens()
    except KeyboardInterrupt:
        print("Остановлено по Ctrl-C")
