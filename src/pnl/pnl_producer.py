from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Dict, List, Callable

from dotenv import load_dotenv

from src.clickhouse_pnl.wallet_fetchers import (
    fetch_pumpswap_pnl,
    fetch_raydium_wallets,
    fetch_meteora_wallets,
)
from src.sdk.queues.redis_connect import get_redis_sync as get_redis

load_dotenv()



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
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "100"))
BLPOP_TIMEOUT = int(os.getenv("BLPOP_TIMEOUT", "30"))




FETCHERS: Dict[str, Callable[[str], List[Dict]]] = {
    "PumpSwap": fetch_pumpswap_pnl,
    "Raydium":  fetch_raydium_wallets,
    "Meteora":  fetch_meteora_wallets,
}



def push_wallets_to_redis(wallets: list[str], *, token: str, src_flag: str) -> None:
    rds = get_redis()
    for i in range(0, len(wallets), CHUNK_SIZE):
        chunk = wallets[i : i + CHUNK_SIZE]
        if not chunk:
            continue
        payload = {"src": src_flag, "token": token, "wallets": chunk}
        rds.rpush(WALLETS_QUEUE, json.dumps(payload))


def consume_tokens_once() -> None:
    """Очищает очереди (опц.) и обрабатывает все токены, затем завершает процесс."""
    rds = get_redis()

    if RESET_WALLETS_QUEUE:
        rds.delete(WALLETS_QUEUE)
        for q in TOKEN_QUEUES:
            rds.delete(q)
        print(f"🧹  Очистили {WALLETS_QUEUE} и все queues из TOKEN_QUEUES")

    Path(OUT_DIR).mkdir(parents=True, exist_ok=True)

    processed = 0
    while True:
        popped = rds.blpop(TOKEN_QUEUES, timeout=BLPOP_TIMEOUT)
        if popped is None:
            print(f"⌛ Нет новых токенов {BLPOP_TIMEOUT} с — выходим")
            break

        queue_name_raw, raw_token = popped
        queue_name = queue_name_raw.decode() if isinstance(queue_name_raw, (bytes, bytearray)) else str(queue_name_raw)
        token       = raw_token.decode()        if isinstance(raw_token,  (bytes, bytearray)) else str(raw_token)
        src_flag    = QUEUE_FLAGS.get(queue_name, queue_name)

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
        print(f"📬  {src_flag} token={token} → {len(wallets)} кошельков отправлено в {WALLETS_QUEUE}")

        if DELAY_SEC:
            time.sleep(DELAY_SEC)


if __name__ == "__main__":
    try:
        consume_tokens_once()
    except KeyboardInterrupt:
        print("Остановлено по Ctrl-C")
