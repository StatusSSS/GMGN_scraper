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

# ──────────────────────────── ENV ─────────────────────────────────────
TOKEN_QUEUES_ENV = os.getenv("TOKEN_QUEUES", "pump_queue,raydium_queue,meteora_queue")
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

WALLETS_QUEUE = os.getenv("WALLETS_QUEUE", "wallet_queue")
# Очистка выполняется только один раз: устанавливаем флаг-маркер в Redis
RESET_WALLETS_QUEUE = os.getenv("RESET_WALLETS_QUEUE", "false").lower() in {
    "1",
    "true",
    "yes",
}
CLEAR_MARKER_KEY = os.getenv("CLEAR_MARKER_KEY", "wallet_queue_cleared")

DELAY_SEC = int(os.getenv("DELAY_SEC", "0"))
OUT_DIR = os.getenv("OUT_DIR", "reports")
BLPOP_TIMEOUT = int(os.getenv("BLPOP_TIMEOUT", "300"))

FETCHERS: Dict[str, Callable[[str], List[Dict]]] = {
    "PumpSwap": fetch_pumpswap_pnl,
    "Raydium": fetch_raydium_wallets,
    "Meteora": fetch_meteora_wallets,
}

# ───────────────────────── helpers ────────────────────────────────────
def clear_queues_once(rds) -> None:
    """Удаляет очереди только один раз, отмечая это маркером в Redis."""
    if not RESET_WALLETS_QUEUE:
        return

    # setnx атомарно создаёт ключ, если его ещё нет
    if rds.setnx(CLEAR_MARKER_KEY, int(time.time())):
        rds.delete(WALLETS_QUEUE)
        for q in TOKEN_QUEUES:
            rds.delete(q)
        print(f"🧹  Очистили {WALLETS_QUEUE} и все queues из TOKEN_QUEUES")
    else:
        print("🔸 Очереди уже были очищены ранее — пропускаем очистку")


def push_wallets_to_redis(wallets: list[str], *, token: str, src_flag: str) -> None:
    """
    Кладёт ВЕСЬ список кошельков токена одной записью в Redis.

    Это позволяет воркерам обрабатывать всю партию целиком,
    а не кусками по CHUNK_SIZE.
    """
    if not wallets:
        return
    rds = get_redis()
    payload = {"src": src_flag, "token": token, "wallets": wallets}
    rds.rpush(WALLETS_QUEUE, json.dumps(payload))

# ─────────────────────── основной цикл ───────────────────────────────
def consume_tokens_once() -> None:
    """Обрабатывает все токены единоразово, затем завершает процесс."""
    rds = get_redis()
    clear_queues_once(rds)

    Path(OUT_DIR).mkdir(parents=True, exist_ok=True)

    processed = 0
    while True:
        popped = rds.blpop(TOKEN_QUEUES, timeout=BLPOP_TIMEOUT)
        if popped is None:
            print(f"⌛ Нет новых токенов {BLPOP_TIMEOUT} с — завершаем работу")
            break  # прекращаем цикл и выходим

        queue_name_raw, raw_token = popped
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

    print("🏁 Работа завершена. До новых встреч!")


if __name__ == "__main__":
    try:
        consume_tokens_once()
    except KeyboardInterrupt:
        print("Остановлено по Ctrl-C")
