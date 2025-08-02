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

# ─────────────────────────── ENV ─────────────────────────────────────
TOKEN_QUEUES: List[str] = [
    q.strip() for q in os.getenv("TOKEN_QUEUES", "pump_queue,raydium_queue,meteora_queue").split(",") if q.strip()
]
QUEUE_FLAGS: Dict[str, str] = {
    k.strip(): v.strip()
    for k, v in (
        pair.split("=", 1)
        for pair in os.getenv(
            "QUEUE_FLAGS",
            "pump_queue=PumpSwap,raydium_queue=Raydium,meteora_queue=Meteora",
        ).split(",")
        if "=" in pair
    )
}

WALLETS_QUEUE       = os.getenv("WALLETS_QUEUE", "wallet_queue")
RESET_WALLETS_QUEUE = os.getenv("RESET_WALLETS_QUEUE", "false").lower() in {"1", "true", "yes"}
CLEAR_MARKER_KEY    = os.getenv("CLEAR_MARKER_KEY", "wallet_queue_cleared")

BLPOP_TIMEOUT = int(os.getenv("BLPOP_TIMEOUT", "300"))
DELAY_SEC     = int(os.getenv("DELAY_SEC", "0"))
OUT_DIR       = os.getenv("OUT_DIR", "reports")

# ▸ размер одной пачки, отправляемой в Redis
BATCH_SIZE    = int(os.getenv("WALLET_BATCH", "1000"))

FETCHERS: Dict[str, Callable[[str], List[Dict]]] = {
    "PumpSwap": fetch_pumpswap_pnl,
    "Raydium":  fetch_raydium_wallets,
    "Meteora":  fetch_meteora_wallets,
}

# ───────────────────────── helpers ────────────────────────────────────
def clear_queues_once(rds) -> None:
    """Удаляет очереди только один раз, отмечая это маркером в Redis."""
    if not RESET_WALLETS_QUEUE:
        return
    if rds.setnx(CLEAR_MARKER_KEY, int(time.time())):
        rds.delete(WALLETS_QUEUE, *TOKEN_QUEUES)
        print(f"🧹  Очистили {WALLETS_QUEUE} и все queues из TOKEN_QUEUES")
    else:
        print("🔸 Очереди уже были очищены ранее — пропускаем очистку")


def push_wallets_to_redis(wallets: list[str], *, token: str, src_flag: str) -> None:
    """Кладёт ВСЮ пачку кошельков одной записью в `WALLETS_QUEUE`."""
    if not wallets:
        return
    payload = {"src": src_flag, "token": token, "wallets": wallets}
    get_redis().rpush(WALLETS_QUEUE, json.dumps(payload))

# ─────────────────────── основной процесс ────────────────────────────
def consume_tokens_once() -> None:
    """
    1. Забираем ВСЕ токены из очередей TOKEN_QUEUES.
    2. Для каждого токена получаем список кошельков.
    3. Отправляем кошельки в Redis батчами по BATCH_SIZE.
    """
    rds = get_redis()
    clear_queues_once(rds)
    Path(OUT_DIR).mkdir(parents=True, exist_ok=True)

    buffer: list[str] = []
    processed = 0

    while True:
        popped = rds.blpop(TOKEN_QUEUES, timeout=BLPOP_TIMEOUT)
        if popped is None:
            print(f"⌛ Нет новых токенов {BLPOP_TIMEOUT} с — завершаем сбор токенов")
            break  # очереди опустели

        queue_raw, token_raw = popped
        queue_name = queue_raw.decode()  if isinstance(queue_raw,  (bytes, bytearray)) else str(queue_raw)
        token      = token_raw.decode()  if isinstance(token_raw,  (bytes, bytearray)) else str(token_raw)
        src_flag   = QUEUE_FLAGS.get(queue_name, queue_name)

        fetcher = FETCHERS.get(src_flag)
        if fetcher is None:
            print(f"⚠️  Неизвестный src_flag={src_flag} — пропускаем токен {token}")
            continue

        processed += 1
        print(f"🛠️  [{processed}] {src_flag}: обрабатываем {token}")

        try:
            rows = fetcher(token)
            push_wallets_to_redis(rows, token=token, src_flag=src_flag)
            print(f"📤  Отправили {len(rows)} кошельков мгновенно")
        except Exception as e:
            print(f"⚠️  Ошибка при обработке токена {token}: {e}")
            continue

        # ➜ добавляем в общий буфер
        buffer.extend(row["signing_wallet"] for row in rows)

        # ➜ по достижении порога — отправляем пачку
        if len(buffer) >= BATCH_SIZE:
            push_wallets_to_redis(buffer, token="batch", src_flag="mix")
            print(f"📤  Отправили {len(buffer)} кошельков в {WALLETS_QUEUE}")
            buffer.clear()

        if DELAY_SEC:
            time.sleep(DELAY_SEC)

    # финальный хвост
    if buffer:
        push_wallets_to_redis(buffer, token="batch", src_flag="mix")
        print(f"📤  Финальный флаш {len(buffer)} кошельков в {WALLETS_QUEUE}")

    print(f"🏁 Готово: обработано {processed} токенов, батч {BATCH_SIZE}, очередь «{WALLETS_QUEUE}»")


if __name__ == "__main__":
    try:
        consume_tokens_once()
    except KeyboardInterrupt:
        print("Остановлено по Ctrl-C")