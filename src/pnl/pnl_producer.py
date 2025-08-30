from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Dict, List, Callable, Iterable, Any

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
    q.strip()
    for q in os.getenv("TOKEN_QUEUES", "pump_queue,raydium_queue,meteora_queue").split(",")
    if q.strip()
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
RESET_WALLETS_QUEUE = False
CLEAR_MARKER_KEY    = os.getenv("CLEAR_MARKER_KEY", "wallet_queue_cleared")

CLEAR_MARKER_TTL    = int(os.getenv("CLEAR_MARKER_TTL", "0"))

BLPOP_TIMEOUT = int(os.getenv("BLPOP_TIMEOUT", "300"))
DELAY_SEC     = float(os.getenv("DELAY_SEC", "0"))
BATCH_SIZE    = int(os.getenv("WALLET_BATCH", "15000"))

LOG_QUEUE_STATS = os.getenv("LOG_QUEUE_STATS", "1") not in ("0", "false", "False")

FETCHERS: Dict[str, Callable[[str], List[Any]]] = {
    "PumpSwap": fetch_pumpswap_pnl,
    "Raydium":  fetch_raydium_wallets,
    "Meteora":  fetch_meteora_wallets,
}

# ───────────────────────── helpers ────────────────────────────────────
def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

CANDIDATE_KEYS = (
    "signing_wallet", "wallet", "wallet_address", "address",
    "owner", "authority", "maker", "trader",
)

def to_wallet_list(rows: Iterable[Any]) -> List[str]:
    rows = list(rows or [])
    if not rows:
        return []
    first = rows[0]
    if isinstance(first, str):
        return list(dict.fromkeys(w for w in rows if isinstance(w, str) and w))
    if isinstance(first, dict):
        for key in CANDIDATE_KEYS:
            vals = [r.get(key) for r in rows if isinstance(r, dict) and r.get(key)]
            if len(vals) >= max(1, len(rows) // 2):
                return list(dict.fromkeys(str(v) for v in vals if isinstance(v, (str, bytes))))
        return []
    return []

def _llen_safe(rds, key: str) -> int:
    try:
        return int(rds.llen(key))
    except Exception:
        return -1

def _log_token_queues_state(rds) -> None:
    if not LOG_QUEUE_STATS:
        return
    try:
        states = {q: _llen_safe(rds, q) for q in TOKEN_QUEUES}
        print(f"ℹ️  Остатки в TOKEN_QUEUES: " + ", ".join(f"{k}={v}" for k, v in states.items()))
    except Exception as e:
        print(f"⚠️  Не удалось получить состояние TOKEN_QUEUES: {e!r}")

def clear_queues_once(rds) -> None:
    if not RESET_WALLETS_QUEUE:
        return
    if CLEAR_MARKER_TTL > 0:
        ok = rds.set(CLEAR_MARKER_KEY, int(time.time()), nx=True, ex=CLEAR_MARKER_TTL)
        if not ok:
            print("🔸 Очистка уже выполнялась в пределах TTL — пропускаем")
            return
    else:
        if not rds.setnx(CLEAR_MARKER_KEY, int(time.time())):
            print("🔸 Очереди уже были очищены ранее — пропускаем очистку")
            return
    rds.delete(WALLETS_QUEUE, *TOKEN_QUEUES)
    print(f"🧹  Очистили {WALLETS_QUEUE} и все queues из TOKEN_QUEUES")

def push_wallets_to_redis(rds, wallets: List[str], *, token: str, src_flag: str) -> None:
    if not wallets:
        return
    payload = {"v": 1, "src": src_flag, "token": token, "wallets": wallets, "ts": int(time.time())}
    rds.rpush(WALLETS_QUEUE, _json_dumps(payload))
    if LOG_QUEUE_STATS:
        llen = _llen_safe(rds, WALLETS_QUEUE)
        print(f"📦  RPUSH → {WALLETS_QUEUE} len={llen}")

def consume_tokens_once() -> None:
    rds = get_redis()
    clear_queues_once(rds)

    buffer: List[str] = []
    processed = 0
    total_pushed = 0

    while True:
        popped = rds.blpop(TOKEN_QUEUES, timeout=BLPOP_TIMEOUT)
        if popped is None:
            print(f"⌛ Нет новых токенов {BLPOP_TIMEOUT} с — завершаем сбор токенов")
            _log_token_queues_state(rds)
            if LOG_QUEUE_STATS:
                print(f"ℹ️  {WALLETS_QUEUE} len={_llen_safe(rds, WALLETS_QUEUE)}")
            break

        queue_raw, token_raw = popped
        queue_name = queue_raw.decode() if isinstance(queue_raw, (bytes, bytearray)) else str(queue_raw)
        token      = token_raw.decode() if isinstance(token_raw, (bytes, bytearray)) else str(token_raw)
        src_flag   = QUEUE_FLAGS.get(queue_name, queue_name)

        if LOG_QUEUE_STATS:
            # показать, сколько осталось в каждой токенной очереди после этого BLPOP
            _log_token_queues_state(rds)

        fetcher = FETCHERS.get(src_flag)
        if fetcher is None:
            print(f"⚠️  Неизвестный src_flag={src_flag} — пропускаем токен {token}")
            continue

        processed += 1
        print(f"🛠️  [{processed}] {src_flag}: обрабатываем {token}")

        try:
            rows = fetcher(token)
            wallets = to_wallet_list(rows)
            if not wallets:
                print(f"⚠️  {src_flag}:{token} — пусто")
                continue

            # накапливаем в общий буфер
            buffer.extend(wallets)

            # выгружаем чанки
            while len(buffer) >= BATCH_SIZE:
                chunk = buffer[:BATCH_SIZE]
                push_wallets_to_redis(rds, chunk, token="batch", src_flag="mix")
                total_pushed += len(chunk)
                print(f"📤  Отправили чанку {len(chunk)} кошельков в {WALLETS_QUEUE}")
                del buffer[:BATCH_SIZE]

            if DELAY_SEC:
                time.sleep(DELAY_SEC)

        except Exception as e:
            print(f"⚠️  Ошибка при обработке токена {token}: {e}")
            continue

    # финальный хвост
    if buffer:
        push_wallets_to_redis(rds, buffer, token="batch", src_flag="mix")
        total_pushed += len(buffer)
        print(f"📤  Финальный хвост {len(buffer)} кошельков в {WALLETS_QUEUE}")
        buffer.clear()

    print(
        f"🏁 Готово: обработано {processed} токенов | "
        f"отправлено кошельков: {total_pushed} | "
        f"размер чанка={BATCH_SIZE}, очередь «{WALLETS_QUEUE}» len={_llen_safe(rds, WALLETS_QUEUE)}"
    )


if __name__ == "__main__":
    try:
        consume_tokens_once()
    except KeyboardInterrupt:
        print("Остановлено по Ctrl-C")
