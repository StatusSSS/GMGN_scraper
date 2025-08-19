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
# Если зададите TTL, очистка «один раз» сможет повторяться после истечения TTL
CLEAR_MARKER_TTL    = int(os.getenv("CLEAR_MARKER_TTL", "0"))  # сек, 0 = без TTL

BLPOP_TIMEOUT = int(os.getenv("BLPOP_TIMEOUT", "300"))
DELAY_SEC     = float(os.getenv("DELAY_SEC", "0"))
BATCH_SIZE    = int(os.getenv("WALLET_BATCH", "5000"))

FETCHERS: Dict[str, Callable[[str], List[Any]]] = {
    "PumpSwap": fetch_pumpswap_pnl,
    "Raydium":  fetch_raydium_wallets,
    "Meteora":  fetch_meteora_wallets,
}

# ───────────────────────── helpers ────────────────────────────────────
def _json_dumps(obj: Any) -> str:
    # компактнее и быстрее при больших батчах
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

CANDIDATE_KEYS = (
    "signing_wallet", "wallet", "wallet_address", "address",
    "owner", "authority", "maker", "trader",
)

def to_wallet_list(rows: Iterable[Any]) -> List[str]:
    """
    Нормализуем вывод фетчера:
    - List[str] -> как есть
    - List[Dict] -> пытаемся выбрать по одному из ключей-кандидатов
    """
    rows = list(rows or [])
    if not rows:
        return []

    first = rows[0]
    if isinstance(first, str):
        # фильтруем пустые и дубликаты, сохраняем порядок
        return list(dict.fromkeys(w for w in rows if isinstance(w, str) and w))

    if isinstance(first, dict):
        for key in CANDIDATE_KEYS:
            vals = [r.get(key) for r in rows if isinstance(r, dict) and r.get(key)]
            if len(vals) >= max(1, len(rows) // 2):  # эвристика «ключ подходит»
                return list(dict.fromkeys(str(v) for v in vals if isinstance(v, (str, bytes))))
        # если ни один ключ не подошёл — пусто (лучше молча не пушить мусор)
        return []

    # неизвестный формат — безопасно вернуть пусто и залогировать выше
    return []

def clear_queues_once(rds) -> None:
    """Удаляет очереди только один раз, отмечая это маркером в Redis."""
    if not RESET_WALLETS_QUEUE:
        return

    if CLEAR_MARKER_TTL > 0:
        # set with NX+EX, чтобы было «один раз в TTL»
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
    """Кладёт ВСЮ пачку кошельков одной записью в `WALLETS_QUEUE`."""
    if not wallets:
        return
    payload = {"v": 1, "src": src_flag, "token": token, "wallets": wallets, "ts": int(time.time())}
    rds.rpush(WALLETS_QUEUE, _json_dumps(payload))

def consume_tokens_once() -> None:
    rds = get_redis()
    clear_queues_once(rds)

    buffer: List[str] = []
    processed = 0
    total_pushed_chunks = 0

    while True:
        popped = rds.blpop(TOKEN_QUEUES, timeout=BLPOP_TIMEOUT)
        if popped is None:
            print(f"⌛ Нет новых токенов {BLPOP_TIMEOUT} с — завершаем сбор токенов")
            break

        queue_raw, token_raw = popped
        queue_name = queue_raw.decode() if isinstance(queue_raw, (bytes, bytearray)) else str(queue_raw)
        token      = token_raw.decode() if isinstance(token_raw, (bytes, bytearray)) else str(token_raw)
        src_flag   = QUEUE_FLAGS.get(queue_name, queue_name)

        fetcher = FETCHERS.get(src_flag)
        if fetcher is None:
            print(f"⚠️  Неизвестный src_flag={src_flag} — пропускаем токен {token}")
            continue

        processed += 1
        print(f"🛠️  [{processed}] {src_flag}: обрабатываем {token}")

        try:
            rows = fetcher(token)
            # Если используешь мою прошлую нормализацию — оставь:
            wallets = to_wallet_list(rows)  # -> List[str]
            # Если фетчеры уже возвращают List[str], то можно: wallets = list(rows or [])

            if not wallets:
                print(f"⚠️  {src_flag}:{token} — пусто")
                continue

            # накапливаем в общий буфер
            buffer.extend(wallets)

            # выгружаем полные чанки по 5000
            while len(buffer) >= BATCH_SIZE:
                chunk = buffer[:BATCH_SIZE]
                push_wallets_to_redis(rds, chunk, token="batch", src_flag="mix")
                total_pushed_chunks += len(chunk)
                print(f"📤  Отправили чанку {len(chunk)} кошельков в {WALLETS_QUEUE}")
                del buffer[:BATCH_SIZE]

            if DELAY_SEC:
                time.sleep(DELAY_SEC)

        except Exception as e:
            print(f"⚠️  Ошибка при обработке токена {token}: {e}")
            continue

    # финальный хвост (<5000)
    if buffer:
        push_wallets_to_redis(rds, buffer, token="batch", src_flag="mix")
        total_pushed_chunks += len(buffer)
        print(f"📤  Финальный хвост {len(buffer)} кошельков в {WALLETS_QUEUE}")
        buffer.clear()

    print(
        f"🏁 Готово: обработано {processed} токенов | "
        f"отправлено кошельков: {total_pushed_chunks} | "
        f"размер чанка={BATCH_SIZE}, очередь «{WALLETS_QUEUE}»"
    )


if __name__ == "__main__":
    try:
        consume_tokens_once()
    except KeyboardInterrupt:
        print("Остановлено по Ctrl-C")
