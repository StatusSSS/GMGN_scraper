# src/sdk/queues/redis_connect.py
"""
Очень тонкая обёртка над redis-py с LRU-кэшом.
Используйте get_redis() где угодно — вернётся один
и тот же подключённый клиент.
"""
from functools import lru_cache
import os

# официальный клиент Redis ≥5.0
import redis
from dotenv import load_dotenv

load_dotenv()  # разрешает задавать параметры в .env


@lru_cache(maxsize=1)
def get_redis() -> redis.Redis:
    """
    Возвращает singleton-клиент Redis.
    Параметры берёт из переменных окружения:
      • REDIS_HOST  (localhost)
      • REDIS_PORT  (6379)
      • REDIS_DB    (0)
      • REDIS_PASSWORD (опционально)
    decode_responses=True → строки str вместо bytes.
    """
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        db=int(os.getenv("REDIS_DB", 0)),
        password=os.getenv("REDIS_PASSWORD", None),
        decode_responses=True,
    )
