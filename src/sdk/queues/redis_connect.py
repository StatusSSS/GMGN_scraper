# src/sdk/queues/redis_connect.py
"""
Тонкая обёртка над redis-py (>=5.0) с LRU-кэшем.
get_redis() возвращает один и тот же асинхронный клиент.
"""

from functools import lru_cache
import os

# redis ≥5 объединяет aioredis: импортируем подручный модуль
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()  # читаем .env, если он есть


def _build_async_client() -> aioredis.Redis:
    """
    Создаёт экземпляр redis.asyncio.Redis согласно переменным окружения.
    Приоритет:
      1) REDIS_URL = redis://:pass@host:port/db
      2) REDIS_HOST / REDIS_PORT / REDIS_DB / REDIS_PASSWORD (как раньше)
    decode_responses=True → сразу получаем str, а не bytes.
    """
    url = os.getenv("REDIS_URL")
    if url:
        # from_url автоматически парсит db, пароль, хост и порт
        return aioredis.from_url(url, decode_responses=True)

    return aioredis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        db=int(os.getenv("REDIS_DB", 0)),
        password=os.getenv("REDIS_PASSWORD", None),
        decode_responses=True,
    )


@lru_cache(maxsize=1)
def get_redis() -> aioredis.Redis:   # тип подсказан для IDE / mypy
    """Singleton-клиент (асинхронный)."""
    return _build_async_client()
