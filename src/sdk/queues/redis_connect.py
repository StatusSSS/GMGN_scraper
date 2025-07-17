# src/sdk/queues/redis_connect.py
"""
Тонкая обёртка над redis-py (>=5.0) с LRU-кэшем.
 * get_redis()       → singleton асинхронного клиента (redis.asyncio.Redis)
 * get_redis_sync()  → singleton синхронного   клиента (redis.Redis)
"""

from __future__ import annotations

import os
from functools import lru_cache

import redis                    # синхронный модуль
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()  # разрешает задавать параметры в .env


# ─────────────────────────── helpers ────────────────────────────
def _redis_url() -> str | None:
    """
    Берём готовый REDIS_URL, если задан, иначе None.
    """
    return os.getenv("REDIS_URL")


def _env_kwargs() -> dict:
    """
    Формирует kwargs для redis.Redis / redis.asyncio.Redis
    из переменных окружения (если REDIS_URL не задан).
    """
    return dict(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        db=int(os.getenv("REDIS_DB", 0)),
        password=os.getenv("REDIS_PASSWORD", None),
        decode_responses=True,
    )


# ─────────────────────── async singleton ───────────────────────
@lru_cache(maxsize=1)
def get_redis() -> aioredis.Redis:
    """
    Singleton-клиент redis.asyncio.
    Используйте в асинхронных функциях: rds = get_redis(); await rds.get("key")
    """
    url = _redis_url()
    if url:
        return aioredis.from_url(url, decode_responses=True)
    return aioredis.Redis(**_env_kwargs())


# ─────────────────────── sync singleton ────────────────────────
@lru_cache(maxsize=1)
def get_redis_sync() -> redis.Redis:
    """
    Singleton-клиент синхронного redis-py.
    Используйте в обычных скриптах без asyncio.
    """
    url = _redis_url()
    if url:
        return redis.from_url(url, decode_responses=True)  # type: ignore[attr-defined]
    return redis.Redis(**_env_kwargs())
