import functools
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import ParamSpec, TypeVar

from sqlalchemy import AsyncAdaptedQueuePool
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from src.sdk.databases.postgres.config import db_config   # ← путь тот же

# ──────────────────────────────────────────────────────────────
engine = create_async_engine(
    db_config.connection_uri,
    connect_args={"ssl": True},
    pool_pre_ping=db_config.pool_pre_ping,
    future=True,
    pool_recycle=db_config.pool_recycle,
    pool_reset_on_return=None,
    echo=db_config.echo,
    pool_size=db_config.pool_size,
    max_overflow=db_config.max_overflow,
    pool_timeout=db_config.pool_timeout,
    isolation_level=db_config.isolation_level,
    poolclass=AsyncAdaptedQueuePool,
)

AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    autocommit=False,
    autoflush=True,
    expire_on_commit=False,
)
# ──────────────────────────────────────────────────────────────

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


P = ParamSpec("P")
R = TypeVar("R")


def with_db_session(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        async with AsyncSessionLocal() as session:
            kwargs["db_session"] = session
            return await func(*args, **kwargs)

    return wrapper
