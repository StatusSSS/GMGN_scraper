import os
from functools import lru_cache
from typing import Any, Iterable, List

from clickhouse_connect import get_client
from dotenv import load_dotenv

load_dotenv()  # загружаем переменные из .env

class ClickHouseDB:
    """
    Обёртка над clickhouse-connect с примитивным пулом (lru_cache)
    и удобными методами command / fetchall / iterate.
    """
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        secure: bool = False,
        compress: bool | str | None = None,
    ):
        self._client = get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
            secure=secure,      # True → https (порт обычно 8443)
            compress=compress,  # 'gzip', 'lz4' … (опционально)
        )

    # ---------- базовые методы ----------
    def command(self, query: str, params: dict | None = None) -> Any:
        """
        Выполняет DDL или INSERT/DELETE и возвращает int | None
        (кол-во затронутых строк, если известно).
        """
        return self._client.command(query, parameters=params or {})

    def fetchall(self, query: str, params: dict | None = None) -> List[tuple]:
        """
        SELECT-запрос: возвращает все строки как список кортежей.
        """
        return self._client.query(query, parameters=params or {}).result_rows

    def iterate(
        self,
        query: str,
        params: dict | None = None,
        settings: dict | None = None,
        chunk_size: int | None = None,
    ) -> Iterable[List[tuple]]:
        """
        Ленивая итерация крупного SELECT-а блоками строк.
        """
        return self._client.query_iter(
            query,
            parameters=params or {},
            settings=settings or {},
            chunk_size=chunk_size,
        )


@lru_cache(maxsize=1)
def get_db() -> ClickHouseDB:
    return ClickHouseDB(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", 8123)),   # HTTP/HTTPS
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DB", "default"),
        secure=os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true",
    )
