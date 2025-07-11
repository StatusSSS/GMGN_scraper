"""
Простой self-contained тест записи в wallet_snapshot через
AsyncSession из src.sdk.databases.postgres.dependency.

Запускайте:  python -m src.tests.pg_insert_demo
"""

import asyncio
from datetime import datetime, timezone

from sqlalchemy import select

from src.sdk.databases.postgres.dependency import AsyncSessionLocal
from src.sdk.databases.postgres.models import WalletSnapshot


async def main() -> None:
    async with AsyncSessionLocal() as session:
        # 1️⃣ Пишем тестовую запись
        row = WalletSnapshot(
            address="TEST_WALLET_123",
            pnl=1.234,  # >0.6
            ts_utc=datetime.now(tz=timezone.utc),
        )
        session.add(row)
        await session.commit()
        print("🟢 insert committed")

        # 2️⃣ Читаем обратно
        q = select(WalletSnapshot).where(WalletSnapshot.address == "TEST_WALLET_123")
        result = await session.execute(q)
        fetched = result.scalar_one_or_none()
        print("🔎 fetched:", fetched)

        # 3️⃣ Чистим за собой (чтобы тест был идемпотентным)
        if fetched:
            await session.delete(fetched)
            await session.commit()
            print("🧹 cleaned")

if __name__ == "__main__":
    asyncio.run(main())
