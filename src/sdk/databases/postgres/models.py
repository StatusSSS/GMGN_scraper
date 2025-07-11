from datetime import datetime
from sqlalchemy import (
    Column, DateTime, Integer, String, Numeric,
    UniqueConstraint, create_engine, TIMESTAMP
)
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import NUMERIC
from src.sdk.databases.postgres.base import Base



class WalletSnapshot(Base):
    """
    Дневной снимок метрик Solana-кошелька.
    PK: (address, ts_utc) — позволяет хранить историю.
    """
    __tablename__ = "wallet_snapshot"
    __table_args__ = (
        UniqueConstraint("address", "ts_utc", name="uix_address_day"),
    )

    # surrogate id (удобно для FK на другие таблицы, если понадобятся)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # ключи
    address: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    ts_utc:  Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.utcnow,
        nullable=True
    )

    profit_factor:        Mapped[float] = mapped_column(NUMERIC(18, 8))
    expectancy:           Mapped[float] = mapped_column(NUMERIC(18, 8))
    risk_reward:          Mapped[float] = mapped_column(NUMERIC(18, 8))
    winrate_pct:          Mapped[float] = mapped_column(NUMERIC(6, 2))
    net_pnl_30d_usd:      Mapped[float] = mapped_column(NUMERIC(18, 2))
    median_liquidity_usd: Mapped[float] = mapped_column(NUMERIC(18, 2))
    hhi:                  Mapped[float] = mapped_column(NUMERIC(10, 6))
    honeypot_share_pct:   Mapped[float] = mapped_column(NUMERIC(6, 2))
    days_idle:            Mapped[float] = mapped_column(NUMERIC(8, 2))
    total_trades:         Mapped[int]   = mapped_column(Integer)
    avg_hold_human:       Mapped[str]   = mapped_column(String(32))
    avg_interval_human:   Mapped[str]   = mapped_column(String(32))
    turnover_usd:         Mapped[float] = mapped_column(NUMERIC(18, 2))
    pnl_per_turnover:     Mapped[float] = mapped_column(NUMERIC(12, 6))


    pnl:                  Mapped[float] = mapped_column(NUMERIC(18, 2))

    def __repr__(self) -> str:
        return (
            f"<Snapshot {self.address} {self.ts_utc:%Y-%m-%d} "
            f"PF={self.profit_factor} PnL30d={self.net_pnl_30d_usd}>"
        )


class Wallet(Base):
    """
    Справочник Solana-кошельков.
    Одна строка — один уникальный адрес.
    """
    __tablename__ = "wallets"
    __table_args__ = (
        UniqueConstraint("address", name="uix_wallets_address"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    address: Mapped[str] = mapped_column(String(64), nullable=False, index=True, unique=True)

    # последняя зафиксированная метрика PnL (или NULL, если ещё не считали)
    pnl: Mapped[float | None] = mapped_column(NUMERIC(18, 2))

    # когда адрес проверяли в последний раз
    last_check: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        default=datetime.utcnow,
        nullable=True,
    )

    def __repr__(self) -> str:
        return f"<Wallet {self.address} pnl={self.pnl} at {self.last_check}>"