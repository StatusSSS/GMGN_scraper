from functools import cached_property
from pydantic_settings import BaseSettings, SettingsConfigDict


class DBConfig(BaseSettings):
    """Настройки подключения к PostgreSQL (asyncpg)."""
    model_config = SettingsConfigDict(
        env_prefix="POSTGRES_",
        case_sensitive=False
    )

    # основные параметры
    user: str = "neondb_owner"
    password: str = "npg_rAyWDCFl5ox0"
    db: str = "neondb"
    host: str = "ep-dawn-meadow-adkiybg0-pooler.c-2.us-east-1.aws.neon.tech"
    port: int = 5432

    # SQLAlchemy / asyncpg tuning
    echo: bool = False
    pool_pre_ping: bool = True
    pool_size: int = 4
    max_overflow: int = 4
    pool_timeout: int = 10
    pool_recycle: int = 600          # секунд
    isolation_level: str = "READ COMMITTED"

    @cached_property
    def connection_uri(self) -> str:
        """
        Пример:
        postgresql+asyncpg://user:pass@host:5432/db?sslmode=require&channel_binding=require
        """

        return (
            f"postgresql+asyncpg://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.db}"
            "?ssl=require"  # <─ обязательно для Neon
        )


# экземпляр, который будет импортироваться извне
db_config = DBConfig()
