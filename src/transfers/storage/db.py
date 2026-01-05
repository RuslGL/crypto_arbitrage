import os
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    async_sessionmaker,
    AsyncSession,
)
from sqlalchemy import text


# --- env ----------------------------------------------------------------

PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB   = os.getenv("POSTGRES_DB", "arb")


DB_DSN = (
    f"postgresql+asyncpg://{PG_USER}:{PG_PASS}"
    f"@{PG_HOST}:{PG_PORT}/{PG_DB}"
)


# --- engine & session factory ------------------------------------------

engine = create_async_engine(
    DB_DSN,
    echo=False,
    pool_pre_ping=True,
)

Session: async_sessionmaker[AsyncSession] = async_sessionmaker(
    engine,
    expire_on_commit=False,
)


# --- schema bootstrap ---------------------------------------------------

async def init_db():
    """
    Создаёт служебные таблицы при первом запуске (idempotent).
    """
    async with engine.begin() as conn:

        await conn.execute(text("""
        CREATE TABLE IF NOT EXISTS transfer_exchanges (
            id                  SERIAL PRIMARY KEY,
            exchange            VARCHAR(32) NOT NULL,
            network_code        VARCHAR(64) NOT NULL,
            withdraw_enabled    BOOLEAN NOT NULL,
            deposit_enabled     BOOLEAN NOT NULL,
            withdraw_fee_usdt   NUMERIC,
            min_withdraw_usdt   NUMERIC,
            updated_at          TIMESTAMP NOT NULL DEFAULT NOW(),
            UNIQUE(exchange, network_code)
        );
        """))

        await conn.execute(text("""
        CREATE TABLE IF NOT EXISTS transfer_assets (
            id                  SERIAL PRIMARY KEY,
            exchange            VARCHAR(32) NOT NULL,
            asset               VARCHAR(64) NOT NULL,
            network_code        VARCHAR(64) NOT NULL,
            withdraw_fee        NUMERIC,
            min_withdraw        NUMERIC,
            withdraw_enabled    BOOLEAN NOT NULL,
            deposit_enabled     BOOLEAN NOT NULL,
            updated_at          TIMESTAMP NOT NULL DEFAULT NOW(),
            UNIQUE(exchange, asset, network_code)
        );
        """))

    print("[DB] schema ensured")
