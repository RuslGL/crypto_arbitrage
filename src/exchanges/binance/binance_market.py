import asyncio
import httpx

from src.config import (
    BINANCE_BASE_REST_URL,
    BINANCE_BOOK_TICKER_ENDPOINT,
    BINANCE_TICKERS_ENDPOINT,   # новый эндпоинт 24h tickers
)


# ----------------------------------------------------------------------
# bookTicker (bid/ask, без объёма)
# ----------------------------------------------------------------------

async def fetch_book_tickers_raw() -> list:
    """
    Возвращает сырые Binance bookTicker (bid/ask) без изменений.
    """
    url = f"{BINANCE_BASE_REST_URL}{BINANCE_BOOK_TICKER_ENDPOINT}"

    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()

    return data if isinstance(data, list) else []


# ----------------------------------------------------------------------
# 24h tickers (источник объёма: quoteVolume)
# ----------------------------------------------------------------------

async def fetch_tickers_24h_raw() -> list:
    """
    Возвращает сырые Binance 24h tickers.
    Здесь есть поле quoteVolume — его используем для фильтра ликвидности.
    """
    url = f"{BINANCE_BASE_REST_URL}{BINANCE_TICKERS_ENDPOINT}"

    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()

    return data if isinstance(data, list) else []


# ----------------------------------------------------------------------
# Demo
# ----------------------------------------------------------------------

async def _demo():
    tickers = await fetch_tickers_24h_raw()
    print(f"items: {len(tickers)}")
    for item in tickers[:2]:
        print(item)


if __name__ == "__main__":
    asyncio.run(_demo())
