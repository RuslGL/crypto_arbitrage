import asyncio
import httpx

from src.config import (
    BINANCE_BASE_REST_URL,
    BINANCE_BOOK_TICKER_ENDPOINT,
    BINANCE_TICKERS_ENDPOINT,
    BINANCE_ORDERBOOK_ENDPOINT,   # эндпоинт depth
    ORDERBOOK_DEPTH,              # ← единая глубина стакана
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
    Здесь есть поле quoteVolume — используем для фильтра ликвидности.
    """
    url = f"{BINANCE_BASE_REST_URL}{BINANCE_TICKERS_ENDPOINT}"

    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()

    return data if isinstance(data, list) else []


# ----------------------------------------------------------------------
# Orderbook / depth
# ----------------------------------------------------------------------

async def fetch_orderbook_raw(symbol: str, limit: int | None = None) -> dict:
    """
    Возвращает стакан Binance (bids / asks) для символа.
    limit — глубина (число уровней), по умолчанию берём из общего конфига.
    """
    if limit is None:
        limit = ORDERBOOK_DEPTH

    url = f"{BINANCE_BASE_REST_URL}{BINANCE_ORDERBOOK_ENDPOINT}"

    params = {
        "symbol": symbol,
        "limit": limit,
    }

    async with httpx.AsyncClient() as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

    return data or {}


# ----------------------------------------------------------------------
# Demo
# ----------------------------------------------------------------------

async def _demo():
    print("[demo] fetching 24h tickers...")
    tickers = await fetch_tickers_24h_raw()
    print(f"[demo] items: {len(tickers)}")

    for item in tickers[:2]:
        print(item)

    # найдём первый USDT-символ
    symbol = None
    for t in tickers:
        s = t.get("symbol")
        if s and s.endswith("USDT"):
            symbol = s
            break

    if not symbol:
        print("[demo] no USDT symbols found")
        return

    print(f"\n[demo] fetching orderbook for {symbol} ...")
    ob = await fetch_orderbook_raw(symbol)

    bids = ob.get("bids", [])
    asks = ob.get("asks", [])

    print(f"[demo] depth = {len(bids)} bids / {len(asks)} asks")

    print("\n[demo] top 3 bids:")
    for p, q in bids[:3]:
        print(f"  {p}  x {q}")

    print("\n[demo] top 3 asks:")
    for p, q in asks[:3]:
        print(f"  {p}  x {q}")


if __name__ == "__main__":
    asyncio.run(_demo())
