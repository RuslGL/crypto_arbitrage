import asyncio
import httpx

from src.config import (
    KUCOIN_BASE_REST_URL,
    KUCOIN_TICKERS_ENDPOINT,
    KUCOIN_ORDERBOOK_ENDPOINT,
    MAX_BOOK_DEPTH_LEVELS,
)


# ----------------------------------------------------------------------
# Tickers (best bid / best ask + 24h stats)
# ----------------------------------------------------------------------

async def fetch_tickers_raw() -> list:
    """
    Возвращает сырые KuCoin tickers.
    Источник best bid/ask + 24h статистики.
    """
    url = f"{KUCOIN_BASE_REST_URL}{KUCOIN_TICKERS_ENDPOINT}"

    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        resp.raise_for_status()
        payload = resp.json()

    data = payload.get("data", {}).get("ticker", [])
    return data if isinstance(data, list) else []


# ----------------------------------------------------------------------
# Orderbook / depth
# ----------------------------------------------------------------------

async def fetch_orderbook_raw(symbol: str, limit: int | None = None) -> dict:
    """
    Возвращает стакан KuCoin (bids / asks) для символа.
    symbol — формат вида 'BTC-USDT'
    limit — глубина стакана, по умолчанию из конфига.
    """
    if limit is None:
        limit = MAX_BOOK_DEPTH_LEVELS

    url = f"{KUCOIN_BASE_REST_URL}{KUCOIN_ORDERBOOK_ENDPOINT}"

    params = {
        "symbol": symbol,
    }

    async with httpx.AsyncClient() as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        payload = resp.json()

    data = payload.get("data", {})
    return data


# ----------------------------------------------------------------------
# Demo
# ----------------------------------------------------------------------

async def _demo():
    print("[demo] fetching KuCoin tickers...")
    tickers = await fetch_tickers_raw()
    print(f"[demo] items: {len(tickers)}")

    for item in tickers[:2]:
        print(item)

    # найдём первую USDT-пару
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
    for p, q, *_ in bids[:3]:
        print(f"  {p}  x {q}")

    print("\n[demo] top 3 asks:")
    for p, q, *_ in asks[:3]:
        print(f"  {p}  x {q}")


if __name__ == "__main__":
    asyncio.run(_demo())
