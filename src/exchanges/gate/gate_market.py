import asyncio
import httpx

from src.config import (
    GATE_BASE_REST_URL,
    GATE_TICKERS_ENDPOINT,
    GATE_ORDERBOOK_ENDPOINT,
    MAX_BOOK_DEPTH_LEVELS,
)


# ----------------------------------------------------------------------
# Tickers (bid/ask + 24h stats)
# ----------------------------------------------------------------------

async def fetch_tickers_raw() -> list:
    """
    Возвращает сырые Gate.io spot tickers.
    Данные включают best bid / best ask + 24h статистику.
    """
    url = f"{GATE_BASE_REST_URL}{GATE_TICKERS_ENDPOINT}"

    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()

    return data if isinstance(data, list) else []


# ----------------------------------------------------------------------
# Orderbook / depth
# ----------------------------------------------------------------------

async def fetch_orderbook_raw(currency_pair: str, limit: int | None = None) -> dict:
    """
    Возвращает стакан Gate.io (bids / asks) для пары.
    currency_pair — формат вида 'BTC_USDT'
    limit — глубина стакана, по умолчанию берём из конфига.
    """
    if limit is None:
        limit = MAX_BOOK_DEPTH_LEVELS

    url = f"{GATE_BASE_REST_URL}{GATE_ORDERBOOK_ENDPOINT}"

    params = {
        "currency_pair": currency_pair,
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
    print("[demo] fetching Gate.io tickers...")
    tickers = await fetch_tickers_raw()
    print(f"[demo] items: {len(tickers)}")

    for item in tickers[:2]:
        print(item)

    # найдём первую USDT-пару
    pair = None
    for t in tickers:
        s = t.get("currency_pair")
        if s and s.endswith("_USDT"):
            pair = s
            break

    if not pair:
        print("[demo] no USDT pairs found")
        return

    print(f"\n[demo] fetching orderbook for {pair} ...")
    ob = await fetch_orderbook_raw(pair)

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
