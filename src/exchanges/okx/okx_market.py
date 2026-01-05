import asyncio
import httpx

from src.config import (
    OKX_BASE_REST_URL,
    OKX_TICKERS_ENDPOINT,
    OKX_ORDERBOOK_ENDPOINT,
    MAX_BOOK_DEPTH_LEVELS,
)


# ----------------------------------------------------------------------
# Tickers (best bid / best ask + 24h stats)
# ----------------------------------------------------------------------

async def fetch_tickers_raw() -> list:
    """
    Возвращает сырые OKX spot tickers.
    Аналог Binance bookTicker + 24h данные.
    """
    url = f"{OKX_BASE_REST_URL}{OKX_TICKERS_ENDPOINT}"

    params = {"instType": "SPOT"}

    async with httpx.AsyncClient() as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        payload = resp.json()

    data = payload.get("data", [])
    return data if isinstance(data, list) else []


# ----------------------------------------------------------------------
# Orderbook / depth
# ----------------------------------------------------------------------

async def fetch_orderbook_raw(inst_id: str, limit: int | None = None) -> dict:
    """
    Возвращает стакан OKX (bids / asks) для инструмента.
    inst_id — формат вида 'BTC-USDT'
    limit — глубина стакана, по умолчанию из конфига.
    """
    if limit is None:
        limit = MAX_BOOK_DEPTH_LEVELS

    url = f"{OKX_BASE_REST_URL}{OKX_ORDERBOOK_ENDPOINT}"

    params = {
        "instId": inst_id,
        "sz": limit,
    }

    async with httpx.AsyncClient() as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        payload = resp.json()

    data = payload.get("data", [])
    return data[0] if data else {}


# ----------------------------------------------------------------------
# Demo
# ----------------------------------------------------------------------

async def _demo():
    print("[demo] fetching OKX tickers...")
    tickers = await fetch_tickers_raw()
    print(f"[demo] items: {len(tickers)}")

    for item in tickers[:2]:
        print(item)

    inst_id = None
    for t in tickers:
        s = t.get("instId")
        if s and s.endswith("USDT"):
            inst_id = s
            break

    if not inst_id:
        print("[demo] no USDT instruments found")
        return

    print(f"\n[demo] fetching orderbook for {inst_id} ...")
    ob = await fetch_orderbook_raw(inst_id)

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
