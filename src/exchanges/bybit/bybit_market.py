import asyncio
import httpx

from src.config import (
    BYBIT_BASE_REST_URL,
    BYBIT_TICKERS_ENDPOINT,
    BYBIT_ORDERBOOK_ENDPOINT,
    BYBIT_ORDERBOOK_DEPTH,
)


# ---------------------------------------------------------------------
#  TICKERS (как раньше — Stage-1)
# ---------------------------------------------------------------------

async def fetch_tickers_raw(category: str = "spot") -> list:
    """
    Возвращает сырые тикеры Bybit из поля result.list без изменений.
    """
    url = f"{BYBIT_BASE_REST_URL}{BYBIT_TICKERS_ENDPOINT}"

    async with httpx.AsyncClient() as client:
        response = await client.get(url, params={"category": category})
        response.raise_for_status()
        data = response.json()

    return data.get("result", {}).get("list", []) or []


# ---------------------------------------------------------------------
#  ORDERBOOK (для Stage-2 Depth-Check)
# ---------------------------------------------------------------------

async def fetch_orderbook_raw(symbol: str, depth: int | None = None) -> dict:
    """
    Загружает стакан по символу.
    Возвращает сырые bids / asks без нормализации.
    """
    url = f"{BYBIT_BASE_REST_URL}{BYBIT_ORDERBOOK_ENDPOINT}"

    params = {
        "symbol": symbol,
        "category": "spot",
        "limit": depth or BYBIT_ORDERBOOK_DEPTH,
    }

    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params)
        response.raise_for_status()
        data = response.json()

    return data.get("result", {}) or {}


# ---------------------------------------------------------------------
#  DEMO
# ---------------------------------------------------------------------

async def _demo():
    print("[demo] fetching tickers...")
    tickers = await fetch_tickers_raw("spot")
    print(f"[demo] items: {len(tickers)}")

    for item in tickers[:2]:
        print(item)

    # выбираем первый символ с USDT
    symbol = next(
        (t["symbol"] for t in tickers if t.get("symbol", "").endswith("USDT")),
        None,
    )

    if not symbol:
        print("[demo] no USDT symbols found — skip orderbook demo")
        return

    print(f"\n[demo] fetching orderbook for {symbol} ...")

    ob = await fetch_orderbook_raw(symbol)

    bids = ob.get("b", [])  # [price, qty]
    asks = ob.get("a", [])

    print(f"[demo] depth = {len(bids)} bids / {len(asks)} asks")

    print("\n[demo] top 3 bids:")
    for p, q in bids[:3]:
        print(f"  {p}  x {q}")

    print("\n[demo] top 3 asks:")
    for p, q in asks[:3]:
        print(f"  {p}  x {q}")


if __name__ == "__main__":
    asyncio.run(_demo())
