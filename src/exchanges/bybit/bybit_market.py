import asyncio
import httpx

from src.config import (
    BYBIT_BASE_REST_URL,
    BYBIT_TICKERS_ENDPOINT,
)


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


async def _demo():
    tickers = await fetch_tickers_raw("spot")
    print(f"items: {len(tickers)}")
    for item in tickers[:2]:
        print(item)


if __name__ == "__main__":
    asyncio.run(_demo())
