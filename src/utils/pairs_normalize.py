"""
PairsNormalize — формирование унифицированного списка пар
(возвращает dict для межпроцессного обмена).
"""

import asyncio
from typing import Dict, Any

from src.config import MIN_24H_VOLUME_USDT
from src.exchanges.binance.binance_market import fetch_tickers_24h_raw
from src.exchanges.bybit.bybit_market import fetch_tickers_raw


def _normalize_to_usdt_key(symbol: str) -> str | None:
    if not symbol:
        return None
    s = symbol.replace("-", "").replace("_", "")
    if not s.endswith("USDT"):
        return None
    return f"{s[:-4]}_USDT"


async def build_normalized_pairs() -> Dict[str, Dict[str, Any]]:
    print("\n[PairsNormalize] fetching 24h tickers...")

    binance_raw, bybit_raw = await asyncio.gather(
        fetch_tickers_24h_raw(),
        fetch_tickers_raw("spot"),
    )

    result: Dict[str, Dict[str, Any]] = {}

    # --- Binance ----------------------------------------------------------
    for item in binance_raw:
        volume = float(item.get("quoteVolume", 0) or 0)
        if volume < MIN_24H_VOLUME_USDT:
            continue

        symbol = item.get("symbol")
        key = _normalize_to_usdt_key(symbol)
        if not key:
            continue

        entry = result.setdefault(key, {"binance": None, "bybit": None})
        entry["binance"] = symbol

    # --- Bybit ------------------------------------------------------------
    for item in bybit_raw:
        volume = float(item.get("turnover24h", 0) or 0)
        if volume < MIN_24H_VOLUME_USDT:
            continue

        symbol = item.get("symbol")
        key = _normalize_to_usdt_key(symbol)
        if not key:
            continue

        entry = result.setdefault(key, {"binance": None, "bybit": None})
        entry["bybit"] = symbol

    print(f"[PairsNormalize] total pairs: {len(result)}")
    return result


# --- local test -----------------------------------------------------------

async def _demo():
    pairs = await build_normalized_pairs()

    print("\n[PairsNormalize] sample preview:")
    for k, v in list(pairs.items())[:10]:
        print(f"  {k}: {v}")


if __name__ == "__main__":
    asyncio.run(_demo())
