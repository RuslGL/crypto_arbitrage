"""
PairsNormalize — формирование унифицированного списка пар
(возвращает dict для межпроцессного обмена).
"""

import asyncio
from typing import Dict, Any

from src.config import MIN_24H_VOLUME_USDT

from src.exchanges.binance.binance_market import fetch_tickers_24h_raw
from src.exchanges.bybit.bybit_market import fetch_tickers_raw

from src.exchanges.okx.okx_market import fetch_tickers_raw as okx_fetch_tickers_raw
from src.exchanges.gate.gate_market import fetch_tickers_raw as gate_fetch_tickers_raw
from src.exchanges.kucoin.kucoin_market import fetch_tickers_raw as kucoin_fetch_tickers_raw


# ----------------------------------------------------------------------
# Normalization helpers
# ----------------------------------------------------------------------

def _normalize_to_usdt_key(symbol: str) -> str | None:
    """
    Приводит символ к каноническому ключу вида:
        BTCUSDT / BTC-USDT / BTC_USDT → BTC_USDT

    Возвращает None, если пара не USDT.
    """
    if not symbol:
        return None

    s = symbol.replace("-", "").replace("_", "")
    if not s.endswith("USDT"):
        return None

    return f"{s[:-4]}_USDT"


# ----------------------------------------------------------------------
# Build normalized pairs table
# ----------------------------------------------------------------------

async def build_normalized_pairs() -> Dict[str, Dict[str, Any]]:
    print("\n[PairsNormalize] fetching 24h tickers...")

    (
        binance_raw,
        bybit_raw,
        okx_raw,
        gate_raw,
        kucoin_raw,
    ) = await asyncio.gather(
        fetch_tickers_24h_raw(),
        fetch_tickers_raw("spot"),

        okx_fetch_tickers_raw(),
        gate_fetch_tickers_raw(),
        kucoin_fetch_tickers_raw(),
    )

    result: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Binance
    # ------------------------------------------------------------------
    for item in binance_raw:
        volume = float(item.get("quoteVolume", 0) or 0)
        if volume < MIN_24H_VOLUME_USDT:
            continue

        symbol = item.get("symbol")
        key = _normalize_to_usdt_key(symbol)
        if not key:
            continue

        entry = result.setdefault(
            key,
            {"binance": None, "bybit": None, "okx": None, "gate": None, "kucoin": None},
        )
        entry["binance"] = symbol

    # ------------------------------------------------------------------
    # Bybit
    # ------------------------------------------------------------------
    for item in bybit_raw:
        volume = float(item.get("turnover24h", 0) or 0)
        if volume < MIN_24H_VOLUME_USDT:
            continue

        symbol = item.get("symbol")
        key = _normalize_to_usdt_key(symbol)
        if not key:
            continue

        entry = result.setdefault(
            key,
            {"binance": None, "bybit": None, "okx": None, "gate": None, "kucoin": None},
        )
        entry["bybit"] = symbol

    # ------------------------------------------------------------------
    # OKX  (instId = BTC-USDT, volume field: volCcy24h or vol24h)
    # ------------------------------------------------------------------
    for item in okx_raw:
        volume = float(item.get("volCcy24h") or item.get("vol24h") or 0)
        if volume < MIN_24H_VOLUME_USDT:
            continue

        symbol = item.get("instId")
        key = _normalize_to_usdt_key(symbol)
        if not key:
            continue

        entry = result.setdefault(
            key,
            {"binance": None, "bybit": None, "okx": None, "gate": None, "kucoin": None},
        )
        entry["okx"] = symbol

    # ------------------------------------------------------------------
    # Gate.io  (currency_pair = BTC_USDT, volume field: quote_volume)
    # ------------------------------------------------------------------
    for item in gate_raw:
        volume = float(item.get("quote_volume", 0) or 0)
        if volume < MIN_24H_VOLUME_USDT:
            continue

        symbol = item.get("currency_pair")
        key = _normalize_to_usdt_key(symbol)
        if not key:
            continue

        entry = result.setdefault(
            key,
            {"binance": None, "bybit": None, "okx": None, "gate": None, "kucoin": None},
        )
        entry["gate"] = symbol

    # ------------------------------------------------------------------
    # KuCoin  (symbol = BTC-USDT, volume field: volValue)
    # ------------------------------------------------------------------
    for item in kucoin_raw:
        volume = float(item.get("volValue", 0) or 0)
        if volume < MIN_24H_VOLUME_USDT:
            continue

        symbol = item.get("symbol")
        key = _normalize_to_usdt_key(symbol)
        if not key:
            continue

        entry = result.setdefault(
            key,
            {"binance": None, "bybit": None, "okx": None, "gate": None, "kucoin": None},
        )
        entry["kucoin"] = symbol

    print(f"[PairsNormalize] total pairs: {len(result)}")
    return result


# ----------------------------------------------------------------------
# Local demo
# ----------------------------------------------------------------------

async def _demo():
    pairs = await build_normalized_pairs()

    print("\n[PairsNormalize] sample preview:")
    for k, v in list(pairs.items())[:10]:
        print(f"  {k}: {v}")


if __name__ == "__main__":
    asyncio.run(_demo())
