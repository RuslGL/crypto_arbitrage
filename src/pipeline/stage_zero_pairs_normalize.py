"""
stage_zero — Pairs Normalize Snapshot

Назначение пайплайна:

• собрать 24h-тикеры с поддерживаемых бирж
• отфильтровать пары по ликвидности
• унифицировать ключи в формате BASE_USDT
• сопоставить символы между биржами
• вернуть snapshot для Stage-1

Stage-0 НИЧЕГО не считает по спредам.
Его задача — только структура рынка (какие пары существуют).
"""

import asyncio
from typing import Dict, Any

from src.config import MIN_24H_VOLUME_USDT

from src.exchanges.binance.binance_market import fetch_tickers_24h_raw
from src.exchanges.bybit.bybit_market import fetch_tickers_raw

from src.exchanges.okx.okx_market import (
    fetch_tickers_raw as okx_tickers_raw,
)

from src.exchanges.gate.gate_market import (
    fetch_tickers_raw as gate_tickers_raw,
)

from src.exchanges.kucoin.kucoin_market import (
    fetch_tickers_raw as kucoin_tickers_raw,
)


# -------------------------------------------------------------------------
# helpers
# -------------------------------------------------------------------------

def _key_usdt(symbol: str | None) -> str | None:
    """
    Приводит символ к унифицированному ключу BASE_USDT.

    Примеры:
      BTCUSDT     → BTC_USDT
      BTC-USDT    → BTC_USDT
      BTC_USDT    → BTC_USDT

    Если пара не в USDT — возвращает None.
    """
    if not symbol:
        return None

    s = symbol.replace("-", "").replace("_", "")

    if not s.endswith("USDT"):
        return None

    return f"{s[:-4]}_USDT"


# -------------------------------------------------------------------------
# main builder
# -------------------------------------------------------------------------

async def build_pairs_snapshot() -> Dict[str, Dict[str, Any]]:
    """
    Возвращает snapshot вида:

    {
      "BTC_USDT": {
          "binance": "BTCUSDT",
          "bybit":   "BTCUSDT",
          "okx":     "BTC-USDT",
          "gate":    "BTC_USDT",
          "kucoin":  "BTC-USDT"
      },
      ...
    }
    """

    (
        binance_raw,
        bybit_raw,
        okx_raw,
        gate_raw,
        kucoin_raw,
    ) = await asyncio.gather(
        fetch_tickers_24h_raw(),       # Binance (есть quoteVolume)
        fetch_tickers_raw("spot"),     # Bybit (есть turnover24h)
        okx_tickers_raw(),             # OKX
        gate_tickers_raw(),            # Gate.io
        kucoin_tickers_raw(),          # KuCoin
    )

    result: Dict[str, Dict[str, Any]] = {}

    def ensure_entry(key: str) -> Dict[str, Any]:
        return result.setdefault(key, {
            "binance": None,
            "bybit":   None,
            "okx":     None,
            "gate":    None,
            "kucoin":  None,
        })

    # --- Binance ---------------------------------------------------------
    for it in binance_raw:
        vol = float(it.get("quoteVolume", 0) or 0)
        if vol < MIN_24H_VOLUME_USDT:
            continue

        key = _key_usdt(it.get("symbol"))
        if not key:
            continue

        ensure_entry(key)["binance"] = it["symbol"]

    # --- Bybit -----------------------------------------------------------
    for it in bybit_raw:
        vol = float(it.get("turnover24h", 0) or 0)
        if vol < MIN_24H_VOLUME_USDT:
            continue

        key = _key_usdt(it.get("symbol"))
        if not key:
            continue

        ensure_entry(key)["bybit"] = it["symbol"]

    # --- OKX -------------------------------------------------------------
    for it in okx_raw:
        key = _key_usdt(it.get("instId"))
        if not key:
            continue

        ensure_entry(key)["okx"] = it["instId"]

    # --- Gate.io ---------------------------------------------------------
    for it in gate_raw:
        key = _key_usdt(it.get("currency_pair"))
        if not key:
            continue

        ensure_entry(key)["gate"] = it["currency_pair"]

    # --- KuCoin ----------------------------------------------------------
    for it in kucoin_raw:
        key = _key_usdt(it.get("symbol"))
        if not key:
            continue

        ensure_entry(key)["kucoin"] = it["symbol"]

    return result


# -------------------------------------------------------------------------
# Local demo / manual test (silent, only errors if any)
# -------------------------------------------------------------------------

async def _demo():
    await build_pairs_snapshot()


if __name__ == "__main__":
    try:
        asyncio.run(_demo())
    except Exception as e:
        print("[Stage-0 ERROR]", e)
