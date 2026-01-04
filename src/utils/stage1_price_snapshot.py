"""
Stage 1 — Price snapshot + spread calculation (timing + fallback).

• Берём цены напрямую из BinanceMarket / BybitMarket
• Парсим bid/ask внутри этой функции
• Если pairs не передан — строим через PairsNormalize
"""

import asyncio
import time
from typing import Dict, Any

from src.config import MIN_PROFIT_PCT
from src.utils.pairs_normalize import build_normalized_pairs

from src.exchanges.binance.binance_market import fetch_book_tickers_raw
from src.exchanges.bybit.bybit_market import fetch_tickers_raw


def _pct(buy: float, sell: float) -> float:
    if buy <= 0:
        return 0.0
    return (sell - buy) / buy * 100.0


async def _load_binance_prices() -> Dict[str, Any]:
    raw = await fetch_book_tickers_raw()
    result: Dict[str, Any] = {}

    for item in raw:
        sym = item.get("symbol")
        if not sym:
            continue

        result[sym] = {
            "bid": float(item["bidPrice"]),
            "ask": float(item["askPrice"]),
            "bid_size": float(item["bidQty"]),
            "ask_size": float(item["askQty"]),
        }

    return result


async def _load_bybit_prices() -> Dict[str, Any]:
    raw = await fetch_tickers_raw("spot")
    result: Dict[str, Any] = {}

    for item in raw:
        sym = item.get("symbol")
        if not sym:
            continue

        result[sym] = {
            "bid": float(item["bid1Price"]),
            "ask": float(item["ask1Price"]),
            "bid_size": float(item["bid1Size"]),
            "ask_size": float(item["ask1Size"]),
        }

    return result


async def build_stage1_snapshot(
    pairs: Dict[str, Dict[str, Any]] | None
) -> Dict[str, Any]:

    # --- fallback: если пар нет — строим локально ---
    if not pairs:
        print("[Stage1] no pairs provided — building local normalized pairs...")
        pairs = await build_normalized_pairs()

    print("\n[Stage1] fetching price snapshot...")

    t0 = time.perf_counter()

    # --- API (параллельно) ---
    b_task = asyncio.create_task(_load_binance_prices())
    y_task = asyncio.create_task(_load_bybit_prices())

    binance = await b_task
    bybit   = await y_task

    t_api = time.perf_counter()

    print(f"[Stage1] API fetched: "
          f"binance={len(binance)}, bybit={len(bybit)}")

    # --- spread calc ---
    t_calc_start = time.perf_counter()
    result: Dict[str, Any] = {}

    for key, m in pairs.items():
        b_sym = m.get("binance")
        y_sym = m.get("bybit")
        if not b_sym or not y_sym:
            continue

        b = binance.get(b_sym)
        y = bybit.get(y_sym)
        if not b or not y:
            continue

        s_y2b = _pct(y["ask"], b["bid"])   # buy Bybit → sell Binance
        s_b2y = _pct(b["ask"], y["bid"])   # buy Binance → sell Bybit

        best_dir = "Y→B" if s_y2b >= s_b2y else "B→Y"
        best_pct = max(s_y2b, s_b2y)

        if best_pct < MIN_PROFIT_PCT:
            continue

        result[key] = {
            "binance": b,
            "bybit": y,
            "spread_pct_y2b": round(s_y2b, 4),
            "spread_pct_b2y": round(s_b2y, 4),
            "best_direction": best_dir,
            "best_spread_pct": round(best_pct, 4),
        }

    t_calc_end = time.perf_counter()

    # --- timing summary ---
    api_ms   = (t_api - t0) * 1000
    calc_ms  = (t_calc_end - t_calc_start) * 1000
    total_ms = (t_calc_end - t0) * 1000

    print(f"[Stage1] timing: api={api_ms:.2f} ms | "
          f"calc={calc_ms:.2f} ms | total={total_ms:.2f} ms")

    print(f"[Stage1] candidates: {len(result)} "
          f"(pct >= {MIN_PROFIT_PCT}%)")

    for k, v in list(result.items())[:10]:
        print(f"  {k:<12} {v['best_direction']} {v['best_spread_pct']}%")

    return result


# --- local test runner ----------------------------------------------------

async def _demo():
    await build_stage1_snapshot(None)


if __name__ == "__main__":
    asyncio.run(_demo())
