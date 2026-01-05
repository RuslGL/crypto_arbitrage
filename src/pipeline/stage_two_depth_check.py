"""
stage_two — Depth-check / executable liquidity (ALL EXCHANGES)

• принимает BATCH сигналов Stage-1
• грузит стаканы нужных бирж параллельно
• считает VWAP-цены исполнения
• учитывает комиссии + защитный буфер
• проверяет чистую прибыль >= TARGET_NET_PROFIT_PCT
• подтверждает / отклоняет сигнал
"""

from __future__ import annotations

import asyncio
import time
from typing import Dict, Any, List, Tuple, Optional

from httpx import HTTPStatusError

from src.config import (
    MIN_EXECUTION_NOTIONAL_USDT,
    MAX_BOOK_DEPTH_LEVELS,
    MIN_PROFIT_PCT,              # используется только как диагностический маркер Stage-1
    TARGET_NET_PROFIT_PCT,       # главный порог ЧИСТОЙ прибыли
    SAFETY_FEE_BUFFER_PCT,
    EXCHANGE_TAKER_FEES,
)

from src.exchanges.binance.binance_market import fetch_orderbook_raw as ob_binance
from src.exchanges.bybit.bybit_market import fetch_orderbook_raw as ob_bybit
from src.exchanges.okx.okx_market import fetch_orderbook_raw as ob_okx
from src.exchanges.gate.gate_market import fetch_orderbook_raw as ob_gate
from src.exchanges.kucoin.kucoin_market import fetch_orderbook_raw as ob_kucoin


# -------------------------------------------------------------------------
# calc executable VWAP price
# -------------------------------------------------------------------------

def _calc_exec_price(levels, want_notional, max_levels) -> Optional[float]:
    filled = 0.0
    cost = 0.0

    for price, qty in levels[:max_levels]:
        price = float(price)
        qty = float(qty)

        level_notional = price * qty
        need = want_notional - filled
        if need <= 0:
            break

        take = min(level_notional, need)
        qty_taken = take / price

        cost += price * qty_taken
        filled += take

        if filled >= want_notional:
            break

    if filled < want_notional:
        return None

    total_qty = filled / price
    if total_qty <= 0:
        return None

    return cost / total_qty


# -------------------------------------------------------------------------
# map BASE_USDT → native exchange symbol
# -------------------------------------------------------------------------

def _symbol_for_exchange(pair: str, exchange: str) -> str:
    base = pair.replace("_USDT", "")

    if exchange in ("binance", "bybit"):
        return f"{base}USDT"

    if exchange == "okx":
        return f"{base}-USDT"

    if exchange == "gate":
        return f"{base}_USDT"

    if exchange == "kucoin":
        return f"{base}-USDT"

    return pair


# -------------------------------------------------------------------------
# normalize orderbooks to (bids, asks)
# -------------------------------------------------------------------------

def _normalize_ob(exchange: str, ob: Dict[str, Any]) -> Tuple[list, list]:
    if not ob:
        return [], []

    if exchange == "binance":
        return ob.get("bids") or [], ob.get("asks") or []

    if exchange == "bybit":
        return ob.get("b") or [], ob.get("a") or []

    if exchange == "okx":
        bids = [[float(x[0]), float(x[2])] for x in ob.get("bids", [])]
        asks = [[float(x[0]), float(x[3])] for x in ob.get("asks", [])]
        return bids, asks

    if exchange == "gate":
        return ob.get("bids") or [], ob.get("asks") or []

    if exchange == "kucoin":
        return ob.get("bids") or [], ob.get("asks") or []

    return [], []


# -------------------------------------------------------------------------
# fetcher table
# -------------------------------------------------------------------------

FETCHERS = {
    "binance": ob_binance,
    "bybit":   ob_bybit,
    "okx":     ob_okx,
    "gate":    ob_gate,
    "kucoin":  ob_kucoin,
}


async def _fetch_ob_safe(exchange: str, symbol_native: str):
    fn = FETCHERS.get(exchange)
    if not fn:
        return None

    try:
        return await fn(symbol_native)
    except HTTPStatusError:
        return None
    except Exception:
        return None


# -------------------------------------------------------------------------
# MAIN — batch Stage-2
# -------------------------------------------------------------------------

async def process_stage_two_batch(
    signals: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:

    if not signals:
        return []

    # collect orderbooks to fetch
    need: set[tuple[str, str]] = set()

    for s in signals:
        pair = s.get("pair")
        direction = s.get("direction", "")

        if not pair or "→" not in direction:
            continue

        buy_ex, sell_ex = [x.strip().lower() for x in direction.split("→")]

        need.add((buy_ex,  _symbol_for_exchange(pair, buy_ex)))
        need.add((sell_ex, _symbol_for_exchange(pair, sell_ex)))

    # fetch all concurrently
    tasks = {
        (ex, sym): asyncio.create_task(_fetch_ob_safe(ex, sym))
        for ex, sym in need
    }

    await asyncio.gather(*tasks.values())
    orderbooks = {k: tasks[k].result() for k in tasks}

    results: List[Dict[str, Any]] = []

    # process signals
    for s in signals:
        pair = s.get("pair")
        direction = s.get("direction", "")
        sig_spread = float(s.get("best_spread_pct", 0.0))

        if not pair or "→" not in direction:
            results.append({
                "status": "rejected",
                "reason": "invalid_signal",
                "pair": pair,
                "direction": direction,
            })
            continue

        buy_ex, sell_ex = [x.strip().lower() for x in direction.split("→")]

        sym_buy  = _symbol_for_exchange(pair, buy_ex)
        sym_sell = _symbol_for_exchange(pair, sell_ex)

        ob_buy  = orderbooks.get((buy_ex,  sym_buy))
        ob_sell = orderbooks.get((sell_ex, sym_sell))

        bids_buy, asks_buy = _normalize_ob(buy_ex, ob_buy)
        bids_sell, asks_sell = _normalize_ob(sell_ex, ob_sell)

        if not bids_buy or not asks_buy or not bids_sell or not asks_sell:
            results.append({
                "status": "rejected",
                "reason": "fetch_failed_or_empty_orderbook",
                "pair": pair,
                "direction": direction,
                "signal_spread_pct": sig_spread,
            })
            continue

        want = float(MIN_EXECUTION_NOTIONAL_USDT)

        buy_price  = _calc_exec_price(asks_buy,  want, MAX_BOOK_DEPTH_LEVELS)
        sell_price = _calc_exec_price(bids_sell, want, MAX_BOOK_DEPTH_LEVELS)

        if buy_price is None or sell_price is None:
            results.append({
                "status": "rejected",
                "reason": "insufficient_depth",
                "pair": pair,
                "direction": direction,
                "signal_spread_pct": sig_spread,
            })
            continue

        # -------------------- FEES + BUFFER ------------------------------

        fee_buy  = EXCHANGE_TAKER_FEES.get(buy_ex, 0.10)
        fee_sell = EXCHANGE_TAKER_FEES.get(sell_ex, 0.10)

        effective_buy  = buy_price  * (1 + fee_buy  / 100)
        effective_sell = sell_price * (1 - fee_sell / 100)

        gross_spread = (effective_sell - effective_buy) / effective_buy * 100
        net_spread   = gross_spread - SAFETY_FEE_BUFFER_PCT

        # ключевое изменение → решаем по ЧИСТОЙ прибыли
        if net_spread < TARGET_NET_PROFIT_PCT:
            results.append({
                "status": "rejected",
                "reason": "spread_after_fees_too_low",
                "pair": pair,
                "direction": direction,
                "signal_spread_pct": sig_spread,
                "exec_spread_pct": net_spread,
                "buy_exchange": buy_ex,
                "sell_exchange": sell_ex,
                "buy_price": buy_price,
                "sell_price": sell_price,
            })
            continue

        results.append({
            "status": "confirmed",
            "reason": "ok",
            "pair": pair,
            "direction": direction,
            "signal_spread_pct": sig_spread,
            "exec_spread_pct": net_spread,
            "buy_exchange": buy_ex,
            "sell_exchange": sell_ex,
            "buy_price": buy_price,
            "sell_price": sell_price,
        })

    return results


# -------------------------------------------------------------------------
# DEMO: Stage-0 → Stage-1 → Stage-2
# -------------------------------------------------------------------------

async def _demo():
    from src.pipeline.stage_zero_pairs_normalize import build_pairs_snapshot
    from src.pipeline.stage_one_price_snapshot_candidates import build_stage_one_snapshot

    t0 = time.perf_counter()
    pairs = await build_pairs_snapshot()
    t1 = time.perf_counter()

    stage1 = await build_stage_one_snapshot(pairs)
    t2 = time.perf_counter()

    signals = [
        {
            "pair": k,
            "direction": v["best_direction"],
            "best_spread_pct": v["best_spread_pct"],
        }
        for k, v in stage1.items()
    ]

    results = await process_stage_two_batch(signals)
    t3 = time.perf_counter()

    confirmed = [r for r in results if r["status"] == "confirmed"]
    rejected  = [r for r in results if r["status"] == "rejected"]

    print()
    print("Stage-0 pairs     :", f"{(t1-t0)*1000:.2f} ms")
    print("Stage-1 signals   :", f"{(t2-t1)*1000:.2f} ms")
    print("Stage-2 depth-chk :", f"{(t3-t2)*1000:.2f} ms")
    print("TOTAL             :", f"{(t3-t0)*1000:.2f} ms")
    print()
    print("confirmed :", len(confirmed))
    print("rejected  :", len(rejected))

    if confirmed[:5]:
        print("\nSAMPLE CONFIRMED:")
        for r in confirmed[:5]:
            print(
                r["pair"],
                r["direction"],
                f"{r['exec_spread_pct']:.3f}%"
            )

    if rejected[:5]:
        print("\nSAMPLE REJECTED:")
        for r in rejected[:5]:
            print(
                r["pair"],
                r["direction"],
                f"spread={r.get('signal_spread_pct',0):.3f}% "
                f"reason={r['reason']}"
            )


if __name__ == "__main__":
    asyncio.run(_demo())
