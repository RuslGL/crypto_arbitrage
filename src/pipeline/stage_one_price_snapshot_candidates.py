"""
stage_one — Price snapshot + spread candidates (multi-exchange)

• получает bid/ask по биржам
• сопоставляет символы через snapshot Stage-0
• считает спреды в обе стороны
• выбирает лучшее направление
• фильтрует по MIN_PROFIT_PCT
• возвращает кандидатов для Stage-2

Stage-1 НЕ строит список пар.
Пары передаются извне (результат Stage-0).
"""

import asyncio
from typing import Dict, Any

from src.config import MIN_PROFIT_PCT

from src.exchanges.binance.binance_market import fetch_book_tickers_raw
from src.exchanges.bybit.bybit_market import fetch_tickers_raw
from src.exchanges.okx.okx_market import fetch_tickers_raw as okx_fetch_tickers_raw
from src.exchanges.gate.gate_market import fetch_tickers_raw as gate_fetch_tickers_raw
from src.exchanges.kucoin.kucoin_market import fetch_tickers_raw as kucoin_fetch_tickers_raw


# -------------------------------------------------------------------------
# helpers
# -------------------------------------------------------------------------

def _pct(buy: float, sell: float) -> float:
    if buy <= 0:
        return 0.0
    return (sell - buy) / buy * 100.0


def _best_spread(a_bid, a_ask, b_bid, b_ask):
    """
    Возвращает:
      direction  — "A→B" или "B→A"
      best_pct   — максимальный спред
      a2b_pct    — спред A→B
      b2a_pct    — спред B→A
    """
    s_a2b = _pct(a_ask, b_bid)
    s_b2a = _pct(b_ask, a_bid)

    if s_a2b >= s_b2a:
        return "A→B", s_a2b, s_a2b, s_b2a

    return "B→A", s_b2a, s_a2b, s_b2a


# -------------------------------------------------------------------------
# exchange loaders
# -------------------------------------------------------------------------

async def _load_binance() -> Dict[str, Any]:
    raw = await fetch_book_tickers_raw()
    out = {}
    for it in raw:
        s = it.get("symbol")
        if not s:
            continue
        out[s] = {
            "bid": float(it["bidPrice"]),
            "ask": float(it["askPrice"]),
            "bid_size": float(it["bidQty"]),
            "ask_size": float(it["askQty"]),
        }
    return out


async def _load_bybit() -> Dict[str, Any]:
    raw = await fetch_tickers_raw("spot")
    out = {}
    for it in raw:
        s = it.get("symbol")
        if not s:
            continue
        out[s] = {
            "bid": float(it["bid1Price"]),
            "ask": float(it["ask1Price"]),
            "bid_size": float(it["bid1Size"]),
            "ask_size": float(it["ask1Size"]),
        }
    return out


async def _load_okx() -> Dict[str, Any]:
    raw = await okx_fetch_tickers_raw()
    out = {}
    for it in raw:
        s = it.get("instId")
        if not s:
            continue
        out[s] = {
            "bid": float(it["bidPx"]),
            "ask": float(it["askPx"]),
            "bid_size": float(it.get("bidSz", 0) or 0),
            "ask_size": float(it.get("askSz", 0) or 0),
        }
    return out


async def _load_gate() -> Dict[str, Any]:
    """
    Gate.io иногда отдаёт пустые строки — считаем рынок неактивным.
    """
    raw = await gate_fetch_tickers_raw()
    out = {}
    for it in raw:
        s = it.get("currency_pair")
        if not s:
            continue

        bid = it.get("highest_bid")
        ask = it.get("lowest_ask")
        if not bid or not ask:
            continue

        out[s] = {
            "bid": float(bid),
            "ask": float(ask),
            "bid_size": float(it.get("base_volume", 0) or 0),
            "ask_size": float(it.get("quote_volume", 0) or 0),
        }
    return out


async def _load_kucoin() -> Dict[str, Any]:
    """
    KuCoin может отдавать None для неактивных рынков.
    Такие пары пропускаем.
    """
    raw = await kucoin_fetch_tickers_raw()
    out = {}
    for it in raw:
        s = it.get("symbol")
        if not s:
            continue

        bid = it.get("buy")
        ask = it.get("sell")
        if bid is None or ask is None:
            continue

        out[s] = {
            "bid": float(bid),
            "ask": float(ask),
            "bid_size": float(it.get("bestBidSize", 0) or 0),
            "ask_size": float(it.get("bestAskSize", 0) or 0),
        }
    return out


# -------------------------------------------------------------------------
# main pipeline
# -------------------------------------------------------------------------

async def build_stage_one_snapshot(
    pairs: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:

    if not pairs:
        return {}

    binance, bybit, okx, gate, kucoin = await asyncio.gather(
        _load_binance(),
        _load_bybit(),
        _load_okx(),
        _load_gate(),
        _load_kucoin(),
    )

    result: Dict[str, Any] = {}

    exchanges = [
        ("binance", binance),
        ("bybit",   bybit),
        ("okx",     okx),
        ("gate",    gate),
        ("kucoin",  kucoin),
    ]

    for key, mapping in pairs.items():

        present = [
            (name, mapping.get(name), book)
            for name, book in exchanges
            if mapping.get(name) and mapping.get(name) in book
        ]

        if len(present) < 2:
            continue

        best = None

        for i in range(len(present)):
            for j in range(i + 1, len(present)):
                a_name, a_sym, a_book = present[i]
                b_name, b_sym, b_book = present[j]

                a = a_book[a_sym]
                b = b_book[b_sym]

                direction, best_pct, a2b, b2a = _best_spread(
                    a["bid"], a["ask"], b["bid"], b["ask"]
                )

                if best_pct < MIN_PROFIT_PCT:
                    continue

                candidate = {
                    "a": a_name,
                    "b": b_name,
                    "a_prices": a,
                    "b_prices": b,
                    "spread_a2b_pct": round(a2b, 4),
                    "spread_b2a_pct": round(b2a, 4),
                    "best_direction": direction.replace("A", a_name).replace("B", b_name),
                    "best_spread_pct": round(best_pct, 4),
                }

                if (
                    not best
                    or candidate["best_spread_pct"] > best["best_spread_pct"]
                ):
                    best = candidate

        if best:
            result[key] = best

    return result


# -------------------------------------------------------------------------
# Local demo — Stage-0 → Stage-1 simulation
# -------------------------------------------------------------------------

async def _demo():
    try:
        from src.pipeline.stage_zero_pairs_normalize import build_pairs_snapshot

        pairs = await build_pairs_snapshot()
        snapshot = await build_stage_one_snapshot(pairs)

        # выводим только сигналы
        for k, v in snapshot.items():
            print(f"{k:<12} {v['best_direction']} {v['best_spread_pct']}%")

    except Exception as e:
        print("[Stage-1 DEMO ERROR]", e)


if __name__ == "__main__":
    asyncio.run(_demo())
