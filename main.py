import time
import os
import csv
import asyncio
import datetime
import multiprocessing as mp
import traceback

from src.utils.pairs_normalize import build_normalized_pairs
from src.utils.stage1_price_snapshot import build_stage1_snapshot

from src.exchanges.binance.binance_market import fetch_orderbook_raw as fetch_binance_orderbook_raw
from src.exchanges.bybit.bybit_market import fetch_orderbook_raw as fetch_bybit_orderbook_raw

from src.config import (
    MIN_PROFIT_PCT,
    MIN_EXECUTION_NOTIONAL_USDT,
    MAX_BOOK_DEPTH_LEVELS,
)

# --- NEW: init DB on startup ---
from src.transfers.storage.db import init_db


# ======================================================================
# logging subsystem
#
# Логируем два типа событий:
#
# 1) SPREAD_LOG — сигналы Stage-1 (арифметический спред по тикерам)
#    здесь ещё НЕ проверен стакан и не доказана исполнимость
#
# 2) TRADE_LOG — сигналы Stage-2 (после проверки стакана)
#    это кандидаты, где есть нужный объём и спред сохраняется
#
# Эти логи — база для аналитики и последующей эмуляции стратегии.
# ======================================================================

LOG_DIR = "logs"

SPREAD_LOG = os.path.join(LOG_DIR, "price_spread_signals.csv")
TRADE_LOG  = os.path.join(LOG_DIR, "trade_confirmed_signals.csv")


def _ensure_logs():
    """
    Создаёт директорию logs и CSV-файлы,
    если они ещё не существуют.
    """
    os.makedirs(LOG_DIR, exist_ok=True)

    if not os.path.exists(SPREAD_LOG):
        with open(SPREAD_LOG, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "ts_utc","pair","direction",
                "buy_exchange","sell_exchange",
                "buy_price","sell_price","spread_pct"
            ])

    if not os.path.exists(TRADE_LOG):
        with open(TRADE_LOG, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "ts_utc","pair","direction",
                "buy_exchange","sell_exchange",
                "exec_notional_usdt",
                "exec_buy_price","exec_sell_price",
                "exec_spread_pct",
            ])


def _append_row(path, row):
    with open(path, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(row)


# ======================================================================
# helpers — расчёт исполнимой средней цены на заданный объём
#
# На вход подаются уровни стакана:
#   levels = [(price, qty), ...]
#
# Мы идём по уровням сверху вниз и считаем,
# сможем ли набрать объём MIN_EXECUTION_NOTIONAL_USDT
# в пределах MAX_BOOK_DEPTH_LEVELS.
#
# Возвращает средневзвешенную цену исполнения
# или None, если объёма не хватает.
# ======================================================================

def _calc_exec_price(levels, want_notional, max_levels):
    filled = 0.0
    cost = 0.0

    for price, qty in levels[:max_levels]:
        price = float(price)
        qty   = float(qty)

        level_notional = price * qty
        take = min(level_notional, want_notional - filled)

        cost += price * (take / price)
        filled += take

        if filled >= want_notional:
            break

    if filled < want_notional:
        return None

    return cost / (filled / price)


# ======================================================================
# Process 1 — Pairs Normalizer
#
# Задача процесса:
#
# 1) асинхронно опрашивает биржи по 24h-тикерам
# 2) нормализует список торговых пар:
#       • формат BASE_USDT
#       • отбраковка по обороту
#       • сопоставление между биржами
# 3) кладёт результат в shared-dict
#
# Это «источник правды» по тому,
# какие пары вообще существуют и торгуются.
#
# обновление выполняется раз в минуту
# ======================================================================

def process_pairs_normalizer(shared):
    print("[PairsNormalizer] process started")

    try:
        while True:
            try:
                print("[PairsNormalizer] building normalized pairs...")

                try:
                    ctx = mp.get_context()
                    pairs = ctx.run(build_normalized_pairs)
                except AttributeError:
                    pairs = asyncio.run(build_normalized_pairs())

                shared["pairs"] = pairs
                print(f"[PairsNormalizer] snapshot updated ({len(pairs)} pairs)")
                time.sleep(60)

            except Exception as e:
                print("[PairsNormalizer] ERROR:", e)
                traceback.print_exc()
                time.sleep(5)

    finally:
        print("[PairsNormalizer] process stopped")


# ======================================================================
# Process 2 — Price Spread Watcher (Stage-1)
#
# Роль процесса:
#
# 1) читает нормализованные пары из shared-dict
# 2) строит «ценовой срез рынка» (bid/ask с обеих бирж)
# 3) считает арифметический спред
# 4) отбирает пары, где спред >= MIN_PROFIT_PCT
# 5) пишет сигнал в SPREAD_LOG
# 6) публикует сигнал в очередь → TradeWorker
#
# ВАЖНО:
#   стакан ещё НЕ проверяется
#   это предварительные кандидаты
# ======================================================================

def process_price_spread_watcher(shared, queue):
    print("[PriceSpreadWatcher] process started")
    _ensure_logs()

    try:
        while True:
            try:
                pairs = shared.get("pairs", {})
                if not pairs:
                    time.sleep(2)
                    continue

                candidates = asyncio.run(build_stage1_snapshot(pairs))
                if not candidates:
                    time.sleep(5)
                    continue

                for key, v in candidates.items():
                    side = v["best_direction"]

                    if side == "B→Y":
                        buy_ex, sell_ex = "BINANCE", "BYBIT"
                        buy_px  = v["binance"]["ask"]
                        sell_px = v["bybit"]["bid"]
                    else:
                        buy_ex, sell_ex = "BYBIT", "BINANCE"
                        buy_px  = v["bybit"]["ask"]
                        sell_px = v["binance"]["bid"]

                    spread = v["best_spread_pct"]
                    ts = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

                    _append_row(SPREAD_LOG,
                        [ts,key,side,buy_ex,sell_ex,buy_px,sell_px,spread]
                    )

                    queue.put({
                        "ts": ts,
                        "pair": key,
                        "direction": side,
                        "buy_exchange": buy_ex,
                        "sell_exchange": sell_ex,
                    })

                time.sleep(5)

            except Exception as e:
                print("[PriceSpreadWatcher] ERROR:", e)
                traceback.print_exc()
                time.sleep(2)

    finally:
        print("[PriceSpreadWatcher] process stopped")


# ======================================================================
# Process 3 — Trade Worker (Stage-2: depth-check)
#
# Роль процесса:
#
# 1) получает сигнал из очереди
# 2) запрашивает стаканы обеих бирж по паре
# 3) проверяет, можно ли исполнить
#       MIN_EXECUTION_NOTIONAL_USDT
#    в пределах MAX_BOOK_DEPTH_LEVELS
# 4) пересчитывает «реальный» спред по стакану
#
# если глубины не хватает → сигнал отклоняется
# если спред пропал       → сигнал отклоняется
# если всё ок             → сигнал подтверждён
#
# ТОРГОВЛЮ ПОКА НЕ ВЫПОЛНЯЕМ
# только логируем в TRADE_LOG
# ======================================================================

async def _handle_signal(sig):
    pair = sig["pair"]
    symbol = pair.replace("_", "")

    ob_b, ob_y = await asyncio.gather(
        fetch_binance_orderbook_raw(symbol),
        fetch_bybit_orderbook_raw(symbol),
    )

    bids_b = ob_b.get("bids", []) or []
    asks_b = ob_b.get("asks", []) or []
    bids_y = ob_y.get("b") or []
    asks_y = ob_y.get("a") or []

    want = MIN_EXECUTION_NOTIONAL_USDT

    if sig["direction"] == "B→Y":
        buy_price  = _calc_exec_price(asks_b, want, MAX_BOOK_DEPTH_LEVELS)
        sell_price = _calc_exec_price(bids_y, want, MAX_BOOK_DEPTH_LEVELS)
    else:
        buy_price  = _calc_exec_price(asks_y, want, MAX_BOOK_DEPTH_LEVELS)
        sell_price = _calc_exec_price(bids_b, want, MAX_BOOK_DEPTH_LEVELS)

    if buy_price is None or sell_price is None:
        print(f"[TradeWorker] {pair} {sig['direction']} → REJECTED — insufficient depth")
        return

    spread_pct = (sell_price - buy_price) / buy_price * 100
    if spread_pct < MIN_PROFIT_PCT:
        print(f"[TradeWorker] {pair} {sig['direction']} → REJECTED — spread vanished")
        return

    print(f"[TradeWorker] CONFIRMED {pair} {sig['direction']} | "
          f"BUY {buy_price:.6f} SELL {sell_price:.6f} "
          f"SPREAD {spread_pct:.3f}%")

    _append_row(TRADE_LOG, [
        sig["ts"], pair, sig["direction"],
        sig["buy_exchange"], sig["sell_exchange"],
        want, buy_price, sell_price, spread_pct
    ])


def process_trade_worker(queue):
    print("[TradeWorker] process started")
    _ensure_logs()

    try:
        while True:
            sig = queue.get()
            try:
                asyncio.run(_handle_signal(sig))
            except Exception as e:
                print("[TradeWorker] ERROR:", e)
                traceback.print_exc()

    finally:
        print("[TradeWorker] process stopped")


# ======================================================================
# Supervisor
#
# Управляющий процесс:
#
# 1) создаёт shared-dict и очередь сообщений
# 2) запускает три рабочих процесса
# 3) следит, чтобы процессы не умирали
#    и автоматически перезапускает их
#
# это мини-оркестратор пайплайна
# ======================================================================

def start_process(target, name, *args):
    p = mp.Process(target=target, args=args, name=name, daemon=False)
    p.start()
    print(f"[MAIN] started {name} (pid={p.pid})")
    return p


if __name__ == "__main__":
    print("[MAIN] supervisor starting...")

    # --- AUTO-INIT DB ON START ---
    asyncio.run(init_db())
    print("[MAIN] DB schema ready")

    manager = mp.Manager()
    shared = manager.dict()
    queue  = manager.Queue()

    processes = {
        "PairsNormalizer": lambda: start_process(
            process_pairs_normalizer, "PairsNormalizer", shared
        ),
        "PriceSpreadWatcher": lambda: start_process(
            process_price_spread_watcher, "PriceSpreadWatcher", shared, queue
        ),
        "TradeWorker": lambda: start_process(
            process_trade_worker, "TradeWorker", queue
        ),
    }

    running = {name: starter() for name, starter in processes.items()}

    try:
        while True:
            time.sleep(5)
            for name, starter in processes.items():
                p = running[name]
                if not p.is_alive():
                    print(f"[MAIN] {name} died — restarting...")
                    running[name] = starter()

    except KeyboardInterrupt:
        print("[MAIN] interrupted, terminating...")

    finally:
        for name, p in running.items():
            if p.is_alive():
                print(f"[MAIN] terminating {name} (pid={p.pid})")
                p.terminate()
                p.join()

        print("[MAIN] supervisor stopped")
