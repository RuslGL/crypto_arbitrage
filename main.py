import time
import os
import csv
import asyncio
import datetime
import multiprocessing as mp
import traceback

from src.utils.pairs_normalize import build_normalized_pairs
from src.utils.stage1_price_snapshot import build_stage1_snapshot


# ======================================================================
# logging for price spread signals
# ======================================================================

LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "price_spread_signals.csv")


def _ensure_log_file():
    os.makedirs(LOG_DIR, exist_ok=True)

    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "ts_utc",
                "pair",
                "direction",
                "buy_exchange",
                "sell_exchange",
                "buy_price",
                "sell_price",
                "spread_pct"
            ])


def _append_signal_log(row: list):
    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(row)


# ======================================================================
# Process 1 — Pairs Normalizer
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
                    import asyncio
                    pairs = asyncio.run(build_normalized_pairs())

                shared["pairs"] = pairs

                print(f"[PairsNormalizer] snapshot updated "
                      f"({len(pairs)} pairs)")

                time.sleep(60)

            except Exception as e:
                print("[PairsNormalizer] ERROR:", e)
                traceback.print_exc()
                time.sleep(5)

    finally:
        print("[PairsNormalizer] process stopped")


# ======================================================================
# Process 2 — Price Spread Watcher (signals → log only)
# ======================================================================

def process_price_spread_watcher(shared):
    print("[PriceSpreadWatcher] process started")

    _ensure_log_file()

    try:
        while True:
            try:
                pairs = shared.get("pairs", {})

                if not pairs:
                    time.sleep(2)
                    continue

                print(f"[PriceSpreadWatcher] pairs loaded from shared: "
                      f"{len(pairs)} items")

                candidates = asyncio.run(build_stage1_snapshot(pairs))

                print(f"[PriceSpreadWatcher] spread candidates: "
                      f"{len(candidates)}")

                if not candidates:
                    time.sleep(5)
                    continue

                for key, v in candidates.items():
                    side = v["best_direction"]

                    if side == "B→Y":
                        buy_ex  = "BINANCE"
                        sell_ex = "BYBIT"
                        buy_px  = v["binance"]["ask"]
                        sell_px = v["bybit"]["bid"]
                    else:
                        buy_ex  = "BYBIT"
                        sell_ex = "BINANCE"
                        buy_px  = v["bybit"]["ask"]
                        sell_px = v["binance"]["bid"]

                    spread = v["best_spread_pct"]

                    # дата+время UTC
                    ts = datetime.datetime.utcnow().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )

                    # ---- сохраняем только в лог ----
                    _append_signal_log([
                        ts,
                        key,
                        side,
                        buy_ex,
                        sell_ex,
                        buy_px,
                        sell_px,
                        spread
                    ])

                time.sleep(5)

            except Exception as e:
                print("[PriceSpreadWatcher] ERROR:", e)
                traceback.print_exc()
                time.sleep(2)

    finally:
        print("[PriceSpreadWatcher] process stopped")


# ======================================================================
# Supervisor
# ======================================================================

def start_process(target, name, shared):
    p = mp.Process(target=target, args=(shared,), name=name, daemon=False)
    p.start()
    print(f"[MAIN] started {name} (pid={p.pid})")
    return p


if __name__ == "__main__":
    print("[MAIN] supervisor starting...")

    manager = mp.Manager()
    shared = manager.dict()

    processes = {
        "PairsNormalizer": lambda: start_process(
            process_pairs_normalizer, "PairsNormalizer", shared
        ),
        "PriceSpreadWatcher": lambda: start_process(
            process_price_spread_watcher, "PriceSpreadWatcher", shared
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
