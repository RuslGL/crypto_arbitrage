import time
import asyncio
import multiprocessing as mp
import traceback
from datetime import datetime

from src.utils.pairs_normalize import build_normalized_pairs
from src.pipeline.stage_one_price_snapshot_candidates import build_stage_one_snapshot
from src.config import MIN_PROFIT_PCT


# ======================================================================
# Stage-0 — Pairs Normalizer
# ======================================================================

def process_pairs_normalizer(shared):
    try:
        while True:
            try:
                pairs = asyncio.run(build_normalized_pairs())
                shared["pairs"] = pairs
                time.sleep(60)

            except Exception:
                print("[PairsNormalizer][ERROR]")
                traceback.print_exc()
                time.sleep(5)

    finally:
        print("[PairsNormalizer] stopped")


# ======================================================================
# Stage-1 — Spread Snapshot → Signals (producer)
# ======================================================================

def process_stage1_producer(shared, queue):
    try:
        while True:
            try:
                pairs = shared.get("pairs")
                if not pairs:
                    time.sleep(1)
                    continue

                snapshot = asyncio.run(build_stage_one_snapshot(pairs))
                if not snapshot:
                    time.sleep(2)
                    continue

                for pair, v in snapshot.items():
                    direction = v["best_direction"]
                    a, b = direction.split("→")

                    queue.put({
                        "pair": pair,
                        "direction": direction,
                        "spread_pct": v["best_spread_pct"],
                        "buy_exchange": a,
                        "sell_exchange": b,
                        "ts": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    })

                time.sleep(3)

            except Exception:
                print("[Stage1Producer][ERROR]")
                traceback.print_exc()
                time.sleep(2)

    finally:
        print("[Stage1Producer] stopped")


# ======================================================================
# Stage-1 Consumer — prints signals only
# ======================================================================

def process_stage1_consumer(queue):
    try:
        while True:
            sig = queue.get()

            try:
                print(
                    f"[SIGNAL] {sig['pair']} | "
                    f"{sig['buy_exchange']} → {sig['sell_exchange']} | "
                    f"{sig['spread_pct']}% | {sig['direction']}"
                )

            except Exception:
                print("[Stage1Consumer][ERROR] bad signal payload")
                traceback.print_exc()

    finally:
        print("[Stage1Consumer] stopped")


# ======================================================================
# Supervisor
# ======================================================================

def start_process(target, name, *args):
    p = mp.Process(target=target, args=args, name=name, daemon=False)
    p.start()
    return p


if __name__ == "__main__":
    manager = mp.Manager()
    shared = manager.dict()
    queue = manager.Queue()

    processes = {
        "PairsNormalizer": lambda: start_process(
            process_pairs_normalizer, "PairsNormalizer", shared
        ),
        "Stage1Producer": lambda: start_process(
            process_stage1_producer, "Stage1Producer", shared, queue
        ),
        "Stage1Consumer": lambda: start_process(
            process_stage1_consumer, "Stage1Consumer", queue
        ),
    }

    running = {name: starter() for name, starter in processes.items()}

    try:
        while True:
            time.sleep(3)
            for name, starter in processes.items():
                p = running[name]
                if not p.is_alive():
                    print(f"[MAIN][ERROR] {name} died — restarting…")
                    running[name] = starter()

    except KeyboardInterrupt:
        pass

    finally:
        for _, p in running.items():
            if p.is_alive():
                p.terminate()
                p.join()
