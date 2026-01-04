"""
PairsNormalize — pandas версия.
"""

import asyncio
import time
import pandas as pd

from src.config import MIN_24H_VOLUME_USDT
from src.exchanges.binance.binance_market import fetch_tickers_24h_raw
from src.exchanges.bybit.bybit_market import fetch_tickers_raw


def _normalize_to_usdt(symbol: str) -> str | None:
    if not symbol:
        return None
    s = symbol.replace("-", "").replace("_", "")
    if not s.endswith("USDT"):
        return None
    return f"{s[:-4]}_USDT"


async def _fetch_all():
    return await asyncio.gather(
        fetch_tickers_24h_raw(),
        fetch_tickers_raw("spot"),
    )


def _df_binance(items: list) -> pd.DataFrame:
    df = pd.DataFrame(items)
    df["volume"] = pd.to_numeric(df.get("quoteVolume"), errors="coerce").fillna(0.0)
    df = df[df["volume"] >= MIN_24H_VOLUME_USDT]
    df["normalized"] = df["symbol"].map(_normalize_to_usdt)
    df = df[df["normalized"].notna()]
    return df[["normalized", "symbol"]].rename(columns={"symbol": "binance"})


def _df_bybit(items: list) -> pd.DataFrame:
    df = pd.DataFrame(items)
    df["volume"] = pd.to_numeric(df.get("turnover24h"), errors="coerce").fillna(0.0)
    df = df[df["volume"] >= MIN_24H_VOLUME_USDT]
    df["normalized"] = df["symbol"].map(_normalize_to_usdt)
    df = df[df["normalized"].notna()]
    return df[["normalized", "symbol"]].rename(columns={"symbol": "bybit"})


async def build_normalized_pairs_pandas() -> pd.DataFrame:
    t0 = time.perf_counter()
    print("\n[normalize:pandas] fetching 24h tickers...")

    binance_raw, bybit_raw = await _fetch_all()

    df_b = _df_binance(binance_raw)
    df_y = _df_bybit(bybit_raw)

    df = pd.merge(df_b, df_y, on="normalized", how="outer")
    df = df.sort_values("normalized").reset_index(drop=True)

    dt = (time.perf_counter() - t0) * 1000
    print(f"[normalize:pandas] total normalized pairs: {len(df)}")
    print(f"[normalize:pandas] time: {dt:.2f} ms\n")

    return df


async def _demo():
    await build_normalized_pairs_pandas()


if __name__ == "__main__":
    asyncio.run(_demo())
