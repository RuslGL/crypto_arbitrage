# src/transfers/collectors/binance/fees_networks_binance.py

import os
import time
import hmac
import hashlib
import asyncio
import urllib.parse

import httpx

from src.config import (
    BINANCE_BASE_REST_URL,
    BINANCE_COIN_INFO_ENDPOINT,
    BINANCE_FEES_ENDPOINT,
)

# ----------------------------------------------------------------------
# Настройки доступа (берём из ENV)
# ----------------------------------------------------------------------

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")


def _require_keys():
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        raise RuntimeError(
            "BINANCE_API_KEY / BINANCE_API_SECRET не заданы в окружении "
            "(нужны для signed-запросов к Binance /sapi/v1/...)"
        )


# ----------------------------------------------------------------------
# Вспомогательные функции для подписанных запросов
# ----------------------------------------------------------------------

def _sign_params(params: dict | None = None) -> dict:
    """
    Добавляет timestamp и signature к параметрам запроса.
    """
    _require_keys()

    if params is None:
        params = {}

    params = dict(params)
    params["timestamp"] = int(time.time() * 1000)

    query = urllib.parse.urlencode(params, doseq=True)
    signature = hmac.new(
        BINANCE_API_SECRET.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    params["signature"] = signature
    return params


async def _signed_get(endpoint: str, params: dict | None = None) -> dict:
    """
    Общая обёртка для signed GET-запросов к Binance /sapi.
    """
    _require_keys()

    url = f"{BINANCE_BASE_REST_URL}{endpoint}"
    signed_params = _sign_params(params)

    headers = {
        "X-MBX-APIKEY": BINANCE_API_KEY,
    }

    async with httpx.AsyncClient() as client:
        resp = await client.get(url, params=signed_params, headers=headers, timeout=10.0)
        resp.raise_for_status()
        return resp.json()


# ----------------------------------------------------------------------
# Сырые данные по монетам / сетям
# ----------------------------------------------------------------------

async def fetch_binance_coin_info_raw() -> list:
    """
    Сырый ответ Binance по монетам и сетям:
    /sapi/v1/capital/config/getall

    Здесь лежит информация о:
      - каких монетах;
      - на каких сетях они доступны;
      - депозит/вывод включён/выключен;
      - минимальные суммы;
      - комиссии вывода и т.п.
    """
    data = await _signed_get(BINANCE_COIN_INFO_ENDPOINT)
    # Binance возвращает список объектов
    return data if isinstance(data, list) else []


# ----------------------------------------------------------------------
# Сырые данные по торговым комиссиям
# ----------------------------------------------------------------------

async def fetch_binance_trade_fees_raw(symbol: str | None = None) -> list:
    """
    Сырые торговые комиссии:
    /sapi/v1/asset/tradeFee

    Если symbol передан — фильтруем только по этому символу,
    иначе Binance может вернуть комиссии по всем доступным символам.
    """
    params: dict = {}
    if symbol:
        params["symbol"] = symbol

    data = await _signed_get(BINANCE_FEES_ENDPOINT, params=params)
    # Обычно это список с объектами вида {"symbol": "...", "makerCommission", "takerCommission", ...}
    return data if isinstance(data, list) else []


# ----------------------------------------------------------------------
# Demo
# ----------------------------------------------------------------------

async def _demo():
    print("[demo] fetching BINANCE coin info (networks)...")
    coins = await fetch_binance_coin_info_raw()
    print(f"[demo] coins: {len(coins)}")

    for item in coins[:2]:
        print(item)

    print("\n[demo] fetching BINANCE trade fees...")
    fees = await fetch_binance_trade_fees_raw()
    print(f"[demo] fee items: {len(fees)}")

    for item in fees[:2]:
        print(item)


if __name__ == "__main__":
    asyncio.run(_demo())
