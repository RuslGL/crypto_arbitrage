"""
Project configuration module.

Содержит только статические настройки и константы,
используется импортом, не предназначен для прямого запуска.
"""


# =======================================================================
# --- GLOBAL / CORE SETTINGS --------------------------------------------
# =======================================================================

# Глубина стакана, используемая на Stage-2 (единая для всех бирж)
ORDERBOOK_DEPTH = 10

# Минимальный суточный оборот пары в USDT.
# Если оборот ниже — пара считается отсутствующей на данной бирже.
MIN_24H_VOLUME_USDT = 300_000.0

# Stage-1: Price-spread candidates (по тикерам)
# Порог по спреду в процентах — тестовый «жёсткий».
MIN_PROFIT_PCT = 0.60   #
TARGET_NET_PROFIT_PCT = 0.20 # чистая цель после комиссий и буфера

# Stage-2: Executable liquidity checks
MIN_EXECUTION_NOTIONAL_USDT = 500.0
MAX_BOOK_DEPTH_LEVELS = 10
MAX_SLIPPAGE_PCT = 0.20   # %

# Safety buffer (будет использоваться позже в расчётах)
SAFETY_FEE_BUFFER_PCT = 0.30



# Taker-fee в процентах (консервативные значения)
EXCHANGE_TAKER_FEES = {
    "binance": 0.10,
    "bybit":   0.10,
    "okx":     0.10,
    "kucoin":  0.10,

    # максимально консервативно для Gate.io
    "gate":    0.20,
}



# =======================================================================
# --- BINANCE ------------------------------------------------------------
# =======================================================================

BINANCE_BASE_REST_URL = "https://api.binance.com"
BINANCE_BASE_WS_URL   = "wss://stream.binance.com:9443/ws"

# Тикеры bid/ask по всем парам (основа Stage-1)
BINANCE_BOOK_TICKER_ENDPOINT = "/api/v3/ticker/bookTicker"

# 24h tickers (объёмы + цены, используется на этапе нормализации пар)
BINANCE_TICKERS_ENDPOINT = "/api/v3/ticker/24hr"

# Orderbook (Stage-2 depth-check)
BINANCE_ORDERBOOK_ENDPOINT = "/api/v3/depth"

# Fees / networks / withdrawals
BINANCE_FEES_ENDPOINT = "/sapi/v1/asset/tradeFee"
BINANCE_COIN_INFO_ENDPOINT = "/sapi/v1/capital/config/getall"



# =======================================================================
# --- BYBIT --------------------------------------------------------------
# =======================================================================

BYBIT_BASE_REST_URL = "https://api.bybit.com"
BYBIT_BASE_WS_URL   = "wss://stream.bybit.com/v5/public/spot"

# 24h tickers (turnover24h / volume)
BYBIT_TICKERS_ENDPOINT = "/v5/market/tickers"

# Orderbook (Stage-2 depth-check)
BYBIT_ORDERBOOK_ENDPOINT = "/v5/market/orderbook"

# Fees / networks / withdrawals
BYBIT_FEES_ENDPOINT = "/v5/account/fee-rate"
BYBIT_COIN_INFO_ENDPOINT = "/v5/asset/coin/query-info"



# =======================================================================
# --- OKX ----------------------------------------------------------------
# =======================================================================

OKX_BASE_REST_URL = "https://www.okx.com"
OKX_BASE_WS_URL   = "wss://ws.okx.com:8443/ws/v5/public"

# Тикеры по всем spot-парам (?instType=SPOT)
OKX_TICKERS_ENDPOINT = "/api/v5/market/tickers"

# Orderbook (Stage-2 depth-check)
OKX_ORDERBOOK_ENDPOINT = "/api/v5/market/books"



# =======================================================================
# --- GATE.IO ------------------------------------------------------------
# =======================================================================

GATE_BASE_REST_URL = "https://api.gateio.ws/api/v4"
GATE_BASE_WS_URL   = "wss://api.gateio.ws/ws/v4"

# Тикеры bid/ask + 24h статистика
GATE_TICKERS_ENDPOINT = "/spot/tickers"

# Orderbook (Stage-2 depth-check)
GATE_ORDERBOOK_ENDPOINT = "/spot/order_book"



# =======================================================================
# --- KUCOIN -------------------------------------------------------------
# =======================================================================

KUCOIN_BASE_REST_URL = "https://api.kucoin.com"
KUCOIN_BASE_WS_URL   = "wss://ws-api.kucoin.com/endpoint"

# Тикеры по всем парам (best bid/ask + 24h)
KUCOIN_TICKERS_ENDPOINT = "/api/v1/market/allTickers"

# Orderbook (Stage-2 depth-check)
KUCOIN_ORDERBOOK_ENDPOINT = "/api/v1/market/orderbook/level2_20"
