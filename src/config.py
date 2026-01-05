"""
Project configuration module.

Содержит только статические настройки и константы,
используется импортом, не предназначен для прямого запуска.
"""


# --- Binance --------------------------------------------------------------

BINANCE_BASE_REST_URL = "https://api.binance.com"
BINANCE_BASE_WS_URL   = "wss://stream.binance.com:9443/ws"

# Тикеры bid/ask по всем парам (основа Stage-1)
BINANCE_BOOK_TICKER_ENDPOINT = "/api/v3/ticker/bookTicker"

# 24h tickers (объёмы + цены, используется на этапе нормализации пар)
BINANCE_TICKERS_ENDPOINT = "/api/v3/ticker/24hr"

# Orderbook (используется на Stage-2 depth-check)
BINANCE_ORDERBOOK_ENDPOINT = "/api/v3/depth"

# Глубина стакана по умолчанию при запросе к API
BINANCE_ORDERBOOK_DEPTH = 10


# --- Binance — fees / networks / withdrawals ------------------------------

# Торговые комиссии по рынкам
BINANCE_FEES_ENDPOINT = "/sapi/v1/asset/tradeFee"

# Информация по монетам:
# сети, минималки, депозиты / выводы, комиссии вывода
BINANCE_COIN_INFO_ENDPOINT = "/sapi/v1/capital/config/getall"


# --- Bybit -----------------------------------------------------------------

BYBIT_BASE_REST_URL = "https://api.bybit.com"
BYBIT_BASE_WS_URL   = "wss://stream.bybit.com/v5/public/spot"

# 24h tickers (turnover24h / volume)
BYBIT_TICKERS_ENDPOINT = "/v5/market/tickers"

# Orderbook (используется на Stage-2 depth-check)
BYBIT_ORDERBOOK_ENDPOINT = "/v5/market/orderbook"

# Глубина стакана по умолчанию при запросе к API
BYBIT_ORDERBOOK_DEPTH = 10


# --- Bybit — fees / networks / withdrawals --------------------------------

# Торговые комиссии по рынкам / символам
BYBIT_FEES_ENDPOINT = "/v5/account/fee-rate"

# Информация по монетам и сетям:
# депозиты, выводы, минималки, комиссии, поддерживаемые сети
BYBIT_COIN_INFO_ENDPOINT = "/v5/asset/coin/query-info"


# --- Liquidity pre-filter (нормализация пар) ------------------------------

# Минимальный суточный оборот пары в USDT.
# Если оборот ниже — пара считается отсутствующей на данной бирже.
MIN_24H_VOLUME_USDT = 300_000.0


# --- Stage-1: Price-spread candidates (по тикерам) ------------------------

# Порог по спреду в процентах — тестовый «жёсткий»,
# чтобы собирать только явные аномалии.
# Если spread_pct >= MIN_PROFIT_PCT → пара становится кандидатом.
MIN_PROFIT_PCT = 0.60   # %


# --- Stage-2: Depth-check / executable liquidity --------------------------

# Минимальный объём, который должен быть реально исполним в стакане
# на стороне входа (в денежном выражении, USDT).
MIN_EXECUTION_NOTIONAL_USDT = 500.0

# Максимальное количество уровней стакана,
# в пределах которых мы допускаем набор объёма.
MAX_BOOK_DEPTH_LEVELS = 10

# Допустимое ухудшение средней цены при частичном исполнении (%).
# Ограничение на проскальзывание относительно цены входа.
MAX_SLIPPAGE_PCT = 0.20


# --- Safety buffer (будет использоваться в расчётах позже) ---------------

# Резерв под комиссии, сетевые задержки и неопределённость.
SAFETY_FEE_BUFFER_PCT = 0.30
