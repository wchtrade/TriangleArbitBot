import asyncio
import aiohttp
import logging
import os
import time
import json
import uuid
import hmac
import hashlib
import base64
import math
import urllib.parse
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# =====================================================================
# WorkerArbBot v2 — переписан с упором на ОДНУ честную формулу порога
# входа вместо трёх спорящих друг с другом (старая версия имела
# компute_dynamic_min_profit_pct + мягкий $ фильтр + strict_honest_gate
# с ПОЛНОЙ, неамортизированной стоимостью — из-за чего реальный порог
# входа был в 2 раза выше, чем показывал /stats, и бот почти не торговал).
#
# Убрано относительно предыдущей версии (сознательно, чтобы уменьшить
# число мест, где логика может незаметно разъехаться):
#   - треугольный арбитраж
#   - KuCoin HF-эксперимент
#   - DEX-проверки цены
#   - режим реального перевода монеты между биржами (был выключен по
#     умолчанию и ни разу не использовался в бою)
#   - HTX (подтверждённо зависающий тикер, см. историю проекта)
#
# Сохранено без изменений по сути (это было проверено и работало
# правильно): WebSocket-стаканы Binance/KuCoin, walk-the-book, честное
# подтверждение исполнения ордера (не верим HTTP 200, ждём FILLED),
# округление количества под шаг лота биржи, лимитные IOC-ордера.
# =====================================================================

TG_TOKEN = os.environ.get("ARB_BOT_TOKEN", "")
CHAT_ID = None

CONFIRM_PHRASE = "YES-I-UNDERSTAND-THE-RISK"
REAL_TRADING_UNLOCKED = os.environ.get("REAL_TRADING_UNLOCKED", "")

BINANCE_KEY    = os.environ.get("BINANCE_API_KEY", "")
BINANCE_SECRET = os.environ.get("BINANCE_API_SECRET", "")
KUCOIN_KEY     = os.environ.get("KUCOIN_API_KEY", "")
KUCOIN_SECRET  = os.environ.get("KUCOIN_API_SECRET", "")
KUCOIN_PASS    = os.environ.get("KUCOIN_PASSPHRASE", "")
MEXC_KEY       = os.environ.get("MEXC_API_KEY", "")
MEXC_SECRET    = os.environ.get("MEXC_API_SECRET", "")

QUOTE = "USDT"
SYMBOLS: List[str] = ["ONE"]          # список торгуемых монет, runtime-изменяемый
DEFAULT_PAIRS: List[Tuple[str, str]] = [("KuCoin", "MEXC")]
PAIR_OVERRIDES: Dict[str, List[Tuple[str, str]]] = {}

def pairs_for_symbol(sym: str) -> List[Tuple[str, str]]:
    return PAIR_OVERRIDES.get(sym, DEFAULT_PAIRS)

FEES = {"Binance": 0.10, "KuCoin": 0.10, "MEXC": 0.10}
MIN_ORDER_VALUE_USD = {"Binance": 5.0, "KuCoin": 1.0, "MEXC": 1.0}

config = {
    "simulation_mode": True,
    "paused": False,
    "trading_active": True,

    # ===== ЭКОНОМИКА ВХОДА — единая, честная формула =====
    # Раньше было ТРИ отдельных фильтра (процентный порог, мягкий $
    # фильтр с амортизацией ÷sell_reserve_lots, и жёсткий strict_gate с
    # ПОЛНОЙ стоимостью) — они считали по-разному и strict_gate тихо
    # требовал вдвое больше, чем показывал /stats. Теперь ОДНА формула:
    #   honest_threshold_pct = buy_fee + sell_fee + crossing_cost_pct
    #                          + safety_margin_pct + erosion_pct
    # без деления crossing_cost на lots — амортизация подменяла
    # реальную стоимость заниженной цифрой, это и было корнем проблемы.
    "empirical_spread_crossing_pct": 1.5,   # начни отсюда, откалибруй заново
                                              # по 15-20 свежим сделкам — НЕ
                                              # тащи старое значение с другого
                                              # проекта/даты, оно устарело
    "threshold_safety_margin_pct": 0.05,
    "max_total_threshold_pct": 5.0,          # общий потолок, что бы ни случилось
    "min_absolute_profit_usd": 0.15,         # доллар-фильтр — отдельная, НЕ
                                              # дублирующая проверка

    "trade_usdt": 20.0,                      # лот СИМУЛЯЦИИ
    "max_real_order_usdt": 15.0,             # лот РЕАЛЬНОГО ордера, жёсткий потолок
    "max_real_lot_ceiling": 15.0,            # можно поднять командой /setlotceiling,
                                              # но осознанно — см. экономику ниже

    "scan_interval": 3,
    "max_trades_per_min": 6,
    "max_real_trades_per_day": 200,
    "real_trades_today": 0,
    "day_start": datetime.now().strftime("%Y-%m-%d"),

    "min_depth_levels_required": 10,
    "max_plausible_spread_pct": 5.0,
    "min_volume_usdt": 0,                    # вторичный фильтр, по умолчанию выкл —
                                              # реальная защита это глубина стакана

    "balance_safety_buffer_pct": 1.0,
    "rebalance_headroom_pct": 10.0,
    "sell_reserve_lots": 3,
    "rebalance_target_lots": 1,

    "use_limit_ioc_orders": True,
    "sell_limit_slippage_pct": 0.05,
    "skip_reactive_topup": True,             # не докупать резерв НА критическом
                                              # пути сделки — только фоном (watchdog)
    "reserve_watchdog_interval_sec": 90,
    "reserve_watchdog_trigger_frac": 0.6,
    "max_topup_spend_per_day": 20.0,

    "real_confirmed": False,
    "real_start_capital": None,

    "max_drawdown_pct": 5.0,
    "max_volatility_pct_15min": 5.0,         # поднято с 3.0 — 3% это шум для крипты,
                                              # не аномалия; при 3% бот спамил
                                              # предупреждениями каждые 10-15 минут
    "volatility_hard_pause": False,
    "pre_trade_max_volatility_pct_1min": 0.8,

    "factual_delta_delay_sec": 2.5,
}

stats = {
    "scans": 0, "signals": 0, "trades": 0, "profit_estimate": 0.0,
    "start_time": datetime.now(),
    "trades_this_minute": 0, "minute_start": datetime.now(),
    "depth_fail": {"Binance": 0, "KuCoin": 0, "MEXC": 0},
    "insufficient_liquidity": 0,
    "implausible_spread_rejected": 0,
    "thin_book_rejected": 0,
    "below_threshold_rejected": 0,
    "absolute_profit_too_low_rejected": 0,
    "buy_leg_failures": 0, "sell_leg_failures": 0,
    "emergency_closes_attempted": 0, "emergency_closes_succeeded": 0,
    "topup_attempts": 0, "topup_success": 0, "topup_cost_usdt": 0.0,
    "realized_trading_pnl": 0.0, "realized_trades_count": 0,
    "factual_realized_pnl": 0.0, "factual_trades_count": 0,
}

trade_history: List[dict] = []
execution_erosion_history: List[float] = []
EXECUTION_EROSION_HISTORY_MAXLEN = 10
exchange_backoff_until: Dict[str, float] = {"Binance": 0.0, "KuCoin": 0.0, "MEXC": 0.0}
_last_exchange_error: Dict[str, str] = {"Binance": "", "KuCoin": "", "MEXC": ""}
price_history_by_symbol: Dict[str, List[Tuple[float, float]]] = {}
_capital_measurement_lock = asyncio.Lock()
_htx_unused = None  # placeholder removed feature marker


def record_execution_erosion(net_pct_at_signal: float, factual_delta: float, vol: float) -> None:
    if vol <= 0:
        return
    factual_realized_pct = factual_delta / vol * 100
    gap_pct = net_pct_at_signal - factual_realized_pct
    execution_erosion_history.append(gap_pct)
    if len(execution_erosion_history) > EXECUTION_EROSION_HISTORY_MAXLEN:
        execution_erosion_history.pop(0)


def get_avg_execution_erosion_pct() -> float:
    if len(execution_erosion_history) < 3:
        return 0.3  # консервативная стартовая оценка, НЕ 2.7% как раньше —
                     # то значение было откалибровано по багованному коду
                     # до фикса IOC-ордеров, оно больше не релевантно
    return round(sum(execution_erosion_history) / len(execution_erosion_history), 4)


def is_backed_off(ex: str) -> bool:
    return time.time() < exchange_backoff_until.get(ex, 0.0)


def trigger_backoff(ex: str, status_code: int, retry_after: Optional[str] = None):
    if retry_after:
        try:
            seconds = float(retry_after)
        except ValueError:
            seconds = 120 if status_code == 429 else 300
    else:
        seconds = 120 if status_code == 429 else 300
    exchange_backoff_until[ex] = time.time() + seconds
    logger.error(f"⛔ {ex} вернул {status_code} — заморожен на {seconds:.0f} сек")


def _remember_error(ex: str, detail) -> None:
    text = str(detail)
    if len(text) > 300:
        text = text[:300] + "…"
    _last_exchange_error[ex] = text


# =====================================================================
# ORDER BOOK — REST-фоллбэки (используются напрямую для MEXC, и как
# фоллбэк для Binance/KuCoin пока WS не синхронизирован)
# =====================================================================

async def get_orderbook_binance_rest(session, symbol: str) -> Optional[Dict]:
    if is_backed_off("Binance"):
        return None
    url = "https://api.binance.com/api/v3/depth"
    params = {"symbol": f"{symbol}{QUOTE}", "limit": 100}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=6)) as r:
            if r.status in (429, 418):
                trigger_backoff("Binance", r.status, r.headers.get("Retry-After"))
                return None
            if r.status != 200:
                stats["depth_fail"]["Binance"] += 1
                return None
            data = await r.json()
            bids = [(float(p), float(q)) for p, q in data.get("bids", [])]
            asks = [(float(p), float(q)) for p, q in data.get("asks", [])]
            if not bids or not asks:
                return None
            return {"bids": bids, "asks": asks}
    except Exception as e:
        stats["depth_fail"]["Binance"] += 1
        logger.error(f"Binance depth {symbol}: {e}")
        return None


async def get_orderbook_mexc_rest(session, symbol: str) -> Optional[Dict]:
    if is_backed_off("MEXC"):
        return None
    url = "https://api.mexc.com/api/v3/depth"
    params = {"symbol": f"{symbol}{QUOTE}", "limit": 100}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=6)) as r:
            if r.status in (429, 418):
                trigger_backoff("MEXC", r.status, r.headers.get("Retry-After"))
                return None
            if r.status != 200:
                stats["depth_fail"]["MEXC"] += 1
                return None
            data = await r.json()
            bids = [(float(p), float(q)) for p, q in data.get("bids", [])]
            asks = [(float(p), float(q)) for p, q in data.get("asks", [])]
            if not bids or not asks:
                return None
            return {"bids": bids, "asks": asks}
    except Exception as e:
        stats["depth_fail"]["MEXC"] += 1
        logger.error(f"MEXC depth {symbol}: {e}")
        return None


async def get_orderbook_kucoin_rest(session, symbol: str) -> Optional[Dict]:
    if is_backed_off("KuCoin"):
        return None
    url = "https://api.kucoin.com/api/v1/market/orderbook/level2_20"
    params = {"symbol": f"{symbol}-{QUOTE}"}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=6)) as r:
            if r.status in (429, 418):
                trigger_backoff("KuCoin", r.status, r.headers.get("Retry-After"))
                return None
            if r.status != 200:
                stats["depth_fail"]["KuCoin"] += 1
                return None
            data = (await r.json()).get("data", {})
            bids = [(float(p), float(q)) for p, q in data.get("bids", [])]
            asks = [(float(p), float(q)) for p, q in data.get("asks", [])]
            if not bids or not asks:
                return None
            return {"bids": bids, "asks": asks}
    except Exception as e:
        stats["depth_fail"]["KuCoin"] += 1
        logger.error(f"KuCoin depth {symbol}: {e}")
        return None


# ===== WebSocket локальный стакан Binance (diff-события) =====

WS_BASE_BINANCE = "wss://stream.binance.com:9443/ws"


class BinanceLocalOrderBook:
    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.last_update_id: Optional[int] = None
        self.synced = False
        self.last_event_time = 0.0
        self.resync_count = 0
        self._buffer: List[dict] = []
        self._stop = False

    def get_book(self, depth: int = 100) -> Optional[Dict[str, List[Tuple[float, float]]]]:
        if not self.synced or time.time() - self.last_event_time > 10:
            return None
        bids_sorted = sorted(self.bids.items(), key=lambda x: -x[0])[:depth]
        asks_sorted = sorted(self.asks.items(), key=lambda x: x[0])[:depth]
        if not bids_sorted or not asks_sorted:
            return None
        return {"bids": bids_sorted, "asks": asks_sorted}

    def is_healthy(self) -> bool:
        return self.synced and (time.time() - self.last_event_time) < 90

    def stop(self):
        self._stop = True

    def _apply_level(self, book, price_str, qty_str):
        price, qty = float(price_str), float(qty_str)
        if qty == 0:
            book.pop(price, None)
        else:
            book[price] = qty

    def _apply_event(self, event):
        for p, q in event.get("b", []):
            self._apply_level(self.bids, p, q)
        for p, q in event.get("a", []):
            self._apply_level(self.asks, p, q)
        self.last_update_id = event["u"]
        self.last_event_time = time.time()

    async def _get_snapshot(self, session) -> Optional[dict]:
        try:
            async with session.get("https://api.binance.com/api/v3/depth",
                                    params={"symbol": self.symbol, "limit": 1000},
                                    timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    return None
                return await r.json()
        except Exception as e:
            logger.error(f"{self.symbol} WS snapshot: {e}")
            return None

    async def _resync(self, session):
        self.resync_count += 1
        self.synced = False
        self.bids.clear()
        self.asks.clear()
        snapshot = await self._get_snapshot(session)
        if not snapshot:
            return
        self.last_update_id = snapshot["lastUpdateId"]
        for p, q in snapshot["bids"]:
            self._apply_level(self.bids, p, q)
        for p, q in snapshot["asks"]:
            self._apply_level(self.asks, p, q)
        applied_first = False
        for event in list(self._buffer):
            if event["u"] <= self.last_update_id:
                continue
            if not applied_first:
                if not (event["U"] <= self.last_update_id + 1 <= event["u"]):
                    continue
                applied_first = True
            self._apply_event(event)
        self._buffer.clear()
        if applied_first or self.last_update_id:
            self.synced = True
            self.last_event_time = time.time()
            logger.info(f"{self.symbol} Binance WS синхронизирован (#{self.resync_count})")

    async def run(self, session):
        stream_url = f"{WS_BASE_BINANCE}/{self.symbol.lower()}@depth"
        backoff = 1
        while not self._stop:
            try:
                async with session.ws_connect(stream_url, heartbeat=20) as ws:
                    logger.info(f"{self.symbol} Binance WS подключен")
                    backoff = 1
                    self._buffer.clear()
                    need_snapshot = True
                    async for msg in ws:
                        if self._stop:
                            break
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        event = json.loads(msg.data)
                        if need_snapshot:
                            self._buffer.append(event)
                            if len(self._buffer) >= 2:
                                await self._resync(session)
                                need_snapshot = False
                            continue
                        if self.last_update_id and event["U"] != self.last_update_id + 1:
                            self._buffer = [event]
                            need_snapshot = True
                            continue
                        self._apply_event(event)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"{self.symbol} Binance WS ошибка {e}, реконнект через {backoff}с")
                self.synced = False
            if self._stop:
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


binance_ws_books: Dict[str, BinanceLocalOrderBook] = {}
binance_ws_tasks: Dict[str, asyncio.Task] = {}


def start_binance_ws_book(session, symbol: str):
    if symbol in binance_ws_books:
        return
    book = BinanceLocalOrderBook(f"{symbol}{QUOTE}")
    binance_ws_books[symbol] = book
    binance_ws_tasks[symbol] = asyncio.create_task(book.run(session))


def stop_binance_ws_book(symbol: str):
    book = binance_ws_books.pop(symbol, None)
    task = binance_ws_tasks.pop(symbol, None)
    if book:
        book.stop()
    if task:
        task.cancel()


async def get_orderbook_binance(session, symbol: str) -> Optional[Dict]:
    book = binance_ws_books.get(symbol)
    if book and book.is_healthy():
        snap = book.get_book()
        if snap:
            return snap
    return await get_orderbook_binance_rest(session, symbol)


# ===== WebSocket локальный стакан KuCoin (level2Depth50 снимки) =====

KUCOIN_BULLET_URL = "https://api.kucoin.com/api/v1/bullet-public"


class KuCoinLocalOrderBook:
    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
        self.bids: List[Tuple[float, float]] = []
        self.asks: List[Tuple[float, float]] = []
        self.synced = False
        self.last_event_time = 0.0
        self._stop = False

    def get_book(self, depth: int = 50) -> Optional[Dict]:
        if not self.synced or time.time() - self.last_event_time > 15:
            return None
        if not self.bids or not self.asks:
            return None
        return {"bids": self.bids[:depth], "asks": self.asks[:depth]}

    def is_healthy(self) -> bool:
        return self.synced and (time.time() - self.last_event_time) < 90

    def stop(self):
        self._stop = True

    async def _get_bullet_token(self, session) -> Optional[dict]:
        try:
            async with session.post(KUCOIN_BULLET_URL, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    return None
                data = await r.json()
                if data.get("code") != "200000":
                    return None
                return data["data"]
        except Exception as e:
            logger.error(f"KuCoin bullet-public: {e}")
            return None

    def _apply_snapshot(self, data: dict):
        bids = [(float(p), float(q)) for p, q in data.get("bids", [])]
        asks = [(float(p), float(q)) for p, q in data.get("asks", [])]
        if not bids or not asks:
            return
        self.bids = sorted(bids, key=lambda x: -x[0])
        self.asks = sorted(asks, key=lambda x: x[0])
        self.synced = True
        self.last_event_time = time.time()

    async def _ping_loop(self, ws, interval_ms):
        interval = max(interval_ms / 1000 - 2, 5)
        try:
            while not self._stop:
                await asyncio.sleep(interval)
                await ws.send_json({"id": str(int(time.time() * 1000)), "type": "ping"})
        except (asyncio.CancelledError, ConnectionResetError):
            pass
        except Exception as e:
            logger.error(f"{self.symbol} KuCoin ping: {e}")

    async def run(self, session):
        topic = f"/spotMarket/level2Depth50:{self.symbol}-{QUOTE}"
        backoff = 1
        while not self._stop:
            try:
                bullet = await self._get_bullet_token(session)
                if not bullet or not bullet.get("instanceServers"):
                    raise ConnectionError("no bullet token")
                server = bullet["instanceServers"][0]
                token = bullet["token"]
                connect_id = str(uuid.uuid4())
                ws_url = f"{server['endpoint']}?token={token}&connectId={connect_id}"
                ping_interval = server.get("pingInterval", 18000)
                async with session.ws_connect(ws_url, heartbeat=None) as ws:
                    logger.info(f"{self.symbol} KuCoin WS подключен")
                    backoff = 1
                    welcome = await ws.receive_json(timeout=10)
                    if welcome.get("type") != "welcome":
                        raise ConnectionError(f"no welcome: {welcome}")
                    ping_task = asyncio.create_task(self._ping_loop(ws, ping_interval))
                    await ws.send_json({"id": str(int(time.time() * 1000)), "type": "subscribe",
                                         "topic": topic, "privateChannel": False, "response": True})
                    try:
                        async for msg in ws:
                            if self._stop:
                                break
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                continue
                            data = json.loads(msg.data)
                            if data.get("type") == "message" and data.get("topic") == topic:
                                self._apply_snapshot(data.get("data", {}))
                    finally:
                        ping_task.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"{self.symbol} KuCoin WS ошибка {e}, реконнект через {backoff}с")
                self.synced = False
            if self._stop:
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


kucoin_ws_books: Dict[str, KuCoinLocalOrderBook] = {}
kucoin_ws_tasks: Dict[str, asyncio.Task] = {}


def start_kucoin_ws_book(session, symbol: str):
    if symbol in kucoin_ws_books:
        return
    book = KuCoinLocalOrderBook(symbol)
    kucoin_ws_books[symbol] = book
    kucoin_ws_tasks[symbol] = asyncio.create_task(book.run(session))


def stop_kucoin_ws_book(symbol: str):
    book = kucoin_ws_books.pop(symbol, None)
    task = kucoin_ws_tasks.pop(symbol, None)
    if book:
        book.stop()
    if task:
        task.cancel()


async def get_orderbook_kucoin(session, symbol: str) -> Optional[Dict]:
    book = kucoin_ws_books.get(symbol)
    if book and book.is_healthy():
        snap = book.get_book()
        if snap:
            return snap
    return await get_orderbook_kucoin_rest(session, symbol)


ORDERBOOK_FN = {"Binance": get_orderbook_binance, "KuCoin": get_orderbook_kucoin, "MEXC": get_orderbook_mexc_rest}


# =====================================================================
# WALK THE BOOK
# =====================================================================

def walk_the_book(levels: List[Tuple[float, float]], target_usdt: float) -> Optional[Dict]:
    if not levels:
        return None
    remaining = target_usdt
    total_coins = 0.0
    total_spent = 0.0
    levels_used = 0
    for price, qty in levels:
        if remaining <= 0:
            break
        level_value = price * qty
        levels_used += 1
        if level_value >= remaining:
            coins = remaining / price
            total_coins += coins
            total_spent += remaining
            remaining = 0.0
        else:
            total_coins += qty
            total_spent += level_value
            remaining -= level_value
    if total_coins == 0:
        return None
    return {
        "avg_price": round(total_spent / total_coins, 10),
        "filled_usdt": round(total_spent, 4),
        "coins": round(total_coins, 8),
        "levels_used": levels_used,
        "fully_filled": remaining <= 0.01,
    }


# =====================================================================
# ЕДИНАЯ, ЧЕСТНАЯ ФОРМУЛА ПОРОГА ВХОДА
# =====================================================================

def get_real_dynamic_adjustment_usd() -> float:
    """Двунаправленная поправка на основе РЕАЛЬНОГО факта последних сделок.
    Убыток в среднем -> требуем больше. Прибыль в среднем -> поправка = 0
    (не задирает требование искусственно вверх, но и не занижает его)."""
    history = stats.get("real_factual_history", [])
    if not history:
        return 0.0
    avg = sum(history) / len(history)
    return round(-avg * 1.3, 4) if avg < 0 else 0.0


def true_honest_threshold_pct(buy_ex: str, sell_ex: str) -> float:
    """ОДНА формула вместо трёх. Ничего не амортизируется искусственно —
    если хочешь снизить порог, снижай empirical_spread_crossing_pct на
    основе СВЕЖИХ реальных замеров, а не подменяй его в отдельном месте
    кода другим числом."""
    buy_fee = FEES.get(buy_ex, 0.1)
    sell_fee = FEES.get(sell_ex, 0.1)
    crossing = config.get("empirical_spread_crossing_pct", 1.5)
    safety = config.get("threshold_safety_margin_pct", 0.05)
    erosion = get_avg_execution_erosion_pct()
    raw = buy_fee + sell_fee + crossing + safety + erosion
    return round(min(raw, config.get("max_total_threshold_pct", 5.0)), 4)


def calc_arb_real(symbol: str, buy_ex: str, buy_ob: Dict, sell_ex: str, sell_ob: Dict,
                   trade_usdt: float) -> Optional[dict]:
    buy_fill = walk_the_book(buy_ob["asks"], trade_usdt)
    sell_fill = walk_the_book(sell_ob["bids"], trade_usdt)
    if not buy_fill or not sell_fill:
        return None
    if not buy_fill["fully_filled"] or not sell_fill["fully_filled"]:
        stats["insufficient_liquidity"] += 1
        return None

    buy_price, sell_price = buy_fill["avg_price"], sell_fill["avg_price"]
    if sell_price <= buy_price:
        return None

    buy_fee_frac = FEES.get(buy_ex, 0.1) / 100
    sell_fee_frac = FEES.get(sell_ex, 0.1) / 100
    gross = (sell_price - buy_price) / buy_price * 100
    net = gross - buy_fee_frac * 100 - sell_fee_frac * 100

    threshold = true_honest_threshold_pct(buy_ex, sell_ex)
    if net < threshold:
        stats["below_threshold_rejected"] = stats.get("below_threshold_rejected", 0) + 1
        return None

    if gross > config["max_plausible_spread_pct"]:
        stats["implausible_spread_rejected"] += 1
        return None

    min_levels = config["min_depth_levels_required"]
    if len(buy_ob["asks"]) < min_levels or len(sell_ob["bids"]) < min_levels:
        stats["thin_book_rejected"] += 1
        return None

    coins = trade_usdt / buy_price
    profit = coins * sell_price * (1 - sell_fee_frac) - trade_usdt * (1 + buy_fee_frac)

    min_abs = config.get("min_absolute_profit_usd", 0.15) + get_real_dynamic_adjustment_usd()
    if profit < min_abs:
        stats["absolute_profit_too_low_rejected"] += 1
        return None

    return {
        "symbol": symbol, "buy_ex": buy_ex, "sell_ex": sell_ex,
        "buy_price": round(buy_price, 8), "sell_price": round(sell_price, 8),
        "gross_pct": round(gross, 4), "net_pct": round(net, 4),
        "threshold_pct": threshold,
        "profit_usdt": round(profit, 4), "coins": round(coins, 6), "vol": trade_usdt,
        "levels_used_buy": buy_fill["levels_used"], "levels_used_sell": sell_fill["levels_used"],
        "time": datetime.now().strftime("%H:%M:%S"),
    }


async def fetch_all_orderbooks(session) -> Tuple[Dict, List[str]]:
    fn_map = ORDERBOOK_FN
    tasks = {}
    for sym in SYMBOLS:
        needed = {ex for pair in pairs_for_symbol(sym) for ex in pair}
        for ex in needed:
            if ex in fn_map:
                tasks[(ex, sym)] = fn_map[ex](session, sym)
    keys = list(tasks.keys())
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)
    books: Dict[str, Dict] = {"Binance": {}, "KuCoin": {}, "MEXC": {}}
    for (ex, sym), res in zip(keys, results):
        if isinstance(res, Exception) or res is None:
            continue
        books[ex][sym] = res
    active = [ex for ex, d in books.items() if d]
    return books, active


async def scan_all(session) -> Tuple[List[dict], List[str]]:
    stats["scans"] += 1
    books, active = await fetch_all_orderbooks(session)
    signals = []
    scan_lot = config["max_real_order_usdt"] if not config["simulation_mode"] else config["trade_usdt"]
    for sym in SYMBOLS:
        for buy_ex, sell_ex in pairs_for_symbol(sym):
            bob = books.get(buy_ex, {}).get(sym)
            sob = books.get(sell_ex, {}).get(sym)
            if not bob or not sob:
                continue
            if sob.get("bids"):
                price_history_by_symbol.setdefault(sym, []).append((time.time(), sob["bids"][0][0]))
            opp = calc_arb_real(sym, buy_ex, bob, sell_ex, sob, scan_lot)
            if opp:
                signals.append(opp)
    signals.sort(key=lambda x: x["net_pct"], reverse=True)
    if signals:
        stats["signals"] += len(signals)
    return signals, active


def get_recent_price_volatility_pct(minutes: int, symbol: Optional[str] = None) -> Optional[float]:
    symbols = [symbol] if symbol else list(price_history_by_symbol.keys())
    if not symbols:
        return None
    now_ts = time.time()
    cutoff = now_ts - minutes * 60
    max_vol = None
    for sym in symbols:
        hist = price_history_by_symbol.get(sym, [])
        recent = [p for ts, p in hist if ts >= cutoff]
        if len(recent) < 2:
            continue
        lo, hi = min(recent), max(recent)
        if lo <= 0:
            continue
        vol = round((hi - lo) / lo * 100, 3)
        if max_vol is None or vol > max_vol:
            max_vol = vol
    return max_vol


# =====================================================================
# ОКРУГЛЕНИЕ ПОД ПРАВИЛА БИРЖИ
# =====================================================================

_binance_lot_step_cache: Dict[str, float] = {}
_kucoin_increment_cache: Dict[str, float] = {}
_mexc_lot_step_cache: Dict[str, float] = {}
_mexc_tick_size_cache: Dict[str, float] = {}


async def get_binance_lot_step(session, symbol: str) -> float:
    if symbol in _binance_lot_step_cache:
        return _binance_lot_step_cache[symbol]
    try:
        async with session.get("https://api.binance.com/api/v3/exchangeInfo",
                                params={"symbol": f"{symbol}{QUOTE}"},
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json()
            for s in data.get("symbols", []):
                for f in s.get("filters", []):
                    if f["filterType"] == "LOT_SIZE":
                        step = float(f["stepSize"])
                        _binance_lot_step_cache[symbol] = step
                        return step
    except Exception as e:
        logger.error(f"Binance lot step {symbol}: {e}")
    return 1.0


async def get_mexc_lot_step(session, symbol: str) -> float:
    if symbol in _mexc_lot_step_cache:
        return _mexc_lot_step_cache[symbol]
    try:
        async with session.get("https://api.mexc.com/api/v3/exchangeInfo",
                                params={"symbol": f"{symbol}{QUOTE}"},
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json()
            for s in data.get("symbols", []):
                for f in s.get("filters", []):
                    if f["filterType"] == "LOT_SIZE":
                        step = float(f["stepSize"])
                        _mexc_lot_step_cache[symbol] = step
                        return step
    except Exception as e:
        logger.error(f"MEXC lot step {symbol}: {e}")
    return 1.0


async def get_mexc_tick_size(session, symbol: str) -> float:
    if symbol in _mexc_tick_size_cache:
        return _mexc_tick_size_cache[symbol]
    try:
        async with session.get("https://api.mexc.com/api/v3/exchangeInfo",
                                params={"symbol": f"{symbol}{QUOTE}"},
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json()
            for s in data.get("symbols", []):
                for f in s.get("filters", []):
                    if f.get("filterType") == "PRICE_FILTER" and "tickSize" in f:
                        tick = float(f["tickSize"])
                        _mexc_tick_size_cache[symbol] = tick
                        return tick
                if "quotePrecision" in s:
                    tick = 10 ** (-int(s["quotePrecision"]))
                    _mexc_tick_size_cache[symbol] = tick
                    return tick
    except Exception as e:
        logger.error(f"MEXC tick size {symbol}: {e}")
    return 0.000001


async def get_kucoin_base_increment(session, symbol: str) -> float:
    if symbol in _kucoin_increment_cache:
        return _kucoin_increment_cache[symbol]
    try:
        async with session.get("https://api.kucoin.com/api/v2/symbols",
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json()
            for s in data.get("data", []):
                if s.get("symbol") == f"{symbol}-{QUOTE}":
                    inc = float(s["baseIncrement"])
                    _kucoin_increment_cache[symbol] = inc
                    return inc
    except Exception as e:
        logger.error(f"KuCoin increment {symbol}: {e}")
    return 1.0


def _round_down_to_step(qty: float, step: float) -> float:
    if step <= 0:
        return qty
    return math.floor(qty / step) * step


def _round_price_to_tick(price: float, tick: float) -> float:
    if tick <= 0:
        return price
    return round(math.floor(price / tick) * tick, 10)


async def round_quantity_for_exchange(session, ex: str, symbol: str, raw_qty: float) -> float:
    if ex == "Binance":
        step = await get_binance_lot_step(session, symbol)
        result = _round_down_to_step(raw_qty, step)
    elif ex == "MEXC":
        step = await get_mexc_lot_step(session, symbol)
        result = _round_down_to_step(raw_qty, step)
    elif ex == "KuCoin":
        inc = await get_kucoin_base_increment(session, symbol)
        result = _round_down_to_step(raw_qty, inc)
    else:
        result = raw_qty
    return round(result, 10)


# =====================================================================
# ПОДПИСИ И ОРДЕРА
# =====================================================================

def sign_binance(params: dict, secret: str) -> str:
    query = urllib.parse.urlencode(params)
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()


def sign_kucoin(secret: str, passphrase: str, ts: str, method: str, endpoint: str, body: str = ""):
    str_to_sign = f"{ts}{method}{endpoint}{body}"
    signature = base64.b64encode(hmac.new(secret.encode(), str_to_sign.encode(), hashlib.sha256).digest()).decode()
    passphrase_signed = base64.b64encode(hmac.new(secret.encode(), passphrase.encode(), hashlib.sha256).digest()).decode()
    return signature, passphrase_signed


async def place_order_binance(session, symbol: str, side: str, quote_or_qty: float) -> Optional[dict]:
    if is_backed_off("Binance"):
        return None
    url = "https://api.binance.com/api/v3/order"
    ts = int(time.time() * 1000)
    params = {"symbol": f"{symbol}{QUOTE}", "side": side, "type": "MARKET", "timestamp": ts, "recvWindow": 5000}
    if side == "BUY":
        params["quoteOrderQty"] = round(quote_or_qty, 2)
    else:
        params["quantity"] = quote_or_qty
    params["signature"] = sign_binance(params, BINANCE_SECRET)
    headers = {"X-MBX-APIKEY": BINANCE_KEY}
    try:
        async with session.post(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status in (429, 418):
                trigger_backoff("Binance", r.status, r.headers.get("Retry-After"))
                return None
            data = await r.json()
            if r.status != 200:
                _remember_error("Binance", data.get("msg", data))
                return None
            return data
    except Exception as e:
        _remember_error("Binance", e)
        return None


async def place_order_binance_limit_ioc(session, symbol: str, side: str, price: float, quantity: float) -> Optional[dict]:
    if is_backed_off("Binance"):
        return None
    step = await get_binance_lot_step(session, symbol)
    quantity = _round_down_to_step(quantity, step)
    url = "https://api.binance.com/api/v3/order"
    ts = int(time.time() * 1000)
    params = {"symbol": f"{symbol}{QUOTE}", "side": side, "type": "LIMIT", "timeInForce": "IOC",
              "quantity": quantity, "price": price, "timestamp": ts, "recvWindow": 5000}
    params["signature"] = sign_binance(params, BINANCE_SECRET)
    headers = {"X-MBX-APIKEY": BINANCE_KEY}
    try:
        async with session.post(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status in (429, 418):
                trigger_backoff("Binance", r.status, r.headers.get("Retry-After"))
                return None
            data = await r.json()
            if r.status != 200:
                _remember_error("Binance", data.get("msg", data))
                return None
            return data
    except Exception as e:
        _remember_error("Binance", e)
        return None


async def confirm_binance_ioc_executed_qty(session, symbol: str, order_id) -> float:
    ts = int(time.time() * 1000)
    params = {"symbol": f"{symbol}{QUOTE}", "orderId": order_id, "timestamp": ts, "recvWindow": 5000}
    params["signature"] = sign_binance(params, BINANCE_SECRET)
    headers = {"X-MBX-APIKEY": BINANCE_KEY}
    try:
        async with session.get("https://api.binance.com/api/v3/order", params=params, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=5)) as r:
            data = await r.json()
            return float(data.get("executedQty", 0) or 0)
    except Exception as e:
        logger.error(f"Binance order status: {e}")
        return 0.0


async def wait_for_binance_fill(session, symbol: str, order_id, timeout: float = 3.0) -> Optional[float]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ts = int(time.time() * 1000)
        params = {"symbol": f"{symbol}{QUOTE}", "orderId": order_id, "timestamp": ts, "recvWindow": 5000}
        params["signature"] = sign_binance(params, BINANCE_SECRET)
        headers = {"X-MBX-APIKEY": BINANCE_KEY}
        try:
            async with session.get("https://api.binance.com/api/v3/order", params=params, headers=headers,
                                    timeout=aiohttp.ClientTimeout(total=5)) as r:
                data = await r.json()
                if data.get("status") == "FILLED":
                    return float(data.get("executedQty", 0))
                if data.get("status") in ("CANCELED", "REJECTED", "EXPIRED"):
                    return None
        except Exception as e:
            logger.error(f"Binance fill check: {e}")
        await asyncio.sleep(0.3)
    return None


async def place_order_mexc(session, symbol: str, side: str, quote_or_qty: float) -> Optional[dict]:
    if is_backed_off("MEXC"):
        return None
    url = "https://api.mexc.com/api/v3/order"
    ts = int(time.time() * 1000)
    params = {"symbol": f"{symbol}{QUOTE}", "side": side, "type": "MARKET", "timestamp": ts, "recvWindow": 5000}
    if side == "BUY":
        params["quoteOrderQty"] = round(quote_or_qty, 2)
    else:
        params["quantity"] = quote_or_qty
    params["signature"] = sign_binance(params, MEXC_SECRET)
    headers = {"X-MEXC-APIKEY": MEXC_KEY, "Content-Type": "application/json"}
    try:
        async with session.post(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status in (429, 418):
                trigger_backoff("MEXC", r.status, r.headers.get("Retry-After"))
                return None
            data = await r.json()
            if r.status != 200:
                _remember_error("MEXC", data.get("msg", data))
                return None
            return data
    except Exception as e:
        _remember_error("MEXC", e)
        return None


async def place_order_mexc_limit_ioc(session, symbol: str, side: str, price: float, quantity: float) -> Optional[dict]:
    if is_backed_off("MEXC"):
        return None
    tick = await get_mexc_tick_size(session, symbol)
    price = _round_price_to_tick(price, tick)
    quantity = await round_quantity_for_exchange(session, "MEXC", symbol, quantity)
    url = "https://api.mexc.com/api/v3/order"
    ts = int(time.time() * 1000)
    params = {"symbol": f"{symbol}{QUOTE}", "side": side, "type": "LIMIT", "timeInForce": "IOC",
              "quantity": quantity, "price": price, "timestamp": ts, "recvWindow": 5000}
    params["signature"] = sign_binance(params, MEXC_SECRET)
    headers = {"X-MEXC-APIKEY": MEXC_KEY, "Content-Type": "application/json"}
    try:
        async with session.post(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status in (429, 418):
                trigger_backoff("MEXC", r.status, r.headers.get("Retry-After"))
                return None
            data = await r.json()
            if r.status != 200:
                _remember_error("MEXC", data.get("msg", data))
                return None
            return data
    except Exception as e:
        _remember_error("MEXC", e)
        return None


async def confirm_mexc_ioc_executed_qty(session, symbol: str, order_id) -> float:
    ts = int(time.time() * 1000)
    params = {"symbol": f"{symbol}{QUOTE}", "orderId": order_id, "timestamp": ts, "recvWindow": 5000}
    params["signature"] = sign_binance(params, MEXC_SECRET)
    headers = {"X-MEXC-APIKEY": MEXC_KEY, "Content-Type": "application/json"}
    try:
        async with session.get("https://api.mexc.com/api/v3/order", params=params, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=5)) as r:
            data = await r.json()
            return float(data.get("executedQty", 0) or 0)
    except Exception as e:
        logger.error(f"MEXC order status: {e}")
        return 0.0


async def place_order_kucoin(session, symbol: str, side: str, funds_or_size: float, use_funds: bool = True) -> Optional[dict]:
    if is_backed_off("KuCoin"):
        return None
    endpoint = "/api/v1/orders"
    url = f"https://api.kucoin.com{endpoint}"
    ts = str(int(time.time() * 1000))
    body_dict = {"clientOid": str(int(time.time() * 1000000)), "side": side.lower(),
                 "symbol": f"{symbol}-{QUOTE}", "type": "market"}
    if use_funds:
        body_dict["funds"] = str(round(funds_or_size, 4))
    else:
        body_dict["size"] = str(funds_or_size)
    body_str = json.dumps(body_dict)
    signature, passphrase_signed = sign_kucoin(KUCOIN_SECRET, KUCOIN_PASS, ts, "POST", endpoint, body_str)
    headers = {"KC-API-KEY": KUCOIN_KEY, "KC-API-SIGN": signature, "KC-API-TIMESTAMP": ts,
               "KC-API-PASSPHRASE": passphrase_signed, "KC-API-KEY-VERSION": "2", "Content-Type": "application/json"}
    try:
        async with session.post(url, data=body_str, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status in (429, 418):
                trigger_backoff("KuCoin", r.status, r.headers.get("Retry-After"))
                return None
            data = await r.json()
            if r.status != 200 or data.get("code") != "200000":
                _remember_error("KuCoin", data.get("msg", data))
                return None
            return data
    except Exception as e:
        _remember_error("KuCoin", e)
        return None


async def place_order_kucoin_limit_ioc(session, symbol: str, side: str, price: float, size: float) -> Optional[dict]:
    if is_backed_off("KuCoin"):
        return None
    endpoint = "/api/v1/orders"
    url = f"https://api.kucoin.com{endpoint}"
    ts = str(int(time.time() * 1000))
    body_dict = {"clientOid": str(int(time.time() * 1000000)), "side": side.lower(),
                 "symbol": f"{symbol}-{QUOTE}", "type": "limit", "price": str(price),
                 "size": str(size), "timeInForce": "IOC"}
    body_str = json.dumps(body_dict)
    signature, passphrase_signed = sign_kucoin(KUCOIN_SECRET, KUCOIN_PASS, ts, "POST", endpoint, body_str)
    headers = {"KC-API-KEY": KUCOIN_KEY, "KC-API-SIGN": signature, "KC-API-TIMESTAMP": ts,
               "KC-API-PASSPHRASE": passphrase_signed, "KC-API-KEY-VERSION": "2", "Content-Type": "application/json"}
    try:
        async with session.post(url, data=body_str, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status in (429, 418):
                trigger_backoff("KuCoin", r.status, r.headers.get("Retry-After"))
                return None
            data = await r.json()
            if r.status != 200 or data.get("code") != "200000":
                _remember_error("KuCoin", data.get("msg", data))
                return None
            return data
    except Exception as e:
        _remember_error("KuCoin", e)
        return None


async def wait_for_kucoin_fill(session, order_id: str, timeout: float = 6.0) -> Optional[float]:
    deadline = time.time() + timeout
    endpoint = f"/api/v1/orders/{order_id}"
    while time.time() < deadline:
        ts = str(int(time.time() * 1000))
        signature, passphrase_signed = sign_kucoin(KUCOIN_SECRET, KUCOIN_PASS, ts, "GET", endpoint, "")
        headers = {"KC-API-KEY": KUCOIN_KEY, "KC-API-SIGN": signature, "KC-API-TIMESTAMP": ts,
                   "KC-API-PASSPHRASE": passphrase_signed, "KC-API-KEY-VERSION": "2"}
        try:
            async with session.get(f"https://api.kucoin.com{endpoint}", headers=headers,
                                    timeout=aiohttp.ClientTimeout(total=5)) as r:
                data = await r.json()
                d = data.get("data", {})
                if d.get("isActive") is False and float(d.get("dealSize", 0)) > 0:
                    return float(d["dealSize"])
                if d.get("cancelExist"):
                    return None
        except Exception as e:
            logger.error(f"KuCoin fill check: {e}")
        await asyncio.sleep(0.3)
    return None


async def confirm_fill_and_get_qty(session, ex: str, buy_result: dict) -> Optional[float]:
    if ex == "Binance":
        order_id = buy_result.get("orderId")
        if buy_result.get("status") == "FILLED":
            return float(buy_result.get("executedQty", 0))
        return await wait_for_binance_fill(session, buy_result.get("symbol", "")[:-len(QUOTE)], order_id)
    elif ex == "MEXC":
        order_id = buy_result.get("orderId")
        if buy_result.get("status") == "FILLED":
            return float(buy_result.get("executedQty", 0))
        return await confirm_mexc_ioc_executed_qty(session, buy_result.get("symbol", "")[:-len(QUOTE)], order_id) or None
    elif ex == "KuCoin":
        order_id = buy_result.get("data", {}).get("orderId")
        if not order_id:
            return None
        return await wait_for_kucoin_fill(session, order_id)
    return None


# =====================================================================
# РЕАЛЬНЫЕ БАЛАНСЫ
# =====================================================================

async def get_real_balances_binance(session) -> Optional[Dict[str, float]]:
    if is_backed_off("Binance"):
        return None
    ts = int(time.time() * 1000)
    params = {"timestamp": ts, "recvWindow": 5000}
    params["signature"] = sign_binance(params, BINANCE_SECRET)
    headers = {"X-MBX-APIKEY": BINANCE_KEY}
    try:
        async with session.get("https://api.binance.com/api/v3/account", params=params, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status in (429, 418):
                trigger_backoff("Binance", r.status, r.headers.get("Retry-After"))
                return None
            data = await r.json()
            if r.status != 200:
                return None
            return {b["asset"]: float(b["free"]) for b in data.get("balances", [])}
    except Exception as e:
        logger.error(f"Binance balance: {e}")
        return None


async def get_real_balances_kucoin(session) -> Optional[Dict[str, float]]:
    if is_backed_off("KuCoin"):
        return None
    endpoint = "/api/v1/accounts"
    ts = str(int(time.time() * 1000))
    signature, passphrase_signed = sign_kucoin(KUCOIN_SECRET, KUCOIN_PASS, ts, "GET", endpoint, "")
    headers = {"KC-API-KEY": KUCOIN_KEY, "KC-API-SIGN": signature, "KC-API-TIMESTAMP": ts,
               "KC-API-PASSPHRASE": passphrase_signed, "KC-API-KEY-VERSION": "2"}
    try:
        async with session.get(f"https://api.kucoin.com{endpoint}", headers=headers,
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status in (429, 418):
                trigger_backoff("KuCoin", r.status, r.headers.get("Retry-After"))
                return None
            data = await r.json()
            if r.status != 200 or data.get("code") != "200000":
                return None
            result = {}
            for acc in data.get("data", []):
                if acc.get("type") == "trade":
                    result[acc["currency"]] = float(acc["available"])
            return result
    except Exception as e:
        logger.error(f"KuCoin balance: {e}")
        return None


async def get_real_balances_mexc(session) -> Optional[Dict[str, float]]:
    if is_backed_off("MEXC"):
        return None
    ts = int(time.time() * 1000)
    params = {"timestamp": ts, "recvWindow": 5000}
    params["signature"] = sign_binance(params, MEXC_SECRET)
    headers = {"X-MEXC-APIKEY": MEXC_KEY, "Content-Type": "application/json"}
    try:
        async with session.get("https://api.mexc.com/api/v3/account", params=params, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status in (429, 418):
                trigger_backoff("MEXC", r.status, r.headers.get("Retry-After"))
                return None
            data = await r.json()
            if r.status != 200:
                return None
            return {b["asset"]: float(b["free"]) for b in data.get("balances", [])}
    except Exception as e:
        logger.error(f"MEXC balance: {e}")
        return None


async def get_real_balances(session, ex: str) -> Optional[Dict[str, float]]:
    if ex == "Binance":
        return await get_real_balances_binance(session)
    elif ex == "KuCoin":
        return await get_real_balances_kucoin(session)
    elif ex == "MEXC":
        return await get_real_balances_mexc(session)
    return None


async def get_valuation_price(session, ex: str, symbol: str) -> Optional[float]:
    ob = await ORDERBOOK_FN.get(ex, lambda *a: None)(session, symbol)
    if not ob or not ob.get("bids"):
        return None
    return ob["bids"][0][0]


async def get_total_real_capital(session, fixed_prices: Optional[Dict] = None) -> Optional[dict]:
    per_exchange = {}
    total = 0.0
    prices_used = {}
    for ex in ["Binance", "KuCoin", "MEXC"]:
        if ex == "MEXC" and not MEXC_KEY:
            continue
        balances = await get_real_balances(session, ex)
        if balances is None:
            continue
        ex_total = balances.get("USDT", 0.0)
        for sym in SYMBOLS:
            qty = balances.get(sym, 0.0)
            if qty > 0:
                if fixed_prices is not None and (ex, sym) in fixed_prices:
                    price = fixed_prices[(ex, sym)]
                else:
                    price = await get_valuation_price(session, ex, sym)
                if price:
                    ex_total += qty * price
                    prices_used[(ex, sym)] = price
        per_exchange[ex] = round(ex_total, 6)
        total += ex_total
    return {"total": round(total, 6), "per_exchange": per_exchange, "prices_used": prices_used}


def is_real_trading_allowed() -> bool:
    env_ok = REAL_TRADING_UNLOCKED == CONFIRM_PHRASE
    runtime_ok = config["real_confirmed"]
    keys_ok = all([BINANCE_KEY or True, KUCOIN_KEY, KUCOIN_SECRET, KUCOIN_PASS, MEXC_KEY, MEXC_SECRET])
    return env_ok and runtime_ok and keys_ok


# =====================================================================
# ДОКУПКА РЕЗЕРВА (упрощено — одна честная функция на каждое направление)
# =====================================================================

async def top_up_coin_reserve(session, ex: str, symbol: str, shortfall_qty: float, price_hint: float) -> bool:
    if is_backed_off(ex) or not price_hint or price_hint <= 0:
        return False
    if stats.get("topup_cost_usdt", 0.0) >= config["max_topup_spend_per_day"]:
        return False
    stats["topup_attempts"] += 1
    usd_needed = round(shortfall_qty * price_hint * 1.08, 2)
    usd_needed = max(usd_needed, MIN_ORDER_VALUE_USD.get(ex, 5.0))
    result = None
    if ex == "Binance":
        result = await place_order_binance(session, symbol, "BUY", usd_needed)
    elif ex == "MEXC":
        result = await place_order_mexc(session, symbol, "BUY", usd_needed)
    elif ex == "KuCoin":
        result = await place_order_kucoin(session, symbol, "buy", usd_needed, use_funds=True)
    if result:
        stats["topup_success"] += 1
        stats["topup_cost_usdt"] = stats.get("topup_cost_usdt", 0.0) + usd_needed
        logger.info(f"✅ Докупка {symbol} на {ex}: ~${usd_needed}")
        if CHAT_ID:
            await send_tg(session, f"🔧 Автодокупка: не хватало {shortfall_qty:.2f} {symbol} на {ex} — докупил ~${usd_needed}")
    return bool(result)


async def top_up_usdt_via_coin_sale(session, ex: str, symbol: str, usdt_needed: float, price_hint: float) -> bool:
    if is_backed_off(ex) or not price_hint or price_hint <= 0:
        return False
    if stats.get("topup_cost_usdt", 0.0) >= config["max_topup_spend_per_day"]:
        return False
    balances = await get_real_balances(session, ex)
    if not balances:
        return False
    have_coin = balances.get(symbol, 0.0)
    if have_coin <= 0:
        return False
    coin_to_sell = min((usdt_needed * 1.08) / price_hint, have_coin * 0.98)
    coin_to_sell = await round_quantity_for_exchange(session, ex, symbol, coin_to_sell)
    if coin_to_sell <= 0:
        return False
    estimated_value = coin_to_sell * price_hint
    if estimated_value < MIN_ORDER_VALUE_USD.get(ex, 1.0):
        return False
    stats["topup_attempts"] += 1
    result = None
    if ex == "Binance":
        result = await place_order_binance(session, symbol, "SELL", coin_to_sell)
    elif ex == "MEXC":
        result = await place_order_mexc(session, symbol, "SELL", coin_to_sell)
    elif ex == "KuCoin":
        result = await place_order_kucoin(session, symbol, "sell", coin_to_sell, use_funds=False)
    if result:
        stats["topup_success"] += 1
        stats["topup_cost_usdt"] = stats.get("topup_cost_usdt", 0.0) + coin_to_sell * price_hint
        if CHAT_ID:
            await send_tg(session, f"🔧 Автодокупка USDT: продал {coin_to_sell} {symbol} на {ex}")
    return bool(result)


# =====================================================================
# РЕАЛЬНОЕ ИСПОЛНЕНИЕ
# =====================================================================

async def execute_real_arbitrage(session, opp: dict) -> dict:
    if not is_real_trading_allowed():
        return {"success": False, "error": "real_trading_not_unlocked"}
    if config["real_trades_today"] >= config["max_real_trades_per_day"]:
        return {"success": False, "error": "daily_real_trade_limit_reached"}

    vol = min(opp["vol"], config["max_real_order_usdt"])
    symbol, buy_ex, sell_ex = opp["symbol"], opp["buy_ex"], opp["sell_ex"]

    required_min = max(MIN_ORDER_VALUE_USD.get(buy_ex, 0), MIN_ORDER_VALUE_USD.get(sell_ex, 0))
    if vol < required_min:
        if required_min > config["max_real_lot_ceiling"]:
            return {"success": False, "error": f"min_order_exceeds_ceiling: нужно ${required_min}"}
        vol = required_min

    buy_balances, sell_balances = await asyncio.gather(
        get_real_balances(session, buy_ex), get_real_balances(session, sell_ex))
    if buy_balances is None:
        return {"success": False, "error": f"could_not_verify_buy_balance_on_{buy_ex}"}
    available_usdt = buy_balances.get("USDT", 0.0)
    buffer_mult = 1 + config["balance_safety_buffer_pct"] / 100
    if available_usdt < vol * buffer_mult:
        shrunk = round(available_usdt / buffer_mult, 2)
        if shrunk >= required_min:
            vol = shrunk
        elif config.get("skip_reactive_topup", True):
            return {"success": False, "error": f"skipped_insufficient_usdt_on_{buy_ex}"}
        else:
            shortfall = vol * buffer_mult - available_usdt
            if not await top_up_usdt_via_coin_sale(session, buy_ex, symbol, shortfall, opp.get("buy_price", 0)):
                return {"success": False, "error": f"insufficient_usdt_on_{buy_ex}"}

    if sell_balances is None:
        return {"success": False, "error": f"could_not_verify_sell_balance_on_{sell_ex}"}
    qty_needed_est = vol / opp["buy_price"] if opp.get("buy_price") else 0
    available_coin = sell_balances.get(symbol, 0.0)
    required_with_buffer = qty_needed_est * buffer_mult
    if available_coin < required_with_buffer:
        if config.get("skip_reactive_topup", True):
            return {"success": False, "error": f"skipped_insufficient_reserve_on_{sell_ex}"}
        shortfall = round(required_with_buffer - available_coin, 4)
        if not await top_up_coin_reserve(session, sell_ex, symbol, shortfall, opp["sell_price"]):
            return {"success": False, "error": f"insufficient_real_balance_on_{sell_ex}"}

    # --- НОГА 1: ПОКУПКА ---
    buy_result = None
    use_ioc = config.get("use_limit_ioc_orders", True)
    if buy_ex == "KuCoin":
        if use_ioc and opp.get("buy_price"):
            coins_est = vol / opp["buy_price"]
            limit_size = await round_quantity_for_exchange(session, "KuCoin", symbol, coins_est)
            if limit_size > 0:
                buy_result = await place_order_kucoin_limit_ioc(session, symbol, "buy", opp["buy_price"], limit_size)
            if not buy_result:
                stats["buy_leg_failures"] += 1
                return {"success": False, "error": f"limit_ioc_buy_not_filled_on_{buy_ex}"}
        else:
            buy_result = await place_order_kucoin(session, symbol, "buy", vol, use_funds=True)
    elif buy_ex == "Binance":
        buy_result = await place_order_binance(session, symbol, "BUY", vol)
    elif buy_ex == "MEXC":
        buy_result = await place_order_mexc(session, symbol, "BUY", vol)

    if not buy_result:
        stats["buy_leg_failures"] += 1
        return {"success": False, "error": f"buy_leg_failed_on_{buy_ex}: {_last_exchange_error.get(buy_ex)}"}

    config["real_trades_today"] += 1
    confirmed_qty = await confirm_fill_and_get_qty(session, buy_ex, buy_result)
    if not confirmed_qty or confirmed_qty <= 0:
        stats["buy_leg_failures"] += 1
        return {"success": False, "error": f"buy_leg_not_confirmed_filled_on_{buy_ex}"}

    sell_qty = await round_quantity_for_exchange(session, sell_ex, symbol, confirmed_qty)
    if sell_qty <= 0:
        return {"success": False, "error": f"sell_qty_rounds_to_zero_on_{sell_ex}"}

    fresh_sell_balances = await get_real_balances(session, sell_ex)
    fresh_available = (fresh_sell_balances or {}).get(symbol, 0.0)
    if fresh_available < sell_qty:
        shortfall = round(sell_qty - fresh_available, 4)
        topped = await top_up_coin_reserve(session, sell_ex, symbol, shortfall, opp["sell_price"])
        if topped:
            await asyncio.sleep(1.5)
            refreshed = await get_real_balances(session, sell_ex)
            fresh_available = (refreshed or {}).get(symbol, fresh_available)
        if fresh_available < sell_qty:
            return {"success": False, "error": f"insufficient_real_balance_on_{sell_ex}_precheck",
                    "buy_result": buy_result}

    # --- НОГА 2: ПРОДАЖА ---
    sell_result = None
    if sell_ex == "MEXC":
        if use_ioc and opp.get("sell_price"):
            slip = config.get("sell_limit_slippage_pct", 0.05)
            sell_limit_price = opp["sell_price"] * (1 - slip / 100)
            sell_result = await place_order_mexc_limit_ioc(session, symbol, "SELL", sell_limit_price, sell_qty)
            if sell_result:
                order_id = sell_result.get("orderId")
                executed_qty = await confirm_mexc_ioc_executed_qty(session, symbol, order_id) if order_id else 0.0
                fill_ratio = (executed_qty / sell_qty) if sell_qty > 0 else 0.0
                if fill_ratio < 0.95:
                    _remember_error("MEXC", f"IOC filled only {fill_ratio*100:.1f}%")
                    stats["sell_leg_failures"] += 1
                    sell_result = None
        else:
            sell_result = await place_order_mexc(session, symbol, "SELL", sell_qty)
    elif sell_ex == "KuCoin":
        sell_result = await place_order_kucoin(session, symbol, "sell", sell_qty, use_funds=False)
    elif sell_ex == "Binance":
        if use_ioc and opp.get("sell_price"):
            slip = config.get("sell_limit_slippage_pct", 0.05)
            sell_limit_price = opp["sell_price"] * (1 - slip / 100)
            sell_result = await place_order_binance_limit_ioc(session, symbol, "SELL", sell_limit_price, sell_qty)
        else:
            sell_result = await place_order_binance(session, symbol, "SELL", sell_qty)

    if not sell_result:
        stats["sell_leg_failures"] += 1
        emergency_qty = await round_quantity_for_exchange(session, buy_ex, symbol, confirmed_qty)
        emergency = None
        stats["emergency_closes_attempted"] += 1
        if emergency_qty > 0:
            if buy_ex == "Binance":
                emergency = await place_order_binance(session, symbol, "SELL", emergency_qty)
            elif buy_ex == "MEXC":
                emergency = await place_order_mexc(session, symbol, "SELL", emergency_qty)
            elif buy_ex == "KuCoin":
                emergency = await place_order_kucoin(session, symbol, "sell", emergency_qty, use_funds=False)
        if emergency:
            stats["emergency_closes_succeeded"] += 1
        return {"success": False, "error": f"sell_leg_failed_on_{sell_ex}: {_last_exchange_error.get(sell_ex)}",
                "emergency_close": bool(emergency), "buy_result": buy_result}

    return {"success": True, "buy_result": buy_result, "sell_result": sell_result, "vol": vol,
            "confirmed_qty": confirmed_qty}


REASON_LABELS = {
    "rate_limit_exceeded": "⏱ превышен лимит сделок/мин",
    "paused_or_stoploss": "⏸ пауза или сработал стоп-лосс",
    "pre_trade_volatility_too_high": "🌪 цена дёргается прямо сейчас — пропущено",
    None: "",
}


def reset_daily():
    today = datetime.now().strftime("%Y-%m-%d")
    if config["day_start"] != today:
        config["day_start"] = today
        config["real_trades_today"] = 0
        stats["topup_cost_usdt"] = 0.0


def can_trade() -> bool:
    reset_daily()
    return config["trading_active"] and not config["paused"]


def check_rate() -> bool:
    now = datetime.now()
    if (now - stats["minute_start"]).total_seconds() >= 60:
        stats["trades_this_minute"] = 0
        stats["minute_start"] = now
    return stats["trades_this_minute"] < config["max_trades_per_min"]


async def execute_trade(session, opp: dict) -> dict:
    if not check_rate():
        return {"executed": False, "reason": "rate_limit_exceeded"}
    if not can_trade():
        return {"executed": False, "reason": "paused_or_stoploss"}

    if not config["simulation_mode"]:
        pre_vol = get_recent_price_volatility_pct(1, symbol=opp.get("symbol"))
        pre_threshold = config.get("pre_trade_max_volatility_pct_1min", 0.8)
        if pre_vol is not None and pre_threshold > 0 and pre_vol > pre_threshold:
            return {"executed": False, "reason": "pre_trade_volatility_too_high"}

    if config["simulation_mode"]:
        stats["trades"] += 1
        stats["profit_estimate"] += opp["profit_usdt"]
        stats["trades_this_minute"] += 1
        trade_history.append({**opp, "mode": "SIM"})
        return {"executed": True, "reason": None}

    async with _capital_measurement_lock:
        capital_before = await get_total_real_capital(session)

    real_result = await execute_real_arbitrage(session, opp)
    if CHAT_ID:
        await send_tg(session, f"🔍 Диагностика: success={real_result.get('success')} error={real_result.get('error')} vol={real_result.get('vol')}")

    if not real_result.get("success"):
        logger.error(f"Реальная сделка не удалась: {real_result}")
        if CHAT_ID:
            await send_tg(session, f"🔴 Сделка отклонена: `{real_result.get('error')}`")
        return {"executed": False, "reason": f"real_execution_failed: {real_result.get('error')}"}

    config["real_trades_today"] += 1
    stats["trades"] += 1
    stats["trades_this_minute"] += 1
    trade_history.append({**opp, "mode": "REAL"})

    try:
        await asyncio.sleep(config.get("factual_delta_delay_sec", 2.5))
        async with _capital_measurement_lock:
            capital_after = await get_total_real_capital(session, fixed_prices=capital_before.get("prices_used") if capital_before else None)
        if capital_before and capital_after:
            factual_delta = round(capital_after["total"] - capital_before["total"], 4)
            stats["factual_realized_pnl"] = round(stats.get("factual_realized_pnl", 0.0) + factual_delta, 4)
            stats["factual_trades_count"] = stats.get("factual_trades_count", 0) + 1
            record_execution_erosion(opp["net_pct"], factual_delta, opp["vol"])
            hist = stats.setdefault("real_factual_history", [])
            hist.append(factual_delta)
            if len(hist) > 10:
                hist.pop(0)
            if CHAT_ID:
                await send_tg(session,
                    f"📐 Фактический результат: до ${capital_before['total']} → после ${capital_after['total']} "
                    f"(`{factual_delta:+.4f} USDT`), оценка на сигнале была `{opp['profit_usdt']:+.4f}`")
    except Exception as e:
        logger.error(f"Факт-замер после сделки не удался: {e}")

    return {"executed": True, "reason": None}


# =====================================================================
# ФОНОВЫЕ ЦИКЛЫ
# =====================================================================

async def reserve_watchdog_loop(session):
    await asyncio.sleep(60)
    while True:
        interval = config.get("reserve_watchdog_interval_sec", 90)
        try:
            if not config["simulation_mode"]:
                for sym in list(SYMBOLS):
                    for buy_ex, sell_ex in pairs_for_symbol(sym):
                        try:
                            balances = await get_real_balances(session, sell_ex)
                            if balances is None:
                                continue
                            have = balances.get(sym, 0.0)
                            price = await get_valuation_price(session, sell_ex, sym)
                            if not price or price <= 0:
                                continue
                            headroom_mult = 1 + config["rebalance_headroom_pct"] / 100
                            target_qty = config["max_real_order_usdt"] * config.get("sell_reserve_lots", 3) / price * headroom_mult
                            trigger = config.get("reserve_watchdog_trigger_frac", 0.6)
                            if target_qty > 0 and have < target_qty * trigger:
                                shortfall = round(target_qty - have, 4)
                                if shortfall > 0:
                                    await top_up_coin_reserve(session, sell_ex, sym, shortfall, price)
                        except Exception as e:
                            logger.error(f"Watchdog {sell_ex}/{sym}: {e}")

                        try:
                            buy_balances = await get_real_balances(session, buy_ex)
                            if buy_balances is None:
                                continue
                            usdt_have = buy_balances.get("USDT", 0.0)
                            usdt_target = config["max_real_order_usdt"] * max(config.get("rebalance_target_lots", 1), 1)
                            trigger = config.get("reserve_watchdog_trigger_frac", 0.6)
                            if usdt_target > 0 and usdt_have < usdt_target * trigger:
                                have_coin = buy_balances.get(sym, 0.0)
                                if have_coin > 0:
                                    price_on_buy = await get_valuation_price(session, buy_ex, sym)
                                    if price_on_buy and price_on_buy > 0:
                                        shortfall_usdt = round(usdt_target - usdt_have, 4)
                                        await top_up_usdt_via_coin_sale(session, buy_ex, sym, shortfall_usdt, price_on_buy)
                        except Exception as e:
                            logger.error(f"Watchdog USDT {buy_ex}/{sym}: {e}")
        except Exception as e:
            logger.error(f"Reserve watchdog loop: {e}")
        await asyncio.sleep(interval)


async def drawdown_guard_loop(session):
    await asyncio.sleep(200)
    already_warned = False
    while True:
        try:
            pct = config.get("max_drawdown_pct", 0)
            if pct > 0 and not config["simulation_mode"] and not config["paused"] and config.get("real_start_capital"):
                real = await get_total_real_capital(session)
                if real:
                    pnl = real["total"] - config["real_start_capital"]
                    pnl_pct = pnl / config["real_start_capital"] * 100
                    if pnl_pct <= -pct and not already_warned:
                        config["paused"] = True
                        already_warned = True
                        if CHAT_ID:
                            await send_tg(session, f"🛑 Предохранитель: P&L {pnl:+.2f} ({pnl_pct:+.1f}%) ниже -{pct}% — торговля на паузе. `/go` для возобновления.")
                    elif pnl_pct > -pct:
                        already_warned = False
        except Exception as e:
            logger.error(f"Drawdown guard: {e}")
        await asyncio.sleep(300)


async def volatility_guard_loop(session):
    await asyncio.sleep(120)
    already_warned = False
    while True:
        try:
            threshold = config.get("max_volatility_pct_15min", 0)
            if threshold > 0 and not config["simulation_mode"]:
                for sym in list(SYMBOLS):
                    price_now = await get_valuation_price(session, pairs_for_symbol(sym)[0][1], sym)
                    if price_now:
                        price_history_by_symbol.setdefault(sym, []).append((time.time(), price_now))
                vol = get_recent_price_volatility_pct(15)
                if vol is not None:
                    if vol > threshold and not already_warned:
                        already_warned = True
                        hard = config.get("volatility_hard_pause", False)
                        if hard:
                            config["paused"] = True
                        if CHAT_ID:
                            note = "торговля на паузе" if hard else "торговля продолжается (точечная защита отсекает опасные попытки)"
                            await send_tg(session, f"🌪 Волатильность {vol}% за 15 мин (порог {threshold}%) — {note}")
                    elif vol <= threshold and already_warned:
                        already_warned = False
                        if CHAT_ID:
                            await send_tg(session, f"✅ Волатильность успокоилась: {vol}%")
        except Exception as e:
            logger.error(f"Volatility guard: {e}")
        await asyncio.sleep(120)


# =====================================================================
# TELEGRAM
# =====================================================================

async def send_tg(session, text):
    if not CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        await session.post(url, json={"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"},
                            timeout=aiohttp.ClientTimeout(total=10))
    except Exception as e:
        logger.error(f"TG: {e}")


async def get_updates(session, offset=0):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
    try:
        async with session.get(url, params={"offset": offset, "timeout": 30},
                                timeout=aiohttp.ClientTimeout(total=35)) as r:
            return (await r.json()).get("result", [])
    except Exception:
        return []


def format_signal(opp: dict) -> str:
    mode = "🔴 РЕАЛЬНАЯ" if not config["simulation_mode"] else "🔵 СИМУЛЯЦИЯ"
    return (
        f"🚨 *{opp['buy_ex']} → {opp['sell_ex']} | {opp['symbol']}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n{mode}\n\n"
        f"📥 Купить {opp['buy_ex']}: `{opp['buy_price']}` ({opp['levels_used_buy']} уровней)\n"
        f"📤 Продать {opp['sell_ex']}: `{opp['sell_price']}` ({opp['levels_used_sell']} уровней)\n\n"
        f"📊 Спред: `{opp['gross_pct']}%` | После комиссий: `{opp['net_pct']}%` | "
        f"Порог сейчас: `{opp['threshold_pct']}%`\n"
        f"💰 Ожидаемая прибыль: `{opp['profit_usdt']} USDT` на лоте ${opp['vol']}\n\n"
        f"🕐 {opp['time']}"
    )


async def handle_command(session, text, chat_id):
    global CHAT_ID
    CHAT_ID = chat_id
    parts = text.strip().split()
    cmd = parts[0].lower()

    if cmd == "/start":
        mode = "🔵 СИМУЛЯЦИЯ" if config["simulation_mode"] else "🔴 РЕАЛЬНАЯ"
        await send_tg(session,
            f"✅ *WorkerArbBot v2*\nРежим: {mode}\nМонеты: {', '.join(SYMBOLS)}\n"
            f"Честный порог входа сейчас: {true_honest_threshold_pct('KuCoin','MEXC')}%\n\n"
            f"Напиши /help для полного списка команд.")

    elif cmd == "/help":
        await send_tg(session,
            "*ОСНОВНОЕ*\n"
            "/start /help /stats /mode\n"
            "/scan — скан сейчас\n"
            "/pause /go /resume — управление торговлей\n"
            "/confirmreal ФРАЗА — подтвердить реальную торговлю\n"
            "/disablereal — выключить реальную торговлю\n\n"
            "*ЭКОНОМИКА / ПОРОГ*\n"
            "/threshold — показать текущий честный порог и его составляющие\n"
            "/setcrossingcost N — % стоимости пересечения спреда (главный параметр!)\n"
            "/setminabsprofit N — абсолютный минимум прибыли в USD\n"
            "/setmaxthreshold N — общий потолок честного порога, %\n"
            "/erosionstats — история реальной эрозии исполнения\n\n"
            "*ЛОТ И ЛИМИТЫ*\n"
            "/setlot N — лот симуляции\n"
            "/setreallot N — реальный лот (не выше потолка)\n"
            "/setlotceiling N — поднять потолок реального лота осознанно\n"
            "/setmaxtrades N — суточный лимит сделок\n"
            "/setbalancebuffer N — % буфер перед сделкой\n"
            "/setheadroom N — % запас ребаланса\n"
            "/setsellreserve N — лотов резерва на продажу\n\n"
            "*ЗАЩИТА*\n"
            "/setmaxdrawdown N — % просадки для автопаузы\n"
            "/setmaxvolatility N — % волатильности за 15 мин для предупреждения\n"
            "/setvolatilityhardpause on|off — жёсткая пауза при волатильности\n"
            "/setpretradevolatility N — % за 1 мин перед КАЖДОЙ попыткой\n"
            "/setskiptopup on|off — докупать резерв в моменте сделки или нет\n\n"
            "*МОНЕТЫ И МАРШРУТЫ*\n"
            "/addcoin SYM /removecoin SYM /listcoins\n"
            "/setroute SYM БИРЖА_КУПИТЬ БИРЖА_ПРОДАТЬ\n\n"
            "*БАЛАНСЫ*\n"
            "/realbalance — балансы и план по каждой бирже\n"
            "/setrealstart — зафиксировать стартовый капитал для P&L\n"
            "/apistatus — не заблокирована ли биржа rate-limit'ом\n"
            "/myip — внешний IP сервера, для whitelist на бирже\n\n"
            "*ОТЧЁТЫ*\n"
            "/history — последние сделки\n"
            "/report — отчёт за сегодня")

    elif cmd == "/threshold":
        for buy_ex, sell_ex in DEFAULT_PAIRS:
            t = true_honest_threshold_pct(buy_ex, sell_ex)
            erosion = get_avg_execution_erosion_pct()
            crossing = config.get("empirical_spread_crossing_pct", 1.5)
            await send_tg(session,
                f"📐 *Честный порог {buy_ex}→{sell_ex}: {t}%*\n\n"
                f"Комиссии: {FEES.get(buy_ex,0.1)+FEES.get(sell_ex,0.1)}%\n"
                f"Стоимость пересечения спреда: {crossing}%\n"
                f"Запас безопасности: {config['threshold_safety_margin_pct']}%\n"
                f"Эрозия исполнения (по факту): {erosion}%\n"
                f"Потолок: {config['max_total_threshold_pct']}%\n\n"
                f"Плюс абсолютный доллар-фильтр: ${config['min_absolute_profit_usd']} "
                f"(+{get_real_dynamic_adjustment_usd():+.4f} динамическая поправка)")

    elif cmd == "/scan":
        if config["paused"]:
            await send_tg(session, "⏸ На паузе. /go для возобновления.")
            return
        await send_tg(session, "🔍 Сканирую...")
        signals, active = await scan_all(session)
        if not signals:
            await send_tg(session,
                f"😔 Нет сигналов. Бирж онлайн: {', '.join(active) if active else 'ни одной!'}\n"
                f"Отклонено ниже порога: {stats.get('below_threshold_rejected', 0)}\n"
                f"Отклонено по $ фильтру: {stats.get('absolute_profit_too_low_rejected', 0)}\n"
                f"Отклонено — тонкий стакан: {stats.get('thin_book_rejected', 0)}")
        else:
            await send_tg(session, f"✅ {len(signals)} сигналов!")
            for opp in signals[:3]:
                result = await execute_trade(session, opp)
                if result["executed"]:
                    await send_tg(session, "✅ *ИСПОЛНЕНО*\n\n" + format_signal(opp))
                else:
                    reason = REASON_LABELS.get(result["reason"], result["reason"])
                    await send_tg(session, f"⛔ {opp['symbol']} пропущено: {reason}")

    elif cmd == "/stats":
        if config["simulation_mode"]:
            await send_tg(session,
                f"📈 *СТАТИСТИКА (СИМУЛЯЦИЯ)*\n\n"
                f"Сканов: {stats['scans']} | Сигналов: {stats['signals']} | Сделок: {stats['trades']}\n"
                f"Расчётная прибыль: {round(stats['profit_estimate'],4)} USDT\n\n"
                f"Отклонено ниже порога: {stats.get('below_threshold_rejected',0)}\n"
                f"Отклонено — $ фильтр: {stats.get('absolute_profit_too_low_rejected',0)}\n"
                f"Отклонено — тонкий стакан: {stats.get('thin_book_rejected',0)}\n"
                f"Отклонено — неправдоподобный спред: {stats.get('implausible_spread_rejected',0)}\n\n"
                f"Честный порог сейчас: {true_honest_threshold_pct('KuCoin','MEXC')}%")
            return
        real = await get_total_real_capital(session)
        balance_line = "не удалось прочитать" if real is None else f"${real['total']} ({real['per_exchange']})"
        pnl_line = ""
        if real and config.get("real_start_capital"):
            pnl = round(real["total"] - config["real_start_capital"], 2)
            pnl_line = f"P&L от старта: {pnl:+.2f} USDT\n"
        await send_tg(session,
            f"📈 *СТАТИСТИКА (РЕАЛЬНЫЙ РЕЖИМ)*\n\n"
            f"Сделок сегодня: {config['real_trades_today']}/{config['max_real_trades_per_day']}\n"
            f"Баланс: {balance_line}\n{pnl_line}\n"
            f"Фактический P&L (по факту, не по оценке): {stats.get('factual_realized_pnl',0):+.4f} "
            f"за {stats.get('factual_trades_count',0)} сделок\n\n"
            f"Неудачных покупок: {stats.get('buy_leg_failures',0)}\n"
            f"Неудачных продаж: {stats.get('sell_leg_failures',0)}\n"
            f"Аварийных закрытий: {stats.get('emergency_closes_succeeded',0)}/{stats.get('emergency_closes_attempted',0)}\n"
            f"Автодокупок: {stats.get('topup_success',0)}/{stats.get('topup_attempts',0)} "
            f"(~${stats.get('topup_cost_usdt',0):.2f})\n\n"
            f"Отклонено ниже порога: {stats.get('below_threshold_rejected',0)}\n"
            f"Отклонено — $ фильтр: {stats.get('absolute_profit_too_low_rejected',0)}\n\n"
            f"Честный порог: {true_honest_threshold_pct('KuCoin','MEXC')}%\n"
            f"Лот: ${config['max_real_order_usdt']}")

    elif cmd == "/erosionstats":
        avg = get_avg_execution_erosion_pct()
        hist = ", ".join(f"{v:+.2f}%" for v in execution_erosion_history) or "(пусто)"
        await send_tg(session, f"📉 Текущий буфер эрозии: {avg}%\nИстория: {hist}")

    elif cmd == "/setcrossingcost":
        if len(parts) < 2:
            await send_tg(session,
                f"Текущее значение: {config['empirical_spread_crossing_pct']}%\n\n"
                f"Это ГЛАВНЫЙ параметр честного порога — стоимость пересечения "
                f"bid/ask спреда при докупке резерва. Калибруй по 15-20 СВЕЖИМ "
                f"реальным сделкам, не бери старые значения с других дат/проектов.\n\n"
                f"Пример: /setcrossingcost 1.5")
            return
        try:
            val = float(parts[1])
            config["empirical_spread_crossing_pct"] = val
            await send_tg(session, f"✅ Стоимость пересечения спреда: {val}%. "
                                     f"Новый честный порог: {true_honest_threshold_pct('KuCoin','MEXC')}%")
        except ValueError:
            await send_tg(session, "❌ Пример: /setcrossingcost 1.5")

    elif cmd == "/setminabsprofit":
        if len(parts) < 2:
            await send_tg(session, f"Текущий минимум: ${config['min_absolute_profit_usd']}\nПример: /setminabsprofit 0.15")
            return
        try:
            config["min_absolute_profit_usd"] = float(parts[1])
            await send_tg(session, f"✅ Абсолютный минимум прибыли: ${config['min_absolute_profit_usd']}")
        except ValueError:
            await send_tg(session, "❌ Пример: /setminabsprofit 0.15")

    elif cmd == "/setmaxthreshold":
        if len(parts) < 2:
            await send_tg(session, f"Текущий потолок: {config['max_total_threshold_pct']}%\nПример: /setmaxthreshold 5.0")
            return
        try:
            config["max_total_threshold_pct"] = float(parts[1])
            await send_tg(session, f"✅ Потолок честного порога: {config['max_total_threshold_pct']}%")
        except ValueError:
            await send_tg(session, "❌ Пример: /setmaxthreshold 5.0")

    elif cmd == "/setlot":
        if len(parts) < 2:
            await send_tg(session, f"Лот симуляции: ${config['trade_usdt']}")
            return
        try:
            config["trade_usdt"] = float(parts[1])
            await send_tg(session, f"✅ Лот симуляции: ${config['trade_usdt']}")
        except ValueError:
            pass

    elif cmd == "/setreallot":
        active_ex = {ex for sym in SYMBOLS for pair in pairs_for_symbol(sym) for ex in pair}
        floor_val = max([MIN_ORDER_VALUE_USD.get(ex, 5.0) for ex in active_ex] or [5.0])
        if len(parts) < 2:
            await send_tg(session,
                f"Текущий реальный лот: ${config['max_real_order_usdt']}\n"
                f"Диапазон: ${floor_val} — ${config['max_real_lot_ceiling']}")
            return
        try:
            val = float(parts[1])
            if val > config["max_real_lot_ceiling"]:
                await send_tg(session, f"❌ Выше потолка ${config['max_real_lot_ceiling']}. "
                                         f"Подними потолок осознанно: /setlotceiling")
                return
            if val < floor_val:
                await send_tg(session, f"❌ Ниже минимума биржи ${floor_val} — ордер будет отклонён.")
                return
            config["max_real_order_usdt"] = val
            await send_tg(session, f"✅ Реальный лот: ${val}")
        except ValueError:
            await send_tg(session, "❌ Пример: /setreallot 15")

    elif cmd == "/setlotceiling":
        if len(parts) < 2:
            await send_tg(session,
                f"Текущий потолок: ${config['max_real_lot_ceiling']}\n\n"
                f"⚠️ Помни экономику: при лоте $10-15 биржевые минимумы ордера "
                f"съедают 10-50% потенциальной прибыли. Рентабельность начинается "
                f"примерно с $100-150 на лот. Поднимай осознанно.")
            return
        try:
            val = float(parts[1])
            config["max_real_lot_ceiling"] = val
            await send_tg(session, f"✅ Потолок реального лота: ${val}")
        except ValueError:
            await send_tg(session, "❌ Пример: /setlotceiling 100")

    elif cmd == "/setmaxtrades":
        if len(parts) < 2:
            await send_tg(session, f"Лимит: {config['max_real_trades_per_day']}/день")
            return
        try:
            config["max_real_trades_per_day"] = int(parts[1])
            await send_tg(session, f"✅ Лимит: {config['max_real_trades_per_day']}/день")
        except ValueError:
            pass

    elif cmd == "/setbalancebuffer":
        if len(parts) < 2:
            await send_tg(session, f"Буфер: {config['balance_safety_buffer_pct']}%")
            return
        try:
            config["balance_safety_buffer_pct"] = float(parts[1])
            await send_tg(session, f"✅ Буфер: {config['balance_safety_buffer_pct']}%")
        except ValueError:
            pass

    elif cmd == "/setheadroom":
        if len(parts) < 2:
            await send_tg(session, f"Запас ребаланса: {config['rebalance_headroom_pct']}%")
            return
        try:
            config["rebalance_headroom_pct"] = float(parts[1])
            await send_tg(session, f"✅ Запас ребаланса: {config['rebalance_headroom_pct']}%")
        except ValueError:
            pass

    elif cmd == "/setsellreserve":
        if len(parts) < 2:
            await send_tg(session, f"Резерв продажи: {config['sell_reserve_lots']} лотов")
            return
        try:
            config["sell_reserve_lots"] = int(parts[1])
            await send_tg(session, f"✅ Резерв продажи: {config['sell_reserve_lots']} лотов")
        except ValueError:
            pass

    elif cmd == "/setmaxdrawdown":
        if len(parts) < 2:
            await send_tg(session, f"Порог просадки: {config['max_drawdown_pct']}%")
            return
        try:
            config["max_drawdown_pct"] = float(parts[1])
            await send_tg(session, f"✅ Порог просадки: {config['max_drawdown_pct']}%")
        except ValueError:
            pass

    elif cmd == "/setmaxvolatility":
        if len(parts) < 2:
            await send_tg(session, f"Порог волатильности: {config['max_volatility_pct_15min']}%")
            return
        try:
            config["max_volatility_pct_15min"] = float(parts[1])
            await send_tg(session, f"✅ Порог волатильности: {config['max_volatility_pct_15min']}%")
        except ValueError:
            pass

    elif cmd == "/setvolatilityhardpause":
        if len(parts) < 2:
            cur = config.get("volatility_hard_pause", False)
            await send_tg(session, f"Жёсткая пауза: {'ВКЛ' if cur else 'выкл'}")
            return
        val = parts[1].lower()
        config["volatility_hard_pause"] = val in ("on", "1", "true")
        await send_tg(session, f"✅ Жёсткая пауза: {'ВКЛ' if config['volatility_hard_pause'] else 'выкл'}")

    elif cmd == "/setpretradevolatility":
        if len(parts) < 2:
            await send_tg(session, f"Порог за 1 мин: {config['pre_trade_max_volatility_pct_1min']}%")
            return
        try:
            config["pre_trade_max_volatility_pct_1min"] = float(parts[1])
            await send_tg(session, f"✅ Порог за 1 мин: {config['pre_trade_max_volatility_pct_1min']}%")
        except ValueError:
            pass

    elif cmd == "/setskiptopup":
        if len(parts) < 2:
            cur = config.get("skip_reactive_topup", True)
            await send_tg(session, f"Пропуск докупки в моменте: {'ВКЛ' if cur else 'выкл'}")
            return
        val = parts[1].lower()
        config["skip_reactive_topup"] = val in ("on", "1", "true")
        await send_tg(session, f"✅ Пропуск докупки в моменте: {'ВКЛ' if config['skip_reactive_topup'] else 'выкл'}")

    elif cmd == "/addcoin":
        if len(parts) < 2:
            await send_tg(session, "Пример: /addcoin ONE")
            return
        sym = parts[1].upper()
        if sym in SYMBOLS:
            await send_tg(session, f"⚠️ {sym} уже в списке.")
            return
        SYMBOLS.append(sym)
        start_binance_ws_book(session, sym)
        start_kucoin_ws_book(session, sym)
        await send_tg(session, f"✅ Добавлено: {sym}\nСписок: {', '.join(SYMBOLS)}")

    elif cmd == "/removecoin":
        if len(parts) < 2:
            await send_tg(session, "Пример: /removecoin ONE")
            return
        sym = parts[1].upper()
        if sym not in SYMBOLS:
            await send_tg(session, f"⚠️ {sym} не найдена.")
            return
        if len(SYMBOLS) <= 1:
            await send_tg(session, "❌ Нельзя удалить последнюю монету.")
            return
        SYMBOLS.remove(sym)
        stop_binance_ws_book(sym)
        stop_kucoin_ws_book(sym)
        await send_tg(session, f"✅ Удалено: {sym}\nСписок: {', '.join(SYMBOLS)}")

    elif cmd == "/listcoins":
        await send_tg(session, f"💱 Монеты: {', '.join(SYMBOLS)}")

    elif cmd == "/setroute":
        if len(parts) < 4:
            await send_tg(session, "Пример: /setroute ONE KuCoin MEXC")
            return
        sym, buy_ex, sell_ex = parts[1].upper(), parts[2], parts[3]
        if buy_ex not in FEES or sell_ex not in FEES:
            await send_tg(session, f"❌ Биржи: {', '.join(FEES.keys())}")
            return
        PAIR_OVERRIDES[sym] = [(buy_ex, sell_ex)]
        if sym not in SYMBOLS:
            SYMBOLS.append(sym)
            start_binance_ws_book(session, sym)
            start_kucoin_ws_book(session, sym)
        await send_tg(session, f"✅ Маршрут {sym}: {buy_ex} → {sell_ex}")

    elif cmd == "/realbalance":
        await send_tg(session, "📡 Читаю балансы...")
        real = await get_total_real_capital(session)
        if real is None:
            await send_tg(session, "🔴 Не удалось прочитать балансы.")
            return
        lines = "\n".join(f"  {ex}: ${v}" for ex, v in real["per_exchange"].items())
        await send_tg(session, f"💰 *Балансы*\n{lines}\n\nВсего: ${real['total']}")

    elif cmd == "/setrealstart":
        real = await get_total_real_capital(session)
        if real is None:
            await send_tg(session, "🔴 Не удалось прочитать баланс.")
            return
        config["real_start_capital"] = real["total"]
        await send_tg(session, f"✅ Стартовая точка: ${real['total']}")

    elif cmd == "/myip":
        try:
            async with session.get("https://api.ipify.org?format=json",
                                    timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
                ip = data.get("ip", "не удалось определить")
                await send_tg(session,
                    f"🌐 *Внешний IP этого сервера:* `{ip}`\n\n"
                    f"Укажи его в настройках API-ключа биржи (KuCoin/MEXC), "
                    f"если у тебя включено ограничение по IP для вывода/торговли.\n\n"
                    f"⚠️ На большинстве облачных хостингов (Railway, Render и т.п.) "
                    f"IP может МЕНЯТЬСЯ при каждом передеплое или перезапуске — "
                    f"если ключ вдруг перестанет работать после следующего деплоя, "
                    f"сначала проверь /myip ещё раз и обнови whitelist на бирже.")
        except Exception as e:
            await send_tg(session, f"❌ Не удалось определить IP: {e}")

    elif cmd == "/apistatus":
        now = time.time()
        msg = "📡 *СТАТУС API*\n\n"
        for ex, until in exchange_backoff_until.items():
            msg += f"⛔ {ex}: заморожен ещё {round(until-now)}с\n" if until > now else f"✅ {ex}: в норме\n"
        await send_tg(session, msg)

    elif cmd == "/history":
        if not trade_history:
            await send_tg(session, "Нет сделок.")
            return
        msg = "📋 *ПОСЛЕДНИЕ СДЕЛКИ*\n\n"
        for t in trade_history[-10:][::-1]:
            msg += f"{t['symbol']} {t['buy_ex']}→{t['sell_ex']} +{t['net_pct']}% +{t['profit_usdt']} USDT [{t['mode']}]\n"
        await send_tg(session, msg)

    elif cmd == "/report":
        today = datetime.now().strftime("%Y-%m-%d")
        today_trades = [t for t in trade_history if t.get("time", "").startswith(datetime.now().strftime("%H")) or True]
        if not trade_history:
            await send_tg(session, "Нет сделок.")
            return
        total = sum(t["profit_usdt"] for t in trade_history)
        await send_tg(session, f"📋 Всего сделок: {len(trade_history)}, суммарная оценка прибыли: {round(total,4)} USDT")

    elif cmd == "/pause":
        config["paused"] = True
        await send_tg(session, "⏸ Пауза активирована.")

    elif cmd == "/go":
        config["paused"] = False
        await send_tg(session, "▶️ Торговля возобновлена.")

    elif cmd == "/resume":
        config["trading_active"] = True
        await send_tg(session, "✅ Торговля разрешена.")

    elif cmd == "/mode":
        if config["simulation_mode"]:
            if not is_real_trading_allowed():
                await send_tg(session,
                    f"❌ Реальная торговля заблокирована. Нужны:\n"
                    f"1) REAL_TRADING_UNLOCKED={CONFIRM_PHRASE} в окружении\n"
                    f"2) Ключи KuCoin+MEXC заданы\n"
                    f"3) /confirmreal {CONFIRM_PHRASE} в этом чате")
                return
            config["simulation_mode"] = False
            await send_tg(session, f"🔴 РЕАЛЬНАЯ ТОРГОВЛЯ АКТИВНА. Лот: ${config['max_real_order_usdt']}")
        else:
            config["simulation_mode"] = True
            await send_tg(session, "🔵 Режим: СИМУЛЯЦИЯ")

    elif cmd == "/confirmreal":
        if len(parts) < 2 or parts[1] != CONFIRM_PHRASE:
            await send_tg(session, f"Напиши точно: /confirmreal {CONFIRM_PHRASE}")
            return
        config["real_confirmed"] = True
        env_ok = REAL_TRADING_UNLOCKED == CONFIRM_PHRASE
        await send_tg(session, f"{'✅' if env_ok else '⚠️'} Runtime OK. ENV: {'✅' if env_ok else '❌ не установлена'}")

    elif cmd == "/disablereal":
        config["real_confirmed"] = False
        config["simulation_mode"] = True
        await send_tg(session, "🔵 Реальная торговля отключена.")

    else:
        await send_tg(session, "Неизвестная команда. /help для списка.")


async def polling_loop(session):
    offset = 0
    while True:
        updates = await get_updates(session, offset)
        for update in updates:
            offset = update["update_id"] + 1
            msg = update.get("message", {})
            if msg:
                global CHAT_ID
                CHAT_ID = msg["chat"]["id"]
                text = msg.get("text", "")
                if text.startswith("/"):
                    try:
                        await handle_command(session, text, CHAT_ID)
                    except Exception as e:
                        logger.error(f"handle_command error: {e}")
                        await send_tg(session, f"⚠️ Ошибка: `{e}`")
        await asyncio.sleep(1)


async def scan_loop(session):
    await asyncio.sleep(15)
    last_signal_time: Dict[str, float] = {}
    while True:
        try:
            reset_daily()
            if not config["paused"] and can_trade():
                signals, active = await scan_all(session)
                logger.info(f"Скан #{stats['scans']}: бирж={len(active)} сигналов={len(signals)}")
                for opp in signals[:3]:
                    key = f"{opp['symbol']}-{opp['buy_ex']}-{opp['sell_ex']}"
                    now = datetime.now().timestamp()
                    if now - last_signal_time.get(key, 0) > 120:
                        last_signal_time[key] = now
                        result = await execute_trade(session, opp)
                        if not CHAT_ID:
                            continue
                        if result["executed"]:
                            await send_tg(session, "✅ *ИСПОЛНЕНО*\n\n" + format_signal(opp))
                        else:
                            reason = REASON_LABELS.get(result["reason"], result["reason"])
                            await send_tg(session, f"⛔ {opp['symbol']} пропущено: {reason}")
        except Exception as e:
            logger.error(f"Scan loop error: {e}")
        await asyncio.sleep(config["scan_interval"])


async def main():
    if not TG_TOKEN:
        logger.error("ARB_BOT_TOKEN не установлен!")
        return
    logger.info("WorkerArbBot v2 стартует")
    connector = aiohttp.TCPConnector(ssl=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        for sym in SYMBOLS:
            start_binance_ws_book(session, sym)
            start_kucoin_ws_book(session, sym)
        await asyncio.gather(
            polling_loop(session), scan_loop(session),
            reserve_watchdog_loop(session), drawdown_guard_loop(session),
            volatility_guard_loop(session),
        )


if __name__ == "__main__":
    asyncio.run(main())
