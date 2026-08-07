import asyncio
import aiohttp
import logging
import os
import csv
import io
import time
import json
import gzip
import uuid
import hmac
import hashlib
import base64
import urllib.parse
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TG_TOKEN = os.environ.get("ARB_BOT_TOKEN", "")
CHAT_ID = None

config = {
    "simulation_mode":    True,
    "min_profit_pct":     0.3,
    "trade_usdt":         20.0,
    "scan_interval":      3,  # СНИЖЕНО (было 10): цена и так живая через
        # WebSocket, интервал влияет только на частоту сверки/решения —
        # короче интервал, больше шансов поймать мимолётное окно возможности.
        # Настраивается командой /setinterval.
    "max_trades_per_min": 6,
    "stop_loss_usdt":     10.0,
    "daily_loss":         0.0,
    "daily_profit":       0.0,
    "day_start":          datetime.now().strftime("%Y-%m-%d"),
    "trading_active":     True,
    "paused":             False,
    "min_volume_usdt":    5000,  # СНИЖЕНО 06.08 (было 100000): слишком высокий
        # порог мог тихо блокировать сигналы по некрупным, но вполне ликвидным
        # монетам (IOST, ZK) — реальная безопасность обеспечивается честной
        # проверкой глубины стакана (walk-the-book) ниже по цепочке, этот
        # фильтр вспомогательный. Настраивается командой /setminvolume.
    "max_plausible_spread_pct": 5.0,  # НОВОЕ 05.08: спред выше этого — почти
        # гарантированно не реальная возможность, а артефакт тонкого/устаревшего
        # стакана (мёртвая заявка, которую никто не обновлял). На двух нормальных
        # биржах реальный устойчивый арбитражный спред живёт секунды и обычно
        # не превышает 1-2%. Спреды 13-23%, которые бот ловил на ZIL/HTX 04.08,
        # были именно таким артефактом — расчётная прибыль на бумаге, а по факту
        # реальное исполнение получало совсем другую (гораздо худшую) цену.
    "min_depth_levels_required": 10,  # ИСПРАВЛЕНО 07.08 (было 2, смысл проверки
        # тоже изменился — раньше считала потраченные на заполнение лота
        # уровни, теперь честно считает ОБЩУЮ глубину стакана, как /verify
        # и /scancandidates). 10 — разумный минимум, чтобы отсечь реально
        # тонкие стаканы, не отбраковывая здоровые (там обычно 20-50+).
    "max_topup_spend_per_day": 20.0,  # ПОВЫШЕНО 08.08 (было 5.0): при
        # активной, подтверждённой торговле (не той истории с ZIL, ради
        # которой лимит когда-то ставился) резерв под ОДНУ монету — уже
        # ~$10.5, и старые $5 не покрывали даже одно полноценное
        # пополнение. Лимит блокировал не мошенническую активность, а
        # нормальную работу — Binance осталась с $0.48 IOST вместо нужных
        # ~$10.5 и не могла пополниться до конца дня. Настраивается
        # командой /setmaxtopup.
        # НОВОЕ 05.08: дневной потолок трат на автодокупки — 04.08 они
        # съели $8.66 за один вечер на одной и той же проблемной паре,
        # нигде не отражаясь в видимой прибыли. Сбрасывается каждые сутки
        # автоматически (как и real_trades_today).
    "depth_limit":        50,      # сколько уровней стакана запрашиваем
    "rebalance_target_lots": 1,    # ЗАФИКСИРОВАНО 05.08 (было 3): сколько лотов держать в резерве USDT на ПОКУПКУ при авто-ребалансе — 1 лот достаточно, эти деньги и так пополняются перед каждой сделкой
    "sell_reserve_lots": 3,    # НОВОЕ 08.08: сколько лотов держать в резерве МОНЕТЫ на ПРОДАЖУ — здесь, наоборот, полезен запас на несколько сделок вперёд, чтобы не платить за докупку почти на каждой сделке. Настраивается через /setsellreserve
    "derating_factor":    0.25,    # реальность ≈ симуляция × 0.25 (ваша же оценка)

    # ===== ЭТАП 6: РЕАЛЬНОЕ ИСПОЛНЕНИЕ — ЖЁСТКИЙ ГЕЙТ =====
    # simulation_mode=False САМО ПО СЕБЕ не включает реальные ордера.
    # Нужны ОБА условия одновременно:
    #   1) переменная окружения REAL_TRADING_UNLOCKED == "YES-I-UNDERSTAND-THE-RISK"
    #   2) runtime-флаг real_confirmed, включаемый командой /confirmreal <фраза>
    # Если хоть одно условие не выполнено — бот принудительно торгует в símulation.
    "real_confirmed":       False,
    "max_real_order_usdt":  10.0,   # ЗАФИКСИРОВАНО 05.08 (было 15): ЖЁСТКИЙ потолок на один ордер, /setlot его не обходит
    "real_rebalance_dry_run": True,  # ПО УМОЛЧАНИЮ включено: первый реальный ребаланс
                                       # только показывает план, не размещает ордера,
                                       # пока вы явно не отключите через /rebalancelive
    "real_trades_today":    0,
    "real_start_capital":   None,  # фиксируется командой /setrealstart, для честного P&L в реальном режиме
    "max_real_trades_per_day": 200,  # поднято с 20 - для круглосуточной работы; /setmaxtrades меняет

    # ===== ЭТАП 4: ТРЕУГОЛЬНЫЙ АРБИТРАЖ =====
    "triangular_enabled": True,

    # ===== ИСПРАВЛЕНИЕ 04.08: ЛОЖНЫЕ ОТКАЗЫ "insufficient_real_balance" =====
    # Раньше буфер preflight-проверки (2%, захардкожен) и буфер, который
    # держит ребаланс (3%, тоже захардкожен), стояли слишком близко друг к
    # другу. На дешёвых монетах (типа ZIL, тысячи штук на $10) 2% превращаются
    # в десятки монет разницы — и любое небольшое движение цены между
    # моментом ребаланса и моментом сделки роняло проверку, хотя по сути
    # денег хватало "почти впритык". Теперь оба буфера настраиваемые и
    # разнесены по умолчанию (ребаланс держит намного больше, чем требует
    # проверка), плюс добавлена мгновенная точечная докупка нехватки.
    "balance_safety_buffer_pct": 1.0,   # % запас, который ТРЕБУЕТ preflight-проверка перед сделкой
    "rebalance_headroom_pct":    5.0,  # ЗАФИКСИРОВАНО 05.08 (было 15): % запас, который ЦЕЛЕНАПРАВЛЕННО держит ребаланс (должен быть заметно больше buffer_pct)
    "rebalance_headroom_overrides": {"HTX": 3.0},  # НОВОЕ 04.08: персональный % запаса для
        # конкретной биржи вместо общего rebalance_headroom_pct. HTX играет
        # ДВЕ роли (покупает в одной паре, продаёт в двух других) на
        # ограниченном капитале — стандартный общий запас (15%) там просто
        # физически не помещается. Здесь можно точечно снизить требование
        # именно для неё, не трогая безопасный запас на остальных биржах.
        # Меняется командой /setheadroomex БИРЖА N.
}

CONFIRM_PHRASE = "YES-I-UNDERSTAND-THE-RISK"

SYMBOLS = ["TRX"]   # ИСПРАВЛЕНО 05.08: этот список — дефолт, с которым бот
    # стартует при КАЖДОМ передеплое (SYMBOLS живёт только в памяти процесса,
    # /addcoin и /removecoin меняют его лишь до следующего рестарта). Раньше
    # тут были BONK/SEI/FET/INJ — мелкие альткоины с дырявой ликвидностью на
    # HTX, из-за которых после каждого обновления кода бот тихо откатывался
    # на них и требовал резерв под 4 монеты сразу ($45+ на биржу), хотя
    # реально торговалась только одна. TRX прошёл проверку /scancandidates
    # (50/50 и 50/20 уровней на всех трёх биржах, разброс цены 0.04%) —
    # меняйте этот список, только когда осознанно переключаетесь на другую
    # монету навсегда, а не через /addcoin в чате (это временно, до рестарта).
QUOTE   = "USDT"
BRIDGE  = "BTC"   # мост для треугольного арбитража: USDT -> COIN -> BTC -> USDT
PAIRS   = [
    ("KuCoin", "Binance"),
    # ИСПРАВЛЕНО 07.08 (было "Binance","KuCoin" — НАПРАВЛЕНИЕ БЫЛО ПЕРЕПУТАНО):
    # несколько дней подряд WorkerArbBot молчал (0 сигналов при ВСЕХ счётчиках
    # фильтров тоже на нуле — не тонкий стакан, не объём, не подозрительный
    # спред, а именно отсутствие положительного спреда в проверяемом
    # направлении). Тем временем монитор (TrialArbBot) на тех же IOST/YFI
    # стабильно подтверждал реальные сделки — но ВСЕГДА на маршруте
    # KuCoin→Binance (покупка на KuCoin, продажа на Binance), а не наоборот.
    # Бот честно считал правильно — просто проверял противоположное от
    # реально работающего направление. Теперь исправлено.
    #
    # ИЗМЕНЕНО 05.08 по решению: HTX убрана из торговли полностью. За всю
    # сессию именно HTX была источником почти всех проблем — тонкие стаканы
    # на альткоинах (ZIL, ZK, RVN), цены, оторванные от реального рынка на
    # 15-45%, постоянная нехватка баланса из-за двойной роли на скромном
    # капитале. Binance и KuCoin, наоборот, ни разу не подвели ни в одной
    # проверке /depthcheck или /scancandidates — стабильно 50/50 и 20/20+
    # уровней, разброс цены между ними — сотые доли процента.
    # HTX остаётся подключена (ключи/баланс не трогаем), но сделок через
    # неё больше не будет — при желании вернуть: добавить обратно строки
    # ("HTX","KuCoin"), ("KuCoin","HTX"), ("Binance","HTX").
]

# ИСПРАВЛЕНИЕ 05.08 (раунд 11): маршруты теперь МОЖНО задавать индивидуально
# для конкретной монеты через PAIR_OVERRIDES — не обязательно всем монетам
# использовать одни и те же биржи. Пример: TRX торгуется Binance→HTX (вместо
# общего Binance→KuCoin) — так HTX включена в оборот БЕЗ дополнительного
# капитала: резерв TRX, который раньше держала KuCoin, просто "переехал" на
# HTX, а не задублировался. Монеты, которых нет в PAIR_OVERRIDES, используют
# общий PAIRS (переименован в DEFAULT_PAIRS ниже) как раньше.
DEFAULT_PAIRS = PAIRS
PAIR_OVERRIDES: Dict[str, List[Tuple[str, str]]] = {
    "TRX": [("Binance", "HTX")],
}


def pairs_for_symbol(sym: str) -> List[Tuple[str, str]]:
    return PAIR_OVERRIDES.get(sym, DEFAULT_PAIRS)


FEES = {"Binance": 0.10, "KuCoin": 0.10, "HTX": 0.20}
SIM_START = 500.0

# Раскладка $500 согласно ролям бирж в PAIRS:
#   Binance — только покупает (Binance→HTX), монеты там не нужны вообще
#   KuCoin  — покупает (KuCoin→HTX) И продаёт (HTX→KuCoin) — нужны оба актива
#   HTX     — покупает (HTX→KuCoin) И продаёт в ДВУХ парах — самая нагруженная по монетам
ALLOCATION_USDT = {"Binance": 50.0, "KuCoin": 115.0, "HTX": 115.0}
ALLOCATION_COINS = {"KuCoin": 110.0, "HTX": 110.0}  # делится поровну между текущими SYMBOLS


def build_default_sim_balances() -> Dict[str, Dict[str, float]]:
    """КРИТИЧНО: каждая монета должна получить баланс минимум в несколько
    лотов (config['trade_usdt']), иначе has_sufficient_sim_balance() будет
    молча отклонять все сделки по этой монете, а сигналы при этом всё равно
    будут приходить (расчёт сигнала не знает о балансе кошелька) — именно
    это и произошло с FET/INJ при неровной ручной аллокации."""
    n = max(1, len(SYMBOLS))
    per_coin = round(ALLOCATION_COINS["KuCoin"] / n, 2)
    balances = {
        "Binance": {"USDT": ALLOCATION_USDT["Binance"]},
        "KuCoin":  {"USDT": ALLOCATION_USDT["KuCoin"]},
        "HTX":     {"USDT": ALLOCATION_USDT["HTX"]},
    }
    for sym in SYMBOLS:
        balances["KuCoin"][sym] = per_coin
        balances["HTX"][sym] = per_coin
    return balances


sim_balances = build_default_sim_balances()

stats = {
    "scans": 0, "signals": 0, "trades": 0, "profit": 0.0, "errors": 0,
    "start_time": datetime.now(),
    "trades_this_minute": 0, "minute_start": datetime.now(),
    "pair_stats":   {f"{b}→{s}": 0 for b, s in PAIRS},
    "symbol_stats": {s: 0 for s in SYMBOLS},
    "depth_fail":   {"Binance": 0, "KuCoin": 0, "HTX": 0},  # счётчик отказов стакана
    "insufficient_liquidity": 0,  # сколько раз стакана не хватило на объём
    "volume_fetch_fail": 0,  # НОВОЕ 04.08: сколько раз не удалось получить 24h-объём с Binance
                              # (раньше это было полностью невидимо и тихо блокировало ВСЕ сигналы)
    "insufficient_balance_skips": 0,  # сколько раз симуляция честно отказала из-за нехватки виртуального баланса
    "hourly_signals": defaultdict(int),
    "hourly_profit":  defaultdict(float),
    "topup_attempts": 0,   # НОВОЕ: сколько раз сработала точечная автодокупка
    "topup_success":  0,
}
trade_history: List[dict] = []
last_signal_time: Dict[str, float] = {}
coin_volumes: Dict[str, float] = {}
triangle_history: List[dict] = []

BINANCE_KEY    = os.environ.get("BINANCE_API_KEY", "")
BINANCE_SECRET = os.environ.get("BINANCE_API_SECRET", "")
KUCOIN_KEY     = os.environ.get("KUCOIN_API_KEY", "")
KUCOIN_SECRET  = os.environ.get("KUCOIN_API_SECRET", "")
KUCOIN_PASS    = os.environ.get("KUCOIN_PASSPHRASE", "")
HTX_KEY        = os.environ.get("HTX_API_KEY", "")
HTX_SECRET     = os.environ.get("HTX_API_SECRET", "")
REAL_TRADING_UNLOCKED = os.environ.get("REAL_TRADING_UNLOCKED", "")


# =====================================================================
# ORDER BOOK — реальная глубина, не top-of-book
# =====================================================================

# =====================================================================
# ЗАЩИТА ОТ БЛОКИРОВКИ БИРЖЕЙ ЗА ЧАСТЫЕ ЗАПРОСЫ
#
# Раньше при ответе 429 (rate limit) или 418 (у Binance — уже бан IP)
# бот просто логировал ошибку и на следующем скане через 10 сек снова
# бил по тому же адресу — то есть только усугублял блокировку.
# Теперь: при первом же 429/418 биржа "замораживается" на explicit
# период, все запросы к ней в это время пропускаются без попытки.
# =====================================================================

exchange_backoff_until: Dict[str, float] = {"Binance": 0.0, "KuCoin": 0.0, "HTX": 0.0}

# ИСПРАВЛЕНИЕ 04.08 (раунд 2): раньше при отказе размещения реального ордера
# биржа возвращала подробный текст ошибки (например, точную причину отказа —
# insufficient balance / precision / min notional и т.п.), но бот его нигде
# не показывал пользователю — ни в Telegram, ни даже в сообщении об ошибке
# внутри execute_real_arbitrage. Наружу уходила только маска вида
# "buy_leg_failed_on_HTX" без единой цифры или слова причины, из-за чего
# невозможно было понять, что именно не так — баланс, шаг лота, минимальная
# сумма ордера или что-то ещё. Теперь текст ответа биржи сохраняется здесь
# и подставляется в сообщение об ошибке.
_last_exchange_error: Dict[str, str] = {"Binance": "", "KuCoin": "", "HTX": ""}


def _remember_error(ex: str, detail) -> None:
    text = str(detail)
    if len(text) > 300:
        text = text[:300] + "…"
    _last_exchange_error[ex] = text


def is_backed_off(ex: str) -> bool:
    return time.time() < exchange_backoff_until.get(ex, 0.0)


def trigger_backoff(ex: str, status_code: int, retry_after: Optional[str] = None):
    """418 у Binance — это уже бан IP, обычно на 2 мин - несколько часов
    в зависимости от повторности нарушения; 429 — обычная перегрузка."""
    if retry_after:
        try:
            seconds = float(retry_after)
        except ValueError:
            seconds = 120 if status_code == 429 else 300
    else:
        seconds = 120 if status_code == 429 else 300
    exchange_backoff_until[ex] = time.time() + seconds
    logger.error(f"⛔ {ex} вернул {status_code} — заморожен на {seconds:.0f} сек")


async def get_orderbook_binance_rest(session, symbol: str) -> Optional[Dict]:
    """REST-фоллбэк: используется, только если WebSocket-стакан для этого
    символа ещё не поднялся/не синхронизирован, или для символов вне
    основного WS-набора (напр. в треугольном арбитраже)."""
    if is_backed_off("Binance"):
        return None
    url = "https://api.binance.com/api/v3/depth"
    params = {"symbol": f"{symbol}{QUOTE}", "limit": config["depth_limit"]}
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


# =====================================================================
# WEBSOCKET ORDER BOOK ДЛЯ BINANCE
#
# Живой локальный стакан через diff-события вместо REST-поллинга каждые
# 10 секунд. Синхронизация snapshot+diff — точно по документации Binance:
# https://binance-docs.github.io/apidocs/spot/en/#how-to-manage-a-local-order-book-correctly
# КуКоин и HTX пока остаются на REST — поэтапный переход, как договорились.
# =====================================================================

WS_BASE_BINANCE = "wss://stream.binance.com:9443/ws"


class BinanceLocalOrderBook:
    """Живой локальный стакан для ОДНОГО символа (напр. 'BONKUSDT')."""

    def __init__(self, symbol: str, depth_limit: int = 1000):
        self.symbol = symbol.upper()
        self.depth_limit = depth_limit
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.last_update_id: Optional[int] = None
        self.synced = False
        self.last_event_time = 0.0
        self.resync_count = 0
        self.event_count = 0
        self._buffer: List[dict] = []
        self._stop = False

    def get_book(self, depth: int = 50) -> Optional[Dict[str, List[Tuple[float, float]]]]:
        if not self.synced:
            return None
        if time.time() - self.last_event_time > 10:
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

    async def _get_snapshot(self, session: aiohttp.ClientSession) -> Optional[dict]:
        params = {"symbol": self.symbol, "limit": self.depth_limit}
        try:
            async with session.get("https://api.binance.com/api/v3/depth", params=params,
                                    timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    logger.error(f"{self.symbol} WS: snapshot HTTP {r.status}")
                    return None
                return await r.json()
        except Exception as e:
            logger.error(f"{self.symbol} WS: snapshot exception {e}")
            return None

    def _apply_level(self, book: Dict[float, float], price_str: str, qty_str: str):
        price = float(price_str)
        qty = float(qty_str)
        if qty == 0:
            book.pop(price, None)
        else:
            book[price] = qty

    def _apply_event(self, event: dict):
        for price, qty in event.get("b", []):
            self._apply_level(self.bids, price, qty)
        for price, qty in event.get("a", []):
            self._apply_level(self.asks, price, qty)
        self.last_update_id = event["u"]
        self.last_event_time = time.time()
        self.event_count += 1

    async def _resync(self, session: aiohttp.ClientSession):
        self.resync_count += 1
        self.synced = False
        self.bids.clear()
        self.asks.clear()

        snapshot = await self._get_snapshot(session)
        if not snapshot:
            return
        self.last_update_id = snapshot["lastUpdateId"]
        for price, qty in snapshot["bids"]:
            self._apply_level(self.bids, price, qty)
        for price, qty in snapshot["asks"]:
            self._apply_level(self.asks, price, qty)

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
            logger.info(f"{self.symbol} WS: синхронизирован (resync #{self.resync_count})")

    async def run(self, session: aiohttp.ClientSession):
        stream_url = f"{WS_BASE_BINANCE}/{self.symbol.lower()}@depth"
        backoff = 1
        while not self._stop:
            try:
                async with session.ws_connect(stream_url, heartbeat=20) as ws:
                    logger.info(f"{self.symbol} WS: подключен")
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
                            logger.warning(f"{self.symbol} WS: разрыв последовательности — пересинхронизация")
                            self._buffer = [event]
                            need_snapshot = True
                            continue

                        self._apply_event(event)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"{self.symbol} WS: ошибка {e}, реконнект через {backoff} сек")
                self.synced = False

            if self._stop:
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


binance_ws_books: Dict[str, BinanceLocalOrderBook] = {}
binance_ws_tasks: Dict[str, asyncio.Task] = {}


def start_binance_ws_book(session: aiohttp.ClientSession, symbol: str):
    """Поднимает WS-стакан для монеты (напр. 'BONK' → подписка на BONKUSDT)."""
    if symbol in binance_ws_books:
        return
    book = BinanceLocalOrderBook(f"{symbol}{QUOTE}")
    binance_ws_books[symbol] = book
    binance_ws_tasks[symbol] = asyncio.create_task(book.run(session))
    logger.info(f"Binance WS: запущен для {symbol}")


def stop_binance_ws_book(symbol: str):
    book = binance_ws_books.pop(symbol, None)
    task = binance_ws_tasks.pop(symbol, None)
    if book:
        book.stop()
    if task:
        task.cancel()
    logger.info(f"Binance WS: остановлен для {symbol}")


async def get_orderbook_binance(session, symbol: str) -> Optional[Dict]:
    """Главная точка входа — сначала пробует живой WS-стакан (мгновенно,
    без сетевого запроса), при недоступности падает на REST."""
    book = binance_ws_books.get(symbol)
    if book and book.is_healthy():
        snap = book.get_book(depth=config["depth_limit"])
        if snap:
            return snap
    return await get_orderbook_binance_rest(session, symbol)


async def get_orderbook_kucoin_rest(session, symbol: str) -> Optional[Dict]:
    """REST-фоллбэк на случай, если WS ещё не синхронизирован."""
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


async def get_orderbook_htx_rest(session, symbol: str) -> Optional[Dict]:
    """REST-фоллбэк на случай, если WS ещё не синхронизирован."""
    if is_backed_off("HTX"):
        return None
    url = "https://api.huobi.pro/market/depth"
    params = {"symbol": f"{symbol.lower()}{QUOTE.lower()}", "type": "step0"}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=6)) as r:
            if r.status in (429, 418):
                trigger_backoff("HTX", r.status, r.headers.get("Retry-After"))
                return None
            if r.status != 200:
                stats["depth_fail"]["HTX"] += 1
                return None
            data = (await r.json()).get("tick", {})
            bids = [(float(p), float(q)) for p, q in data.get("bids", [])][:config["depth_limit"]]
            asks = [(float(p), float(q)) for p, q in data.get("asks", [])][:config["depth_limit"]]
            if not bids or not asks:
                return None
            return {"bids": bids, "asks": asks}
    except Exception as e:
        stats["depth_fail"]["HTX"] += 1
        logger.error(f"HTX depth {symbol}: {e}")
        return None


# =====================================================================
# WEBSOCKET ORDER BOOK ДЛЯ KUCOIN
#
# Используется публичный канал level2Depth50 — биржа сама присылает готовый
# снимок топ-50 уровней при каждом изменении, не нужно мержить дельты по
# sequence-номерам вручную (проще и надёжнее, чем полный diff-подход).
# Протокол требует: 1) получить bullet-токен через REST,
# 2) подключиться с этим токеном, 3) слать ping каждые pingInterval мс.
# =====================================================================

KUCOIN_BULLET_URL = "https://api.kucoin.com/api/v1/bullet-public"


class KuCoinLocalOrderBook:
    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
        self.bids: List[Tuple[float, float]] = []
        self.asks: List[Tuple[float, float]] = []
        self.synced = False
        self.last_event_time = 0.0
        self.event_count = 0
        self.reconnect_count = 0
        self._stop = False

    def get_book(self, depth: int = 50) -> Optional[Dict[str, List[Tuple[float, float]]]]:
        if not self.synced or time.time() - self.last_event_time > 15:
            return None
        if not self.bids or not self.asks:
            return None
        return {"bids": self.bids[:depth], "asks": self.asks[:depth]}

    def is_healthy(self) -> bool:
        return self.synced and (time.time() - self.last_event_time) < 90

    def stop(self):
        self._stop = True

    async def _get_bullet_token(self, session: aiohttp.ClientSession) -> Optional[dict]:
        try:
            async with session.post(KUCOIN_BULLET_URL, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    logger.error(f"KuCoin bullet-public HTTP {r.status}")
                    return None
                data = await r.json()
                if data.get("code") != "200000":
                    logger.error(f"KuCoin bullet-public: {data}")
                    return None
                return data["data"]
        except Exception as e:
            logger.error(f"KuCoin bullet-public exception: {e}")
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
        self.event_count += 1

    async def _ping_loop(self, ws, interval_ms: int):
        interval = max(interval_ms / 1000 - 2, 5)
        try:
            while not self._stop:
                await asyncio.sleep(interval)
                await ws.send_json({"id": str(int(time.time() * 1000)), "type": "ping"})
        except (asyncio.CancelledError, ConnectionResetError):
            pass
        except Exception as e:
            logger.error(f"{self.symbol} KuCoin ping error: {e}")

    async def run(self, session: aiohttp.ClientSession):
        topic = f"/spotMarket/level2Depth50:{self.symbol}-{QUOTE}"
        backoff = 1
        while not self._stop:
            try:
                bullet = await self._get_bullet_token(session)
                if not bullet or not bullet.get("instanceServers"):
                    raise ConnectionError("Не удалось получить bullet-токен")

                server = bullet["instanceServers"][0]
                token = bullet["token"]
                connect_id = str(uuid.uuid4())
                ws_url = f"{server['endpoint']}?token={token}&connectId={connect_id}"
                ping_interval = server.get("pingInterval", 18000)

                async with session.ws_connect(ws_url, heartbeat=None) as ws:
                    logger.info(f"{self.symbol} KuCoin WS: подключен")
                    backoff = 1
                    welcome = await ws.receive_json(timeout=10)
                    if welcome.get("type") != "welcome":
                        raise ConnectionError(f"Не получен welcome: {welcome}")

                    ping_task = asyncio.create_task(self._ping_loop(ws, ping_interval))
                    await ws.send_json({
                        "id": str(int(time.time() * 1000)), "type": "subscribe",
                        "topic": topic, "privateChannel": False, "response": True,
                    })
                    try:
                        async for msg in ws:
                            if self._stop:
                                break
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                continue
                            data = json.loads(msg.data)
                            if data.get("type") == "message" and data.get("topic") == topic:
                                self._apply_snapshot(data.get("data", {}))
                            elif data.get("type") == "error":
                                logger.error(f"{self.symbol} KuCoin WS error msg: {data}")
                    finally:
                        ping_task.cancel()

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.reconnect_count += 1
                logger.error(f"{self.symbol} KuCoin WS: ошибка {e}, реконнект через {backoff} сек")
                self.synced = False

            if self._stop:
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


kucoin_ws_books: Dict[str, KuCoinLocalOrderBook] = {}
kucoin_ws_tasks: Dict[str, asyncio.Task] = {}


def start_kucoin_ws_book(session: aiohttp.ClientSession, symbol: str):
    if symbol in kucoin_ws_books:
        return
    book = KuCoinLocalOrderBook(symbol)
    kucoin_ws_books[symbol] = book
    kucoin_ws_tasks[symbol] = asyncio.create_task(book.run(session))
    logger.info(f"KuCoin WS: запущен для {symbol}")


def stop_kucoin_ws_book(symbol: str):
    book = kucoin_ws_books.pop(symbol, None)
    task = kucoin_ws_tasks.pop(symbol, None)
    if book:
        book.stop()
    if task:
        task.cancel()
    logger.info(f"KuCoin WS: остановлен для {symbol}")


async def get_orderbook_kucoin(session, symbol: str) -> Optional[Dict]:
    book = kucoin_ws_books.get(symbol)
    if book and book.is_healthy():
        snap = book.get_book(depth=config["depth_limit"])
        if snap:
            return snap
    return await get_orderbook_kucoin_rest(session, symbol)


# =====================================================================
# WEBSOCKET ORDER BOOK ДЛЯ HTX
#
# Особенность HTX: сообщения приходят GZIP-сжатыми бинарными фреймами
# (не текстом!), нужно распаковывать перед json.loads. Плюс сервер сам
# шлёт {"ping": ts} — клиент ОБЯЗАН ответить {"pong": ts}, иначе разрыв.
# =====================================================================

HTX_WS_URL = "wss://api.huobi.pro/ws"


class HTXLocalOrderBook:
    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
        self.bids: List[Tuple[float, float]] = []
        self.asks: List[Tuple[float, float]] = []
        self.synced = False
        self.last_event_time = 0.0
        self.event_count = 0
        self.reconnect_count = 0
        self._stop = False

    def get_book(self, depth: int = 50) -> Optional[Dict[str, List[Tuple[float, float]]]]:
        if not self.synced or time.time() - self.last_event_time > 15:
            return None
        if not self.bids or not self.asks:
            return None
        return {"bids": self.bids[:depth], "asks": self.asks[:depth]}

    def is_healthy(self) -> bool:
        return self.synced and (time.time() - self.last_event_time) < 90

    def stop(self):
        self._stop = True

    def _apply_snapshot(self, tick: dict):
        bids = [(float(p), float(q)) for p, q in tick.get("bids", [])]
        asks = [(float(p), float(q)) for p, q in tick.get("asks", [])]
        if not bids or not asks:
            return
        self.bids = sorted(bids, key=lambda x: -x[0])
        self.asks = sorted(asks, key=lambda x: x[0])
        self.synced = True
        self.last_event_time = time.time()
        self.event_count += 1

    async def run(self, session: aiohttp.ClientSession):
        channel = f"market.{self.symbol.lower()}{QUOTE.lower()}.depth.step0"
        backoff = 1
        while not self._stop:
            try:
                async with session.ws_connect(HTX_WS_URL, heartbeat=None) as ws:
                    logger.info(f"{self.symbol} HTX WS: подключен")
                    backoff = 1
                    await ws.send_json({"sub": channel, "id": f"sub_{self.symbol}"})

                    async for msg in ws:
                        if self._stop:
                            break
                        if msg.type != aiohttp.WSMsgType.BINARY:
                            continue
                        try:
                            raw = gzip.decompress(msg.data)
                            data = json.loads(raw)
                        except Exception as e:
                            logger.error(f"{self.symbol} HTX WS: ошибка распаковки {e}")
                            continue

                        if "ping" in data:
                            # Обязательный ответ, иначе биржа закроет соединение
                            await ws.send_json({"pong": data["ping"]})
                            continue

                        if data.get("ch") == channel and "tick" in data:
                            self._apply_snapshot(data["tick"])
                        elif data.get("status") == "error":
                            logger.error(f"{self.symbol} HTX WS error msg: {data}")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.reconnect_count += 1
                logger.error(f"{self.symbol} HTX WS: ошибка {e}, реконнект через {backoff} сек")
                self.synced = False

            if self._stop:
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


htx_ws_books: Dict[str, HTXLocalOrderBook] = {}
htx_ws_tasks: Dict[str, asyncio.Task] = {}


def start_htx_ws_book(session: aiohttp.ClientSession, symbol: str):
    if symbol in htx_ws_books:
        return
    book = HTXLocalOrderBook(symbol)
    htx_ws_books[symbol] = book
    htx_ws_tasks[symbol] = asyncio.create_task(book.run(session))
    logger.info(f"HTX WS: запущен для {symbol}")


def stop_htx_ws_book(symbol: str):
    book = htx_ws_books.pop(symbol, None)
    task = htx_ws_tasks.pop(symbol, None)
    if book:
        book.stop()
    if task:
        task.cancel()
    logger.info(f"HTX WS: остановлен для {symbol}")


async def get_orderbook_htx(session, symbol: str) -> Optional[Dict]:
    book = htx_ws_books.get(symbol)
    if book and book.is_healthy():
        snap = book.get_book(depth=config["depth_limit"])
        if snap:
            return snap
    return await get_orderbook_htx_rest(session, symbol)


async def get_24h_volume(session) -> Dict[str, float]:
    """ИСПРАВЛЕНИЕ 04.08 (раунд 8): раньше запрашивался ПОЛНЫЙ тикер по ВСЕМ
    парам Binance (без фильтра по символу) — это огромный ответ (тысячи
    пар), и при таймауте 8 сек запрос регулярно не успевал на медленной сети
    Railway. Исключение тихо ловилось и логировалось только в Railway-логи —
    ни в Telegram, ни в /stats это никак не было видно. В результате
    coin_volumes оставался пустым, и calc_arb_real() отбрасывал АБСОЛЮТНО
    ВСЕ сигналы на первой же строке (0 < min_volume_usdt), без единого следа
    в статистике — то, что и произошло: /top показывал "нет данных" при
    полностью исправных стаканах на всех трёх биржах.
    Теперь запрашиваем объём ТОЧЕЧНО по каждому нужному символу (маленький
    быстрый ответ, параллельно), и любой сбой инкрементирует видимый
    счётчик stats['volume_fetch_fail']."""
    volumes = {}
    if is_backed_off("Binance"):
        return volumes

    async def fetch_one(sym: str) -> Tuple[str, Optional[float]]:
        try:
            async with session.get(
                "https://api.binance.com/api/v3/ticker/24hr",
                params={"symbol": f"{sym}{QUOTE}"},
                timeout=aiohttp.ClientTimeout(total=6)
            ) as r:
                if r.status in (429, 418):
                    trigger_backoff("Binance", r.status, r.headers.get("Retry-After"))
                    return sym, None
                if r.status != 200:
                    return sym, None
                data = await r.json()
                return sym, float(data.get("quoteVolume", 0) or 0)
        except Exception as e:
            logger.error(f"Volume fetch {sym}: {e}")
            return sym, None

    results = await asyncio.gather(*[fetch_one(s) for s in SYMBOLS], return_exceptions=True)
    for res in results:
        if isinstance(res, Exception):
            stats["volume_fetch_fail"] = stats.get("volume_fetch_fail", 0) + 1
            continue
        sym, vol = res
        if vol is None:
            stats["volume_fetch_fail"] = stats.get("volume_fetch_fail", 0) + 1
        else:
            volumes[sym] = vol
    return volumes


# =====================================================================
# WALK THE BOOK — честный расчёт исполнения ордера
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
        "avg_price":    round(total_spent / total_coins, 8),
        "filled_usdt":  round(total_spent, 4),
        "coins":        round(total_coins, 6),
        "levels_used":  levels_used,
        "fully_filled": remaining <= 0.01,
    }


def walk_the_book_sell(levels: List[Tuple[float, float]], base_amount: float) -> Optional[Dict]:
    """Продаёт фиксированное количество БАЗОВОЙ монеты (не USDT) по стакану bids.
    Нужно для треугольного арбитража, где на каждом шаге меняется актив,
    а не сумма в USDT."""
    if not levels:
        return None
    remaining = base_amount
    total_quote = 0.0
    total_base = 0.0
    levels_used = 0

    for price, qty in levels:
        if remaining <= 0:
            break
        levels_used += 1
        take = min(qty, remaining)
        total_quote += take * price
        total_base += take
        remaining -= take

    if total_base == 0:
        return None

    return {
        "avg_price":    round(total_quote / total_base, 8),
        "quote_out":    round(total_quote, 8),
        "base_in":      round(total_base, 8),
        "levels_used":  levels_used,
        "fully_filled": remaining <= 1e-9,
    }


async def get_orderbook_pair_binance(session, pair_symbol: str) -> Optional[Dict]:
    """Обобщённая версия — принимает готовый символ пары (напр. 'FETBTC'),
    а не base+QUOTE. Нужна для треугольного арбитража."""
    if is_backed_off("Binance"):
        return None
    url = "https://api.binance.com/api/v3/depth"
    params = {"symbol": pair_symbol, "limit": config["depth_limit"]}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=6)) as r:
            if r.status in (429, 418):
                trigger_backoff("Binance", r.status, r.headers.get("Retry-After"))
                return None
            if r.status != 200:
                return None
            data = await r.json()
            bids = [(float(p), float(q)) for p, q in data.get("bids", [])]
            asks = [(float(p), float(q)) for p, q in data.get("asks", [])]
            if not bids or not asks:
                return None
            return {"bids": bids, "asks": asks}
    except Exception as e:
        logger.error(f"Binance pair depth {pair_symbol}: {e}")
        return None


# =====================================================================
# ЭТАП 4: ТРЕУГОЛЬНЫЙ АРБИТРАЖ (внутри одной биржи, Binance)
#   Путь A: USDT -> COIN -> BTC -> USDT
#   Путь B: USDT -> BTC -> COIN -> USDT
# =====================================================================

async def calc_triangle(session, symbol: str, start_usdt: float) -> Optional[dict]:
    """Считает оба направления треугольника COIN/USDT + COIN/BTC + BTC/USDT.
    Возвращает лучшее из двух направлений, если оно прибыльно после комиссий.
    Требует, чтобы пара COIN/BTC существовала на Binance — не для всех монет так,
    функция вернёт None, если пары нет (это нормально, не ошибка)."""

    ob_coin_usdt = await get_orderbook_pair_binance(session, f"{symbol}{QUOTE}")
    ob_coin_btc  = await get_orderbook_pair_binance(session, f"{symbol}{BRIDGE}")
    ob_btc_usdt  = await get_orderbook_pair_binance(session, f"{BRIDGE}{QUOTE}")

    if not ob_coin_usdt or not ob_coin_btc or not ob_btc_usdt:
        return None  # пары COIN/BTC может просто не существовать

    fee = FEES.get("Binance", 0.1) / 100
    results = []

    # --- Путь A: USDT -> COIN -> BTC -> USDT ---
    leg1 = walk_the_book(ob_coin_usdt["asks"], start_usdt)          # покупаем COIN за USDT
    if leg1 and leg1["fully_filled"]:
        coins_after_fee = leg1["coins"] * (1 - fee)
        leg2 = walk_the_book_sell(ob_coin_btc["bids"], coins_after_fee)  # продаём COIN за BTC
        if leg2 and leg2["fully_filled"]:
            btc_after_fee = leg2["quote_out"] * (1 - fee)
            leg3 = walk_the_book_sell(ob_btc_usdt["bids"], btc_after_fee)  # продаём BTC за USDT
            if leg3 and leg3["fully_filled"]:
                final_usdt = leg3["quote_out"] * (1 - fee)
                profit = final_usdt - start_usdt
                net_pct = profit / start_usdt * 100
                results.append({
                    "path": f"USDT→{symbol}→{BRIDGE}→USDT",
                    "final_usdt": round(final_usdt, 4),
                    "profit_usdt": round(profit, 4),
                    "net_pct": round(net_pct, 4),
                    "levels": [leg1["levels_used"], leg2["levels_used"], leg3["levels_used"]],
                })

    # --- Путь B: USDT -> BTC -> COIN -> USDT ---
    leg1b = walk_the_book(ob_btc_usdt["asks"], start_usdt)          # покупаем BTC за USDT
    if leg1b and leg1b["fully_filled"]:
        btc_after_fee = leg1b["coins"] * (1 - fee)
        leg2b = walk_the_book(ob_coin_btc["asks"], btc_after_fee)  # покупаем COIN за BTC
        # ВНИМАНИЕ: walk_the_book считает target в quote-валюте уровня (тут BTC) — подходит
        if leg2b and leg2b["fully_filled"]:
            coins_after_fee = leg2b["coins"] * (1 - fee)
            leg3b = walk_the_book_sell(ob_coin_usdt["bids"], coins_after_fee)  # продаём COIN за USDT
            if leg3b and leg3b["fully_filled"]:
                final_usdt = leg3b["quote_out"] * (1 - fee)
                profit = final_usdt - start_usdt
                net_pct = profit / start_usdt * 100
                results.append({
                    "path": f"USDT→{BRIDGE}→{symbol}→USDT",
                    "final_usdt": round(final_usdt, 4),
                    "profit_usdt": round(profit, 4),
                    "net_pct": round(net_pct, 4),
                    "levels": [leg1b["levels_used"], leg2b["levels_used"], leg3b["levels_used"]],
                })

    if not results:
        return None

    best = max(results, key=lambda x: x["net_pct"])
    if best["net_pct"] < config["min_profit_pct"]:
        return None
    best["symbol"] = symbol
    best["time"] = datetime.now().strftime("%H:%M:%S")
    return best


async def scan_triangles(session) -> List[dict]:
    if not config["triangular_enabled"]:
        return []
    found = []
    for sym in SYMBOLS:
        try:
            res = await calc_triangle(session, sym, config["trade_usdt"])
            if res:
                found.append(res)
        except Exception as e:
            logger.error(f"Triangle {sym}: {e}")
    found.sort(key=lambda x: x["net_pct"], reverse=True)
    return found


# =====================================================================
# АРБИТРАЖ — расчёт на основе реальной глубины
# =====================================================================

def calc_arb_real(symbol: str, buy_ex: str, buy_ob: Dict, sell_ex: str, sell_ob: Dict,
                   trade_usdt: float) -> Optional[dict]:
    # ИСПРАВЛЕНИЕ 04.08 (раунд 8): раньше отсутствие данных об объёме
    # (coin_volumes.get(symbol, 0) == 0 из-за сбоя get_24h_volume) трактовалось
    # как "объём заведомо мал" и БЕЗ следа в статистике отбрасывало сигнал.
    # Реальная ликвидность и так честно проверяется чуть ниже через
    # walk-the-book (fully_filled) — это первичная и куда более надёжная
    # проверка. 24h-объём — вторичный, вспомогательный фильтр. Поэтому
    # теперь он блокирует сигнал ТОЛЬКО если данные реально есть и объём
    # реально ниже порога; отсутствие данных (символ не значится в
    # coin_volumes вообще) сигнал не блокирует, а полагается на реальную
    # глубину стакана ниже.
    known_volume = coin_volumes.get(symbol)
    if known_volume is not None and known_volume < config["min_volume_usdt"]:
        stats["volume_too_low_rejected"] = stats.get("volume_too_low_rejected", 0) + 1
        return None

    buy_fill  = walk_the_book(buy_ob["asks"], trade_usdt)
    sell_fill = walk_the_book(sell_ob["bids"], trade_usdt)

    if not buy_fill or not sell_fill:
        return None

    if not buy_fill["fully_filled"] or not sell_fill["fully_filled"]:
        stats["insufficient_liquidity"] += 1
        return None  # стакана не хватило на заявленный объём — сигнал не считаем валидным

    buy_price  = buy_fill["avg_price"]
    sell_price = sell_fill["avg_price"]

    if sell_price <= buy_price:
        return None

    buy_fee  = FEES.get(buy_ex, 0.1) / 100
    sell_fee = FEES.get(sell_ex, 0.1) / 100

    gross = (sell_price - buy_price) / buy_price * 100
    net   = gross - buy_fee * 100 - sell_fee * 100

    if net < config["min_profit_pct"]:
        return None

    # ИСПРАВЛЕНИЕ 05.08: раньше сигнал с ЛЮБЫМ спредом выше порога считался
    # валидным — но 04.08 бот регулярно ловил спреды 13-23% на ZIL/HTX,
    # которые оказались не реальной прибылью, а искажением из-за тонкого,
    # неактуального стакана (1-2 заявки, которые никто не обновлял). Реальное
    # исполнение получало совсем другую цену, чем показывал снимок стакана,
    # и по факту баланс не рос, а падал, несмотря на "прибыльные" сделки на
    # бумаге. Теперь два защитных фильтра:
    if gross > config["max_plausible_spread_pct"]:
        stats["implausible_spread_rejected"] = stats.get("implausible_spread_rejected", 0) + 1
        return None
    # ИСПРАВЛЕНИЕ 07.08: раньше проверялось, сколько уровней стакана
    # ПОТРАЧЕНО на заполнение лота (buy_fill["levels_used"]) — но на
    # маленьком лоте ($10) и здоровом, глубоком стакане (50+ уровней) сделка
    # сплошь и рядом заполняется из ОДНОГО первого уровня — это НЕ признак
    # тонкого рынка, а наоборот, лучший возможный случай (минимальное
    # проскальзывание). Фильтр путал "мало уровней понадобилось" с "мало
    # уровней вообще есть" — из-за этого 19 честных сигналов подряд были
    # забракованы зря. Теперь проверяем ОБЩУЮ глубину стакана (сколько
    # уровней вообще выставлено), а не сколько из них съела наша маленькая
    # сделка — именно так эта же проверка уже давно и правильно устроена
    # в /scancandidates и /verify.
    min_levels = config["min_depth_levels_required"]
    if len(buy_ob["asks"]) < min_levels or len(sell_ob["bids"]) < min_levels:
        stats["thin_book_rejected"] = stats.get("thin_book_rejected", 0) + 1
        return None

    coins  = trade_usdt / buy_price
    profit = coins * sell_price * (1 - sell_fee) - trade_usdt * (1 + buy_fee)

    # Для сравнения — что показал бы старый наивный расчёт по top-of-book
    naive_buy  = buy_ob["asks"][0][0]
    naive_sell = sell_ob["bids"][0][0]
    naive_gross = (naive_sell - naive_buy) / naive_buy * 100
    slippage_impact_pct = round(naive_gross - gross, 4)

    return {
        "symbol":       symbol,
        "buy_ex":       buy_ex,
        "sell_ex":      sell_ex,
        "buy_price":    round(buy_price, 8),
        "sell_price":   round(sell_price, 8),
        "gross_pct":    round(gross, 4),
        "net_pct":      round(net, 4),
        "profit_usdt":  round(profit, 4),
        "coins":        round(coins, 6),
        "vol":          trade_usdt,
        "vol_24h":      round(coin_volumes.get(symbol, 0) / 1e6, 2),
        "levels_used_buy":  buy_fill["levels_used"],
        "levels_used_sell": sell_fill["levels_used"],
        "slippage_impact_pct": slippage_impact_pct,  # насколько наивный расчёт врал
        "time":         datetime.now().strftime("%H:%M:%S"),
    }


async def fetch_all_orderbooks(session) -> Tuple[Dict, Dict, Dict, List[str]]:
    tasks = {}
    for ex, fn in [("Binance", get_orderbook_binance),
                    ("KuCoin", get_orderbook_kucoin),
                    ("HTX", get_orderbook_htx)]:
        for sym in SYMBOLS:
            tasks[(ex, sym)] = fn(session, sym)

    keys = list(tasks.keys())
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)

    books = {"Binance": {}, "KuCoin": {}, "HTX": {}}
    for (ex, sym), res in zip(keys, results):
        if isinstance(res, Exception) or res is None:
            continue
        books[ex][sym] = res

    volumes = await get_24h_volume(session)
    for sym in SYMBOLS:
        if sym in volumes:
            coin_volumes[sym] = volumes[sym]

    active = [ex for ex, d in books.items() if d]
    return books["Binance"], books["KuCoin"], books["HTX"], active


async def scan_all(session) -> Tuple[List[dict], List[str]]:
    stats["scans"] += 1
    bn, kc, hx, active = await fetch_all_orderbooks(session)
    ex_map = {"Binance": bn, "KuCoin": kc, "HTX": hx}
    signals = []

    hour = datetime.now().hour
    for sym in SYMBOLS:
        for buy_ex, sell_ex in pairs_for_symbol(sym):
            bob = ex_map.get(buy_ex, {}).get(sym)
            sob = ex_map.get(sell_ex, {}).get(sym)
            if not bob or not sob:
                continue
            opp = calc_arb_real(sym, buy_ex, bob, sell_ex, sob, config["trade_usdt"])
            if opp:
                signals.append(opp)
                key = f"{buy_ex}→{sell_ex}"
                stats["pair_stats"][key] = stats["pair_stats"].get(key, 0) + 1
                stats["symbol_stats"][sym] = stats["symbol_stats"].get(sym, 0) + 1
                stats["hourly_signals"][hour] += 1

    signals.sort(key=lambda x: x["net_pct"], reverse=True)
    if signals:
        stats["signals"] += len(signals)
    return signals, active


# =====================================================================
# ЭТАП 6: РЕАЛЬНОЕ ИСПОЛНЕНИЕ ОРДЕРОВ
#
# ВНИМАНИЕ: эти функции ни разу не тестировались на реальном API —
# сетевой доступ к биржам недоступен в среде разработки. Схемы подписи
# реализованы по документации каждой биржи. ОБЯЗАТЕЛЬНО протестируйте
# сначала на минимальном ордере ($5-10), прежде чем доверять боту капитал.
# =====================================================================

def is_real_trading_allowed() -> bool:
    """Жёсткий гейт: ОБА условия обязательны, ни одно не заменяет другое."""
    env_ok = (REAL_TRADING_UNLOCKED == CONFIRM_PHRASE)
    runtime_ok = config["real_confirmed"]
    keys_ok = all([BINANCE_KEY, BINANCE_SECRET, KUCOIN_KEY, KUCOIN_SECRET,
                    KUCOIN_PASS, HTX_KEY, HTX_SECRET])
    return env_ok and runtime_ok and keys_ok


# =====================================================================
# ТРЕБОВАНИЕ 4 (ТЗ 30.07): ОКРУГЛЕНИЕ ПОД ПРАВИЛА БИРЖИ
#
# Причина сегодняшних отказов (LOT_SIZE у Binance, "increment invalid" у
# KuCoin, "precision-error" у HTX) — количество монеты в ордере считалось
# из симуляции "как есть", без округления под реальный шаг лота биржи.
# Получаем правила один раз на символ, кэшируем, округляем ВНИЗ (никогда
# не вверх — иначе можем продать/купить больше, чем реально есть).
# =====================================================================

import math

_binance_lot_step_cache: Dict[str, float] = {}
_kucoin_increment_cache: Dict[str, float] = {}
_htx_precision_cache: Dict[str, int] = {}


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
        logger.error(f"Binance lot step fetch {symbol}: {e}")
    return 1.0  # безопасный дефолт: округлит до целого, ордер хотя бы не отклонится по фильтру


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
        logger.error(f"KuCoin increment fetch {symbol}: {e}")
    return 1.0


async def get_htx_amount_precision(session, symbol: str) -> int:
    if symbol in _htx_precision_cache:
        return _htx_precision_cache[symbol]
    try:
        async with session.get("https://api.huobi.pro/v1/common/symbols",
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json()
            for s in data.get("data", []):
                if s.get("symbol") == f"{symbol.lower()}{QUOTE.lower()}":
                    prec = int(s["amount-precision"])
                    _htx_precision_cache[symbol] = prec
                    return prec
    except Exception as e:
        logger.error(f"HTX precision fetch {symbol}: {e}")
    return 0  # безопасный дефолт: округлит до целого


def _round_down_to_step(qty: float, step: float) -> float:
    if step <= 0:
        return qty
    return math.floor(qty / step) * step


def _round_down_to_precision(qty: float, decimals: int) -> float:
    factor = 10 ** decimals
    return math.floor(qty * factor) / factor


async def round_quantity_for_exchange(session, ex: str, symbol: str, raw_qty: float) -> float:
    """Округляет количество монеты ВНИЗ под реальные правила конкретной
    биржи. Обязательно вызывать перед ЛЮБЫМ реальным ордером на продажу
    (и на покупку, если объём задаётся в количестве монет, а не в USDT)."""
    if ex == "Binance":
        step = await get_binance_lot_step(session, symbol)
        result = _round_down_to_step(raw_qty, step)
    elif ex == "KuCoin":
        inc = await get_kucoin_base_increment(session, symbol)
        result = _round_down_to_step(raw_qty, inc)
    elif ex == "HTX":
        prec = await get_htx_amount_precision(session, symbol)
        result = _round_down_to_precision(raw_qty, prec)
    else:
        result = raw_qty
    return round(result, 10)  # чистим float-мусор (1901.1000000000001 → 1901.1)


# =====================================================================
# ТРЕБОВАНИЕ 1 (ТЗ 30.07): ПРОВЕРКА РЕАЛЬНОГО ИСПОЛНЕНИЯ ОРДЕРА
#
# Раньше бот считал ногу сделки успешной сразу по факту, что POST-запрос
# на размещение ордера прошёл (HTTP 200) — но у KuCoin и HTX ответ на
# размещение MARKET-ордера возвращает только orderId, БЕЗ подтверждения
# реального исполнения. Именно так родилась зависшая позиция на Binance:
# бот решил, что купил, хотя реального fill не проверял по факту с биржи.
# Теперь — обязательный опрос статуса ордера (до 3 сек, каждые 300 мс),
# и только подтверждённый FILLED считается успехом.
# =====================================================================

async def wait_for_binance_fill(session, symbol: str, order_id, timeout: float = 3.0) -> Optional[float]:
    """Возвращает РЕАЛЬНО исполненное количество монет, или None если не
    исполнилось/отменилось за отведённое время."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        ts = int(time.time() * 1000)
        params = {"symbol": f"{symbol}{QUOTE}", "orderId": order_id, "timestamp": ts, "recvWindow": 5000}
        params["signature"] = sign_binance(params, BINANCE_SECRET)
        headers = {"X-MBX-APIKEY": BINANCE_KEY}
        try:
            async with session.get("https://api.binance.com/api/v3/order", params=params,
                                    headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as r:
                data = await r.json()
                if data.get("status") == "FILLED":
                    return float(data.get("executedQty", 0))
                if data.get("status") in ("CANCELED", "REJECTED", "EXPIRED"):
                    return None
        except Exception as e:
            logger.error(f"Binance fill check {symbol}: {e}")
        await asyncio.sleep(0.3)
    return None


async def wait_for_kucoin_fill(session, order_id: str, timeout: float = 3.0) -> Optional[float]:
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
            logger.error(f"KuCoin fill check {order_id}: {e}")
        await asyncio.sleep(0.3)
    return None


async def wait_for_htx_fill(session, order_id, timeout: float = 3.0) -> Optional[float]:
    deadline = time.time() + timeout
    host = "api.huobi.pro"
    endpoint = f"/v1/order/orders/{order_id}"
    while time.time() < deadline:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        params = {"AccessKeyId": HTX_KEY, "SignatureMethod": "HmacSHA256",
                  "SignatureVersion": "2", "Timestamp": ts}
        sorted_params = sorted(params.items())
        query = urllib.parse.urlencode(sorted_params)
        payload = f"GET\n{host}\n{endpoint}\n{query}"
        signature = base64.b64encode(
            hmac.new(HTX_SECRET.encode(), payload.encode(), hashlib.sha256).digest()
        ).decode()
        params["Signature"] = signature
        try:
            async with session.get(f"https://{host}{endpoint}", params=params,
                                    timeout=aiohttp.ClientTimeout(total=5)) as r:
                data = await r.json()
                order_data = data.get("data", {})
                state = order_data.get("state")
                if state == "filled":
                    return float(order_data.get("field-amount", 0))
                if state in ("canceled", "partial-canceled"):
                    return None
        except Exception as e:
            logger.error(f"HTX fill check {order_id}: {e}")
        await asyncio.sleep(0.3)
    return None


async def confirm_fill_and_get_qty(session, ex: str, buy_result: dict) -> Optional[float]:
    """Единая точка: извлекает order_id из ответа биржи на размещение
    ордера и ждёт подтверждения РЕАЛЬНОГО исполнения. Возвращает None,
    если исполнение не подтвердилось — тогда вторую ногу открывать нельзя."""
    if ex == "Binance":
        order_id = buy_result.get("orderId")
        if buy_result.get("status") == "FILLED":
            return float(buy_result.get("executedQty", 0))  # уже пришло сразу в ответе
        return await wait_for_binance_fill(session, buy_result.get("symbol", "")[:-len(QUOTE)], order_id)
    elif ex == "KuCoin":
        order_id = buy_result.get("data", {}).get("orderId")
        if not order_id:
            return None
        return await wait_for_kucoin_fill(session, order_id)
    elif ex == "HTX":
        order_id = buy_result.get("data")
        if not order_id:
            return None
        return await wait_for_htx_fill(session, order_id)
    return None


def sign_binance(params: dict, secret: str) -> str:
    query = urllib.parse.urlencode(params)
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()


async def place_order_binance(session, symbol: str, side: str, quote_usdt: float) -> Optional[dict]:
    """MARKET ордер на Binance. side: 'BUY' или 'SELL'.
    quoteOrderQty — тратим/получаем ровно X USDT, биржа сама считает количество монет
    (для BUY). Для SELL используем quantity в монетах — нужно передавать заранее
    посчитанное количество через отдельный параметр (см. execute_real_arbitrage)."""
    if is_backed_off("Binance"):
        logger.error("Binance в бэкоффе — реальный ордер НЕ отправлен")
        return None
    url = "https://api.binance.com/api/v3/order"
    ts = int(time.time() * 1000)
    params = {
        "symbol": f"{symbol}{QUOTE}", "side": side, "type": "MARKET",
        "timestamp": ts, "recvWindow": 5000,
    }
    if side == "BUY":
        params["quoteOrderQty"] = round(quote_usdt, 2)
    else:
        # для SELL quote_usdt здесь на самом деле означает "количество монет"
        # (см. вызывающий код) — параметр переиспользован, чтобы не плодить сигнатуры
        params["quantity"] = quote_usdt
    params["signature"] = sign_binance(params, BINANCE_SECRET)
    headers = {"X-MBX-APIKEY": BINANCE_KEY}
    try:
        async with session.post(url, params=params, headers=headers,
                                 timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status in (429, 418):
                trigger_backoff("Binance", r.status, r.headers.get("Retry-After"))
                return None
            data = await r.json()
            if r.status != 200:
                logger.error(f"Binance order failed: {data}")
                _remember_error("Binance", data.get("msg", data))
                return None
            return data
    except Exception as e:
        logger.error(f"Binance order exception: {e}")
        _remember_error("Binance", e)
        return None


def sign_kucoin(secret: str, passphrase: str, ts: str, method: str, endpoint: str, body: str = ""):
    str_to_sign = f"{ts}{method}{endpoint}{body}"
    signature = base64.b64encode(
        hmac.new(secret.encode(), str_to_sign.encode(), hashlib.sha256).digest()
    ).decode()
    passphrase_signed = base64.b64encode(
        hmac.new(secret.encode(), passphrase.encode(), hashlib.sha256).digest()
    ).decode()
    return signature, passphrase_signed


async def place_order_kucoin(session, symbol: str, side: str, funds_or_size: float,
                               use_funds: bool = True) -> Optional[dict]:
    """MARKET ордер на KuCoin. use_funds=True: сумма в USDT (для BUY).
    use_funds=False: количество монет (для SELL)."""
    if is_backed_off("KuCoin"):
        logger.error("KuCoin в бэкоффе — реальный ордер НЕ отправлен")
        return None
    endpoint = "/api/v1/orders"
    url = f"https://api.kucoin.com{endpoint}"
    ts = str(int(time.time() * 1000))
    body_dict = {
        "clientOid": str(int(time.time() * 1000000)),
        "side": side.lower(), "symbol": f"{symbol}-{QUOTE}", "type": "market",
    }
    if use_funds:
        body_dict["funds"] = str(round(funds_or_size, 4))
    else:
        body_dict["size"] = str(funds_or_size)

    import json
    body_str = json.dumps(body_dict)
    signature, passphrase_signed = sign_kucoin(KUCOIN_SECRET, KUCOIN_PASS, ts, "POST", endpoint, body_str)
    headers = {
        "KC-API-KEY": KUCOIN_KEY, "KC-API-SIGN": signature, "KC-API-TIMESTAMP": ts,
        "KC-API-PASSPHRASE": passphrase_signed, "KC-API-KEY-VERSION": "2",
        "Content-Type": "application/json",
    }
    try:
        async with session.post(url, data=body_str, headers=headers,
                                 timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status in (429, 418):
                trigger_backoff("KuCoin", r.status, r.headers.get("Retry-After"))
                return None
            data = await r.json()
            if r.status != 200 or data.get("code") != "200000":
                logger.error(f"KuCoin order failed: {data}")
                _remember_error("KuCoin", data.get("msg", data))
                return None
            return data
    except Exception as e:
        logger.error(f"KuCoin order exception: {e}")
        _remember_error("KuCoin", e)
        return None


async def place_order_htx(session, account_id: str, symbol: str, side: str,
                            amount: float) -> Optional[dict]:
    """MARKET ордер на HTX. side: 'buy-market' или 'sell-market'.
    Для buy-market amount = сумма в USDT. Для sell-market amount = количество монет.
    Требует account_id — получить через /v1/account/accounts (см. get_htx_account_id)."""
    if is_backed_off("HTX"):
        logger.error("HTX в бэкоффе — реальный ордер НЕ отправлен")
        return None
    host = "api.huobi.pro"
    endpoint = "/v1/order/orders/place"
    method = "POST"
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    params = {
        "AccessKeyId": HTX_KEY, "SignatureMethod": "HmacSHA256",
        "SignatureVersion": "2", "Timestamp": ts,
    }
    sorted_params = sorted(params.items())
    query = urllib.parse.urlencode(sorted_params)
    payload = f"{method}\n{host}\n{endpoint}\n{query}"
    signature = base64.b64encode(
        hmac.new(HTX_SECRET.encode(), payload.encode(), hashlib.sha256).digest()
    ).decode()
    params["Signature"] = signature

    body = {
        "account-id": account_id, "symbol": f"{symbol.lower()}{QUOTE.lower()}",
        "type": side, "amount": str(amount), "source": "spot-api",
    }
    url = f"https://{host}{endpoint}"
    try:
        async with session.post(url, params=params, json=body,
                                 timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status in (429, 418):
                trigger_backoff("HTX", r.status, r.headers.get("Retry-After"))
                return None
            data = await r.json()
            if data.get("status") != "ok":
                logger.error(f"HTX order failed: {data}")
                _remember_error("HTX", data.get("err-msg", data))
                return None
            return data
    except Exception as e:
        logger.error(f"HTX order exception: {e}")
        _remember_error("HTX", e)
        return None


# =====================================================================
# ТРЕБОВАНИЕ 3 (ТЗ 30.07): ДИНАМИЧЕСКИЕ КОМИССИИ ВМЕСТО СТАТИЧНЫХ
#
# FEES = {"Binance": 0.10, "KuCoin": 0.10, "HTX": 0.20} — это разумные
# дефолты, но реальная комиссия аккаунта может отличаться (скидки за объём,
# использование биржевого токена для оплаты комиссии и т.п.). Команда
# /realfees подтягивает фактические комиссии и обновляет FEES на лету.
# Намеренно НЕ вызывается автоматически при старте — если хоть один запрос
# упадёт (неверный формат ответа, смена биржей API), последствия должны
# быть локальными ("не обновили один процент"), а не блокировать весь бот.
# =====================================================================

async def fetch_real_fee_binance(session, symbol: str) -> Optional[float]:
    if is_backed_off("Binance"):
        return None
    ts = int(time.time() * 1000)
    params = {"symbol": f"{symbol}{QUOTE}", "timestamp": ts, "recvWindow": 5000}
    params["signature"] = sign_binance(params, BINANCE_SECRET)
    headers = {"X-MBX-APIKEY": BINANCE_KEY}
    try:
        async with session.get("https://api.binance.com/sapi/v1/asset/tradeFee", params=params,
                                headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json()
            if isinstance(data, list) and data:
                return round(float(data[0]["takerCommission"]) * 100, 4)  # доля → проценты
            logger.error(f"Binance fee: неожиданный формат ответа {data}")
    except Exception as e:
        logger.error(f"Binance fee fetch exception: {e}")
    return None


async def fetch_real_fee_kucoin(session, symbol: str) -> Optional[float]:
    if is_backed_off("KuCoin"):
        return None
    endpoint = "/api/v1/trade-fees"
    query = f"symbols={symbol}-{QUOTE}"
    ts = str(int(time.time() * 1000))
    signature, passphrase_signed = sign_kucoin(KUCOIN_SECRET, KUCOIN_PASS, ts, "GET", f"{endpoint}?{query}", "")
    headers = {"KC-API-KEY": KUCOIN_KEY, "KC-API-SIGN": signature, "KC-API-TIMESTAMP": ts,
               "KC-API-PASSPHRASE": passphrase_signed, "KC-API-KEY-VERSION": "2"}
    try:
        async with session.get(f"https://api.kucoin.com{endpoint}?{query}", headers=headers,
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json()
            items = data.get("data", [])
            if items:
                return round(float(items[0]["takerFeeRate"]) * 100, 4)
            logger.error(f"KuCoin fee: неожиданный формат ответа {data}")
    except Exception as e:
        logger.error(f"KuCoin fee fetch exception: {e}")
    return None


async def fetch_real_fee_htx(session, symbol: str) -> Optional[float]:
    if is_backed_off("HTX"):
        return None
    host = "api.huobi.pro"
    endpoint = "/v2/reference/transact-fee-rate"
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    params = {"AccessKeyId": HTX_KEY, "SignatureMethod": "HmacSHA256", "SignatureVersion": "2",
              "Timestamp": ts, "symbols": f"{symbol.lower()}{QUOTE.lower()}"}
    sorted_params = sorted(params.items())
    query = urllib.parse.urlencode(sorted_params)
    payload = f"GET\n{host}\n{endpoint}\n{query}"
    signature = base64.b64encode(
        hmac.new(HTX_SECRET.encode(), payload.encode(), hashlib.sha256).digest()
    ).decode()
    params["Signature"] = signature
    try:
        async with session.get(f"https://{host}{endpoint}", params=params,
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json()
            items = data.get("data", [])
            if items:
                return round(float(items[0]["actualTakerRate"]) * 100, 4)
            logger.error(f"HTX fee: неожиданный формат ответа {data}")
    except Exception as e:
        logger.error(f"HTX fee fetch exception: {e}")
    return None


async def refresh_real_fees(session, symbol: str) -> Dict[str, Optional[float]]:
    """Обновляет FEES реальными значениями там, где удалось получить.
    Возвращает то, что реально получилось (для сообщения в Telegram)."""
    results = {}
    binance_fee = await fetch_real_fee_binance(session, symbol)
    if binance_fee is not None:
        FEES["Binance"] = binance_fee
    results["Binance"] = binance_fee

    kucoin_fee = await fetch_real_fee_kucoin(session, symbol)
    if kucoin_fee is not None:
        FEES["KuCoin"] = kucoin_fee
    results["KuCoin"] = kucoin_fee

    htx_fee = await fetch_real_fee_htx(session, symbol)
    if htx_fee is not None:
        FEES["HTX"] = htx_fee
    results["HTX"] = htx_fee

    return results


async def get_htx_account_id(session) -> Optional[str]:
    if is_backed_off("HTX"):
        return None
    host = "api.huobi.pro"
    endpoint = "/v1/account/accounts"
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    params = {"AccessKeyId": HTX_KEY, "SignatureMethod": "HmacSHA256",
              "SignatureVersion": "2", "Timestamp": ts}
    sorted_params = sorted(params.items())
    query = urllib.parse.urlencode(sorted_params)
    payload = f"GET\n{host}\n{endpoint}\n{query}"
    signature = base64.b64encode(
        hmac.new(HTX_SECRET.encode(), payload.encode(), hashlib.sha256).digest()
    ).decode()
    params["Signature"] = signature
    try:
        async with session.get(f"https://{host}{endpoint}", params=params,
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status in (429, 418):
                trigger_backoff("HTX", r.status, r.headers.get("Retry-After"))
                return None
            data = await r.json()
            acc_list = data.get("data") or []
            for acc in acc_list:
                if acc.get("type") == "spot":
                    return str(acc["id"])
            # Дошли сюда — либо data пуст/None, либо нет spot-аккаунта.
            # Логируем ПОЛНЫЙ ответ биржи, чтобы видеть настоящую причину
            # (обычно это ошибка прав ключа или неверная подпись).
            logger.error(f"HTX account id: не нашли spot-аккаунт, полный ответ: {data}")
    except Exception as e:
        logger.error(f"HTX account id exception: {e}")
    return None


_htx_account_id_cache: Optional[str] = None
_last_auto_rebalance_attempt: float = 0.0
AUTO_REBALANCE_COOLDOWN = 30  # сек — не пытаться ребалансить чаще, чем раз в 30 сек


MIN_ORDER_VALUE_USD = {"Binance": 5.0, "KuCoin": 1.0, "HTX": 10.0}  # НАХОДКА 02.08:
# HTX отклоняет любой ордер дешевле $10 ("order-value-min-error") — именно
# поэтому ребаланс молча не мог докупить ZK на HTX, когда цель ($10) была
# впритык к минимуму: нужная докупка ($9.92) оказывалась ЧУТЬ ниже порога.


async def top_up_coin_reserve(session, ex: str, symbol: str, shortfall_qty: float,
                               price_hint: float) -> bool:
    """ИСПРАВЛЕНИЕ 04.08: точечная мгновенная докупка нехватающей монеты.

    Раньше, если preflight-проверка перед сделкой обнаруживала нехватку
    буквально на пару процентов (как было с ZIL: нужно 3595.83, есть
    3665.50 — не хватало 2% буфера), сделка просто отклонялась, а
    восстановление баланса откладывалось на общий /rebalance (который
    считает по ВСЕМ монетам сразу и с тем же тесным буфером мог опять
    попасть впритык).

    Эта функция вместо этого докупает НАПРЯМУЮ и НЕМЕДЛЕННО именно
    недостающее количество, с запасом сверху, прямо на бирже продажи —
    и сделка может продолжиться в той же попытке, без ожидания."""
    if is_backed_off(ex):
        return False
    if not price_hint or price_hint <= 0:
        return False

    # ИСПРАВЛЕНИЕ 05.08: раньше автодокупка срабатывала БЕЗ ограничений —
    # 04.08 она сработала 5 раз подряд на одной и той же паре ZIL/HTX-KuCoin
    # (реальная стоимость которых, ~$8.66, нигде не отражалась в отчётах).
    # Если докупка нужна СНОВА и СНОВА на одной и той же связке — это не
    # разовая мелкая коррекция, а признак структурной проблемы (нестабильный,
    # слишком тонкий рынок), и продолжать докупать за реальные деньги —
    # значит просто платить за симптом снова и снова. Дневной потолок трат
    # на автодокупки останавливает это автоматически.
    if stats.get("topup_cost_usdt", 0.0) >= config["max_topup_spend_per_day"]:
        logger.error(f"⛔ Дневной лимит трат на автодокупки (${config['max_topup_spend_per_day']}) "
                      f"исчерпан — докупка {symbol} на {ex} пропущена")
        return False

    stats["topup_attempts"] += 1
    # Берём не только сам shortfall, но и запас сверху (+8%), чтобы после
    # этой докупки следующая сделка не уткнулась в тот же порог снова.
    usd_needed = round(shortfall_qty * price_hint * 1.08, 2)
    usd_needed = max(usd_needed, MIN_ORDER_VALUE_USD.get(ex, 5.0))  # не меньше минимума биржи

    result = None
    if ex == "Binance":
        result = await place_order_binance(session, symbol, "BUY", usd_needed)
    elif ex == "KuCoin":
        result = await place_order_kucoin(session, symbol, "buy", usd_needed, use_funds=True)
    elif ex == "HTX":
        global _htx_account_id_cache
        if not _htx_account_id_cache:
            _htx_account_id_cache = await get_htx_account_id(session)
        if _htx_account_id_cache:
            result = await place_order_htx(session, _htx_account_id_cache, symbol, "buy-market", usd_needed)

    if result:
        stats["topup_success"] += 1
        stats["topup_cost_usdt"] = stats.get("topup_cost_usdt", 0.0) + usd_needed
        logger.info(f"✅ Точечная докупка {symbol} на {ex}: ~${usd_needed} размещена "
                     f"(нехватка была {shortfall_qty:.2f} {symbol})")
        if CHAT_ID:
            await send_tg(session,
                f"🔧 *Автодокупка*: не хватало {shortfall_qty:.2f} {symbol} на {ex} "
                f"перед сделкой — докупил на ~${usd_needed} и продолжаю. "
                f"(итого потрачено на автодокупки сегодня: ~${stats['topup_cost_usdt']:.2f} "
                f"из лимита ${config['max_topup_spend_per_day']})")
    else:
        logger.error(f"❌ Точечная докупка {symbol} на {ex} не удалась")
    return bool(result)


async def execute_real_arbitrage(session, opp: dict) -> dict:
    """Исполняет РЕАЛЬНУЮ сделку с ЖЁСТКИМ лимитом на объём.
    Возвращает результат с полями success/error/emergency_close для логирования.
    КРИТИЧНО: если вторая нога не исполнилась — пытаемся аварийно закрыть
    позицию, купленную на первой ноге, продав её обратно на той же бирже."""
    global _htx_account_id_cache

    if not is_real_trading_allowed():
        return {"success": False, "error": "real_trading_not_unlocked"}

    if config["real_trades_today"] >= config["max_real_trades_per_day"]:
        return {"success": False, "error": "daily_real_trade_limit_reached"}

    vol = min(opp["vol"], config["max_real_order_usdt"])  # ЖЁСТКИЙ потолок, /setlot его не обходит
    symbol, buy_ex, sell_ex = opp["symbol"], opp["buy_ex"], opp["sell_ex"]

    # НАХОДКА 03.08: та же проблема с минимумом биржи, что чинили в ребалансе,
    # оказывается актуальна и для самой сделки — HTX отклоняет ЛЮБОЙ ордер
    # дешевле $10 ('order-value-min-error'). Если лот меньше минимума хотя бы
    # одной из двух бирж сделки — поднимаем объём до минимума (не выше
    # потолка безопасности $15), иначе сделка гарантированно отклонится.
    required_min = max(MIN_ORDER_VALUE_USD.get(buy_ex, 0), MIN_ORDER_VALUE_USD.get(sell_ex, 0))
    if vol < required_min:
        if required_min > 15.0:
            return {"success": False,
                    "error": f"min_order_value_exceeds_safety_ceiling: "
                             f"{buy_ex}/{sell_ex} требуют ${required_min}, потолок $15"}
        vol = required_min

    # ИСПРАВЛЕНИЕ 04.08 (раунд 4): раньше здесь ВООБЩЕ не проверялся баланс
    # USDT на бирже ПОКУПКИ (buy_ex) — проверялась только монета на бирже
    # продажи. HTX в вашей конфигурации играет ДВОЙНУЮ роль (и покупает
    # HTX→KuCoin, и продаёт KuCoin→HTX), поэтому её капитал разделён между
    # свободными USDT (нужны для покупки) и резервом монеты (нужен для
    # продажи) — при небольшом общем балансе USDT может НЕ хватать на
    # полный лот, хотя формально биржа "в целом" не пустая. Раньше бот
    # слепо пытался купить на всю сумму vol и получал прямой отказ биржи
    # ("trade account balance is not enough"). Теперь: если свободных USDT
    # чуть меньше vol — сделка автоматически уменьшается до того, что
    # реально есть (не ниже биржевого минимума), вместо полного отказа.
    buy_balances = await get_real_balances(session, buy_ex)
    if buy_balances is None:
        return {"success": False, "error": f"could_not_verify_buy_balance_on_{buy_ex}"}
    available_usdt_on_buy_ex = buy_balances.get("USDT", 0.0)
    usdt_buffer_mult = 1 + config["balance_safety_buffer_pct"] / 100
    if available_usdt_on_buy_ex < vol * usdt_buffer_mult:
        # Пробуем ужать сделку до реально доступных USDT (оставляя чуть-чуть
        # запаса на комиссию), а не просто отказывать
        shrunk_vol = round(available_usdt_on_buy_ex / usdt_buffer_mult, 2)
        if shrunk_vol >= required_min:
            logger.info(f"Уменьшаю объём сделки {buy_ex}: ${vol} → ${shrunk_vol} "
                         f"(свободно USDT: {available_usdt_on_buy_ex:.2f})")
            vol = shrunk_vol
        else:
            return {"success": False,
                    "error": f"insufficient_usdt_on_{buy_ex}: "
                             f"нужно ~${vol} для лота, свободно ${available_usdt_on_buy_ex:.2f} "
                             f"(меньше биржевого минимума ${required_min}) — "
                             f"нужен ручной перевод USDT на {buy_ex} или /rebalance"}

    # НАХОДКА 31.07: у каждой биржи своя НЕЗАВИСИМАЯ предзаведённая монета —
    # купленное на buy_ex физически не переносится на sell_ex. Раньше бот
    # покупал вслепую, не проверив, хватит ли монеты на sell_ex для продажи —
    # отсюда застревания. Теперь проверяем РЕАЛЬНЫЙ баланс sell_ex ДО покупки.
    sell_balances = await get_real_balances(session, sell_ex)
    if sell_balances is None:
        return {"success": False, "error": f"could_not_verify_sell_balance_on_{sell_ex}"}
    # ИСПРАВЛЕНИЕ 04.08 (раунд 3, КОРНЕВАЯ ПРИЧИНА): раньше здесь бралась
    # opp["sell_price"] — но реальное количество монеты, которое придётся
    # продать на sell_ex, определяется тем, СКОЛЬКО РЕАЛЬНО КУПЯТ на buy_ex
    # (confirmed_qty = vol / buy_price), а НЕ ценой продажи. Поскольку в
    # арбитраже buy_price ВСЕГДА меньше sell_price (иначе сделки бы не было),
    # vol/buy_price systematически БОЛЬШЕ, чем vol/sell_price — то есть эта
    # проверка систематически НЕДООЦЕНИВАЛА нужное количество монеты на
    # величину самого спреда сделки. Отсюда и "Balance insufficient!" от
    # KuCoin при том, что наша проверка была уверена, что монеты хватает.
    qty_needed_estimate = vol / opp["buy_price"] if opp.get("buy_price") else 0
    available_on_sell_ex = sell_balances.get(symbol, 0.0)

    # ИСПРАВЛЕНИЕ 04.08 (было: жёстко зашитые 2% — регулярно ложно отклоняли
    # сделки, когда реального запаса было ~1.9%, то есть денег хватало почти
    # впритык). Буфер теперь настраиваемый через /setbalancebuffer, а если
    # не хватает — сразу пробуем точечно докупить разницу, вместо отказа.
    buffer_mult = 1 + config["balance_safety_buffer_pct"] / 100
    required_with_buffer = qty_needed_estimate * buffer_mult
    if available_on_sell_ex < required_with_buffer:
        shortfall = round(required_with_buffer - available_on_sell_ex, 4)
        topped = await top_up_coin_reserve(session, sell_ex, symbol, shortfall, opp["sell_price"])
        if topped:
            await asyncio.sleep(1.5)  # даём бирже время зачислить монету на баланс
            refreshed = await get_real_balances(session, sell_ex)
            available_on_sell_ex = (refreshed or {}).get(symbol, available_on_sell_ex)
        if available_on_sell_ex < required_with_buffer:
            return {"success": False,
                    "error": f"insufficient_real_balance_on_{sell_ex}: "
                             f"нужно ~{qty_needed_estimate:.2f} {symbol} "
                             f"(+{config['balance_safety_buffer_pct']}% буфер = {required_with_buffer:.2f}), "
                             f"есть {available_on_sell_ex:.2f}, не хватает "
                             f"{round(required_with_buffer - available_on_sell_ex, 2)} "
                             f"{'(автодокупка не удалась)' if not topped else '(даже после автодокупки)'}"}

    # --- НОГА 1: ПОКУПКА ---
    buy_result = None
    if buy_ex == "Binance":
        buy_result = await place_order_binance(session, symbol, "BUY", vol)
    elif buy_ex == "KuCoin":
        buy_result = await place_order_kucoin(session, symbol, "buy", vol, use_funds=True)
    elif buy_ex == "HTX":
        if not _htx_account_id_cache:
            _htx_account_id_cache = await get_htx_account_id(session)
        if _htx_account_id_cache:
            buy_result = await place_order_htx(session, _htx_account_id_cache, symbol, "buy-market", vol)

    if not buy_result:
        return {"success": False,
                "error": f"buy_leg_failed_on_{buy_ex}: {_last_exchange_error.get(buy_ex) or 'нет деталей от биржи'}"}

    config["real_trades_today"] += 1

    # ТРЕБОВАНИЕ 1: не верим на слово, что покупка исполнилась — подтверждаем
    # реальным опросом биржи и берём ФАКТИЧЕСКОЕ количество, а не расчётное
    confirmed_qty = await confirm_fill_and_get_qty(session, buy_ex, buy_result)
    if not confirmed_qty or confirmed_qty <= 0:
        return {"success": False, "error": f"buy_leg_not_confirmed_filled_on_{buy_ex}"}

    # ТРЕБОВАНИЕ 4: округляем ВНИЗ под реальный шаг лота биржи ПРОДАЖИ,
    # прежде чем размещать вторую ногу — иначе LOT_SIZE/precision-error
    sell_qty = await round_quantity_for_exchange(session, sell_ex, symbol, confirmed_qty)
    if sell_qty <= 0:
        return {"success": False, "error": f"sell_qty_rounds_to_zero_on_{sell_ex}"}

    # ИСПРАВЛЕНИЕ 04.08 (раунд 3): второй, уже ТОЧНЫЙ рубеж защиты. Первая
    # preflight-проверка (в начале функции) — это оценка ДО покупки, по
    # ожидаемой цене. Теперь, когда покупка на buy_ex реально прошла и
    # confirmed_qty известен точно, сверяем его напрямую с фактическим
    # балансом sell_ex — без всяких оценок. Если и тут не хватает —
    # пробуем точечно докупить именно этот остаток перед тем, как вообще
    # пытаться разместить ордер на продажу (а не после отказа биржи).
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
            return {"success": False,
                    "error": f"insufficient_real_balance_on_{sell_ex}_precheck: "
                             f"после покупки на {buy_ex} нужно продать {sell_qty} {symbol}, "
                             f"реально на {sell_ex} есть {fresh_available:.4f} "
                             f"{'(автодокупка не помогла)' if not topped else '(даже после автодокупки)'}",
                    "buy_result": buy_result}

    # --- НОГА 2: ПРОДАЖА ---
    sell_result = None
    if sell_ex == "Binance":
        sell_result = await place_order_binance(session, symbol, "SELL", sell_qty)
    elif sell_ex == "KuCoin":
        sell_result = await place_order_kucoin(session, symbol, "sell", sell_qty, use_funds=False)
    elif sell_ex == "HTX":
        if not _htx_account_id_cache:
            _htx_account_id_cache = await get_htx_account_id(session)
        if _htx_account_id_cache:
            sell_result = await place_order_htx(session, _htx_account_id_cache, symbol, "sell-market", sell_qty)

    if not sell_result:
        # АВАРИЙНОЕ ЗАКРЫТИЕ: продаём купленное обратно на бирже покупки,
        # чтобы не остаться с открытой направленной позицией. Округляем
        # под правила ИМЕННО buy_ex (это другая биржа с другим шагом лота).
        emergency_qty = await round_quantity_for_exchange(session, buy_ex, symbol, confirmed_qty)
        emergency = None
        if emergency_qty > 0:
            if buy_ex == "Binance":
                emergency = await place_order_binance(session, symbol, "SELL", emergency_qty)
            elif buy_ex == "KuCoin":
                emergency = await place_order_kucoin(session, symbol, "sell", emergency_qty, use_funds=False)
            elif buy_ex == "HTX":
                if _htx_account_id_cache:
                    emergency = await place_order_htx(session, _htx_account_id_cache, symbol, "sell-market", emergency_qty)
        return {
            "success": False,
            "error": f"sell_leg_failed_on_{sell_ex}: {_last_exchange_error.get(sell_ex) or 'нет деталей от биржи'}",
            "emergency_close": bool(emergency),
            "buy_result": buy_result,
        }

    return {"success": True, "buy_result": buy_result, "sell_result": sell_result, "vol": vol,
             "confirmed_qty": confirmed_qty}


# =====================================================================
# РЕАЛЬНЫЙ АВТО-РЕБАЛАНС (по вашему запросу от 24.07)
#
# ВНИМАНИЕ: как и весь Этап 6, этот код не тестировался на живом API.
# Комиссия и небольшое проскальзывание за каждую ногу — неизбежная и
# честная цена ребаланса реальными деньгами, порядка 0.1-0.2% за ногу
# (Binance/KuCoin) или 0.2% (HTX), т.е. ~0.2-0.4% за цикл продажа+покупка.
# =====================================================================

async def get_real_balances_binance(session) -> Optional[Dict[str, float]]:
    if is_backed_off("Binance"):
        return None
    url = "https://api.binance.com/api/v3/account"
    ts = int(time.time() * 1000)
    params = {"timestamp": ts, "recvWindow": 5000}
    params["signature"] = sign_binance(params, BINANCE_SECRET)
    headers = {"X-MBX-APIKEY": BINANCE_KEY}
    try:
        async with session.get(url, params=params, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status in (429, 418):
                trigger_backoff("Binance", r.status, r.headers.get("Retry-After"))
                return None
            data = await r.json()
            if r.status != 200:
                logger.error(f"Binance balance fetch failed: {data}")
                return None
            return {b["asset"]: float(b["free"]) for b in data.get("balances", [])}
    except Exception as e:
        logger.error(f"Binance balance exception: {e}")
        return None


async def get_real_balances_kucoin(session) -> Optional[Dict[str, float]]:
    if is_backed_off("KuCoin"):
        return None
    endpoint = "/api/v1/accounts"
    url = f"https://api.kucoin.com{endpoint}"
    ts = str(int(time.time() * 1000))
    signature, passphrase_signed = sign_kucoin(KUCOIN_SECRET, KUCOIN_PASS, ts, "GET", endpoint, "")
    headers = {
        "KC-API-KEY": KUCOIN_KEY, "KC-API-SIGN": signature, "KC-API-TIMESTAMP": ts,
        "KC-API-PASSPHRASE": passphrase_signed, "KC-API-KEY-VERSION": "2",
    }
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status in (429, 418):
                trigger_backoff("KuCoin", r.status, r.headers.get("Retry-After"))
                return None
            data = await r.json()
            if r.status != 200 or data.get("code") != "200000":
                logger.error(f"KuCoin balance fetch failed: {data}")
                return None
            result = {}
            for acc in data.get("data", []):
                if acc.get("type") == "trade":
                    result[acc["currency"]] = float(acc["available"])
            return result
    except Exception as e:
        logger.error(f"KuCoin balance exception: {e}")
        return None


async def get_real_balances_htx(session) -> Optional[Dict[str, float]]:
    if is_backed_off("HTX"):
        return None
    global _htx_account_id_cache
    if not _htx_account_id_cache:
        _htx_account_id_cache = await get_htx_account_id(session)
    if not _htx_account_id_cache:
        return None
    host = "api.huobi.pro"
    endpoint = f"/v1/account/accounts/{_htx_account_id_cache}/balance"
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    params = {"AccessKeyId": HTX_KEY, "SignatureMethod": "HmacSHA256",
              "SignatureVersion": "2", "Timestamp": ts}
    sorted_params = sorted(params.items())
    query = urllib.parse.urlencode(sorted_params)
    payload = f"GET\n{host}\n{endpoint}\n{query}"
    signature = base64.b64encode(
        hmac.new(HTX_SECRET.encode(), payload.encode(), hashlib.sha256).digest()
    ).decode()
    params["Signature"] = signature
    try:
        async with session.get(f"https://{host}{endpoint}", params=params,
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status in (429, 418):
                trigger_backoff("HTX", r.status, r.headers.get("Retry-After"))
                return None
            data = await r.json()
            if data.get("status") != "ok":
                logger.error(f"HTX balance fetch failed: {data}")
                return None
            result = {}
            for item in (data.get("data") or {}).get("list", []):
                if item.get("type") == "trade":
                    cur = item["currency"].upper()
                    result[cur] = result.get(cur, 0.0) + float(item["balance"])
            return result
    except Exception as e:
        logger.error(f"HTX balance exception: {e}")
        return None


async def get_real_balances(session, ex: str) -> Optional[Dict[str, float]]:
    if ex == "Binance":
        return await get_real_balances_binance(session)
    elif ex == "KuCoin":
        return await get_real_balances_kucoin(session)
    elif ex == "HTX":
        return await get_real_balances_htx(session)
    return None


async def get_valuation_price(session, ex: str, symbol: str) -> Optional[float]:
    """Best bid как консервативная оценка стоимости позиции (если продавать)."""
    if ex == "Binance":
        ob = await get_orderbook_binance(session, symbol)
    elif ex == "KuCoin":
        ob = await get_orderbook_kucoin(session, symbol)
    elif ex == "HTX":
        ob = await get_orderbook_htx(session, symbol)
    else:
        return None
    if not ob or not ob.get("bids"):
        return None
    return ob["bids"][0][0]


async def get_total_real_capital(session) -> Optional[dict]:
    """Реальный совокупный капитал на всех трёх биржах — используется в /stats
    вместо симуляционного SIM_START/sim_balances, когда бот в реальном режиме."""
    per_exchange = {}
    total = 0.0
    for ex in ["Binance", "KuCoin", "HTX"]:
        balances = await get_real_balances(session, ex)
        if balances is None:
            return None
        ex_total = balances.get("USDT", 0.0)
        for sym in SYMBOLS:
            qty = balances.get(sym, 0.0)
            if qty > 0:
                price = await get_valuation_price(session, ex, sym)
                if price:
                    ex_total += qty * price
        per_exchange[ex] = round(ex_total, 2)
        total += ex_total
    return {"total": round(total, 2), "per_exchange": per_exchange}


async def real_exchange_rebalance_plan(session, ex: str) -> Optional[dict]:
    """Реальный аналог exchange_rebalance_plan — читает ФАКТИЧЕСКИЕ балансы
    с биржи через API, а не виртуальный sim_balances."""
    balances = await get_real_balances(session, ex)
    if balances is None:
        return None

    real_lot = config["max_real_order_usdt"]  # жёсткий потолок реального ордера
    lots = config["rebalance_target_lots"]
    # ИСПРАВЛЕНИЕ 08.08: раньше coin_target_usd (резерв монеты на ПРОДАЖУ) и
    # usdt_target (резерв USDT на ПОКУПКУ) считались от одного и того же
    # "lots" — но это разные по смыслу вещи. Резерв на покупку не нужно
    # копить впрок: он и так пополняется из живых денег перед каждой
    # сделкой. А вот резерв на продажу ИМЕННО стоит держать на несколько
    # сделок вперёд — иначе (как и произошло на практике) почти каждая
    # сделка выжигает почти весь резерв и требует дорогой платной докупки
    # (топ-ап) буквально каждый раз, съедая всю тонкую маржу лишними
    # комиссиями. sell_reserve_lots — отдельный множитель именно для
    # резерва продажи, настраивается через /setsellreserve.
    sell_lots = config.get("sell_reserve_lots", lots)
    coin_target_usd = real_lot * sell_lots
    usdt_target = real_lot * lots

    def is_seller_for(sym: str) -> bool:
        return ex in {s for _, s in pairs_for_symbol(sym)}

    def is_buyer_for(sym: str) -> bool:
        return ex in {b for b, _ in pairs_for_symbol(sym)}

    # ИСПРАВЛЕНИЕ 05.08 (раунд 11): роль биржи теперь проверяется ОТДЕЛЬНО
    # для каждой монеты (маршруты у монет могут отличаться — см.
    # PAIR_OVERRIDES), а не одним флагом "биржа вообще продаёт хоть что-то".
    # Иначе KuCoin (продаёт ORDI/THETA, но для TRX теперь не участвует)
    # ошибочно продолжила бы требовать резерв TRX, а HTX (продаёт только
    # TRX) не получила бы для неё цель вообще.
    coin_values: Dict[str, float] = {}
    coin_targets: Dict[str, float] = {}
    coin_reserve_syms = 0
    for sym in SYMBOLS:
        qty = balances.get(sym, 0.0)
        seller_here = is_seller_for(sym)
        if seller_here:
            coin_reserve_syms += 1
            coin_targets[sym] = coin_target_usd
        else:
            coin_targets[sym] = 0.0
        if qty <= 0:
            if seller_here:
                coin_values[sym] = 0.0  # монета нужна по роли, но резерва пока нет — не пропускаем
            continue
        price = await get_valuation_price(session, ex, sym)
        if price:
            coin_values[sym] = round(qty * price, 4)

    usdt_balance = balances.get("USDT", 0.0)
    total_usd = usdt_balance + sum(coin_values.values())
    # usdt_target требуется, только если биржа реально покупает хотя бы одну
    # монету по её фактическому маршруту.
    effective_usdt_target = usdt_target if any(is_buyer_for(sym) for sym in SYMBOLS) else 0.0
    headroom_mult = 1 + get_headroom_pct(ex) / 100
    # Считаем от ЦЕЛИ (сколько монет ex реально обязана продавать по
    # маршрутам), а не от того, что уже случайно есть на балансе — иначе
    # цель "рассасывается" именно тогда, когда она нужнее всего: при
    # старте резерва с нуля.
    needed_total = effective_usdt_target + (coin_target_usd * headroom_mult) * coin_reserve_syms

    return {
        "exchange": ex, "balances_qty": balances, "coin_values": coin_values,
        "usdt_balance": round(usdt_balance, 2), "total_usd": round(total_usd, 2),
        "needed_total": round(needed_total, 2), "surplus": round(total_usd - needed_total, 2),
        "coin_targets": coin_targets, "usdt_target": effective_usdt_target,
    }


async def apply_real_intra_exchange_rebalance(session, ex: str, plan: dict) -> dict:
    """Продаёт излишек монет в USDT, докупает дефицитные — РЕАЛЬНЫМИ ордерами.
    Вызывать только когда plan['surplus'] >= 0 (иначе останется дефицит).
    Пока config['real_rebalance_dry_run'] == True — ордера НЕ размещаются,
    только считается и показывается план (см. /rebalancelive для включения)."""
    dry_run = config["real_rebalance_dry_run"]
    actions = []
    coin_targets = plan["coin_targets"]  # теперь словарь — своя цель на каждую монету
    threshold = 1.0  # не гоняем ребаланс из-за $1 — комиссия того не стоит
    min_order = MIN_ORDER_VALUE_USD.get(ex, 5.0)

    # Сначала продажи — освобождаем USDT для последующих покупок
    for sym, value in plan["coin_values"].items():
        coin_target = coin_targets.get(sym, 0.0)
        if value > coin_target + threshold:
            qty = plan["balances_qty"].get(sym, 0)
            price = value / qty if qty else None
            if not price:
                continue
            excess_usd = value - coin_target
            # ПРЕДОХРАНИТЕЛЬ 01.08: circuit breaker против ошибки расчёта —
            # ребаланс НИКОГДА не продаёт больше 3x реального лимита ордера
            # за одну операцию, даже если формула выше почему-то посчитала больше.
            max_single_sell_usd = config["max_real_order_usdt"] * 3
            if excess_usd > max_single_sell_usd:
                logger.error(f"⚠️ РЕБАЛАНС {ex}/{sym}: расчётный излишек ${excess_usd:.2f} "
                              f"превышает потолок ${max_single_sell_usd} — ограничиваю, "
                              f"это подозрительно большая цифра для одного ребаланса")
                if CHAT_ID:
                    await send_tg(session,
                        f"⚠️ *ПРЕДОХРАНИТЕЛЬ СРАБОТАЛ*\n\n"
                        f"Ребаланс {ex}/{sym} рассчитал излишек ${excess_usd:.2f} к продаже — "
                        f"это подозрительно много (больше ${max_single_sell_usd}). "
                        f"Продажа ограничена этим потолком вместо полной суммы. "
                        f"Проверьте баланс {ex} вручную после исполнения.")
                excess_usd = max_single_sell_usd
            # НАХОДКА 02.08: если излишек меньше минимума биржи — биржа откажет.
            # Продаём ВЕСЬ остаток (не только излишек), если после этого не
            # уйдём в серьёзный дефицит, иначе пропускаем это действие вовсе.
            if excess_usd < min_order:
                if value >= min_order:
                    excess_usd = value  # продаём всё — проще, чем мучить биржу микросуммой
                else:
                    continue  # даже вся позиция меньше минимума биржи — нечего продавать
            qty_to_sell_raw = excess_usd / price
            # ВТОРОЙ ПРЕДОХРАНИТЕЛЬ: физически нельзя продать больше, чем реально
            # есть на балансе — даже если выше в расчётах где-то ошибка
            qty_to_sell_raw = min(qty_to_sell_raw, qty * 0.98)  # 2% запас на комиссию/округление
            # ТРЕБОВАНИЕ 4: та же защита, что и в арбитраже — округляем ВНИЗ
            # под реальный шаг лота, иначе LOT_SIZE/precision-error как 30.07
            qty_to_sell = qty_to_sell_raw if dry_run else \
                await round_quantity_for_exchange(session, ex, sym, qty_to_sell_raw)
            if not dry_run and qty_to_sell <= 0:
                continue  # после округления продавать нечего — пропускаем
            result = "DRY_RUN" if dry_run else None
            if not dry_run:
                if ex == "Binance":
                    result = await place_order_binance(session, sym, "SELL", qty_to_sell)
                elif ex == "KuCoin":
                    result = await place_order_kucoin(session, sym, "sell", qty_to_sell, use_funds=False)
                elif ex == "HTX":
                    if _htx_account_id_cache:
                        result = await place_order_htx(session, _htx_account_id_cache, sym, "sell-market", qty_to_sell)
            actions.append({"action": "sell", "symbol": sym, "usd_estimate": round(excess_usd, 2),
                             "success": bool(result), "dry_run": dry_run})

    # Затем покупки дефицитных монет
    # НАХОДКА 02.08 (вторая часть): общий порог $1 хорош для продажи (не
    # гонять ребаланс из-за мелочи), но для ПОКУПКИ он вреден — если на
    # бирже полно свободного USDT, мелкий недобор ($0.5-0.9) должен
    # закрываться сразу, а не игнорироваться до следующего похода в минус.
    #
    # ИСПРАВЛЕНИЕ 04.08: цель по монете теперь считается с бОльшим запасом
    # (rebalance_headroom_pct, по умолчанию +15%, а не жёсткие +3%) — именно
    # тонкий зазор между этим запасом и буфером preflight-проверки
    # (balance_safety_buffer_pct) регулярно приводил к ложным отказам
    # "insufficient_real_balance" на дешёвых монетах вроде ZIL.
    buy_threshold = 0.30
    for sym, value in plan["coin_values"].items():
        coin_target = coin_targets.get(sym, 0.0)
        if value < coin_target - buy_threshold:
            headroom_mult = 1 + get_headroom_pct(ex) / 100
            effective_target = coin_target * headroom_mult
            deficit_usd = round(effective_target - value, 2)
            # НАХОДКА 02.08: та же проблема, что и с продажей — если нужная
            # докупка меньше минимума биржи ($10 у HTX), ордер будет отклонён.
            # Поднимаем до минимума биржи (с небольшим запасом), если хватает
            # свободного USDT; иначе честно пропускаем это действие.
            if deficit_usd < min_order:
                bumped = min_order * 1.02  # 2% запас, чтобы не упереться в минимум ещё раз
                if plan["usdt_balance"] >= bumped:
                    deficit_usd = round(bumped, 2)
                else:
                    continue  # даже минимума биржи не хватает свободного USDT — пропускаем
            result = "DRY_RUN" if dry_run else None
            if not dry_run:
                if ex == "Binance":
                    result = await place_order_binance(session, sym, "BUY", deficit_usd)
                elif ex == "KuCoin":
                    result = await place_order_kucoin(session, sym, "buy", deficit_usd, use_funds=True)
                elif ex == "HTX":
                    if _htx_account_id_cache:
                        result = await place_order_htx(session, _htx_account_id_cache, sym, "buy-market", deficit_usd)
            actions.append({"action": "buy", "symbol": sym, "usd_estimate": deficit_usd,
                             "success": bool(result), "dry_run": dry_run})

    return {"exchange": ex, "actions": actions}


async def real_auto_rebalance_all(session) -> dict:
    """Реальная версия auto_rebalance_all для боевого режима.
    ВНУТРИ биржи — реальные ордера (комиссия неизбежна).
    МЕЖДУ биржами — только инструкция, как и в симуляции, автоперевод
    между биржами не делаем никогда."""
    plans = {}
    for ex in ["Binance", "KuCoin", "HTX"]:
        p = await real_exchange_rebalance_plan(session, ex)
        if p is None:
            return {"fully_rebalanced": False, "error": f"could_not_fetch_balance_{ex}",
                     "applied": [], "cross_exchange_needed": None, "dry_run": config["real_rebalance_dry_run"],
                     "safe_to_resume": False}
        plans[ex] = p

    dry_run = config["real_rebalance_dry_run"]
    # ИСПРАВЛЕНИЕ 04.08 (раунд 6): порог классификации дефицита раньше был
    # жёстко зашит в $1 — на старте (лоты $15-20) это было разумно, но при
    # текущих лотах $8-10 нехватка в $0.2-0.8 (типичная причина повторяющихся
    # "insufficient_usdt_on_HTX") была МЕНЬШЕ этого порога и НЕ считалась
    # дефицитом вообще — биржа молча пролетала мимо ребаланса, план говорил
    # "всё в порядке", а реальная сделка потом честно отказывала. Порог
    # теперь пропорционален размеру лота (5% от лимита ордера, но не меньше
    # $0.10), чтобы ловить именно такие небольшие, но критичные разрывы.
    # СНИЖЕНО 07.08 (было 5% от лота, то есть $0.5 при лоте $10): именно
    # такой грубый порог только что "потерял" $0.23 излишка на Binance —
    # деньги были там, но не считались ни дефицитом, ни достаточным
    # излишком, и просто зависали, не превращаясь в нужный резерв монеты.
    # 1% от лота даёт гораздо более чувствительный порог ($0.10 при лоте $10).
    deficit_threshold = max(0.10, config["max_real_order_usdt"] * 0.01)
    deficits = {ex: p for ex, p in plans.items() if p["surplus"] < -deficit_threshold}
    surpluses = {ex: p for ex, p in plans.items() if p["surplus"] > deficit_threshold}

    applied = []
    if not deficits:
        for ex, p in plans.items():
            applied.append(await apply_real_intra_exchange_rebalance(session, ex, p))
        # safe_to_resume: средств достаточно И (либо реально исполнили, либо
        # ничего не требовалось исполнять) — в dry-run с реальными действиями
        # НЕЛЬЗЯ возобновлять торговлю, т.к. балансы физически не поменялись
        had_actions = any(a["actions"] for a in applied)
        safe = True if not (dry_run and had_actions) else False
        return {"fully_rebalanced": True, "applied": applied, "cross_exchange_needed": None,
                "dry_run": dry_run, "safe_to_resume": safe}

    # НОВОЕ 07.08: дефицит $0.10-2 технически существует, но переводить его
    # между биржами экономически бессмысленно — комиссия сети (обычно ~$1
    # на TRC-20) съест сумму перевода целиком или почти целиком. Раньше
    # ЛЮБОЙ дефицит выше $0.10 останавливал торговлю и требовал перевода —
    # бот раз за разом упирался в одну и ту же копеечную "недостачу" при
    # каждом плановом ребалансе (раз в ~30 мин), заставляя вручную жать
    # /go без какого-либо реального решения проблемы. Теперь останавливаем
    # торговлю и просим перевод, только если сумма ДЕЙСТВИТЕЛЬНО стоит
    # затраченной на неё комиссии.
    CROSS_EXCHANGE_MIN_WORTH_TRANSFER = 2.0
    real_deficits = {ex: p for ex, p in deficits.items()
                      if -p["surplus"] >= CROSS_EXCHANGE_MIN_WORTH_TRANSFER}

    if not real_deficits:
        for ex, p in plans.items():
            applied.append(await apply_real_intra_exchange_rebalance(session, ex, p))
        had_actions = any(a["actions"] for a in applied)
        safe = True if not (dry_run and had_actions) else False
        return {"fully_rebalanced": True, "applied": applied, "cross_exchange_needed": None,
                "dry_run": dry_run, "safe_to_resume": safe,
                "note": f"Есть мелкий дефицит ниже ${CROSS_EXCHANGE_MIN_WORTH_TRANSFER} — "
                        f"перевод не стоит комиссии, торговля продолжается."}

    # ИСПРАВЛЕНИЕ 07.08: раньше внутренний ребаланс (продажа лишней/чужой
    # монеты обратно в USDT, докупка нужной) вызывался ТОЛЬКО для бирж с
    # излишком — дефицитные биржи пропускались целиком. Но продажа монеты,
    # которую бирже вообще не положено держать (например, KuCoin с $7.71 в
    # IOST при новом направлении, где IOST должна быть только на Binance) —
    # это БЕЗОПАСНАЯ операция, не требует новых денег, только освобождает
    # то, что уже есть, просто в неправильной форме. Раньше эти деньги
    # просто лежали мёртвым грузом, а бот требовал перевод СВЕРХУ, даже не
    # попробовав сначала освободить то, что уже было на месте.
    for ex, p in real_deficits.items():
        applied.append(await apply_real_intra_exchange_rebalance(session, ex, p))
    for ex, p in surpluses.items():
        applied.append(await apply_real_intra_exchange_rebalance(session, ex, p))

    # После продажи "чужой" монеты на дефицитных биржах реальный дефицит
    # мог уменьшиться (или исчезнуть) — пересчитываем перед тем, как просить
    # перевод, вместо того чтобы полагаться на цифры ДО этой продажи.
    updated_plans = {}
    for ex in real_deficits:
        updated_plans[ex] = await real_exchange_rebalance_plan(session, ex)
    real_deficits = {ex: p for ex, p in updated_plans.items() if p and
                      -p["surplus"] >= CROSS_EXCHANGE_MIN_WORTH_TRANSFER}
    if not real_deficits:
        had_actions = any(a["actions"] for a in applied)
        safe = True if not (dry_run and had_actions) else False
        return {"fully_rebalanced": True, "applied": applied, "cross_exchange_needed": None,
                "dry_run": dry_run, "safe_to_resume": safe,
                "note": "Продажа лишней монеты на месте закрыла дефицит без перевода между биржами."}

    instructions = []
    remaining_surplus = {ex: p["surplus"] for ex, p in surpluses.items()}
    for ex, p in sorted(real_deficits.items(), key=lambda kv: kv[1]["surplus"]):
        need = round(-p["surplus"], 2)
        source = max(remaining_surplus, key=remaining_surplus.get, default=None)
        if source and remaining_surplus[source] > 0:
            amount = round(min(need, remaining_surplus[source]), 2)
            remaining_surplus[source] -= amount
            instructions.append({"from": source, "to": ex, "amount_usdt": amount, "still_needed": round(need - amount, 2)})
        else:
            instructions.append({"from": None, "to": ex, "amount_usdt": 0, "still_needed": need})

    return {"fully_rebalanced": False, "applied": applied, "cross_exchange_needed": instructions, "dry_run": dry_run, "safe_to_resume": False}


# =====================================================================
# СИМУЛЯЦИЯ БАЛАНСОВ / ИСПОЛНЕНИЕ (как и раньше — это НЕ реальная торговля)
# =====================================================================

def reset_daily():
    today = datetime.now().strftime("%Y-%m-%d")
    if config["day_start"] != today:
        config["day_start"] = today
        config["daily_loss"] = 0.0
        config["daily_profit"] = 0.0
        config["trading_active"] = True
        config["real_trades_today"] = 0  # БАГ 31.07: раньше не сбрасывался вообще,
                                           # после 20 сделок с момента старта бот
                                           # навсегда блокировал реальную торговлю
        stats["topup_cost_usdt"] = 0.0  # НОВОЕ 05.08: дневной счётчик трат на автодокупки


def can_trade() -> bool:
    reset_daily()
    if config["paused"]:
        return False
    if config["daily_loss"] >= config["stop_loss_usdt"]:
        config["trading_active"] = False
    return config["trading_active"]


def check_rate() -> bool:
    now = datetime.now()
    if (now - stats["minute_start"]).total_seconds() >= 60:
        stats["trades_this_minute"] = 0
        stats["minute_start"] = now
    return stats["trades_this_minute"] < config["max_trades_per_min"]


def get_balance_usdt() -> float:
    return round(sum(v for assets in sim_balances.values() for v in assets.values()), 2)


def has_sufficient_sim_balance(opp: dict) -> bool:
    """КРИТИЧНАЯ ПРОВЕРКА (добавлена после найденного бага 21.07.2026):
    раньше update_sim_balances зачисляла полный объём сделки даже если
    списать USDT/монету удавалось лишь частично (из-за max(0,...)).
    Это создавало деньги из воздуха, как только баланс биржи истощался.
    Теперь сделка в симуляции просто не происходит, если реально
    не хватает баланса — как было бы и на настоящей бирже."""
    bex, sex, sym, vol = opp["buy_ex"], opp["sell_ex"], opp["symbol"], opp["vol"]
    buy_usdt  = sim_balances.get(bex, {}).get("USDT", 0)
    sell_coin = sim_balances.get(sex, {}).get(sym, 0)
    return buy_usdt >= vol and sell_coin >= vol


def update_sim_balances(opp: dict):
    """Вызывается ТОЛЬКО после has_sufficient_sim_balance() == True."""
    sym, bex, sex, vol, profit = opp["symbol"], opp["buy_ex"], opp["sell_ex"], opp["vol"], opp["profit_usdt"]
    if bex in sim_balances:
        sim_balances[bex]["USDT"] = sim_balances[bex].get("USDT", 0) - vol
        sim_balances[bex][sym] = sim_balances[bex].get(sym, 0) + vol
    if sex in sim_balances:
        sim_balances[sex][sym] = sim_balances[sex].get(sym, 0) - vol
        sim_balances[sex]["USDT"] = sim_balances[sex].get("USDT", 0) + vol + profit


def check_balance_warnings() -> List[str]:
    warns = []
    min_needed = config["trade_usdt"] * config["rebalance_target_lots"]
    for ex, assets in sim_balances.items():
        usdt = assets.get("USDT", 0)
        if usdt < min_needed:
            warns.append(f"⚠️ {ex}: USDT = ${round(usdt,1)} — мало! (нужно от ${min_needed})")
        for sym in SYMBOLS:
            val = assets.get(sym, 0)
            if ex in ["KuCoin", "HTX"] and 0 <= val < min_needed:
                warns.append(f"⚠️ {ex}: {sym} = ${round(val,1)} — мало!")
    return warns


def suggest_withdrawal() -> dict:
    """Сколько можно теоретически вывести как прибыль, не трогая рабочий капитал."""
    total = get_balance_usdt()
    min_operating = SIM_START * 1.5  # держим минимум 150% старта в обороте для 3 бирж
    withdrawable = max(0, total - min_operating)
    return {"total": round(total, 2), "min_operating": min_operating, "withdrawable": round(withdrawable, 2)}


# =====================================================================
# АВТОМАТИЧЕСКИЙ РЕБАЛАНС
#
# Принцип (по вашему запросу):
#   - ВНУТРИ одной биржи — полностью автоматически: излишек монеты
#     конвертируется в USDT, дефицит монеты докупается за USDT.
#     USDT — основная валюта, накапливается сверху цели как резерв
#     для вывода прибыли.
#   - МЕЖДУ биржами — НИКОГДА автоматически. Бот останавливает торговлю
#     и даёт точную инструкцию (откуда, куда, сколько), ждёт ручного
#     подтверждения.
# =====================================================================

def get_sell_exchanges() -> set:
    """Биржи, которые хоть раз выступают sell_ex — теперь считается по
    ФАКТИЧЕСКИМ маршрутам каждой монеты (pairs_for_symbol), а не по одному
    общему PAIRS — раз маршруты стали индивидуальными для каждой монеты
    (см. PAIR_OVERRIDES), агрегат должен строиться по всем реально
    используемым маршрутам, иначе биржа, играющая роль только для одной
    конкретной монеты (как HTX для TRX), выпадет из расчёта капитала."""
    exs = set()
    for sym in SYMBOLS:
        exs |= {sell_ex for _, sell_ex in pairs_for_symbol(sym)}
    return exs


def get_buy_exchanges() -> set:
    """Симметрично get_sell_exchanges — биржи, которые хоть раз выступают
    buy_ex хоть для одной монеты по её фактическому маршруту."""
    exs = set()
    for sym in SYMBOLS:
        exs |= {buy_ex for buy_ex, _ in pairs_for_symbol(sym)}
    return exs


def get_headroom_pct(ex: str) -> float:
    """НОВОЕ 04.08 (раунд 9): персональный % запаса для конкретной биржи,
    если задан в rebalance_headroom_overrides — иначе общий config
    rebalance_headroom_pct. Нужно для бирж с двойной ролью (одновременно
    покупают и продают) на ограниченном капитале — им физически не хватает
    места под стандартный общий запас."""
    return config["rebalance_headroom_overrides"].get(ex, config["rebalance_headroom_pct"])


def exchange_rebalance_plan(ex: str) -> dict:
    """Считает, хватает ли ОБЩЕЙ суммы на бирже, чтобы держать целевой
    остаток по каждой отслеживаемой монете + буфер USDT. Не изменяет
    балансы — только считает."""
    assets = sim_balances.get(ex, {})
    coin_target = config["trade_usdt"] * config["rebalance_target_lots"]
    usdt_target = config["trade_usdt"] * config["rebalance_target_lots"]

    # Монету держим целенаправленно ТОЛЬКО если: (а) биржа реально продаёт
    # (фигурирует как sell_ex), И (б) монета всё ещё в активном списке SYMBOLS.
    # Всё остальное (удалённые монеты, монеты на бирже-покупателе) — считается
    # "мёртвым" остатком и подлежит полной конвертации в USDT.
    sell_exchanges = get_sell_exchanges()
    if ex in sell_exchanges:
        coins_here = [s for s in SYMBOLS if s in assets]
    else:
        coins_here = []

    needed_total = usdt_target + coin_target * len(coins_here)
    total = round(sum(assets.values()), 2)

    return {
        "exchange": ex, "total": total, "needed_total": round(needed_total, 2),
        "surplus": round(total - needed_total, 2),  # может быть отрицательным (дефицит)
        "coins_here": coins_here, "coin_target": coin_target, "usdt_target": usdt_target,
    }


def apply_intra_exchange_rebalance(ex: str, plan: dict):
    """Физически применяет ребаланс ВНУТРИ биржи — вызывать только когда
    plan['surplus'] >= 0, иначе останется дефицит."""
    assets = sim_balances[ex]
    assets["USDT"] = plan["usdt_target"] + plan["surplus"]  # избыток стекает в USDT
    for sym in plan["coins_here"]:
        assets[sym] = plan["coin_target"]
    # КРИТИЧНО: обнуляем все прочие "монетные" ключи, которых нет в
    # coins_here (удалённые через /removecoin, либо монеты на бирже, которая
    # не должна их держать вроде Binance). Их стоимость уже учтена в USDT
    # через surplus выше — если не обнулить сам ключ, баланс задвоится.
    for key in list(assets.keys()):
        if key != "USDT" and key not in plan["coins_here"]:
            assets[key] = 0.0


def auto_rebalance_all() -> dict:
    """Главная функция. Возвращает:
    {"fully_rebalanced": bool, "applied": [ex,...], "cross_exchange_needed": {...}|None}
    Если хотя бы одна биржа в дефиците — НИЧЕГО не меняет на ней и
    возвращает точную инструкцию по межбиржевому переводу."""
    plans = {ex: exchange_rebalance_plan(ex) for ex in sim_balances}
    deficits = {ex: p for ex, p in plans.items() if p["surplus"] < -0.01}
    surpluses = {ex: p for ex, p in plans.items() if p["surplus"] > 0.01}

    if not deficits:
        # Всем биржам хватает своих же средств — ребалансируем каждую независимо
        applied = []
        for ex, p in plans.items():
            apply_intra_exchange_rebalance(ex, p)
            applied.append({"exchange": ex, "surplus_to_usdt": p["surplus"]})
        return {"fully_rebalanced": True, "applied": applied, "cross_exchange_needed": None}

    # Есть дефицит хотя бы на одной бирже — ребалансируем ТОЛЬКО биржи с
    # избытком (чтобы явно увидеть, сколько свободных USDT можно перекинуть),
    # дефицитную биржу не трогаем, торговлю не возобновляем.
    applied = []
    for ex, p in surpluses.items():
        apply_intra_exchange_rebalance(ex, p)
        applied.append({"exchange": ex, "surplus_to_usdt": p["surplus"]})

    # Формируем инструкцию: для каждой дефицитной биржи ищем биржу-источник
    # с наибольшим свободным излишком
    instructions = []
    remaining_surplus = {ex: p["surplus"] for ex, p in surpluses.items()}
    for ex, p in sorted(deficits.items(), key=lambda kv: kv[1]["surplus"]):  # сначала самый большой дефицит
        need = round(-p["surplus"], 2)
        source = max(remaining_surplus, key=remaining_surplus.get, default=None)
        if source and remaining_surplus[source] > 0:
            amount = round(min(need, remaining_surplus[source]), 2)
            remaining_surplus[source] -= amount
            instructions.append({"from": source, "to": ex, "amount_usdt": amount, "still_needed": round(need - amount, 2)})
        else:
            instructions.append({"from": None, "to": ex, "amount_usdt": 0, "still_needed": need})

    return {"fully_rebalanced": False, "applied": applied, "cross_exchange_needed": instructions}


def apply_manual_transfer(from_ex: str, to_ex: str, amount: float) -> bool:
    """Применяет к симуляции перевод USDT, который вы УЖЕ сделали руками
    между реальными биржами (TRC-20 и т.п.). Используется после /crosstransfer."""
    if from_ex not in sim_balances or to_ex not in sim_balances:
        return False
    if sim_balances[from_ex].get("USDT", 0) < amount:
        return False
    sim_balances[from_ex]["USDT"] -= amount
    sim_balances[to_ex]["USDT"] = sim_balances[to_ex].get("USDT", 0) + amount
    return True


def reset_simulation():
    """Полный сброс симуляции — нужен после найденного 21.07 бага, т.к. вся
    накопленная статистика/баланс недостоверны."""
    global sim_balances
    sim_balances = build_default_sim_balances()
    trade_history.clear()
    stats["scans"] = 0
    stats["signals"] = 0
    stats["trades"] = 0
    stats["profit"] = 0.0
    stats["insufficient_balance_skips"] = 0
    config["daily_loss"] = 0.0
    config["daily_profit"] = 0.0


async def execute_trade(session, opp: dict) -> dict:
    """Возвращает {'executed': bool, 'reason': str|None} — вызывающий код
    ОБЯЗАН использовать это для формирования сообщения пользователю.
    Раньше карточка сигнала отправлялась независимо от результата —
    это создавало иллюзию, что сделка прошла, даже когда она была
    тихо отклонена (баланс/рейт-лимит/стоп-лосс)."""
    global _last_auto_rebalance_attempt
    if not check_rate():
        return {"executed": False, "reason": "rate_limit_exceeded"}
    if not can_trade():
        return {"executed": False, "reason": "paused_or_stoploss"}

    real_result = None
    if not config["simulation_mode"] and is_real_trading_allowed():
        real_result = await execute_real_arbitrage(session, opp)
        if not real_result.get("success"):
            logger.error(f"РЕАЛЬНАЯ сделка не удалась: {real_result}")
            error = real_result.get("error", "")
            # ИСПРАВЛЕНИЕ 04.08 (раунд 5): добавлена проверка "insufficient_usdt_on_"
            # (нехватка USDT на бирже покупки) — раньше эта ошибка НЕ входила
            # в список триггеров авто-ребаланса, поэтому при её появлении бот
            # просто показывал сообщение и ничего не предпринимал сам, ждал,
            # пока вы вручную наберёте /rebalance + /go. Теперь эта причина
            # тоже автоматически запускает мгновенный реальный ребаланс.
            if ("buy_leg_failed" in error or "sell_leg_failed" in error
                    or "insufficient_real_balance" in error or "insufficient_usdt_on" in error):
                # ИСПРАВЛЕНИЕ 04.08 (раунд 2): раньше здесь сразу запускался
                # ребаланс, а РЕАЛЬНЫЙ текст ошибки биржи (error, который к
                # этому моменту уже содержит детали благодаря _remember_error)
                # никогда не попадал в Telegram — вы видели только общую
                # маску "buy_leg_failed_on_HTX" в сообщении о пропуске сигнала,
                # без единого слова о настоящей причине. Теперь показываем
                # error целиком ПЕРЕД тем, как пробовать ребаланс — если
                # причина НЕ в балансе (например API-ключ без прав на
                # торговлю, неверный формат ордера, биржевой минимум), гонять
                # ребаланс вообще бессмысленно, и вы это сразу увидите.
                if CHAT_ID:
                    await send_tg(session, f"🔴 *Реальная сделка отклонена биржей*\n`{error}`")
                # Скорее всего нехватка средств именно на этой ноге —
                # мгновенная попытка реального ребаланса вместо ожидания
                # планового цикла в 30 минут (с тем же cooldown-защитником)
                now_ts = time.time()
                if now_ts - _last_auto_rebalance_attempt > AUTO_REBALANCE_COOLDOWN:
                    _last_auto_rebalance_attempt = now_ts
                    rb_result = await real_auto_rebalance_all(session)
                    if CHAT_ID:
                        await send_tg(session, "🔄 Пробую реальный ребаланс:\n\n" +
                                       format_real_rebalance_result(rb_result))
                    if not rb_result.get("safe_to_resume", False):
                        config["paused"] = True
            elif CHAT_ID:
                msg = f"🔴 *РЕАЛЬНАЯ СДЕЛКА ОТКЛОНЕНА/ОШИБКА*\n`{real_result}`"
                if real_result.get("emergency_close"):
                    msg += "\n⚠️ Выполнено аварийное закрытие позиции."
                await send_tg(session, msg)
            return {"executed": False, "reason": f"real_execution_failed: {real_result.get('error')}"}

    profit = opp["profit_usdt"]

    if config["simulation_mode"]:
        if not has_sufficient_sim_balance(opp):
            # Раньше это просто тихо отклонялось до следующего планового
            # ребаланса (раз в ~30 мин). Теперь — мгновенная попытка
            # авто-ребаланса прямо здесь, с cooldown против спама, если
            # подряд идёт несколько отказов за секунды.
            now_ts = time.time()
            if now_ts - _last_auto_rebalance_attempt > AUTO_REBALANCE_COOLDOWN:
                _last_auto_rebalance_attempt = now_ts
                rb_result = auto_rebalance_all()
                if rb_result["fully_rebalanced"]:
                    if CHAT_ID:
                        await send_tg(session, "🔄 Обнаружена нехватка баланса — "
                                                "авто-ребаланс внутри бирж выполнен:\n\n" +
                                       format_rebalance_result(rb_result))
                    if has_sufficient_sim_balance(opp):
                        pass  # хватило — проваливаемся дальше и исполняем сделку
                    else:
                        stats["insufficient_balance_skips"] = stats.get("insufficient_balance_skips", 0) + 1
                        return {"executed": False, "reason": "insufficient_sim_balance"}
                else:
                    config["paused"] = True
                    if CHAT_ID:
                        await send_tg(session, format_rebalance_result(rb_result))
                    stats["insufficient_balance_skips"] = stats.get("insufficient_balance_skips", 0) + 1
                    return {"executed": False, "reason": "insufficient_sim_balance_cross_exchange"}
            else:
                stats["insufficient_balance_skips"] = stats.get("insufficient_balance_skips", 0) + 1
                return {"executed": False, "reason": "insufficient_sim_balance"}

    hour = datetime.now().hour
    stats["hourly_profit"][hour] += profit
    trade_history.append({
        "id": len(trade_history) + 1,
        "date": datetime.now().strftime("%Y-%m-%d"), "time": opp["time"],
        "symbol": opp["symbol"], "buy_ex": opp["buy_ex"], "sell_ex": opp["sell_ex"],
        "buy_price": opp["buy_price"], "sell_price": opp["sell_price"], "vol": opp["vol"],
        "gross_pct": opp["gross_pct"], "net_pct": opp["net_pct"], "profit_usdt": profit,
        "slippage_impact_pct": opp.get("slippage_impact_pct", 0),
        "mode": "SIM" if config["simulation_mode"] else "REAL",
    })
    stats["trades"] += 1
    stats["profit"] += profit
    stats["trades_this_minute"] += 1
    if profit >= 0:
        config["daily_profit"] += profit
    else:
        config["daily_loss"] += abs(profit)
        if config["daily_loss"] >= config["stop_loss_usdt"]:
            config["trading_active"] = False
    if config["simulation_mode"]:
        update_sim_balances(opp)

    return {"executed": True, "reason": None}


REASON_LABELS = {
    "rate_limit_exceeded":     "⏱ превышен лимит сделок/мин",
    "paused_or_stoploss":      "⏸ пауза или сработал стоп-лосс",
    "insufficient_sim_balance": "💰 не хватает баланса, авто-ребаланс не смог покрыть (см. сообщение выше)",
    "insufficient_sim_balance_cross_exchange": "🔴 нужен ручной перевод между биржами — торговля на паузе",
    None: "",
}


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


async def send_document(session, filename, content, caption=""):
    if not CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendDocument"
    try:
        data = aiohttp.FormData()
        data.add_field("chat_id", str(CHAT_ID))
        data.add_field("caption", caption)
        data.add_field("document", io.BytesIO(content.encode("utf-8")),
                        filename=filename, content_type="text/plain")
        await session.post(url, data=data, timeout=aiohttp.ClientTimeout(total=15))
    except Exception as e:
        logger.error(f"Doc: {e}")


async def get_updates(session, offset=0):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
    try:
        async with session.get(url, params={"offset": offset, "timeout": 30},
                                timeout=aiohttp.ClientTimeout(total=35)) as r:
            return (await r.json()).get("result", [])
    except Exception:
        return []


def format_real_rebalance_result(result: dict) -> str:
    if result.get("error"):
        return (f"🔴 *РЕАЛЬНЫЙ РЕБАЛАНС НЕ ВЫПОЛНЕН*\n\n"
                f"Не удалось получить реальный баланс: `{result['error']}`\n"
                f"Проверьте API-ключи и логи Railway.")

    is_dry_run = any(act.get("dry_run") for a in result["applied"] for act in a["actions"])
    header = "🔍 *ПЛАН РЕБАЛАНСА (dry-run, ордера НЕ размещены)*" if is_dry_run else \
             "⚖️ *РЕАЛЬНЫЙ АВТО-РЕБАЛАНС (ордера исполнены)*"
    msg = header + "\n━━━━━━━━━━━━━━━━━━━━━━\n\n"

    any_actions = False
    for a in result["applied"]:
        if a["actions"]:
            any_actions = True
            msg += f"*{a['exchange']}:*\n"
            for act in a["actions"]:
                if act.get("dry_run"):
                    icon = "🔍"
                else:
                    icon = "✅" if act["success"] else "❌"
                verb = "Продать" if act["action"] == "sell" else "Купить"
                msg += f"   {icon} {verb} {act['symbol']} на ~${act['usd_estimate']}\n"
            msg += "\n"
    if not any_actions:
        msg += "Реальные балансы уже в целевых диапазонах, действия не требуются.\n\n"

    if is_dry_run and any_actions:
        msg += ("💡 Это только план — ни один ордер не размещён.\n"
                "Проверьте цифры, и когда будете готовы включить реальное "
                "исполнение: `/rebalancelive on`\n\n")
    elif any_actions:
        msg += "⚠️ Комиссии биржи за каждую ногу применились автоматически — это ожидаемо.\n\n"

    if result["fully_rebalanced"]:
        msg += "✅ *Все биржи в целевом диапазоне" + (", торговля продолжается." if not is_dry_run else " (по плану).*")
    else:
        msg += "🔴 *ТОРГОВЛЯ НА ПАУЗЕ* — не хватает реальных средств внутри отдельных бирж:\n\n"
        for instr in result["cross_exchange_needed"]:
            if instr["from"]:
                msg += f"➡️ Переведите *${instr['amount_usdt']}* USDT: *{instr['from']} → {instr['to']}*\n"
            else:
                msg += f"⚠️ На {instr['to']} нужно ещё ${instr['still_needed']}, свободных излишков нет — довнесите извне.\n"
        msg += "\nПосле перевода на реальных биржах — просто `/go` (реальные балансы бот прочитает заново из API)."
    return msg


def format_rebalance_result(result: dict) -> str:
    msg = "⚖️ *АВТО-РЕБАЛАНС*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"

    if result["applied"]:
        msg += "✅ *Сделано внутри бирж (авто):*\n"
        for a in result["applied"]:
            sign = "+" if a["surplus_to_usdt"] >= 0 else ""
            msg += f"   {a['exchange']}: излишки → USDT ({sign}{a['surplus_to_usdt']})\n"
        msg += "\n"

    if result["fully_rebalanced"]:
        wd = suggest_withdrawal()
        msg += (
            f"✅ *Все биржи сбалансированы, торговля продолжается.*\n\n"
            f"💸 Свободно для вывода: ${wd['withdrawable']}"
        )
    else:
        msg += (
            "🔴 *ТОРГОВЛЯ ОСТАНОВЛЕНА* — не хватает средств внутри "
            "отдельных бирж, нужен ручной перевод между биржами:\n\n"
        )
        for instr in result["cross_exchange_needed"]:
            if instr["from"]:
                msg += (f"➡️ Переведите *${instr['amount_usdt']}* USDT: "
                        f"*{instr['from']} → {instr['to']}*\n")
                if instr["still_needed"] > 0.01:
                    msg += f"   (после этого на {instr['to']} всё ещё не хватит ${instr['still_needed']})\n"
            else:
                msg += f"⚠️ На {instr['to']} нужно ещё ${instr['still_needed']}, но свободных излишков на других биржах не найдено — требуется довнесение извне.\n"
        msg += "\n💡 Перевод USDT через TRC-20 = ~$1 комиссии.\n"
        first_real = next((i for i in result["cross_exchange_needed"] if i["from"]), None)
        if first_real:
            msg += (
                "После перевода на реальных биржах примените его в симуляции:\n"
                f"`/crosstransfer {first_real['from']} {first_real['to']} {first_real['amount_usdt']}`\n\n"
            )
        else:
            msg += "После перевода примените его: `/crosstransfer ОТКУДА КУДА СУММА`\n\n"
        msg += "Затем `/go` для возобновления торговли."
    return msg


def format_signal(opp: dict) -> str:
    is_real = not config["simulation_mode"]
    mode = "🔴 РЕАЛЬНАЯ" if is_real else "🔵 СИМУЛЯЦИЯ"

    if is_real:
        # НОВОЕ 07.08: карточка показывала только прибыль самой сделки, но
        # почти сразу после неё требуется ребаланс (продать скопившуюся
        # монету на бирже-покупателе обратно в USDT, докупить монету на
        # бирже-продавце взамен проданной) — это ДВЕ ДОПОЛНИТЕЛЬНЫЕ комиссии,
        # которых не было видно в исходной цифре. При лоте $10 и марже
        # 0.1778% ребаланс "съедал" почти половину показанной прибыли —
        # не убыток, но карточка вводила в заблуждение, выглядя вдвое
        # прибыльнее, чем цикл сделка+ребаланс даёт по факту.
        rebalance_cost = round(opp["vol"] * (FEES.get(opp["buy_ex"], 0.1) +
                                              FEES.get(opp["sell_ex"], 0.1)) / 100, 4)
        honest_profit = round(opp["profit_usdt"] - rebalance_cost, 4)
        profit_line = (
            f"💰 Прибыль сделки (до исполнения): `{opp['profit_usdt']} USDT`\n"
            f"⚖️ Ожидаемая стоимость ребаланса после неё: `~{rebalance_cost} USDT`\n"
            f"✅ *Честная прибыль полного цикла: `~{honest_profit} USDT`*\n"
            f"⚠️ Точная сумма зависит от факта исполнения — сверяйте с историей "
            f"ордеров на бирже, это предварительный расчёт, не гарантированный результат.\n\n"
        )
    else:
        derated = round(opp["profit_usdt"] * config["derating_factor"], 4)
        profit_line = (
            f"💰 Прибыль (симуляция): `{opp['profit_usdt']} USDT`\n"
            f"💡 Реалистичная оценка (×{config['derating_factor']}): "
            f"`~{derated} USDT`\n\n"
        )

    return (
        f"🚨 *{opp['buy_ex']} → {opp['sell_ex']} | {opp['symbol']}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{mode}\n\n"
        f"📥 Купить на {opp['buy_ex']}: `{opp['buy_price']}` "
        f"(реальная средняя цена, {opp['levels_used_buy']} уровней стакана)\n"
        f"📤 Продать на {opp['sell_ex']}: `{opp['sell_price']}` "
        f"({opp['levels_used_sell']} уровней)\n\n"
        f"📊 Спред (реальный, после проскальзывания): `{opp['gross_pct']}%`\n"
        f"📊 После комиссий: `{opp['net_pct']}%`\n"
        f"⚠️ Наивный расчёт (top-of-book) переоценивал спред на: "
        f"`{opp['slippage_impact_pct']}%`\n\n"
        f"{profit_line}"
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
            f"✅ *DepthArbBot* (Этап 3.1 — реальная глубина стакана)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Режим: {mode}\n"
            f"Монеты: {', '.join(SYMBOLS)}\n"
            f"Глубина стакана: {config['depth_limit']} уровней\n"
            f"Лот: ${config['trade_usdt']} | Порог: {config['min_profit_pct']}%\n\n"
            f"*Ключевое отличие от старого бота:*\n"
            f"Цена берётся не с first bid/ask, а честно "
            f"считается через walk-the-book по реальной глубине.\n\n"
            f"*Команды:*\n"
            f"/scan — скан сейчас | /top — все пары без порога\n"
            f"/triangle — треугольный арбитраж (Binance)\n"
            f"/depthcheck SYMBOL — сырой стакан + проскальзывание\n"
            f"/scancandidates СИМВОЛ1 СИМВОЛ2 ... — сравнить глубину нескольких "
            f"кандидатов на 3 биржах сразу, без добавления в торговлю\n"
            f"/stats — статистика | /balances — балансы\n"
            f"/rebalance — авто-ребаланс внутри бирж (+ инструкция если нужен перевод между биржами)\n"
            f"/crosstransfer FROM TO СУММА — записать ручной перевод\n"
            f"/rebalancelive on|off — включить/выключить РЕАЛЬНЫЕ ордера ребаланса (по умолчанию OFF — только план)\n"
            f"/apistatus — не заблокирована ли какая-то биржа rate-limit'ом\n"
            f"/realfees SYMBOL — подтянуть реальные комиссии аккаунта вместо дефолтных\n"
            f"/wsstatus — здоровье WebSocket-стаканов Binance\n"
            f"/setrebalance N — целевой запас (в лотах) на монету\n"
            f"/setreallot N — снизить реальный лимит ордера (не выше $15)\n"
            f"/setmaxtrades N — суточный лимит реальных сделок (сбрасывается каждый день)\n"
            f"/setbalancebuffer N — % запас в preflight-проверке перед сделкой (по умолч. {config['balance_safety_buffer_pct']}%)\n"
            f"/setheadroom N — % запас, который держит ребаланс сверх цели (по умолч. {config['rebalance_headroom_pct']}%)\n"
            f"/realbalance — точный разбор реального баланса и плана ребаланса по каждой бирже\n"
            f"/setrealstart — зафиксировать стартовый реальный капитал для честного P&L\n"
            f"/hours — активность по часам | /report — отчёт за день\n"
            f"/history — последние сделки | /csv — экспорт\n"
            f"/howtoread — как читать отчёты | /guide — инструкция\n"
            f"/pause /go /resume — управление торговлей\n"
            f"/addcoin /removecoin /listcoins — управление монетами\n"
            f"/withdraw — сколько можно вывести\n"
            f"/resetsim CONFIRM — сброс симуляции\n"
            f"/mode — переключить режим\n"
            f"/confirmreal /disablereal — гейт реальной торговли\n"
            f"/setlot 20 /setprofit 0.3 /setstop 10 /setinterval 3"
        )

    elif cmd == "/scancandidates":
        if len(parts) < 2:
            await send_tg(session,
                "Проверяет глубину стакана СРАЗУ по нескольким кандидатам на "
                "всех трёх биржах, без добавления их в торговлю — чтобы выбирать "
                "монету по цифрам, а не по одной вслепую.\n\n"
                "Пример: `/scancandidates TRX DOGE XRP ADA LTC TON`\n"
                "(до 8 монет за раз)"
            )
            return
        # ИСПРАВЛЕНИЕ: запятые/точки с запятой в списке монет (частый способ
        # ввода) раньше ломали разбор — "TRX," воспринималось как отдельный
        # несуществующий тикер.
        raw_candidates = " ".join(parts[1:]).replace(",", " ").replace(";", " ").split()
        candidates = [p.upper() for p in raw_candidates if p][:8]
        if not candidates:
            await send_tg(session, "Не нашёл ни одной монеты в команде. Пример: `/scancandidates TRX DOGE XRP`")
            return
        await send_tg(session, f"🔍 Проверяю глубину стакана на 3 биржах для: {', '.join(candidates)}...")

        results = []
        for sym in candidates:
            bn_ob = await get_orderbook_binance_rest(session, sym)
            kc_ob = await get_orderbook_kucoin_rest(session, sym)
            hx_ob = await get_orderbook_htx_rest(session, sym)
            books = {"Binance": bn_ob, "KuCoin": kc_ob, "HTX": hx_ob}

            row = {"symbol": sym, "exchanges": {}, "ok": True, "reasons": []}
            for ex, ob in books.items():
                if not ob:
                    row["ok"] = False
                    row["reasons"].append(f"{ex}: нет данных (пары нет или биржа не ответила)")
                    row["exchanges"][ex] = None
                    continue
                ask_levels, bid_levels = len(ob["asks"]), len(ob["bids"])
                fill500 = walk_the_book(ob["asks"], 500)
                slip500 = (round((fill500['avg_price'] - ob['asks'][0][0]) / ob['asks'][0][0] * 100, 2)
                           if fill500 else None)
                row["exchanges"][ex] = {
                    "ask": ob["asks"][0][0], "bid": ob["bids"][0][0],
                    "ask_levels": ask_levels, "bid_levels": bid_levels,
                    "slip500": slip500,
                }
                if ask_levels < 15 or bid_levels < 15:
                    row["ok"] = False
                    row["reasons"].append(f"{ex}: тонкий стакан ({ask_levels} ask / {bid_levels} bid уровней)")

            # Кросс-биржевой разброс цены (best bid каждой биржи между собой) —
            # если он аномально большой (>5%), это тот же симптом, что подвёл
            # ZIL/HTX: цена на одной из бирж оторвана от реального рынка.
            valid_bids = [v["bid"] for v in row["exchanges"].values() if v]
            if len(valid_bids) >= 2:
                spread_pct = round((max(valid_bids) - min(valid_bids)) / min(valid_bids) * 100, 2)
                row["cross_spread"] = spread_pct
                if spread_pct > 5:
                    row["ok"] = False
                    row["reasons"].append(f"цены между биржами расходятся на {spread_pct}% — подозрительно")
            else:
                row["cross_spread"] = None

            results.append(row)

        msg = "📊 *СРАВНЕНИЕ КАНДИДАТОВ*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for row in sorted(results, key=lambda r: (not r["ok"], r.get("cross_spread") or 999)):
            icon = "✅" if row["ok"] else "❌"
            msg += f"{icon} *{row['symbol']}*"
            if row["cross_spread"] is not None:
                msg += f" (разброс цены между биржами: {row['cross_spread']}%)"
            msg += "\n"
            for ex, d in row["exchanges"].items():
                if d is None:
                    msg += f"   {ex}: нет данных\n"
                else:
                    msg += (f"   {ex}: {d['ask_levels']}/{d['bid_levels']} уровней, "
                            f"проскальз. $500: {d['slip500']}%\n")
            if row["reasons"]:
                msg += f"   ⚠️ {'; '.join(row['reasons'])}\n"
            msg += "\n"
        msg += ("_✅ = минимум 15 уровней с обеих сторон на всех биржах, цены "
                "не расходятся сильнее 5% — годится для добавления через /addcoin._")
        await send_tg(session, msg)

    elif cmd == "/depthcheck":
        if len(parts) < 2 or parts[1].upper() not in SYMBOLS:
            await send_tg(session, f"Пример: `/depthcheck BONK`\nДоступно: {', '.join(SYMBOLS)}")
            return
        sym = parts[1].upper()
        await send_tg(session, f"🔍 Запрашиваю реальный стакан {sym} с трёх бирж...")
        bn, kc, hx, active = await fetch_all_orderbooks(session)
        msg = f"📖 *Стакан {sym}USDT*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for ex, books in [("Binance", bn), ("KuCoin", kc), ("HTX", hx)]:
            ob = books.get(sym)
            if not ob:
                msg += f"❌ *{ex}:* не удалось получить (отказов подряд: {stats['depth_fail'][ex]})\n\n"
                continue
            fill20 = walk_the_book(ob["asks"], 20)
            fill500 = walk_the_book(ob["asks"], 500)
            msg += f"✅ *{ex}:*\n"
            msg += f"   Best ask: `{ob['asks'][0][0]}` | Best bid: `{ob['bids'][0][0]}`\n"
            msg += f"   Уровней: {len(ob['asks'])} ask / {len(ob['bids'])} bid\n"
            if fill20:
                slip = round((fill20['avg_price'] - ob['asks'][0][0]) / ob['asks'][0][0] * 100, 4)
                msg += f"   $20 → avg `{fill20['avg_price']}` (проскальз. {slip}%, filled={fill20['fully_filled']})\n"
            if fill500:
                slip = round((fill500['avg_price'] - ob['asks'][0][0]) / ob['asks'][0][0] * 100, 4)
                msg += f"   $500 → avg `{fill500['avg_price']}` (проскальз. {slip}%, filled={fill500['fully_filled']})\n"
            msg += "\n"
        await send_tg(session, msg)

    elif cmd == "/scan":
        if config["paused"]:
            await send_tg(session, "⏸ Бот на паузе. /go для возобновления.")
            return
        await send_tg(session, "🔍 Сканирую реальную глубину стакана на 3 биржах...")
        signals, active = await scan_all(session)
        if not signals:
            await send_tg(session,
                f"😔 Нет валидных сигналов (порог {config['min_profit_pct']}%).\n"
                f"Бирж онлайн: {', '.join(active) if active else 'ни одной!'}\n"
                f"Отказов стакана: Binance={stats['depth_fail']['Binance']} "
                f"KuCoin={stats['depth_fail']['KuCoin']} HTX={stats['depth_fail']['HTX']}\n"
                f"Недостаточно ликвидности (за всё время): {stats['insufficient_liquidity']}\n"
                f"Сбоев получения 24h-объёма: {stats.get('volume_fetch_fail', 0)}"
            )
        else:
            await send_tg(session, f"✅ {len(signals)} валидных сигналов (после проверки реальной глубины)!")
            for opp in signals[:3]:
                result = await execute_trade(session, opp)
                if result["executed"]:
                    await send_tg(session, "✅ *ИСПОЛНЕНО*\n\n" + format_signal(opp))
                else:
                    reason = REASON_LABELS.get(result["reason"], result["reason"])
                    await send_tg(session,
                        f"⛔ {opp['symbol']} {opp['buy_ex']}→{opp['sell_ex']} "
                        f"пропущено: {reason}")

    elif cmd == "/stats":
        per_trade = round(stats["profit"] / stats["trades"], 4) if stats["trades"] else 0

        if not config["simulation_mode"]:
            # РЕАЛЬНЫЙ РЕЖИМ — честные цифры с бирж, не симуляционные
            await send_tg(session, "📡 Читаю реальный баланс с трёх бирж...")
            real = await get_total_real_capital(session)
            if real is None:
                balance_block = "⚠️ Не удалось прочитать реальный баланс — см. /realbalance для деталей.\n"
            else:
                per_ex = " | ".join(f"{ex}: ${v}" for ex, v in real["per_exchange"].items())
                if config["real_start_capital"]:
                    pnl_real = round(real["total"] - config["real_start_capital"], 2)
                    balance_block = (
                        f"💵 Реальный баланс: ${real['total']} ({per_ex})\n"
                        f"   Старт (зафиксирован): ${config['real_start_capital']} | "
                        f"P&L: {pnl_real:+.2f}\n"
                    )
                else:
                    balance_block = (
                        f"💵 Реальный баланс: ${real['total']} ({per_ex})\n"
                        f"   💡 Стартовая точка не зафиксирована — `/setrealstart` "
                        f"чтобы считать P&L честно\n"
                    )
            await send_tg(session,
                f"📈 *СТАТИСТИКА*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🔴 РЕАЛЬНЫЙ | сделок сегодня: {config['real_trades_today']}/"
                f"{config['max_real_trades_per_day']}\n\n"
                f"Сканов: {stats['scans']} | Сигналов: {stats['signals']} | "
                f"Реальных сделок исполнено: {stats['trades']}\n\n"
                f"⚠️ *Отказы API стакана:*\n"
                f"   Binance: {stats['depth_fail']['Binance']}\n"
                f"   KuCoin: {stats['depth_fail']['KuCoin']}\n"
                f"   HTX: {stats['depth_fail']['HTX']}\n"
                f"   Сбоев 24h-объёма: {stats.get('volume_fetch_fail', 0)}\n\n"
                f"🔧 Автодокупок при нехватке баланса: {stats['topup_success']}/{stats['topup_attempts']} "
                f"(потрачено сегодня: ~${stats.get('topup_cost_usdt', 0.0):.2f} из "
                f"${config['max_topup_spend_per_day']} лимита)\n"
                f"🚫 Отклонено как неправдоподобный спред (>{config['max_plausible_spread_pct']}%): "
                f"{stats.get('implausible_spread_rejected', 0)}\n"
                f"🚫 Отклонено из-за тонкого стакана (<{config['min_depth_levels_required']} уровней): "
                f"{stats.get('thin_book_rejected', 0)}\n"
                f"🚫 Отклонено из-за низкого 24h-объёма (<${config['min_volume_usdt']:,.0f}): "
                f"{stats.get('volume_too_low_rejected', 0)}\n\n"
                f"{balance_block}\n"
                f"⚙️ Реальный лимит ордера: ${config['max_real_order_usdt']} | "
                f"Порог: {config['min_profit_pct']}% | "
                f"Буфер баланса: {config['balance_safety_buffer_pct']}% | "
                f"Запас ребаланса: {config['rebalance_headroom_pct']}%"
            )
            return

        # СИМУЛЯЦИЯ — как раньше
        total_bal = get_balance_usdt()
        pnl = round(total_bal - SIM_START, 2)
        wd = suggest_withdrawal()
        await send_tg(session,
            f"📈 *СТАТИСТИКА*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🔵 СИМУЛЯЦИЯ\n\n"
            f"Сканов: {stats['scans']} | Сигналов: {stats['signals']} | Сделок: {stats['trades']}\n"
            f"Прибыль (сим.): {round(stats['profit'],2)} USDT | На сделку: ~{per_trade}\n"
            f"Реалистичная оценка (×{config['derating_factor']}): "
            f"~{round(stats['profit']*config['derating_factor'],2)} USDT\n\n"
            f"⚠️ *Отказы API стакана:*\n"
            f"   Binance: {stats['depth_fail']['Binance']}\n"
            f"   KuCoin: {stats['depth_fail']['KuCoin']}\n"
            f"   HTX: {stats['depth_fail']['HTX']}\n\n"
            f"⚠️ Отклонено (нехватка ликвидности стакана): {stats['insufficient_liquidity']}\n"
            f"⚠️ Отклонено (нехватка виртуального баланса): {stats.get('insufficient_balance_skips', 0)}\n\n"
            f"💵 Баланс: старт ${SIM_START} → сейчас ${total_bal} (P&L {pnl:+.2f})\n"
            f"💸 Можно вывести (оценка): ${wd['withdrawable']} "
            f"(держим ${wd['min_operating']} в обороте)\n\n"
            f"⚙️ Лот: ${config['trade_usdt']} | Порог: {config['min_profit_pct']}%"
        )

    elif cmd == "/withdraw":
        wd = suggest_withdrawal()
        await send_tg(session,
            f"💸 *ОЦЕНКА ВЫВОДА*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Текущий баланс: ${wd['total']}\n"
            f"Минимум в обороте (150% старта): ${wd['min_operating']}\n"
            f"Можно вывести: *${wd['withdrawable']}*\n\n"
            f"⚠️ Это расчёт по симуляции. Перед реальным выводом обязательно "
            f"сверьте с фактическими балансами на биржах через /balances."
        )

    elif cmd == "/resetsim":
        if len(parts) < 2 or parts[1] != "CONFIRM":
            await send_tg(session,
                "⚠️ Это обнулит ВСЮ статистику и балансы симуляции.\n"
                "Для подтверждения: `/resetsim CONFIRM`"
            )
            return
        reset_simulation()
        await send_tg(session, "✅ Симуляция сброшена к стартовому состоянию ($500).")

    elif cmd == "/pause":
        config["paused"] = True
        await send_tg(session,
            "⏸ *ПАУЗА АКТИВИРОВАНА*\n\n"
            "Можешь спокойно переводить деньги между биржами,\n"
            "покупать/продавать вручную, делать ребаланс.\n\n"
            "Когда закончишь — /go"
        )

    elif cmd == "/go":
        config["paused"] = False
        await send_tg(session, f"▶️ Торговля возобновлена. Следующий скан через {config['scan_interval']} сек.")

    elif cmd == "/resume":
        config["trading_active"] = True
        config["daily_loss"] = 0.0
        await send_tg(session, "✅ Стоп-лосс снят. Торговля возобновлена.")

    elif cmd == "/balances":
        total = get_balance_usdt()
        msg = "💰 *БАЛАНСЫ СИМУЛЯЦИИ*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for ex, assets in sim_balances.items():
            ex_total = sum(assets.values())
            msg += f"🏦 *{ex}:* ${round(ex_total, 2)}\n"
            for asset, val in assets.items():
                icon = "🟢" if val >= 20 else "🔴"
                msg += f"   {icon} {asset}: ${round(val, 2)}\n"
            msg += "\n"
        pnl = round(total - SIM_START, 2)
        sign = "+" if pnl >= 0 else ""
        msg += f"💵 *Итого: ${total}*\nСтарт: ${SIM_START} | P&L: {sign}{pnl}"
        await send_tg(session, msg)

    elif cmd == "/rebalance":
        if not config["simulation_mode"]:
            config["paused"] = True
            result = await real_auto_rebalance_all(session)
            await send_tg(session, format_real_rebalance_result(result))
            if result.get("safe_to_resume", False):
                config["paused"] = False
            return
        warns = check_balance_warnings()
        if not warns:
            await send_tg(session, "✅ Все балансы в норме! Ребалансировка не нужна.")
            return
        config["paused"] = True
        result = auto_rebalance_all()
        await send_tg(session, format_rebalance_result(result))
        if result["fully_rebalanced"]:
            config["paused"] = False

    elif cmd == "/autorebalance":
        config["paused"] = True
        if not config["simulation_mode"]:
            result = await real_auto_rebalance_all(session)
            await send_tg(session, format_real_rebalance_result(result))
            if result.get("safe_to_resume", False):
                config["paused"] = False
        else:
            result = auto_rebalance_all()
            await send_tg(session, format_rebalance_result(result))
            if result["fully_rebalanced"]:
                config["paused"] = False

    elif cmd == "/rebalancelive":
        if len(parts) < 2 or parts[1].lower() not in ("on", "off"):
            state = "ВЫКЛЮЧЕН (dry-run, безопасно)" if config["real_rebalance_dry_run"] else "🔴 ВКЛЮЧЁН (реальные ордера)"
            await send_tg(session,
                f"Текущий режим реального ребаланса: {state}\n\n"
                f"`/rebalancelive on` — включить реальные ордера ребаланса\n"
                f"`/rebalancelive off` — вернуть в безопасный dry-run режим"
            )
            return
        if parts[1].lower() == "on":
            config["real_rebalance_dry_run"] = False
            await send_tg(session,
                "🔴 *Реальные ордера ребаланса ВКЛЮЧЕНЫ.*\n\n"
                "Следующий `/rebalance` или `/autorebalance` будет реально "
                "продавать/покупать на бирже, с реальной комиссией."
            )
        else:
            config["real_rebalance_dry_run"] = True
            await send_tg(session, "🔵 Реальный ребаланс возвращён в безопасный режим (только план, без ордеров).")

    elif cmd == "/crosstransfer":
        if len(parts) < 4:
            await send_tg(session,
                "Пример: `/crosstransfer HTX KuCoin 50`\n"
                "(записывает в симуляцию перевод, который вы УЖЕ сделали "
                "руками между реальными биржами)")
            return
        from_ex, to_ex = parts[1], parts[2]
        try:
            amount = float(parts[3])
        except ValueError:
            await send_tg(session, "❌ Сумма должна быть числом.")
            return
        if apply_manual_transfer(from_ex, to_ex, amount):
            await send_tg(session,
                f"✅ Записано: ${amount} USDT перенесено {from_ex} → {to_ex}.\n\n"
                f"Теперь можно `/autorebalance` (докупить нужные монеты на {to_ex}) "
                f"или сразу `/go`, если балансов хватает."
            )
        else:
            await send_tg(session,
                f"❌ Не удалось: либо биржа не найдена, либо на {from_ex} "
                f"недостаточно USDT (${round(sim_balances.get(from_ex,{}).get('USDT',0),2)})."
            )

    elif cmd == "/setrebalance":
        if len(parts) < 2:
            await send_tg(session,
                f"Текущая цель: {config['rebalance_target_lots']} лотов на монету/USDT.\n"
                f"Пример: `/setrebalance 3`")
            return
        try:
            config["rebalance_target_lots"] = int(parts[1])
            await send_tg(session, f"✅ Цель ребаланса: {config['rebalance_target_lots']} лотов "
                                    f"(${config['trade_usdt']*config['rebalance_target_lots']} на монету/USDT в СИМУЛЯЦИИ, "
                                    f"${config['max_real_order_usdt']*config['rebalance_target_lots']} в РЕАЛЬНОМ режиме)")
        except ValueError:
            await send_tg(session, "❌ Пример: `/setrebalance 3`")

    elif cmd == "/setsellreserve":
        if len(parts) < 2:
            await send_tg(session,
                f"Текущий резерв ПРОДАЖИ: {config.get('sell_reserve_lots', config['rebalance_target_lots'])} лотов "
                f"на монету (≈${config['max_real_order_usdt']*config.get('sell_reserve_lots', config['rebalance_target_lots'])} "
                f"в реальном режиме).\n\n"
                f"Это ОТДЕЛЬНО от `/setrebalance` (тот — резерв USDT на покупку). "
                f"Держать здесь больше лотов полезно — не нужно докупать монету почти "
                f"на каждой сделке.\n\n"
                f"Пример: `/setsellreserve 3`")
            return
        try:
            config["sell_reserve_lots"] = int(parts[1])
            await send_tg(session, f"✅ Резерв продажи: {config['sell_reserve_lots']} лотов "
                                    f"(≈${config['max_real_order_usdt']*config['sell_reserve_lots']} в реальном режиме)")
        except ValueError:
            await send_tg(session, "❌ Пример: `/setsellreserve 3`")

    elif cmd == "/setbalancebuffer":
        if len(parts) < 2:
            await send_tg(session,
                f"Текущий буфер preflight-проверки перед сделкой: {config['balance_safety_buffer_pct']}%\n\n"
                f"Это % запас сверх расчётной нужной суммы, который должен быть "
                f"на бирже продажи, иначе сделка отклоняется (или пробуется "
                f"точечная автодокупка).\n\n"
                f"Пример: `/setbalancebuffer 1` (было жёстко зашито 2%, из-за чего "
                f"сделки ложно отклонялись при почти достаточном балансе)"
            )
            return
        try:
            val = float(parts[1])
            if val < 0 or val > 20:
                await send_tg(session, "❌ Разумный диапазон: 0–20%.")
                return
            config["balance_safety_buffer_pct"] = val
            await send_tg(session, f"✅ Буфер preflight-проверки: {val}%")
        except ValueError:
            await send_tg(session, "❌ Пример: `/setbalancebuffer 1`")

    elif cmd == "/setheadroom":
        if len(parts) < 2:
            overrides_str = ", ".join(f"{ex}={v}%" for ex, v in config["rebalance_headroom_overrides"].items()) or "нет"
            await send_tg(session,
                f"Текущий ОБЩИЙ запас ребаланса: {config['rebalance_headroom_pct']}%\n"
                f"Персональные переопределения по биржам: {overrides_str}\n\n"
                f"Должен быть заметно БОЛЬШЕ, чем `/setbalancebuffer` — иначе баланс "
                f"после ребаланса снова окажется впритык к порогу проверки.\n\n"
                f"Пример: `/setheadroom 15`\n"
                f"Для отдельной биржи: `/setheadroomex HTX 3`"
            )
            return
        try:
            val = float(parts[1])
            if val < 0 or val > 100:
                await send_tg(session, "❌ Разумный диапазон: 0–100%.")
                return
            config["rebalance_headroom_pct"] = val
            await send_tg(session, f"✅ Общий запас ребаланса: {val}%")
        except ValueError:
            await send_tg(session, "❌ Пример: `/setheadroom 15`")

    elif cmd == "/setheadroomex":
        if len(parts) < 3:
            overrides_str = ", ".join(f"{ex}={v}%" for ex, v in config["rebalance_headroom_overrides"].items()) or "нет"
            await send_tg(session,
                f"Персональные переопределения запаса ребаланса по биржам: {overrides_str}\n\n"
                f"Нужно для бирж с двойной ролью (одновременно покупают и продают) на "
                f"ограниченном капитале — общий запас там физически не помещается.\n\n"
                f"Пример: `/setheadroomex HTX 3` — поставить HTX персональный запас 3%\n"
                f"`/setheadroomex HTX reset` — убрать переопределение, вернуть общий запас"
            )
            return
        ex_name = parts[1]
        if ex_name not in ("Binance", "KuCoin", "HTX"):
            await send_tg(session, "❌ Биржа должна быть одной из: Binance, KuCoin, HTX")
            return
        if parts[2].lower() == "reset":
            config["rebalance_headroom_overrides"].pop(ex_name, None)
            await send_tg(session, f"✅ Переопределение для {ex_name} снято, используется общий запас "
                                    f"{config['rebalance_headroom_pct']}%")
            return
        try:
            val = float(parts[2])
            if val < 0 or val > 100:
                await send_tg(session, "❌ Разумный диапазон: 0–100%.")
                return
            config["rebalance_headroom_overrides"][ex_name] = val
            await send_tg(session, f"✅ Персональный запас ребаланса для {ex_name}: {val}%")
        except ValueError:
            await send_tg(session, "❌ Пример: `/setheadroomex HTX 3`")

    elif cmd == "/setmaxtopup":
        if len(parts) < 2:
            await send_tg(session,
                f"Текущий дневной лимит на автодокупку резерва: ${config['max_topup_spend_per_day']}\n\n"
                f"Потрачено сегодня: ~${stats.get('topup_cost_usdt', 0.0):.2f}\n\n"
                f"Пример: `/setmaxtopup 20`"
            )
            return
        try:
            val = float(parts[1])
            if val < 0:
                await send_tg(session, "❌ Не может быть отрицательным.")
                return
            config["max_topup_spend_per_day"] = val
            await send_tg(session, f"✅ Дневной лимит автодокупки: ${val}")
        except ValueError:
            await send_tg(session, "❌ Пример: `/setmaxtopup 20`")

    elif cmd == "/setminvolume":
        # НОВОЕ 06.08: раньше этот порог (по умолчанию $100,000 24h-объёма
        # на Binance) можно было изменить только через деплой. Обнаружено,
        # что он мог тихо блокировать сигналы по некрупным, но вполне
        # ликвидным монетам (IOST, ZK) — реальная безопасность и так
        # обеспечена честной проверкой глубины стакана (walk-the-book) чуть
        # ниже по цепочке, этот фильтр — вторичный и вспомогательный.
        if len(parts) < 2:
            await send_tg(session,
                f"Текущий порог минимального 24h-объёма (Binance): ${config['min_volume_usdt']:,.0f}\n\n"
                f"Это ВСПОМОГАТЕЛЬНЫЙ фильтр — реальная безопасность обеспечивается "
                f"честной проверкой глубины стакана (walk-the-book), не этим числом. "
                f"Слишком высокий порог может молча блокировать сигналы по некрупным, "
                f"но вполне ликвидным монетам.\n\n"
                f"Пример: `/setminvolume 10000` (снизить) или `/setminvolume 0` (отключить совсем)"
            )
            return
        try:
            val = float(parts[1])
            if val < 0:
                await send_tg(session, "❌ Не может быть отрицательным.")
                return
            config["min_volume_usdt"] = val
            await send_tg(session, f"✅ Порог минимального 24h-объёма: ${val:,.0f}")
        except ValueError:
            await send_tg(session, "❌ Пример: `/setminvolume 10000`")

    elif cmd == "/setmaxspread":
        if len(parts) < 2:
            await send_tg(session,
                f"Текущий потолок правдоподобного спреда: {config['max_plausible_spread_pct']}%\n\n"
                f"Сигналы со спредом ВЫШЕ этого значения отклоняются как вероятный "
                f"артефакт тонкого/неактуального стакана, а не реальная возможность.\n\n"
                f"Пример: `/setmaxspread 5`"
            )
            return
        try:
            val = float(parts[1])
            if val <= 0 or val > 50:
                await send_tg(session, "❌ Разумный диапазон: 0.1-50%.")
                return
            config["max_plausible_spread_pct"] = val
            await send_tg(session, f"✅ Потолок правдоподобного спреда: {val}%")
        except ValueError:
            await send_tg(session, "❌ Пример: `/setmaxspread 5`")

    elif cmd == "/setreallot":
        # ИСПРАВЛЕНИЕ 08.08: раньше минимум лота считался от ВСЕХ бирж в
        # словаре MIN_ORDER_VALUE_USD, включая HTX ($10) — даже если HTX
        # сейчас не участвует в маршруте ни одной активной монеты (как
        # сейчас, после удаления TRX). Теперь минимум считается только от
        # бирж, которые ДЕЙСТВИТЕЛЬНО используются текущими монетами.
        active_exchanges = set()
        for sym in SYMBOLS:
            for buy_ex, sell_ex in pairs_for_symbol(sym):
                active_exchanges.add(buy_ex)
                active_exchanges.add(sell_ex)
        relevant_minimums = [MIN_ORDER_VALUE_USD.get(ex, 5.0) for ex in active_exchanges] or [5.0]
        floor_val = max(relevant_minimums)
        if len(parts) < 2:
            await send_tg(session,
                f"Текущий реальный лимит ордера: ${config['max_real_order_usdt']}\n"
                f"Диапазон: от ${floor_val} до $15 (минимум ДЕЙСТВУЮЩИХ бирж ↔ потолок безопасности).\n"
                f"Активные биржи сейчас: {', '.join(sorted(active_exchanges)) or '—'}\n"
                f"Пример: `/setreallot 10`")
            return
        try:
            new_val = float(parts[1])
            if new_val > 15.0:
                await send_tg(session,
                    "❌ Нельзя установить больше $15 — это намеренный потолок безопасности, "
                    "заложенный при построении реального исполнения.")
                return
            if new_val < floor_val:
                await send_tg(session,
                    f"❌ Нельзя установить меньше ${floor_val} — это минимальная сумма ордера "
                    f"среди бирж, которые СЕЙЧАС реально используются ({', '.join(sorted(active_exchanges))}). "
                    f"Сделка с лотом меньше этого будет ГАРАНТИРОВАННО отклонена биржей. "
                    f"Минимум — ${floor_val}.")
                return
            config["max_real_order_usdt"] = new_val
            needed = round(new_val * config["rebalance_target_lots"] * 5, 2)
            await send_tg(session,
                f"✅ Реальный лимит ордера: ${new_val}\n"
                f"При текущей цели ребаланса ({config['rebalance_target_lots']} лот(а)) "
                f"нужно суммарно ~${needed} на всех биржах для комфортного буфера."
            )
        except ValueError:
            await send_tg(session, "❌ Пример: `/setreallot 10`")

    elif cmd == "/setmaxtrades":
        if len(parts) < 2:
            await send_tg(session,
                f"Текущий суточный лимит реальных сделок: {config['max_real_trades_per_day']}\n"
                f"Пример: `/setmaxtrades 200`\n\n"
                f"⚠️ Раньше счётчик НЕ сбрасывался вообще — после 20 сделок с момента "
                f"старта бот навсегда блокировал торговлю. Теперь исправлено: "
                f"сбрасывается каждые сутки автоматически."
            )
            return
        try:
            config["max_real_trades_per_day"] = int(parts[1])
            await send_tg(session, f"✅ Суточный лимит: {config['max_real_trades_per_day']} сделок")
        except ValueError:
            await send_tg(session, "❌ Пример: `/setmaxtrades 200`")

    elif cmd == "/setrealstart":
        await send_tg(session, "📡 Читаю реальный баланс для фиксации стартовой точки...")
        real = await get_total_real_capital(session)
        if real is None:
            await send_tg(session, "🔴 Не удалось прочитать баланс. Попробуйте /realbalance для диагностики.")
            return
        config["real_start_capital"] = real["total"]
        await send_tg(session,
            f"✅ Стартовая точка зафиксирована: ${real['total']}\n"
            f"Дальше `/stats` будет честно считать P&L от этой суммы."
        )

    elif cmd == "/mode":
        if config["simulation_mode"]:
            # Переход в реальный режим — только если гейт уже пройден
            if not is_real_trading_allowed():
                await send_tg(session,
                    "❌ *Реальная торговля заблокирована.*\n\n"
                    "Для включения нужны ВСЕ условия:\n"
                    f"1️⃣ Переменная Railway `REAL_TRADING_UNLOCKED` = `{CONFIRM_PHRASE}`\n"
                    "2️⃣ Все 7 API-ключей (Binance/KuCoin/HTX) заданы в Railway\n"
                    "3️⃣ Команда `/confirmreal " + CONFIRM_PHRASE + "` в этом чате\n\n"
                    f"⚙️ Лимит на ордер в реальном режиме: ${config['max_real_order_usdt']} "
                    f"(жёстко, /setlot его не увеличит)\n"
                    f"⚙️ Лимит сделок в день: {config['max_real_trades_per_day']}"
                )
                return
            config["simulation_mode"] = False
            await send_tg(session,
                "🔴 *РЕАЛЬНАЯ ТОРГОВЛЯ АКТИВНА*\n\n"
                f"Лимит на ордер: ${config['max_real_order_usdt']}\n"
                f"Лимит сделок/день: {config['max_real_trades_per_day']}\n\n"
                "При ручных операциях — /pause"
            )
        else:
            config["simulation_mode"] = True
            await send_tg(session, "🔵 Режим: СИМУЛЯЦИЯ")

    elif cmd == "/confirmreal":
        if len(parts) < 2 or parts[1] != CONFIRM_PHRASE:
            await send_tg(session,
                f"Для подтверждения реальной торговли напишите ТОЧНО:\n"
                f"`/confirmreal {CONFIRM_PHRASE}`\n\n"
                f"⚠️ Это включит возможность реальных сделок реальными деньгами "
                f"(лимит ${config['max_real_order_usdt']}/ордер). Убедитесь, что "
                f"понимаете риски: код НЕ тестировался на реальном API."
            )
            return
        config["real_confirmed"] = True
        env_ok = REAL_TRADING_UNLOCKED == CONFIRM_PHRASE
        await send_tg(session,
            f"{'✅' if env_ok else '⚠️'} Runtime-подтверждение получено.\n"
            f"Переменная окружения REAL_TRADING_UNLOCKED: "
            f"{'✅ установлена' if env_ok else '❌ НЕ установлена — /mode всё ещё заблокирует реальный режим'}\n\n"
            f"Теперь используйте `/mode` для фактического переключения."
        )

    elif cmd == "/disablereal":
        config["real_confirmed"] = False
        config["simulation_mode"] = True
        await send_tg(session, "🔵 Реальная торговля отключена, гейт сброшен. Режим: СИМУЛЯЦИЯ")

    elif cmd == "/addcoin":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/addcoin DOGE`")
            return
        sym = parts[1].upper()
        if sym in SYMBOLS:
            await send_tg(session, f"⚠️ {sym} уже в списке.")
            return
        SYMBOLS.append(sym)
        stats["symbol_stats"][sym] = 0
        start_binance_ws_book(session, sym)
        start_kucoin_ws_book(session, sym)
        start_htx_ws_book(session, sym)
        # Без начального баланса монета будет получать сигналы, но НИКОГДА не
        # сможет исполниться в симуляции (has_sufficient_sim_balance всегда
        # откажет) — та же ситуация, что случилась с FET/INJ. Даём стартовый
        # виртуальный баланс в 5 лотов на каждой бирже, где монета продаётся.
        seed = config["trade_usdt"] * 5
        for ex in ["KuCoin", "HTX"]:
            sim_balances.setdefault(ex, {})[sym] = seed
        await send_tg(session,
            f"✅ Добавлено: *{sym}*\n"
            f"📡 Binance WS-стакан запускается (может занять до 15 сек до синхронизации)\n"
            f"💰 Выдан стартовый баланс ${seed} на KuCoin и HTX (виртуально, "
            f"для теста — не забудьте пополнить реально при переходе в реальный режим)\n\n"
            f"⚠️ Учтите: для {sym} нужна ликвидность и реальная проверка через "
            f"`/depthcheck {sym}` перед тем, как доверять сигналам по нему.\n\n"
            f"Текущий список: {', '.join(SYMBOLS)}"
        )

    elif cmd == "/removecoin":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/removecoin BONK`")
            return
        sym = parts[1].upper()
        if sym not in SYMBOLS:
            await send_tg(session, f"⚠️ {sym} не найдена в списке.")
            return
        if len(SYMBOLS) <= 1:
            await send_tg(session, "❌ Нельзя удалить последнюю монету из списка.")
            return
        SYMBOLS.remove(sym)
        stop_binance_ws_book(sym)
        stop_kucoin_ws_book(sym)
        stop_htx_ws_book(sym)
        # Немедленно ликвидируем остаток монеты в USDT на всех биржах —
        # иначе баланс "зависает" видимым в /balances, но недоступным
        # для торговли и авто-ребаланса (это и произошло с BONK).
        liquidated = {}
        for ex, assets in sim_balances.items():
            if sym in assets:
                amount = assets.pop(sym)
                assets["USDT"] = assets.get("USDT", 0) + amount
                if amount:
                    liquidated[ex] = round(amount, 2)
        liq_lines = "\n".join(f"   {ex}: ${amt} → USDT" for ex, amt in liquidated.items())

        # ИСПРАВЛЕНИЕ 05.08: раньше в РЕАЛЬНОМ режиме остаток монеты на
        # реальных биржах вообще не трогался — просто исчезал из SYMBOLS и
        # становился НЕВИДИМЫМ для /realbalance и ребаланса (real_exchange_
        # rebalance_plan сверяется только с текущим SYMBOLS), то есть навсегда
        # застревал бы на бирже, требуя ручной продажи. Теперь при удалении
        # монеты в реальном режиме бот сам пытается продать реальный остаток
        # обратно в USDT на каждой бирже, где он есть.
        real_liquidated = {}
        real_liquidation_failed = []
        if not config["simulation_mode"]:
            for ex in ["Binance", "KuCoin", "HTX"]:
                real_balances = await get_real_balances(session, ex)
                if not real_balances:
                    continue
                qty = real_balances.get(sym, 0.0)
                if qty <= 0:
                    continue
                sell_qty = await round_quantity_for_exchange(session, ex, sym, qty)
                if sell_qty <= 0:
                    continue
                result = None
                if ex == "Binance":
                    result = await place_order_binance(session, sym, "SELL", sell_qty)
                elif ex == "KuCoin":
                    result = await place_order_kucoin(session, sym, "sell", sell_qty, use_funds=False)
                elif ex == "HTX":
                    global _htx_account_id_cache
                    if not _htx_account_id_cache:
                        _htx_account_id_cache = await get_htx_account_id(session)
                    if _htx_account_id_cache:
                        result = await place_order_htx(session, _htx_account_id_cache, sym, "sell-market", sell_qty)
                if result:
                    real_liquidated[ex] = sell_qty
                else:
                    real_liquidation_failed.append(f"{ex} ({sell_qty} {sym}, {_last_exchange_error.get(ex, 'нет деталей')})")

        real_liq_lines = "\n".join(f"   {ex}: продано {qty} {sym} → USDT" for ex, qty in real_liquidated.items())
        msg = f"✅ Удалено: *{sym}*\n"
        if liquidated:
            msg += f"💰 Остатки симуляции конвертированы в USDT:\n{liq_lines}\n\n"
        if real_liquidated:
            msg += f"💰 РЕАЛЬНЫЙ остаток продан в USDT:\n{real_liq_lines}\n\n"
        if real_liquidation_failed:
            msg += ("🔴 Не удалось продать реальный остаток (продайте вручную в приложении биржи):\n   " +
                    "\n   ".join(real_liquidation_failed) + "\n\n")
        msg += f"Текущий список: {', '.join(SYMBOLS)}"
        await send_tg(session, msg)

    elif cmd == "/listcoins":
        await send_tg(session, f"💱 *Торгуемые монеты:* {', '.join(SYMBOLS)}\n\n"
                                f"Добавить: `/addcoin SYMBOL`\nУдалить: `/removecoin SYMBOL`")

    elif cmd == "/sellcoin":
        # НОВОЕ 05.08: продать ЛЮБОЙ реальный остаток монеты на всех трёх
        # биржах, даже если её СЕЙЧАС нет в SYMBOLS. Нужно для ситуаций вроде
        # ZIL: список монет сбросился при передеплое (или монету удалили
        # раньше), а реальный остаток на биржах остался и стал невидим для
        # /removecoin (тот требует, чтобы монета сначала была в списке).
        if len(parts) < 2:
            await send_tg(session,
                "Продаёт РЕАЛЬНЫЙ остаток монеты на всех трёх биржах в USDT, "
                "даже если её нет в текущем списке SYMBOLS.\n\n"
                "Пример: `/sellcoin ZIL`"
            )
            return
        sym = parts[1].upper()
        if config["simulation_mode"]:
            await send_tg(session, "⚠️ Бот в режиме симуляции — на реальных биржах продавать нечего "
                                    "(переключитесь `/mode`, если нужно продать реальный остаток).")
            return
        await send_tg(session, f"📡 Проверяю реальный остаток {sym} на трёх биржах...")
        sold, failed, none_found = {}, [], []
        for ex in ["Binance", "KuCoin", "HTX"]:
            real_balances = await get_real_balances(session, ex)
            if not real_balances:
                failed.append(f"{ex}: не удалось прочитать баланс")
                continue
            qty = real_balances.get(sym, 0.0)
            if qty <= 0:
                none_found.append(ex)
                continue
            sell_qty = await round_quantity_for_exchange(session, ex, sym, qty)
            if sell_qty <= 0:
                none_found.append(f"{ex} (остаток {qty} слишком мал после округления)")
                continue
            result = None
            if ex == "Binance":
                result = await place_order_binance(session, sym, "SELL", sell_qty)
            elif ex == "KuCoin":
                result = await place_order_kucoin(session, sym, "sell", sell_qty, use_funds=False)
            elif ex == "HTX":
                if not _htx_account_id_cache:
                    _htx_account_id_cache = await get_htx_account_id(session)
                if _htx_account_id_cache:
                    result = await place_order_htx(session, _htx_account_id_cache, sym, "sell-market", sell_qty)
            if result:
                sold[ex] = sell_qty
            else:
                failed.append(f"{ex} ({sell_qty} {sym}, {_last_exchange_error.get(ex, 'нет деталей')})")

        msg = f"📊 *Продажа {sym}*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        if sold:
            msg += "✅ Продано:\n" + "\n".join(f"   {ex}: {qty} {sym} → USDT" for ex, qty in sold.items()) + "\n\n"
        if none_found:
            msg += "➖ Остатка нет: " + ", ".join(none_found) + "\n\n"
        if failed:
            msg += "🔴 Не удалось продать:\n   " + "\n   ".join(failed) + "\n\n"
        if not sold and not failed:
            msg += "На всех биржах остатка не найдено — продавать нечего."
        await send_tg(session, msg)

    elif cmd == "/setfee":
        # НОВОЕ 06.08: /realfees дёргает API биржи, но у Binance этот
        # эндпоинт не отражает скидку от оплаты комиссии в BNB (возвращает
        # номинальную ставку, а не фактическую после скидки) — скидка
        # применяется в момент самой сделки, но не видна через этот запрос.
        # Раз включили "Оплата комиссии в BNB/KCS" вручную в приложении
        # биржи — можно задать реальную ставку сюда напрямую, без API.
        if len(parts) < 3:
            await send_tg(session,
                f"Текущие комиссии: {FEES}\n\n"
                f"Задать вручную (например, после включения скидки BNB/KCS, "
                f"которую /realfees не видит через API):\n"
                f"`/setfee Binance 0.075` — Binance со скидкой BNB (25% от 0.1%)\n"
                f"`/setfee KuCoin 0.08` — KuCoin со скидкой KCS (20% от 0.1%)\n"
                f"`/setfee HTX 0.2` — вернуть обычное значение"
            )
            return
        ex_name = parts[1]
        if ex_name not in ("Binance", "KuCoin", "HTX"):
            await send_tg(session, "❌ Биржа должна быть одной из: Binance, KuCoin, HTX")
            return
        try:
            val = float(parts[2])
            if val < 0 or val > 2:
                await send_tg(session, "❌ Разумный диапазон: 0-2%.")
                return
            FEES[ex_name] = val
            await send_tg(session, f"✅ Комиссия {ex_name}: {val}%\nТекущий FEES: {FEES}")
        except ValueError:
            await send_tg(session, "❌ Пример: `/setfee Binance 0.075`")

    elif cmd == "/realfees":
        if not SYMBOLS:
            await send_tg(session, "Список монет пуст.")
            return
        sym = parts[1].upper() if len(parts) > 1 else SYMBOLS[0]
        await send_tg(session, f"📡 Запрашиваю реальные комиссии для {sym} на трёх биржах...")
        results = await refresh_real_fees(session, sym)
        msg = f"💳 *РЕАЛЬНЫЕ КОМИССИИ ({sym})*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for ex, fee in results.items():
            if fee is not None:
                msg += f"✅ {ex}: {fee}% (было {FEES.get(ex, '?')}% → обновлено)\n"
            else:
                msg += f"⚠️ {ex}: не удалось получить, оставлена прежняя {FEES.get(ex)}%\n"
        msg += f"\nТекущий FEES: {FEES}"
        await send_tg(session, msg)

    elif cmd == "/realbalance":
        await send_tg(session, "📡 Читаю реальные балансы и считаю план по каждой бирже...")
        for ex in ["Binance", "KuCoin", "HTX"]:
            plan = await real_exchange_rebalance_plan(session, ex)
            if plan is None:
                await send_tg(session, f"🔴 *{ex}*: не удалось получить баланс")
                continue
            msg = (
                f"📊 *{ex}*\n"
                f"   USDT: ${plan['usdt_balance']}\n"
            )
            for sym, val in plan["coin_values"].items():
                qty = plan["balances_qty"].get(sym, 0)
                msg += f"   {sym}: {qty} шт ≈ ${val}\n"
            msg += (
                f"   Всего: ${plan['total_usd']} | Нужно: ${plan['needed_total']} | "
                f"Излишек/дефицит: ${plan['surplus']}\n"
            )
            await send_tg(session, msg)

    elif cmd == "/apistatus":
        now = time.time()
        msg = "📡 *СТАТУС API БИРЖ*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        any_backed_off = False
        for ex, until in exchange_backoff_until.items():
            if until > now:
                any_backed_off = True
                remaining = round(until - now)
                msg += f"⛔ *{ex}*: заморожен ещё {remaining} сек (rate limit/бан)\n"
            else:
                msg += f"✅ *{ex}*: в норме\n"
        if not any_backed_off:
            msg += "\nВсе биржи отвечают нормально, блокировок не обнаружено."
        else:
            msg += "\n⚠️ Пока биржа заморожена, бот пропускает запросы к ней, не долбит повторно."
        await send_tg(session, msg)

    elif cmd == "/wsstatus":
        msg = "🔌 *WEBSOCKET СТАКАНЫ*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"

        msg += "*Binance:*\n"
        if not binance_ws_books:
            msg += "   Ни один стакан ещё не запущен.\n"
        for sym, book in binance_ws_books.items():
            icon = "✅" if book.is_healthy() else "⏳" if book.synced else "🔴"
            age = round(time.time() - book.last_event_time) if book.last_event_time else "—"
            msg += (f"   {icon} {sym}: events={book.event_count} "
                    f"resyncs={book.resync_count} {age}с назад\n")

        msg += "\n*KuCoin:*\n"
        if not kucoin_ws_books:
            msg += "   Ни один стакан ещё не запущен.\n"
        for sym, book in kucoin_ws_books.items():
            icon = "✅" if book.is_healthy() else "⏳" if book.synced else "🔴"
            age = round(time.time() - book.last_event_time) if book.last_event_time else "—"
            msg += (f"   {icon} {sym}: events={book.event_count} "
                    f"reconnects={book.reconnect_count} {age}с назад\n")

        msg += "\n*HTX:*\n"
        if not htx_ws_books:
            msg += "   Ни один стакан ещё не запущен.\n"
        for sym, book in htx_ws_books.items():
            icon = "✅" if book.is_healthy() else "⏳" if book.synced else "🔴"
            age = round(time.time() - book.last_event_time) if book.last_event_time else "—"
            msg += (f"   {icon} {sym}: events={book.event_count} "
                    f"reconnects={book.reconnect_count} {age}с назад\n")

        msg += "\n💡 REST-фоллбэк подключается автоматически, если WS ещё не готов."
        await send_tg(session, msg)

    elif cmd == "/triangle":
        await send_tg(session, "🔺 Сканирую треугольный арбитраж на Binance...")
        results = await scan_triangles(session)
        if not results:
            await send_tg(session,
                f"😔 Нет треугольных возможностей выше порога {config['min_profit_pct']}%.\n"
                f"(Либо пары COIN/{BRIDGE} не существуют для ваших монет на Binance — "
                f"это нормально для части альткоинов.)"
            )
        else:
            msg = "🔺 *ТРЕУГОЛЬНЫЙ АРБИТРАЖ (Binance)*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            for r in results[:5]:
                msg += (f"*{r['symbol']}* via {r['path']}\n"
                        f"   Чистая: `{r['net_pct']}%` | Профит: `{r['profit_usdt']} USDT`\n"
                        f"   Уровней задействовано: {r['levels']}\n\n")
            await send_tg(session, msg)

    elif cmd == "/report":
        today = datetime.now().strftime("%Y-%m-%d")
        today_trades = [t for t in trade_history if t.get("date") == today]
        if not today_trades:
            await send_tg(session, "📋 Нет сделок за сегодня.")
            return

        real_trades = [t for t in today_trades if t.get("mode") == "REAL"]
        sim_trades = [t for t in today_trades if t.get("mode") == "SIM"]
        msg = f"📋 *ОТЧЁТ — {today}*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"

        if real_trades:
            total_real = sum(t["profit_usdt"] for t in real_trades)
            wins_real = sum(1 for t in real_trades if t["profit_usdt"] > 0)
            sym_profit, pair_profit = defaultdict(float), defaultdict(float)
            for t in real_trades:
                sym_profit[t["symbol"]] += t["profit_usdt"]
                pair_profit[f"{t['buy_ex']}→{t['sell_ex']}"] += t["profit_usdt"]
            msg += (
                f"🔴 *РЕАЛЬНЫЕ СДЕЛКИ*\n"
                f"✅ Сделок: {len(real_trades)}\n"
                f"💰 Расчётная прибыль (по цифрам до исполнения): {round(total_real, 4)} USDT\n"
                f"⚠️ Точные цифры — только в истории ордеров бирж, здесь предварительный расчёт\n"
                f"📈 Прибыльных (по расчёту): {wins_real}/{len(real_trades)}\n\n💱 По монетам:\n"
            )
            for sym, p in sorted(sym_profit.items(), key=lambda x: x[1], reverse=True):
                msg += f"   {sym}: {'+' if p>=0 else ''}{round(p, 4)} USDT\n"
            msg += "🔀 По парам:\n"
            for pair, p in sorted(pair_profit.items(), key=lambda x: x[1], reverse=True):
                msg += f"   {pair}: {'+' if p>=0 else ''}{round(p, 4)} USDT\n"
            msg += "\n"

        if sim_trades:
            total_sim = sum(t["profit_usdt"] for t in sim_trades)
            wins_sim = sum(1 for t in sim_trades if t["profit_usdt"] > 0)
            sym_profit, pair_profit = defaultdict(float), defaultdict(float)
            for t in sim_trades:
                sym_profit[t["symbol"]] += t["profit_usdt"]
                pair_profit[f"{t['buy_ex']}→{t['sell_ex']}"] += t["profit_usdt"]
            msg += (
                f"🔵 *СИМУЛЯЦИЯ*\n"
                f"✅ Сделок: {len(sim_trades)}\n"
                f"💰 Прибыль (сим.): {round(total_sim, 4)} USDT\n"
                f"💡 Реалистично (×{config['derating_factor']}): {round(total_sim*config['derating_factor'], 4)} USDT\n"
                f"📈 Прибыльных: {wins_sim}/{len(sim_trades)}\n\n💱 По монетам:\n"
            )
            for sym, p in sorted(sym_profit.items(), key=lambda x: x[1], reverse=True):
                msg += f"   {sym}: {'+' if p>=0 else ''}{round(p, 4)} USDT\n"
            msg += "🔀 По парам:\n"
            for pair, p in sorted(pair_profit.items(), key=lambda x: x[1], reverse=True):
                msg += f"   {pair}: {'+' if p>=0 else ''}{round(p, 4)} USDT\n"

        await send_tg(session, msg)

    elif cmd == "/hours":
        msg = "⏰ *СИГНАЛЫ ПО ЧАСАМ (UTC)*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        hour_data = [(h, stats["hourly_signals"].get(h, 0), stats["hourly_profit"].get(h, 0.0))
                     for h in range(24) if stats["hourly_signals"].get(h, 0) > 0]
        if not hour_data:
            msg += "Нет данных пока."
        else:
            hour_data.sort(key=lambda x: x[1], reverse=True)
            for h, sigs, profit in hour_data[:10]:
                bar = "█" * min(10, sigs // 5 + 1)
                msg += f"*{h:02d}:00* {bar}\n   Сигналов: {sigs} | Прибыль: {round(profit,2)} USDT\n\n"
            best = max(hour_data, key=lambda x: x[1])
            msg += f"🏆 Лучший час: *{best[0]:02d}:00 UTC*"
        await send_tg(session, msg)

    elif cmd == "/history":
        if not trade_history:
            await send_tg(session, "📋 Нет сделок.")
            return
        msg = "📋 *ПОСЛЕДНИЕ СДЕЛКИ*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for t in trade_history[-10:][::-1]:
            sign = "+" if t["profit_usdt"] > 0 else ""
            msg += (f"#{t['id']} *{t['symbol']}* {t['buy_ex']}→{t['sell_ex']}\n"
                    f"   {sign}{t['net_pct']}% | {sign}{t['profit_usdt']} USDT | {t['time']}\n\n")
        await send_tg(session, msg)

    elif cmd == "/top":
        await send_tg(session, "📊 Сканирую без порога (реальная глубина)...")
        bn, kc, hx, active = await fetch_all_orderbooks(session)
        ex_map = {"Binance": bn, "KuCoin": kc, "HTX": hx}
        saved = config["min_profit_pct"]
        config["min_profit_pct"] = -999
        all_opps = []
        for sym in SYMBOLS:
            for buy_ex, sell_ex in pairs_for_symbol(sym):
                bob = ex_map.get(buy_ex, {}).get(sym)
                sob = ex_map.get(sell_ex, {}).get(sym)
                if bob and sob:
                    opp = calc_arb_real(sym, buy_ex, bob, sell_ex, sob, config["trade_usdt"])
                    if opp:
                        all_opps.append(opp)
        config["min_profit_pct"] = saved
        all_opps.sort(key=lambda x: x["net_pct"], reverse=True)
        msg = f"📊 *ВСЕ ПАРЫ (реальная глубина) — {datetime.now().strftime('%H:%M:%S')}*\n"
        msg += f"Бирж: {', '.join(active)}\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        if not all_opps:
            msg += "Нет данных (либо стакана не хватает на объём — см. /depthcheck)"
        for i, o in enumerate(all_opps, 1):
            icon = "🟢" if o["net_pct"] >= saved else "🔴"
            msg += f"{icon} *{i}. {o['symbol']}* {o['buy_ex']}→{o['sell_ex']}\n   Чистая: `{o['net_pct']}%`\n\n"
        msg += f"_Порог: {saved}%_"
        await send_tg(session, msg)

    elif cmd == "/howtoread":
        await send_tg(session,
            "📖 *КАК ЧИТАТЬ ОТЧЁТЫ*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "*slippage_impact_pct / проскальзывание* — насколько наивный расчёт "
            "по первой цене стакана завысил бы спред. Чем выше — тем важнее, что "
            "мы теперь считаем честно.\n\n"
            "*Реалистичная оценка (×0.25)* — по вашему опыту, реальная торговля "
            "даёт примерно четверть от симулированной прибыли из-за конкуренции "
            "и остаточного проскальзывания сверх того, что уже учтено.\n\n"
            "*Отказы API стакана* — если растут, конкретная биржа нестабильна, "
            "проверьте вручную её endpoint.\n\n"
            "*Недостаточно ликвидности* — сколько раз стакана не хватило на "
            "заявленный объём; такие сигналы не считаются валидными и не торгуются.\n\n"
            "*Буфер баланса / запас ребаланса* — buffer нужен ПЕРЕД сделкой (сколько "
            "монеты обязано быть в наличии), headroom — сколько ребаланс держит СВЕРХ "
            "цели про запас. headroom должен быть заметно больше buffer."
        )

    elif cmd == "/guide":
        await send_tg(session,
            "📖 *ИНСТРУКЦИЯ*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Этот бот на Этапе 3.1: честный расчёт цены через реальную "
            "глубину стакана (walk-the-book), а не наивный top-of-book.\n\n"
            "Реальное исполнение ордеров (Этап 6) ещё НЕ реализовано — "
            "переключение /mode в реальный режим заблокировано намеренно, "
            "пока не построены: проверка исполнения обеих ног сделки и "
            "аварийное закрытие позиции при частичном исполнении.\n\n"
            "*Порядок работы:*\n"
            "1. /depthcheck SYMBOL — проверить качество данных по монете\n"
            "2. /scan или дождаться авто-скана\n"
            "3. /stats — следить за отказами API и insufficient_liquidity\n"
            "4. /report /hours — вечерний разбор\n"
            "5. /rebalance при необходимости"
        )

    elif cmd == "/csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["ID","Дата","Время","Монета","Купить","Продать","Цена покупки",
                         "Цена продажи","Объём","Спред%","Чистая%","Прибыль","Проскальз.влияние%","Режим"])
        for t in trade_history:
            writer.writerow([t["id"],t["date"],t["time"],t["symbol"],t["buy_ex"],t["sell_ex"],
                             t["buy_price"],t["sell_price"],t["vol"],t["gross_pct"],t["net_pct"],
                             t["profit_usdt"],t.get("slippage_impact_pct",0),t["mode"]])
        await send_document(session, f"depth_report_{datetime.now().strftime('%Y-%m-%d')}.csv",
                            output.getvalue(), f"{stats['trades']} сделок")

    elif cmd == "/setlot":
        if len(parts) > 1:
            try:
                config["trade_usdt"] = float(parts[1])
                await send_tg(session, f"✅ Лот: ${config['trade_usdt']}")
            except Exception:
                pass

    elif cmd == "/setinterval":
        # НОВОЕ: как часто bot СВЕРЯЕТ уже живые (обновляемые по WebSocket
        # в реальном времени) локальные стаканы и принимает решение —
        # не то же самое, что "как часто он видит цену" (цена и так всегда
        # свежая через WS). Уменьшение интервала помогает ловить короткие
        # окна возможности, которые могут закрываться быстрее, чем раз в
        # 10 секунд, ценой чуть большей нагрузки на CPU (не на биржи —
        # запросов к ним больше не становится, WS-соединения и так висят
        # постоянно).
        if len(parts) < 2:
            await send_tg(session,
                f"Текущий интервал сканирования: {config['scan_interval']} сек\n\n"
                f"Это НЕ задержка получения цены (она всегда свежая через WebSocket) — "
                f"это как часто бот сверяет уже живые данные и решает, действовать ли. "
                f"Меньше — больше шансов поймать короткое окно возможности.\n\n"
                f"Пример: `/setinterval 3` (минимум 2 сек — разумный предел, "
                f"чтобы не грузить CPU впустую)"
            )
            return
        try:
            val = int(parts[1])
            if val < 2 or val > 60:
                await send_tg(session, "❌ Разумный диапазон: 2-60 сек.")
                return
            config["scan_interval"] = val
            await send_tg(session, f"✅ Интервал сканирования: {val} сек")
        except ValueError:
            await send_tg(session, "❌ Пример: `/setinterval 3`")

    elif cmd == "/setprofit":
        if len(parts) > 1:
            try:
                config["min_profit_pct"] = float(parts[1])
                await send_tg(session, f"✅ Порог: {config['min_profit_pct']}%")
            except Exception:
                pass

    elif cmd == "/setstop":
        if len(parts) > 1:
            try:
                config["stop_loss_usdt"] = float(parts[1])
                await send_tg(session, f"✅ Стоп-лосс: ${config['stop_loss_usdt']}")
            except Exception:
                pass

    else:
        await send_tg(session,
            "/start /scan /top /triangle /depthcheck BONK\n"
            "/stats /balances /rebalance /crosstransfer\n"
            "/hours /report /history /csv\n"
            "/howtoread /guide /mode\n"
            "/addcoin /removecoin /listcoins\n"
            "/confirmreal /disablereal\n"
            "/setbalancebuffer /setheadroom\n"
            "/pause /go /resume\n"
            "/setlot /setprofit /setstop")


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
                    await handle_command(session, text, CHAT_ID)
        await asyncio.sleep(1)


async def scan_loop(session):
    await asyncio.sleep(15)
    while True:
        try:
            reset_daily()
            if not config["paused"] and can_trade():
                signals, active = await scan_all(session)
                logger.info(
                    f"Скан #{stats['scans']}: бирж={len(active)} сигналов={len(signals)} "
                    f"отказов_стакана=B:{stats['depth_fail']['Binance']}/"
                    f"K:{stats['depth_fail']['KuCoin']}/H:{stats['depth_fail']['HTX']}"
                )
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
                            # НОВОЕ 07.08: после КАЖДОЙ успешной реальной сделки
                            # монета/USDT на обеих биржах естественным образом
                            # "перетекают" в форму, нужную для СЛЕДУЮЩЕЙ сделки
                            # неправильно (биржа-покупатель тратит USDT, биржа-
                            # продавец тратит запас монеты) — раньше это
                            # выяснялось только на следующей попытке, с отказом
                            # "insufficient_usdt_on_..." и ручным /rebalance.
                            # Теперь ребалансируем сразу, не дожидаясь отказа.
                            if not config["simulation_mode"]:
                                now_ts = time.time()
                                global _last_auto_rebalance_attempt
                                if now_ts - _last_auto_rebalance_attempt > AUTO_REBALANCE_COOLDOWN:
                                    _last_auto_rebalance_attempt = now_ts
                                    rb_result = await real_auto_rebalance_all(session)
                                    if CHAT_ID and (rb_result.get("applied") or rb_result.get("cross_exchange_needed")):
                                        await send_tg(session, "⚖️ Авто-ребаланс после сделки:\n\n" +
                                                       format_real_rebalance_result(rb_result))
                                    if not rb_result.get("safe_to_resume", True):
                                        config["paused"] = True
                        else:
                            reason = REASON_LABELS.get(result["reason"], result["reason"])
                            await send_tg(session,
                                f"⛔ {opp['symbol']} {opp['buy_ex']}→{opp['sell_ex']} "
                                f"пропущено: {reason}")

                # Треугольный скан — реже, т.к. требует 3x больше запросов на монету
                if config["triangular_enabled"] and stats["scans"] % 3 == 0:
                    triangles = await scan_triangles(session)
                    for t in triangles[:2]:
                        key = f"tri-{t['symbol']}-{t['path']}"
                        now = datetime.now().timestamp()
                        if now - last_signal_time.get(key, 0) > 120:
                            last_signal_time[key] = now
                            triangle_history.append(t)
                            if CHAT_ID:
                                await send_tg(session,
                                    f"🔺 *Треугольный сигнал: {t['symbol']}*\n"
                                    f"Путь: {t['path']}\n"
                                    f"Чистая: `{t['net_pct']}%` | "
                                    f"Профит: `{t['profit_usdt']} USDT`"
                                )

                # Авто-ребаланс — каждые ~30 мин (180 сканов × 10 сек)
                if stats["scans"] % 180 == 0:
                    if not config["simulation_mode"]:
                        config["paused"] = True
                        result = await real_auto_rebalance_all(session)
                        had_actions = any(a["actions"] for a in result.get("applied", []))
                        if result.get("error") or not result.get("safe_to_resume", False) or had_actions:
                            if CHAT_ID:
                                await send_tg(session, format_real_rebalance_result(result))
                        if result.get("safe_to_resume", False):
                            config["paused"] = False
                    else:
                        warns = check_balance_warnings()
                        if warns:
                            config["paused"] = True  # останавливаем торговлю на время ребаланса
                            result = auto_rebalance_all()
                            if CHAT_ID:
                                await send_tg(session, format_rebalance_result(result))
                            if result["fully_rebalanced"]:
                                config["paused"] = False  # ребаланс закрыл всё сам — продолжаем
                            # если fully_rebalanced == False — остаёмся на паузе,
                            # ждём ручного /crosstransfer + /go

        except Exception as e:
            stats["errors"] += 1
            logger.error(f"Scan error: {e}")
        await asyncio.sleep(config["scan_interval"])


async def main():
    if not TG_TOKEN:
        logger.error("ARB_BOT_TOKEN не установлен!")
        return
    logger.info("DepthArbBot стартует — WebSocket-стаканы на всех трёх биржах")
    connector = aiohttp.TCPConnector(ssl=True)  # SSL включён, не отключаем проверку сертификатов
    async with aiohttp.ClientSession(connector=connector) as session:
        for sym in SYMBOLS:
            start_binance_ws_book(session, sym)
            start_kucoin_ws_book(session, sym)
            start_htx_ws_book(session, sym)
        logger.info(f"WS-стаканы запущены на Binance/KuCoin/HTX для {SYMBOLS}")
        await asyncio.gather(polling_loop(session), scan_loop(session))


if __name__ == "__main__":
    asyncio.run(main())
