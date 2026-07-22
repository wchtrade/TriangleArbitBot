import asyncio
import aiohttp
import logging
import os
import csv
import io
import time
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
    "scan_interval":      10,
    "max_trades_per_min": 6,
    "stop_loss_usdt":     10.0,
    "daily_loss":         0.0,
    "daily_profit":       0.0,
    "day_start":          datetime.now().strftime("%Y-%m-%d"),
    "trading_active":     True,
    "paused":             False,
    "min_volume_usdt":    100000,
    "depth_limit":        50,      # сколько уровней стакана запрашиваем
    "rebalance_target_lots": 3,    # сколько лотов держать в резерве на каждую монету/USDT при авто-ребалансе
    "derating_factor":    0.25,    # реальность ≈ симуляция × 0.25 (ваша же оценка)

    # ===== ЭТАП 6: РЕАЛЬНОЕ ИСПОЛНЕНИЕ — ЖЁСТКИЙ ГЕЙТ =====
    # simulation_mode=False САМО ПО СЕБЕ не включает реальные ордера.
    # Нужны ОБА условия одновременно:
    #   1) переменная окружения REAL_TRADING_UNLOCKED == "YES-I-UNDERSTAND-THE-RISK"
    #   2) runtime-флаг real_confirmed, включаемый командой /confirmreal <фраза>
    # Если хоть одно условие не выполнено — бот принудительно торгует в símulation.
    "real_confirmed":       False,
    "max_real_order_usdt":  15.0,   # ЖЁСТКИЙ потолок на один ордер, /setlot его не обходит
    "real_trades_today":    0,
    "max_real_trades_per_day": 20,  # доп. защита от разгона в реальном режиме

    # ===== ЭТАП 4: ТРЕУГОЛЬНЫЙ АРБИТРАЖ =====
    "triangular_enabled": True,
}

CONFIRM_PHRASE = "YES-I-UNDERSTAND-THE-RISK"

SYMBOLS = ["BONK", "SEI", "FET", "INJ"]   # теперь можно менять на лету через /addcoin /removecoin
QUOTE   = "USDT"
BRIDGE  = "BTC"   # мост для треугольного арбитража: USDT -> COIN -> BTC -> USDT
PAIRS   = [
    ("HTX",     "KuCoin"),
    ("KuCoin",  "HTX"),
    ("Binance", "HTX"),
]
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
    "insufficient_balance_skips": 0,  # сколько раз симуляция честно отказала из-за нехватки виртуального баланса
    "hourly_signals": defaultdict(int),
    "hourly_profit":  defaultdict(float),
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

async def get_orderbook_binance(session, symbol: str) -> Optional[Dict]:
    url = "https://api.binance.com/api/v3/depth"
    params = {"symbol": f"{symbol}{QUOTE}", "limit": config["depth_limit"]}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=6)) as r:
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


async def get_orderbook_kucoin(session, symbol: str) -> Optional[Dict]:
    # Публичный уровень 2 (агрегированный, 20 уровней). Если KuCoin потребует
    # авторизацию на этом endpoint — вернётся ошибка, бот просто пропустит биржу
    # для этого символа (как и раньше делал при недоступности API).
    url = "https://api.kucoin.com/api/v1/market/orderbook/level2_20"
    params = {"symbol": f"{symbol}-{QUOTE}"}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=6)) as r:
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


async def get_orderbook_htx(session, symbol: str) -> Optional[Dict]:
    url = "https://api.huobi.pro/market/depth"
    params = {"symbol": f"{symbol.lower()}{QUOTE.lower()}", "type": "step0"}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=6)) as r:
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


async def get_24h_volume(session) -> Dict[str, float]:
    """Отдельно берём 24h объём с Binance (для фильтра ликвидности,
    как и раньше — это не заменяет реальную глубину, а дополняет её)."""
    volumes = {}
    try:
        async with session.get(
            "https://api.binance.com/api/v3/ticker/24hr",
            timeout=aiohttp.ClientTimeout(total=8)
        ) as r:
            for item in await r.json():
                sym = item.get("symbol", "")
                if sym.endswith(QUOTE):
                    base = sym[:-len(QUOTE)]
                    if base in SYMBOLS:
                        volumes[base] = float(item.get("quoteVolume", 0) or 0)
    except Exception as e:
        logger.error(f"Volume fetch: {e}")
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
    url = "https://api.binance.com/api/v3/depth"
    params = {"symbol": pair_symbol, "limit": config["depth_limit"]}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=6)) as r:
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
    if coin_volumes.get(symbol, 0) < config["min_volume_usdt"]:
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
        for buy_ex, sell_ex in PAIRS:
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


def sign_binance(params: dict, secret: str) -> str:
    query = urllib.parse.urlencode(params)
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()


async def place_order_binance(session, symbol: str, side: str, quote_usdt: float) -> Optional[dict]:
    """MARKET ордер на Binance. side: 'BUY' или 'SELL'.
    quoteOrderQty — тратим/получаем ровно X USDT, биржа сама считает количество монет
    (для BUY). Для SELL используем quantity в монетах — нужно передавать заранее
    посчитанное количество через отдельный параметр (см. execute_real_arbitrage)."""
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
            data = await r.json()
            if r.status != 200:
                logger.error(f"Binance order failed: {data}")
                return None
            return data
    except Exception as e:
        logger.error(f"Binance order exception: {e}")
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
            data = await r.json()
            if r.status != 200 or data.get("code") != "200000":
                logger.error(f"KuCoin order failed: {data}")
                return None
            return data
    except Exception as e:
        logger.error(f"KuCoin order exception: {e}")
        return None


async def place_order_htx(session, account_id: str, symbol: str, side: str,
                            amount: float) -> Optional[dict]:
    """MARKET ордер на HTX. side: 'buy-market' или 'sell-market'.
    Для buy-market amount = сумма в USDT. Для sell-market amount = количество монет.
    Требует account_id — получить через /v1/account/accounts (см. get_htx_account_id)."""
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
            data = await r.json()
            if data.get("status") != "ok":
                logger.error(f"HTX order failed: {data}")
                return None
            return data
    except Exception as e:
        logger.error(f"HTX order exception: {e}")
        return None


async def get_htx_account_id(session) -> Optional[str]:
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
            data = await r.json()
            for acc in data.get("data", []):
                if acc.get("type") == "spot":
                    return str(acc["id"])
    except Exception as e:
        logger.error(f"HTX account id: {e}")
    return None


_htx_account_id_cache: Optional[str] = None


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

    vol = min(opp["vol"], config["max_real_order_usdt"])  # ЖЁСТКИЙ потолок, /setlot не обходит
    symbol, buy_ex, sell_ex = opp["symbol"], opp["buy_ex"], opp["sell_ex"]

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
        return {"success": False, "error": f"buy_leg_failed_on_{buy_ex}"}

    config["real_trades_today"] += 1

    # Сколько монет реально куплено — по-хорошему нужно запросить факт исполнения
    # ордера (GET order status), здесь используем расчётное количество как
    # консервативную оценку. ЭТО МЕСТО ТРЕБУЕТ ДОРАБОТКИ: добавить polling
    # реального fill amount перед второй ногой.
    coins_bought = opp["coins"]

    # --- НОГА 2: ПРОДАЖА ---
    sell_result = None
    if sell_ex == "Binance":
        sell_result = await place_order_binance(session, symbol, "SELL", coins_bought)
    elif sell_ex == "KuCoin":
        sell_result = await place_order_kucoin(session, symbol, "sell", coins_bought, use_funds=False)
    elif sell_ex == "HTX":
        if not _htx_account_id_cache:
            _htx_account_id_cache = await get_htx_account_id(session)
        if _htx_account_id_cache:
            sell_result = await place_order_htx(session, _htx_account_id_cache, symbol, "sell-market", coins_bought)

    if not sell_result:
        # АВАРИЙНОЕ ЗАКРЫТИЕ: продаём купленное обратно на бирже покупки,
        # чтобы не остаться с открытой направленной позицией
        emergency = None
        if buy_ex == "Binance":
            emergency = await place_order_binance(session, symbol, "SELL", coins_bought)
        elif buy_ex == "KuCoin":
            emergency = await place_order_kucoin(session, symbol, "sell", coins_bought, use_funds=False)
        elif buy_ex == "HTX":
            if _htx_account_id_cache:
                emergency = await place_order_htx(session, _htx_account_id_cache, symbol, "sell-market", coins_bought)
        return {
            "success": False, "error": f"sell_leg_failed_on_{sell_ex}",
            "emergency_close": bool(emergency),
            "buy_result": buy_result,
        }

    return {"success": True, "buy_result": buy_result, "sell_result": sell_result, "vol": vol}


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

def exchange_rebalance_plan(ex: str) -> dict:
    """Считает, хватает ли ОБЩЕЙ суммы на бирже, чтобы держать целевой
    остаток по каждой отслеживаемой монете + буфер USDT. Не изменяет
    балансы — только считает."""
    assets = sim_balances.get(ex, {})
    coin_target = config["trade_usdt"] * config["rebalance_target_lots"]
    usdt_target = config["trade_usdt"] * config["rebalance_target_lots"]

    coins_here = [s for s in SYMBOLS if s in assets]  # какие монеты вообще есть на этой бирже
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
    между биржами (TRC-20 и т.п.). Используется после /crosstransfer."""
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
    if not check_rate():
        return {"executed": False, "reason": "rate_limit_exceeded"}
    if not can_trade():
        return {"executed": False, "reason": "paused_or_stoploss"}

    real_result = None
    if not config["simulation_mode"] and is_real_trading_allowed():
        real_result = await execute_real_arbitrage(session, opp)
        if not real_result.get("success"):
            logger.error(f"РЕАЛЬНАЯ сделка не удалась: {real_result}")
            if CHAT_ID:
                msg = f"🔴 *РЕАЛЬНАЯ СДЕЛКА ОТКЛОНЕНА/ОШИБКА*\n`{real_result}`"
                if real_result.get("emergency_close"):
                    msg += "\n⚠️ Выполнено аварийное закрытие позиции."
                await send_tg(session, msg)
            return {"executed": False, "reason": f"real_execution_failed: {real_result.get('error')}"}

    profit = opp["profit_usdt"]

    if config["simulation_mode"]:
        if not has_sufficient_sim_balance(opp):
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
    "insufficient_sim_balance": "💰 не хватает баланса именно этой монеты на бирже — нужен /rebalance",
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
    mode = "🔵 СИМУЛЯЦИЯ" if config["simulation_mode"] else "🔴 РЕАЛЬНАЯ"
    derated = round(opp["profit_usdt"] * config["derating_factor"], 4)
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
        f"💰 Прибыль (симуляция): `{opp['profit_usdt']} USDT`\n"
        f"💡 Реалистичная оценка (×{config['derating_factor']}): "
        f"`~{derated} USDT`\n\n"
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
            f"/stats — статистика | /balances — балансы\n"
            f"/rebalance — авто-ребаланс внутри бирж (+ инструкция если нужен перевод между биржами)\n"
            f"/crosstransfer FROM TO СУММА — записать ручной перевод\n"
            f"/setrebalance N — целевой запас (в лотах) на монету\n"
            f"/hours — активность по часам | /report — отчёт за день\n"
            f"/history — последние сделки | /csv — экспорт\n"
            f"/howtoread — как читать отчёты | /guide — инструкция\n"
            f"/pause /go /resume — управление торговлей\n"
            f"/addcoin /removecoin /listcoins — управление монетами\n"
            f"/withdraw — сколько можно вывести\n"
            f"/resetsim CONFIRM — сброс симуляции\n"
            f"/mode — переключить режим\n"
            f"/confirmreal /disablereal — гейт реальной торговли\n"
            f"/setlot 20 /setprofit 0.3 /setstop 10"
        )

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
                f"Недостаточно ликвидности (за всё время): {stats['insufficient_liquidity']}"
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
        total_bal = get_balance_usdt()
        pnl = round(total_bal - SIM_START, 2)
        per_trade = round(stats["profit"] / stats["trades"], 4) if stats["trades"] else 0
        wd = suggest_withdrawal()
        await send_tg(session,
            f"📈 *СТАТИСТИКА*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
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
        # Синоним /rebalance — форсирует авто-ребаланс прямо сейчас, даже без warnings
        config["paused"] = True
        result = auto_rebalance_all()
        await send_tg(session, format_rebalance_result(result))
        if result["fully_rebalanced"]:
            config["paused"] = False

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
                                    f"(${config['trade_usdt']*config['rebalance_target_lots']} на монету/USDT)")
        except ValueError:
            await send_tg(session, "❌ Пример: `/setrebalance 3`")

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
        # Без начального баланса монета будет получать сигналы, но НИКОГДА не
        # сможет исполниться в симуляции (has_sufficient_sim_balance всегда
        # откажет) — та же ситуация, что случилась с FET/INJ. Даём стартовый
        # виртуальный баланс в 5 лотов на каждой бирже, где монета продаётся.
        seed = config["trade_usdt"] * 5
        for ex in ["KuCoin", "HTX"]:
            sim_balances.setdefault(ex, {})[sym] = seed
        await send_tg(session,
            f"✅ Добавлено: *{sym}*\n"
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
        await send_tg(session, f"✅ Удалено: *{sym}*\nТекущий список: {', '.join(SYMBOLS)}")

    elif cmd == "/listcoins":
        await send_tg(session, f"💱 *Торгуемые монеты:* {', '.join(SYMBOLS)}\n\n"
                                f"Добавить: `/addcoin SYMBOL`\nУдалить: `/removecoin SYMBOL`")

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
        total = sum(t["profit_usdt"] for t in today_trades)
        wins = sum(1 for t in today_trades if t["profit_usdt"] > 0)
        sym_profit, pair_profit = defaultdict(float), defaultdict(float)
        for t in today_trades:
            sym_profit[t["symbol"]] += t["profit_usdt"]
            pair_profit[f"{t['buy_ex']}→{t['sell_ex']}"] += t["profit_usdt"]
        msg = (
            f"📋 *ОТЧЁТ — {today}*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"✅ Сделок: {len(today_trades)}\n"
            f"💰 Прибыль (сим.): {round(total, 4)} USDT\n"
            f"💡 Реалистично (×{config['derating_factor']}): {round(total*config['derating_factor'], 4)} USDT\n"
            f"📈 Прибыльных: {wins}/{len(today_trades)}\n\n💱 *По монетам:*\n"
        )
        for sym, p in sorted(sym_profit.items(), key=lambda x: x[1], reverse=True):
            msg += f"   {sym}: {'+' if p>=0 else ''}{round(p, 4)} USDT\n"
        msg += "\n🔀 *По парам:*\n"
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
            for buy_ex, sell_ex in PAIRS:
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
            "заявленный объём; такие сигналы не считаются валидными и не торгуются."
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
    logger.info("DepthArbBot стартует — реальная глубина стакана вместо top-of-book")
    connector = aiohttp.TCPConnector(ssl=True)  # SSL включён, не отключаем проверку сертификатов
    async with aiohttp.ClientSession(connector=connector) as session:
        await asyncio.gather(polling_loop(session), scan_loop(session))


if __name__ == "__main__":
    asyncio.run(main())
