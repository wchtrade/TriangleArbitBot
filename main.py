import asyncio
import aiohttp
import logging
import os
import csv
import io
from datetime import datetime
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
    "derating_factor":    0.25,    # реальность ≈ симуляция × 0.25 (ваша же оценка)
}

SYMBOLS = ["BONK", "SEI", "FET", "INJ"]
QUOTE   = "USDT"
PAIRS   = [
    ("HTX",     "KuCoin"),
    ("KuCoin",  "HTX"),
    ("Binance", "HTX"),
]
FEES = {"Binance": 0.10, "KuCoin": 0.10, "HTX": 0.20}
SIM_START = 500.0

sim_balances = {
    "KuCoin":  {"USDT": 125.0, "BONK": 62.5, "SEI": 31.25, "FET": 15.62, "INJ": 15.63},
    "HTX":     {"USDT": 125.0, "BONK": 62.5, "SEI": 31.25, "FET": 15.62, "INJ": 15.63},
    "Binance": {"USDT": 125.0},
}

stats = {
    "scans": 0, "signals": 0, "trades": 0, "profit": 0.0, "errors": 0,
    "start_time": datetime.now(),
    "trades_this_minute": 0, "minute_start": datetime.now(),
    "pair_stats":   {f"{b}→{s}": 0 for b, s in PAIRS},
    "symbol_stats": {s: 0 for s in SYMBOLS},
    "depth_fail":   {"Binance": 0, "KuCoin": 0, "HTX": 0},  # счётчик отказов стакана
    "insufficient_liquidity": 0,  # сколько раз стакана не хватило на объём
    "hourly_signals": defaultdict(int),
    "hourly_profit":  defaultdict(float),
}
trade_history: List[dict] = []
last_signal_time: Dict[str, float] = {}
coin_volumes: Dict[str, float] = {}


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


def update_sim_balances(opp: dict):
    sym, bex, sex, vol, profit = opp["symbol"], opp["buy_ex"], opp["sell_ex"], opp["vol"], opp["profit_usdt"]
    if bex in sim_balances:
        sim_balances[bex]["USDT"] = max(0, sim_balances[bex].get("USDT", 0) - vol)
        sim_balances[bex][sym] = sim_balances[bex].get(sym, 0) + vol
    if sex in sim_balances:
        cur = sim_balances[sex].get(sym, 0)
        sim_balances[sex][sym] = max(0, cur - vol)
        sim_balances[sex]["USDT"] = sim_balances[sex].get("USDT", 0) + vol + profit


async def execute_trade(opp: dict):
    if not check_rate() or not can_trade():
        return
    profit = opp["profit_usdt"]
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
            f"/depthcheck SYMBOL — сырой стакан + проскальзывание\n"
            f"/stats — статистика | /balances — балансы\n"
            f"/rebalance — что перебалансировать\n"
            f"/hours — активность по часам | /report — отчёт за день\n"
            f"/history — последние сделки | /csv — экспорт\n"
            f"/howtoread — как читать отчёты | /guide — инструкция\n"
            f"/pause /go /resume — управление торговлей\n"
            f"/mode — переключить режим (реальный пока заблокирован)\n"
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
                await send_tg(session, format_signal(opp))
                await execute_trade(opp)

    elif cmd == "/stats":
        total_bal = get_balance_usdt()
        pnl = round(total_bal - SIM_START, 2)
        per_trade = round(stats["profit"] / stats["trades"], 4) if stats["trades"] else 0
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
            f"⚠️ Отклонено из-за нехватки ликвидности: {stats['insufficient_liquidity']}\n\n"
            f"💵 Баланс: старт ${SIM_START} → сейчас ${total_bal} (P&L {pnl:+.2f})\n"
            f"⚙️ Лот: ${config['trade_usdt']} | Порог: {config['min_profit_pct']}%"
        )

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
        target = {
            "KuCoin":  {"USDT": 125, "BONK": 62.5, "SEI": 31.25, "FET": 15.62, "INJ": 15.63},
            "HTX":     {"USDT": 125, "BONK": 62.5, "SEI": 31.25, "FET": 15.62, "INJ": 15.63},
            "Binance": {"USDT": 125},
        }
        msg = "⚖️ *РЕБАЛАНСИРОВКА*\n━━━━━━━━━━━━━━━━━━━━━━\n\n*(суммы в USD-эквиваленте)*\n\n"
        actions = []
        for ex, tgt in target.items():
            cur = sim_balances.get(ex, {})
            ex_act = []
            for asset, tgt_val in tgt.items():
                diff = tgt_val - cur.get(asset, 0)
                if diff > 2:
                    ex_act.append(f"   ➕ Докупить {asset}: +${round(diff, 1)}")
                elif diff < -2:
                    ex_act.append(f"   ➖ Продать {asset}: ${round(abs(diff), 1)}")
            if ex_act:
                actions.append(f"*{ex}:*\n" + "\n".join(ex_act))
        if not actions:
            msg += "✅ Все балансы в норме! Ребалансировка не нужна."
        else:
            msg += "\n\n".join(actions)
            msg += ("\n\n⚠️ *Перед ребалансировкой:*\n1. /pause\n2. Сделай операции на биржах\n3. /go\n\n"
                    "💡 Перевод USDT через TRC-20 = ~$1")
        await send_tg(session, msg)

    elif cmd == "/mode":
        config["simulation_mode"] = not config["simulation_mode"]
        if config["simulation_mode"]:
            await send_tg(session, "🔵 Режим: СИМУЛЯЦИЯ")
        else:
            # ВНИМАНИЕ: реальное исполнение ордеров в этой версии НЕ реализовано.
            # Переключение режима меняет только метку в логах/сообщениях,
            # реальные ордера бот пока не размещает — это следующий этап (Этап 6).
            config["simulation_mode"] = True
            await send_tg(session,
                "❌ *Реальное исполнение ордеров ещё не реализовано в этой версии.*\n\n"
                "Этот бот сейчас умеет честно СЧИТАТЬ возможности (реальная глубина "
                "стакана, walk-the-book), но не размещает реальные ордера на биржах.\n\n"
                "Включать реальный режим преждевременно — сначала нужен Этап 6 "
                "(исполнение + проверка обеих ног сделки + аварийное закрытие).\n\n"
                "Режим остаётся: 🔵 СИМУЛЯЦИЯ"
            )

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
            "/start /scan /top /depthcheck BONK\n"
            "/stats /balances /rebalance\n"
            "/hours /report /history /csv\n"
            "/howtoread /guide /mode\n"
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
                        if CHAT_ID:
                            await send_tg(session, format_signal(opp))
                        await execute_trade(opp)
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
