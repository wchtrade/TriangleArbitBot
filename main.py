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

# ═══════════════════════════════════════
# КОНФИГУРАЦИЯ
# ═══════════════════════════════════════
config = {
    "simulation_mode":    True,        # True = симуляция, False = реал
    "min_profit_pct":     0.3,         # мин. прибыль после комиссий
    "trade_usdt":         20.0,        # лот $20 при капитале $500
    "scan_interval":      10,          # 10 секунд между сканами = 6 в минуту
    "max_trades_per_min": 6,           # не более 6 сделок в минуту
    "stop_loss_usdt":     10.0,        # стоп-лосс $10/день при капитале $500
    "daily_loss":         0.0,
    "daily_profit":       0.0,
    "day_start":          datetime.now().strftime("%Y-%m-%d"),
    "trading_active":     True,
    "min_volume_usdt":    100000,      # мин. объём монеты $100k
}

# API ключи (из Railway Variables)
BINANCE_KEY    = os.environ.get("BINANCE_API_KEY", "")
BINANCE_SECRET = os.environ.get("BINANCE_API_SECRET", "")
KUCOIN_KEY     = os.environ.get("KUCOIN_API_KEY", "")
KUCOIN_SECRET  = os.environ.get("KUCOIN_API_SECRET", "")
KUCOIN_PASS    = os.environ.get("KUCOIN_PASSPHRASE", "")
HTX_KEY        = os.environ.get("HTX_API_KEY", "")
HTX_SECRET     = os.environ.get("HTX_API_SECRET", "")

# Только 3 площадки
EXCHANGES = ["Binance", "KuCoin", "HTX"]

# Только рабочие пары из отчётов
PAIRS = [
    ("HTX",     "KuCoin"),   # главная пара — 98% сигналов
    ("KuCoin",  "HTX"),      # обратная
    ("Binance", "HTX"),      # дополнительная
]

# Только рабочие монеты по данным отчётов
SYMBOLS = ["BONK", "SEI", "FET"]
QUOTE = "USDT"

# Комиссии
FEES = {"Binance": 0.10, "KuCoin": 0.10, "HTX": 0.20}

# Стартовый капитал симуляции $500
SIM_START = 500.0
sim_balances = {
    "KuCoin":  {"USDT": 125.0, "BONK": 62.5, "SEI": 31.25, "FET": 31.25},
    "HTX":     {"USDT": 125.0, "BONK": 62.5, "SEI": 31.25, "FET": 31.25},
    "Binance": {"USDT": 125.0},
}

# Статистика
stats = {
    "scans": 0, "signals": 0,
    "trades": 0, "profit": 0.0,
    "errors": 0, "start_time": datetime.now(),
    "trades_this_minute": 0,
    "minute_start": datetime.now(),
    "pair_stats":   {f"{b}→{s}": 0 for b, s in PAIRS},
    "symbol_stats": {s: 0 for s in SYMBOLS},
    "hourly_signals": defaultdict(int),
    "hourly_profit":  defaultdict(float),
}
trade_history: List[dict] = []
last_signal_time: Dict[str, float] = {}
coin_volumes: Dict[str, float] = {}


# ═══════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ═══════════════════════════════════════

def reset_daily():
    today = datetime.now().strftime("%Y-%m-%d")
    if config["day_start"] != today:
        config["day_start"]      = today
        config["daily_loss"]     = 0.0
        config["daily_profit"]   = 0.0
        config["trading_active"] = True
        logger.info("Daily reset — новый день")


def can_trade() -> bool:
    reset_daily()
    if config["daily_loss"] >= config["stop_loss_usdt"]:
        config["trading_active"] = False
    return config["trading_active"]


def check_rate() -> bool:
    now = datetime.now()
    if (now - stats["minute_start"]).total_seconds() >= 60:
        stats["trades_this_minute"] = 0
        stats["minute_start"] = now
    return stats["trades_this_minute"] < config["max_trades_per_min"]


def get_total_balance() -> float:
    total = 0.0
    for assets in sim_balances.values():
        for asset, val in assets.items():
            total += val
    return total


def check_balance_warnings() -> List[str]:
    warns = []
    for ex, assets in sim_balances.items():
        usdt = assets.get("USDT", 0)
        if usdt < 20:
            warns.append(f"⚠️ {ex}: USDT = ${round(usdt,1)} — мало!")
        for sym in SYMBOLS:
            val = assets.get(sym, 0)
            if ex in ["KuCoin", "HTX"] and val < 10:
                warns.append(f"⚠️ {ex}: {sym} = ${round(val,1)} — мало!")
    return warns


async def send_tg(session, text):
    if not CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        await session.post(url, json={
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": "Markdown"
        }, timeout=aiohttp.ClientTimeout(total=10))
    except Exception as e:
        logger.error(f"TG: {e}")


async def send_document(session, filename: str, content: str, caption: str = ""):
    if not CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendDocument"
    try:
        data = aiohttp.FormData()
        data.add_field("chat_id", str(CHAT_ID))
        data.add_field("caption", caption)
        data.add_field(
            "document",
            io.BytesIO(content.encode("utf-8")),
            filename=filename,
            content_type="text/plain"
        )
        await session.post(url, data=data, timeout=aiohttp.ClientTimeout(total=15))
    except Exception as e:
        logger.error(f"Doc: {e}")


async def get_updates(session, offset=0):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
    try:
        async with session.get(url,
            params={"offset": offset, "timeout": 30},
            timeout=aiohttp.ClientTimeout(total=35)) as r:
            return (await r.json()).get("result", [])
    except:
        return []


# ═══════════════════════════════════════
# ПОЛУЧЕНИЕ ЦЕН — интервал 10 сек
# ═══════════════════════════════════════

async def get_binance(session) -> Tuple[Dict, Dict]:
    try:
        async with session.get(
            "https://api.binance.com/api/v3/ticker/24hr",
            timeout=aiohttp.ClientTimeout(total=8)) as r:
            prices, volumes = {}, {}
            for item in await r.json():
                sym = item.get("symbol", "")
                if sym.endswith(QUOTE):
                    base = sym[:-len(QUOTE)]
                    if base in SYMBOLS:
                        bid = float(item.get("bidPrice", 0) or 0)
                        ask = float(item.get("askPrice", 0) or 0)
                        vol = float(item.get("quoteVolume", 0) or 0)
                        if bid > 0 and ask > 0:
                            prices[base]  = {"bid": bid, "ask": ask}
                            volumes[base] = vol
            return prices, volumes
    except Exception as e:
        logger.error(f"Binance: {e}")
        return {}, {}


async def get_kucoin(session) -> Tuple[Dict, Dict]:
    try:
        async with session.get(
            "https://api.kucoin.com/api/v1/market/allTickers",
            timeout=aiohttp.ClientTimeout(total=8)) as r:
            prices, volumes = {}, {}
            for item in (await r.json()).get("data", {}).get("ticker", []):
                sym = item.get("symbol", "")
                if sym.endswith(f"-{QUOTE}"):
                    base = sym[:-len(f"-{QUOTE}")]
                    if base in SYMBOLS:
                        bid = float(item.get("buy",      0) or 0)
                        ask = float(item.get("sell",     0) or 0)
                        vol = float(item.get("volValue", 0) or 0)
                        if bid > 0 and ask > 0:
                            prices[base]  = {"bid": bid, "ask": ask}
                            volumes[base] = vol
            return prices, volumes
    except Exception as e:
        logger.error(f"KuCoin: {e}")
        return {}, {}


async def get_htx(session) -> Tuple[Dict, Dict]:
    try:
        async with session.get(
            "https://api.huobi.pro/market/tickers",
            timeout=aiohttp.ClientTimeout(total=8)) as r:
            prices, volumes = {}, {}
            for item in (await r.json()).get("data", []):
                sym = item.get("symbol", "")
                if sym.endswith("usdt"):
                    base = sym[:-4].upper()
                    if base in SYMBOLS:
                        bid = float(item.get("bid", 0) or 0)
                        ask = float(item.get("ask", 0) or 0)
                        vol = float(item.get("vol", 0) or 0) * ask
                        if bid > 0 and ask > 0:
                            prices[base]  = {"bid": bid, "ask": ask}
                            volumes[base] = vol
            return prices, volumes
    except Exception as e:
        logger.error(f"HTX: {e}")
        return {}, {}


async def fetch_all(session):
    # Параллельные запросы ко всем биржам
    res = await asyncio.gather(
        get_binance(session),
        get_kucoin(session),
        get_htx(session),
        return_exceptions=True
    )
    bn_p, bn_v = res[0] if not isinstance(res[0], Exception) else ({}, {})
    kc_p, kc_v = res[1] if not isinstance(res[1], Exception) else ({}, {})
    hx_p, hx_v = res[2] if not isinstance(res[2], Exception) else ({}, {})

    # Обновляем объёмы
    for sym in SYMBOLS:
        vols = [v.get(sym, 0) for v in [bn_v, kc_v, hx_v] if v.get(sym, 0) > 0]
        if vols:
            coin_volumes[sym] = sum(vols) / len(vols)

    active = []
    if bn_p: active.append("Binance")
    if kc_p: active.append("KuCoin")
    if hx_p: active.append("HTX")

    return bn_p, kc_p, hx_p, active


# ═══════════════════════════════════════
# АРБИТРАЖ
# ═══════════════════════════════════════

def calc_arb(symbol, buy_ex, buy_d, sell_ex, sell_d) -> Optional[dict]:
    buy_price  = buy_d.get("ask", 0)
    sell_price = sell_d.get("bid", 0)
    if buy_price <= 0 or sell_price <= buy_price:
        return None

    # Фильтр по объёму
    if coin_volumes.get(symbol, 0) < config["min_volume_usdt"]:
        return None

    buy_fee  = FEES.get(buy_ex,  0.1) / 100
    sell_fee = FEES.get(sell_ex, 0.1) / 100
    gross    = (sell_price - buy_price) / buy_price * 100
    net      = gross - buy_fee * 100 - sell_fee * 100

    if net < config["min_profit_pct"]:
        return None

    vol    = config["trade_usdt"]
    coins  = vol / buy_price
    profit = coins * sell_price * (1 - sell_fee) - vol * (1 + buy_fee)

    return {
        "symbol":      symbol,
        "buy_ex":      buy_ex,
        "sell_ex":     sell_ex,
        "buy_price":   buy_price,
        "sell_price":  sell_price,
        "gross_pct":   round(gross, 4),
        "net_pct":     round(net, 4),
        "profit_usdt": round(profit, 4),
        "coins":       round(coins, 6),
        "vol":         vol,
        "vol_24h":     round(coin_volumes.get(symbol, 0) / 1e6, 2),
        "time":        datetime.now().strftime("%H:%M:%S"),
    }


async def scan_all(session) -> Tuple[List[dict], List[str]]:
    stats["scans"] += 1
    bn, kc, hx, active = await fetch_all(session)
    ex_map = {"Binance": bn, "KuCoin": kc, "HTX": hx}
    signals = []
    hour = datetime.now().hour

    for sym in SYMBOLS:
        for buy_ex, sell_ex in PAIRS:
            bd = ex_map.get(buy_ex, {}).get(sym)
            sd = ex_map.get(sell_ex, {}).get(sym)
            if not bd or not sd:
                continue
            opp = calc_arb(sym, buy_ex, bd, sell_ex, sd)
            if opp:
                signals.append(opp)
                key = f"{buy_ex}→{sell_ex}"
                stats["pair_stats"][key]      = stats["pair_stats"].get(key, 0) + 1
                stats["symbol_stats"][sym]    = stats["symbol_stats"].get(sym, 0) + 1
                stats["hourly_signals"][hour] += 1

    signals.sort(key=lambda x: x["net_pct"], reverse=True)
    if signals:
        stats["signals"] += len(signals)

    return signals, active


def update_sim_balances(opp: dict):
    sym    = opp["symbol"]
    bex    = opp["buy_ex"]
    sex    = opp["sell_ex"]
    vol    = opp["vol"]
    coins  = opp["coins"]
    profit = opp["profit_usdt"]

    if bex in sim_balances:
        sim_balances[bex]["USDT"] = max(0, sim_balances[bex].get("USDT", 0) - vol)
        sim_balances[bex][sym]    = sim_balances[bex].get(sym, 0) + coins

    if sex in sim_balances:
        sim_balances[sex][sym]    = max(0, sim_balances[sex].get(sym, 0) - coins)
        sim_balances[sex]["USDT"] = sim_balances[sex].get("USDT", 0) + vol + profit


async def execute_trade(opp: dict):
    if not check_rate() or not can_trade():
        return

    profit = opp["profit_usdt"]
    hour   = datetime.now().hour

    trade_history.append({
        "id":          len(trade_history) + 1,
        "date":        datetime.now().strftime("%Y-%m-%d"),
        "time":        datetime.now().strftime("%H:%M:%S"),
        "symbol":      opp["symbol"],
        "buy_ex":      opp["buy_ex"],
        "sell_ex":     opp["sell_ex"],
        "buy_price":   opp["buy_price"],
        "sell_price":  opp["sell_price"],
        "vol":         opp["vol"],
        "coins":       opp["coins"],
        "gross_pct":   opp["gross_pct"],
        "net_pct":     opp["net_pct"],
        "profit_usdt": profit,
        "mode":        "SIM" if config["simulation_mode"] else "REAL",
    })

    stats["trades"]             += 1
    stats["profit"]             += profit
    stats["trades_this_minute"] += 1
    stats["hourly_profit"][hour] += profit

    if profit >= 0:
        config["daily_profit"] += profit
    else:
        config["daily_loss"] += abs(profit)
        if config["daily_loss"] >= config["stop_loss_usdt"]:
            config["trading_active"] = False

    if config["simulation_mode"]:
        update_sim_balances(opp)

    mode = "SIM" if config["simulation_mode"] else "REAL"
    logger.info(
        f"{mode} #{len(trade_history)}: {opp['symbol']} "
        f"{opp['buy_ex']}→{opp['sell_ex']} "
        f"+{opp['net_pct']}% +{profit} USDT"
    )


def format_signal(opp: dict) -> str:
    mode = "🔵 СИМУЛЯЦИЯ" if config["simulation_mode"] else "🔴 РЕАЛЬНАЯ"
    p100  = round(opp["profit_usdt"] * (100  / opp["vol"]), 4)
    p500  = round(opp["profit_usdt"] * (500  / opp["vol"]), 4)
    p1000 = round(opp["profit_usdt"] * (1000 / opp["vol"]), 4)
    return (
        f"🚨 *{opp['buy_ex']} → {opp['sell_ex']} | {opp['symbol']}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{mode}\n\n"
        f"📥 *КУПИТЬ на {opp['buy_ex']}*\n"
        f"   Цена: `{opp['buy_price']} USDT`\n"
        f"   Лот: `{opp['vol']} USDT`\n"
        f"   Монет: `{opp['coins']} {opp['symbol']}`\n\n"
        f"📤 *ПРОДАТЬ на {opp['sell_ex']}*\n"
        f"   Цена: `{opp['sell_price']} USDT`\n\n"
        f"📊 *Расчёт:*\n"
        f"   Спред: `{opp['gross_pct']}%`\n"
        f"   После комиссий: `{opp['net_pct']}%`\n"
        f"   Объём монеты 24ч: `${opp['vol_24h']}М`\n\n"
        f"💰 *Прибыль при разных депозитах:*\n"
        f"   $100 → `~{p100} USDT`\n"
        f"   $500 → `~{p500} USDT`\n"
        f"   $1000 → `~{p1000} USDT`\n\n"
        f"⚠️ Цена актуальна только сейчас!\n"
        f"🕐 {opp['time']}"
    )


def generate_csv() -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "ID", "Дата", "Время", "Монета",
        "Купить на", "Продать на",
        "Цена покупки", "Цена продажи",
        "Объём USDT", "Монет",
        "Спред%", "Чистая%", "Прибыль USDT", "Режим"
    ])
    today = datetime.now().strftime("%Y-%m-%d")
    today_trades = [t for t in trade_history if t.get("date") == today]
    for t in today_trades:
        writer.writerow([
            t["id"], t["date"], t["time"], t["symbol"],
            t["buy_ex"], t["sell_ex"],
            t["buy_price"], t["sell_price"],
            t["vol"], t["coins"],
            t["gross_pct"], t["net_pct"], t["profit_usdt"], t["mode"]
        ])
    writer.writerow([])
    writer.writerow(["ИТОГИ ЗА ДЕНЬ"])
    total = sum(t["profit_usdt"] for t in today_trades)
    writer.writerow(["Сделок", len(today_trades)])
    writer.writerow(["Прибыль USDT", round(total, 4)])
    avg = round(total / len(today_trades), 4) if today_trades else 0
    writer.writerow(["Средняя прибыль", avg])
    return output.getvalue()


# ═══════════════════════════════════════
# КОМАНДЫ
# ═══════════════════════════════════════

async def handle_command(session, text, chat_id):
    global CHAT_ID
    CHAT_ID = chat_id
    parts = text.strip().split()
    cmd = parts[0].lower()

    if cmd == "/start":
        mode = "🔵 СИМУЛЯЦИЯ $500" if config["simulation_mode"] else "🔴 РЕАЛЬНАЯ ТОРГОВЛЯ"
        sl   = "🟢 Активна" if config["trading_active"] else "🔴 СТОП-ЛОСС"
        await send_tg(session,
            f"✅ *TriangleArbBot запущен!*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Режим: {mode}\n"
            f"Торговля: {sl}\n\n"
            f"📊 *Площадки:* Binance | KuCoin | HTX\n"
            f"💱 *Монеты:* BONK | SEI | FET\n"
            f"🔀 *Пары:* HTX→KuCoin | KuCoin→HTX | Binance→HTX\n\n"
            f"⚙️ Лот: `${config['trade_usdt']} USDT`\n"
            f"⚙️ Мин. прибыль: `{config['min_profit_pct']}%`\n"
            f"⚙️ Стоп-лосс: `${config['stop_loss_usdt']}/день`\n"
            f"⚙️ Интервал: `{config['scan_interval']} сек`\n"
            f"⚙️ Макс. сделок: `{config['max_trades_per_min']}/мин`\n\n"
            f"*Команды:*\n"
            f"/scan — скан прямо сейчас\n"
            f"/top — все пары без порога\n"
            f"/stats — статистика\n"
            f"/balances — балансы бирж\n"
            f"/rebalance — что ребалансировать\n"
            f"/hours — активность по часам\n"
            f"/report — отчёт за день\n"
            f"/csv — скачать CSV\n"
            f"/history — последние сделки\n"
            f"/guide — инструкция реальной торговли\n"
            f"/mode — симуляция ↔ реал\n"
            f"/setprofit 0.3 — мин. прибыль %\n"
            f"/setlot 20 — размер лота\n"
            f"/setstop 10 — стоп-лосс $\n"
            f"/resume — снять стоп-лосс\n"
        )

    elif cmd == "/scan":
        if not config["trading_active"] and not config["simulation_mode"]:
            await send_tg(session,
                f"🔴 *СТОП-ЛОСС АКТИВЕН*\n"
                f"Потеря >${config['stop_loss_usdt']}/день.\n"
                f"/resume — возобновить."
            )
            return
        await send_tg(session, "🔍 Сканирую Binance / KuCoin / HTX...")
        signals, active = await scan_all(session)
        if not signals:
            await send_tg(session,
                f"😔 Нет сигналов (порог {config['min_profit_pct']}%).\n"
                f"Бирж онлайн: {', '.join(active)}\n\n"
                f"Напиши /top — увидишь текущие спреды."
            )
        else:
            await send_tg(session, f"✅ {len(signals)} сигналов! Топ-3:")
            for opp in signals[:3]:
                await send_tg(session, format_signal(opp))
                await execute_trade(opp)

            warns = check_balance_warnings()
            if warns:
                await send_tg(session,
                    "⚠️ *БАЛАНСЫ ТРЕБУЮТ ВНИМАНИЯ:*\n"
                    + "\n".join(warns)
                )

    elif cmd == "/top":
        await send_tg(session, "📊 Сканирую без порога...")
        bn, kc, hx, active = await fetch_all(session)
        ex_map = {"Binance": bn, "KuCoin": kc, "HTX": hx}
        saved = config["min_profit_pct"]
        config["min_profit_pct"] = -999
        all_opps = []
        for sym in SYMBOLS:
            for buy_ex, sell_ex in PAIRS:
                bd = ex_map.get(buy_ex, {}).get(sym)
                sd = ex_map.get(sell_ex, {}).get(sym)
                if bd and sd:
                    opp = calc_arb(sym, buy_ex, bd, sell_ex, sd)
                    if opp:
                        all_opps.append(opp)
        config["min_profit_pct"] = saved
        all_opps.sort(key=lambda x: x["net_pct"], reverse=True)

        msg = f"📊 *ВСЕ ПАРЫ — {datetime.now().strftime('%H:%M:%S')}*\n"
        msg += f"Бирж онлайн: {', '.join(active)}\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        if not all_opps:
            msg += "Нет данных или объём монет ниже минимума"
        for i, o in enumerate(all_opps, 1):
            icon = "🟢" if o["net_pct"] >= saved else "🔴"
            msg += (
                f"{icon} *{i}. {o['symbol']}* "
                f"{o['buy_ex']}→{o['sell_ex']}\n"
                f"   Спред: `{o['gross_pct']}%` | "
                f"Чистая: `{o['net_pct']}%` | "
                f"${o['vol_24h']}М\n\n"
            )
        msg += f"_Порог: {saved}%_"
        await send_tg(session, msg)

    elif cmd == "/stats":
        uptime = datetime.now() - stats["start_time"]
        h = int(uptime.total_seconds() // 3600)
        m = int((uptime.total_seconds() % 3600) // 60)
        mode = "Симуляция $500 🔵" if config["simulation_mode"] else "Реальная 🔴"
        sl   = "🟢 Активна" if config["trading_active"] else "🔴 СТОП-ЛОСС"

        pair_lines = "\n".join([
            f"   {p}: {c} сигналов"
            for p, c in sorted(stats["pair_stats"].items(),
                                key=lambda x: x[1], reverse=True)
        ])
        sym_lines = "\n".join([
            f"   {s}: {c} сигналов"
            for s, c in sorted(stats["symbol_stats"].items(),
                                key=lambda x: x[1], reverse=True)
        ])
        per_trade = round(stats["profit"] / stats["trades"], 4) if stats["trades"] else 0
        total_bal = round(get_total_balance(), 2)
        pnl = round(total_bal - SIM_START, 2)
        pnl_sign = "+" if pnl >= 0 else ""

        await send_tg(session,
            f"📈 *СТАТИСТИКА*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Режим: {mode} | {sl}\n"
            f"Аптайм: {h}ч {m}м\n\n"
            f"🔍 Сканов: {stats['scans']}\n"
            f"🎯 Сигналов: {stats['signals']}\n"
            f"✅ Сделок: {stats['trades']}\n"
            f"💰 Прибыль: {round(stats['profit'], 2)} USDT\n"
            f"📊 Прибыль/сделка: ~{per_trade} USDT\n"
            f"❌ Ошибок: {stats['errors']}\n\n"
            f"📅 *Сегодня:*\n"
            f"   +{round(config['daily_profit'], 2)} USDT\n"
            f"   -{round(config['daily_loss'], 2)} USDT\n"
            f"   Стоп: ${config['stop_loss_usdt']}\n\n"
            f"💵 *Баланс симуляции:*\n"
            f"   Старт: ${SIM_START}\n"
            f"   Сейчас: ${total_bal}\n"
            f"   P&L: {pnl_sign}${pnl}\n\n"
            f"🔀 *Активность пар:*\n{pair_lines}\n\n"
            f"💱 *Монеты:*\n{sym_lines}\n\n"
            f"⚙️ Лот: ${config['trade_usdt']} | "
            f"Порог: {config['min_profit_pct']}%\n"
            f"⚙️ {stats['trades_this_minute']}/"
            f"{config['max_trades_per_min']} сделок этой минуты\n\n"
            f"/hours /report /csv"
        )

    elif cmd == "/balances":
        total = 0.0
        msg = "💰 *БАЛАНСЫ СИМУЛЯЦИИ*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for ex, assets in sim_balances.items():
            ex_total = sum(assets.values())
            total += ex_total
            msg += f"🏦 *{ex}:* ${round(ex_total, 2)}\n"
            for asset, val in assets.items():
                icon = "🟢" if val >= 20 else "🔴"
                msg += f"   {icon} {asset}: ${round(val, 2)}\n"
            msg += "\n"
        pnl = round(total - SIM_START, 2)
        sign = "+" if pnl >= 0 else ""
        msg += f"💵 *Итого: ${round(total, 2)}*\n"
        msg += f"Старт: ${SIM_START} | P&L: {sign}${pnl}\n\n"
        warns = check_balance_warnings()
        if warns:
            msg += "⚠️ *Предупреждения:*\n" + "\n".join(warns)
        await send_tg(session, msg)

    elif cmd == "/rebalance":
        target = {
            "KuCoin":  {"USDT": 125, "BONK": 62.5, "SEI": 31.25, "FET": 31.25},
            "HTX":     {"USDT": 125, "BONK": 62.5, "SEI": 31.25, "FET": 31.25},
            "Binance": {"USDT": 125},
        }
        msg = "⚖️ *РЕБАЛАНСИРОВКА*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        actions = []
        for ex, tgt in target.items():
            cur = sim_balances.get(ex, {})
            ex_act = []
            for asset, tgt_val in tgt.items():
                cur_val = cur.get(asset, 0)
                diff = tgt_val - cur_val
                if diff > 5:
                    ex_act.append(f"   ➕ Купить {asset}: +${round(diff)}")
                elif diff < -5:
                    ex_act.append(f"   ➖ Продать {asset}: -${round(abs(diff))}")
            if ex_act:
                actions.append(f"*{ex}:*\n" + "\n".join(ex_act))

        if not actions:
            msg += "✅ Все балансы в норме!"
        else:
            msg += "\n\n".join(actions)
            msg += (
                "\n\n💡 *Как ребалансировать:*\n"
                "Продай лишнее → купи нужное\n"
                "Или переведи USDT через TRC-20 (~$1)"
            )
        await send_tg(session, msg)

    elif cmd == "/hours":
        msg = f"⏰ *СИГНАЛЫ ПО ЧАСАМ (UTC)*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        hour_data = [
            (h, stats["hourly_signals"].get(h, 0), stats["hourly_profit"].get(h, 0.0))
            for h in range(24)
            if stats["hourly_signals"].get(h, 0) > 0
        ]
        if not hour_data:
            msg += "Нет данных."
        else:
            hour_data.sort(key=lambda x: x[1], reverse=True)
            for h, sigs, profit in hour_data[:10]:
                bar = "█" * min(10, sigs // 3 + 1)
                msg += (
                    f"*{h:02d}:00* {bar}\n"
                    f"   Сигналов: {sigs} | "
                    f"Прибыль: {round(profit, 2)} USDT\n\n"
                )
            best = max(hour_data, key=lambda x: x[1])
            msg += f"🏆 Лучший час: *{best[0]:02d}:00 UTC*"
        await send_tg(session, msg)

    elif cmd == "/report":
        today = datetime.now().strftime("%Y-%m-%d")
        today_trades = [t for t in trade_history if t.get("date") == today]
        if not today_trades:
            await send_tg(session, "📋 Нет сделок за сегодня.")
            return
        total = sum(t["profit_usdt"] for t in today_trades)
        wins  = sum(1 for t in today_trades if t["profit_usdt"] > 0)

        sym_profit: Dict[str, float] = defaultdict(float)
        for t in today_trades:
            sym_profit[t["symbol"]] += t["profit_usdt"]

        pair_profit: Dict[str, float] = defaultdict(float)
        for t in today_trades:
            pair_profit[f"{t['buy_ex']}→{t['sell_ex']}"] += t["profit_usdt"]

        msg = (
            f"📋 *ОТЧЁТ — {today}*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"✅ Сделок: {len(today_trades)}\n"
            f"💰 Прибыль: {round(total, 4)} USDT\n"
            f"📈 Прибыльных: {wins}/{len(today_trades)}\n"
            f"📊 Средняя: {round(total/len(today_trades),4)} USDT\n\n"
            f"💱 *По монетам:*\n"
        )
        for sym, p in sorted(sym_profit.items(), key=lambda x: x[1], reverse=True):
            sign = "+" if p >= 0 else ""
            msg += f"   {sym}: {sign}{round(p, 4)} USDT\n"
        msg += "\n🔀 *По парам:*\n"
        for pair, p in sorted(pair_profit.items(), key=lambda x: x[1], reverse=True):
            sign = "+" if p >= 0 else ""
            msg += f"   {pair}: {sign}{round(p, 4)} USDT\n"
        await send_tg(session, msg)

    elif cmd == "/csv":
        await send_tg(session, "📊 Генерирую CSV...")
        content = generate_csv()
        today = datetime.now().strftime("%Y-%m-%d")
        await send_document(
            session,
            f"arb_report_{today}.csv",
            content,
            f"📊 Отчёт {today} | {stats['trades']} сделок"
        )

    elif cmd == "/history":
        if not trade_history:
            await send_tg(session, "📋 Нет сделок.")
            return
        msg = "📋 *ПОСЛЕДНИЕ СДЕЛКИ*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for t in trade_history[-10:][::-1]:
            sign = "+" if t["profit_usdt"] > 0 else ""
            msg += (
                f"#{t['id']} *{t['symbol']}* "
                f"{t['buy_ex']}→{t['sell_ex']}\n"
                f"   {sign}{t['net_pct']}% | "
                f"{sign}{t['profit_usdt']} USDT | "
                f"{t['time']}\n\n"
            )
        await send_tg(session, msg)

    elif cmd == "/guide":
        await send_tg(session,
            f"📖 *ПОШАГОВАЯ ИНСТРУКЦИЯ РЕАЛЬНОЙ ТОРГОВЛИ*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"*ШАГ 1 — Регистрация (1-2 дня)*\n"
            f"✅ kucoin.com — KYC паспорт + селфи\n"
            f"✅ htx.com — KYC паспорт + селфи\n"
            f"✅ binance.com — KYC паспорт + селфи\n\n"
            f"*ШАГ 2 — API ключи*\n"
            f"На каждой бирже:\n"
            f"Профиль → API Management → Create\n"
            f"✅ Разрешить: Read + Spot Trading\n"
            f"❌ НЕ разрешать: Withdrawal!\n\n"
            f"В Railway Variables добавь:\n"
            f"`BINANCE_API_KEY` / `BINANCE_API_SECRET`\n"
            f"`KUCOIN_API_KEY` / `KUCOIN_API_SECRET`\n"
            f"`KUCOIN_PASSPHRASE`\n"
            f"`HTX_API_KEY` / `HTX_API_SECRET`\n\n"
            f"*ШАГ 3 — Деньги*\n"
            f"Купи USDT на Binance P2P\n"
            f"Выведи на KuCoin: $250 (TRC-20, ~$1)\n"
            f"Выведи на HTX: $250 (TRC-20, ~$1)\n"
            f"На Binance оставь: $0 (только для сигналов)\n\n"
            f"*ШАГ 4 — Купи монеты (на KuCoin и HTX)*\n"
            f"BONK: $62.5 на каждой бирже\n"
            f"SEI: $31.25 на каждой\n"
            f"FET: $31.25 на каждой\n"
            f"Остаток USDT: $125 на каждой\n\n"
            f"*ШАГ 5 — Переключи в реальный режим*\n"
            f"1. /setstop 10 (стоп $10 при $500 капитале)\n"
            f"2. /setlot 20 (лот $20)\n"
            f"3. /mode → реальный режим\n"
            f"4. Наблюдай 24 часа\n\n"
            f"*ШАГ 6 — Контроль каждый день*\n"
            f"Утро: /stats /balances\n"
            f"Вечер: /report /rebalance\n"
            f"Еженедельно: /csv + сравни P&L\n\n"
            f"*КАК УВИДЕТЬ ПРИБЫЛЬ:*\n"
            f"/balances → смотри P&L строку\n"
            f"Реальная прибыль = текущий баланс - стартовый\n"
            f"Выводи прибыль раз в неделю на Binance"
        )

    elif cmd == "/mode":
        config["simulation_mode"] = not config["simulation_mode"]
        if config["simulation_mode"]:
            mode = "🔵 СИМУЛЯЦИЯ $500"
            msg = f"Режим: {mode}\nВиртуальный капитал $500 восстановлен."
        else:
            mode = "🔴 РЕАЛЬНАЯ ТОРГОВЛЯ"
            has_keys = all([BINANCE_KEY, KUCOIN_KEY, HTX_KEY])
            if not has_keys:
                config["simulation_mode"] = True
                await send_tg(session,
                    "❌ *Нет API ключей!*\n\n"
                    "Добавь в Railway Variables:\n"
                    "`BINANCE_API_KEY` / `BINANCE_API_SECRET`\n"
                    "`KUCOIN_API_KEY` / `KUCOIN_API_SECRET` / `KUCOIN_PASSPHRASE`\n"
                    "`HTX_API_KEY` / `HTX_API_SECRET`\n\n"
                    "Режим остался: 🔵 СИМУЛЯЦИЯ"
                )
                return
            msg = f"Режим: {mode}\n\n⚠️ Торгую реальными деньгами!\nСмотри /guide"
        await send_tg(session, msg)

    elif cmd == "/resume":
        config["trading_active"] = True
        config["daily_loss"] = 0.0
        await send_tg(session, "✅ Торговля возобновлена. Стоп-лосс сброшен.")

    elif cmd == "/setprofit":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/setprofit 0.3`")
            return
        try:
            config["min_profit_pct"] = float(parts[1])
            await send_tg(session, f"✅ Мин. прибыль: `{config['min_profit_pct']}%`")
        except:
            await send_tg(session, "❌ Пример: `/setprofit 0.3`")

    elif cmd == "/setlot":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/setlot 20`")
            return
        try:
            config["trade_usdt"] = float(parts[1])
            await send_tg(session, f"✅ Лот: `${config['trade_usdt']} USDT`")
        except:
            await send_tg(session, "❌ Пример: `/setlot 20`")

    elif cmd == "/setstop":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/setstop 10`")
            return
        try:
            config["stop_loss_usdt"] = float(parts[1])
            await send_tg(session, f"✅ Стоп-лосс: `${config['stop_loss_usdt']}/день`")
        except:
            await send_tg(session, "❌ Пример: `/setstop 10`")

    else:
        await send_tg(session,
            "/start /scan /top /stats\n"
            "/balances /rebalance /hours\n"
            "/report /csv /history\n"
            "/guide /mode /resume\n"
            "/setprofit 0.3\n"
            "/setlot 20\n"
            "/setstop 10"
        )


# ═══════════════════════════════════════
# ОСНОВНЫЕ ЦИКЛЫ
# ═══════════════════════════════════════

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
    last_report_date = None

    while True:
        try:
            reset_daily()

            if not can_trade() and not config["simulation_mode"]:
                await asyncio.sleep(config["scan_interval"])
                continue

            signals, active = await scan_all(session)
            mode = "SIM" if config["simulation_mode"] else "REAL"
            logger.info(
                f"[{mode}] Scan #{stats['scans']}: "
                f"{len(active)} бирж | {len(signals)} сигналов | "
                f"P&L: +{round(config['daily_profit'],2)}"
                f"/-{round(config['daily_loss'],2)}"
            )

            for opp in signals[:3]:
                key = f"{opp['symbol']}-{opp['buy_ex']}-{opp['sell_ex']}"
                now = datetime.now().timestamp()
                if now - last_signal_time.get(key, 0) > 120:
                    last_signal_time[key] = now
                    if CHAT_ID:
                        await send_tg(session, format_signal(opp))
                    await execute_trade(opp)

                    if not config["trading_active"] and CHAT_ID:
                        await send_tg(session,
                            f"🔴 *СТОП-ЛОСС СРАБОТАЛ*\n"
                            f"Потеря >${config['stop_loss_usdt']}/день.\n"
                            f"/resume — возобновить."
                        )
                        break

            # Предупреждение о балансах каждые 30 минут
            if stats["scans"] % 180 == 0:
                warns = check_balance_warnings()
                if warns and CHAT_ID:
                    await send_tg(session,
                        "⚠️ *БАЛАНСЫ:*\n" + "\n".join(warns) +
                        "\n\nНапиши /rebalance"
                    )

            # Автоотчёт в 23:55
            now_dt = datetime.now()
            if now_dt.hour == 23 and now_dt.minute >= 55:
                if last_report_date != now_dt.date():
                    last_report_date = now_dt.date()
                    if CHAT_ID:
                        today = now_dt.strftime("%Y-%m-%d")
                        today_trades = [t for t in trade_history if t.get("date") == today]
                        if today_trades:
                            total = sum(t["profit_usdt"] for t in today_trades)
                            await send_tg(session,
                                f"📋 *АВТООТЧЁТ {today}*\n"
                                f"Сделок: {len(today_trades)}\n"
                                f"Прибыль: {round(total, 2)} USDT\n"
                                f"P&L симуляции: ${round(get_total_balance()-SIM_START, 2)}"
                            )
                            content = generate_csv()
                            await send_document(
                                session,
                                f"arb_report_{today}.csv",
                                content,
                                "📊 Ежедневный автоотчёт"
                            )

        except Exception as e:
            stats["errors"] += 1
            logger.error(f"Scan error: {e}")

        await asyncio.sleep(config["scan_interval"])


async def main():
    if not TG_TOKEN:
        logger.error("ARB_BOT_TOKEN не установлен!")
        return
    logger.info(
        f"TriangleArbBot START | "
        f"СИМУЛЯЦИЯ $500 | "
        f"Монеты: {SYMBOLS} | "
        f"Пары: {len(PAIRS)} | "
        f"Интервал: {config['scan_interval']}сек"
    )
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        await asyncio.gather(polling_loop(session), scan_loop(session))


if __name__ == "__main__":
    asyncio.run(main())
