import asyncio
import aiohttp
import logging
import os
import json
import csv
import io
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

TG_TOKEN = os.environ.get("ARB_BOT_TOKEN", "")
CHAT_ID = None

config = {
    "min_profit_pct":     float(os.environ.get("MIN_PROFIT_PCT", "0.3")),
    "trade_usdt":         100.0,
    "scan_interval":      6,
    "simulation_mode":    os.environ.get("SIMULATION_MODE", "true").lower() == "true",
    "max_trades_per_min": 10,
    "stop_loss_usdt":     20.0,
    "daily_loss":         0.0,
    "daily_profit":       0.0,
    "day_start":          datetime.now().strftime("%Y-%m-%d"),
    "trading_active":     True,
    "min_volume_usdt":    500000,  # мин. суточный объём монеты $500k
}

PAIRS = [
    ("KuCoin",  "HTX"),
    ("HTX",     "KuCoin"),
    ("Binance", "HTX"),
]

FEES = {"Binance": 0.10, "KuCoin": 0.10, "HTX": 0.20}

SYMBOLS = ["FET", "NEAR", "WIF", "BONK", "SEI"]
QUOTE = "USDT"

# Плановые балансы для ребалансировки
TARGET_BALANCE = {
    "KuCoin": {"USDT": 600, "FET": 120, "NEAR": 120, "WIF": 120, "BONK": 120, "SEI": 120},
    "HTX":    {"USDT": 600, "FET": 120, "NEAR": 120, "WIF": 120, "BONK": 120, "SEI": 120},
    "Binance":{"USDT": 600},
}

TOTAL_DEPOSIT = 3000

stats = {
    "scans": 0, "signals": 0,
    "trades_sim": 0, "profit_sim": 0.0,
    "errors": 0, "start_time": datetime.now(),
    "trades_this_minute": 0,
    "minute_start": datetime.now(),
    "pair_stats":   {f"{b}→{s}": 0 for b, s in PAIRS},
    "symbol_stats": {s: 0 for s in SYMBOLS},
    "hourly_signals": defaultdict(int),  # час -> кол-во сигналов
    "hourly_profit":  defaultdict(float),
}
trade_history: List[dict] = []
last_signal_time: Dict[str, float] = {}

# Текущие объёмы монет (обновляются при скане)
coin_volumes: Dict[str, float] = {}

# Симулируемые балансы для ребалансировки
sim_balances = {
    "KuCoin": {"USDT": 600.0, "FET": 120.0, "NEAR": 120.0, "WIF": 120.0, "BONK": 120.0, "SEI": 120.0},
    "HTX":    {"USDT": 600.0, "FET": 120.0, "NEAR": 120.0, "WIF": 120.0, "BONK": 120.0, "SEI": 120.0},
    "Binance":{"USDT": 600.0},
}


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
        logger.info("Daily reset")


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


def update_sim_balances(opp: dict):
    """Обновляем симулированные балансы после сделки"""
    sym      = opp["symbol"]
    buy_ex   = opp["buy_ex"]
    sell_ex  = opp["sell_ex"]
    vol      = opp["vol"]
    coins    = opp["coins"]

    if buy_ex in sim_balances:
        sim_balances[buy_ex]["USDT"] = sim_balances[buy_ex].get("USDT", 0) - vol
        sim_balances[buy_ex][sym]    = sim_balances[buy_ex].get(sym, 0)    + coins

    if sell_ex in sim_balances:
        sim_balances[sell_ex][sym]    = max(0, sim_balances[sell_ex].get(sym, 0) - coins)
        sim_balances[sell_ex]["USDT"] = sim_balances[sell_ex].get("USDT", 0) + vol + opp["profit_usdt"]


def check_balance_health() -> List[str]:
    """Возвращает список предупреждений если баланс критически низкий"""
    warnings = []
    for ex, assets in sim_balances.items():
        usdt = assets.get("USDT", 0)
        if usdt < 100:
            warnings.append(f"⚠️ {ex}: USDT = ${round(usdt,0)} — мало!")
        for sym in SYMBOLS:
            val = assets.get(sym, 0)
            if ex in ["KuCoin", "HTX"] and val < 20:
                warnings.append(f"⚠️ {ex}: {sym} = ${round(val,0)} — мало монет!")
    return warnings


async def send_tg(session, text):
    if not CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        await session.post(url, json={
            "chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"
        }, timeout=aiohttp.ClientTimeout(total=10))
    except Exception as e:
        logger.error(f"TG: {e}")


async def send_document(session, filename: str, content: str, caption: str = ""):
    """Отправляет файл в Telegram"""
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
        logger.error(f"Send doc error: {e}")


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
# БИРЖИ
# ═══════════════════════════════════════

async def get_binance(session) -> Tuple[Dict, Dict]:
    """Возвращает (цены, объёмы)"""
    try:
        async with session.get(
            "https://api.binance.com/api/v3/ticker/24hr",
            timeout=aiohttp.ClientTimeout(total=6)) as r:
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
            timeout=aiohttp.ClientTimeout(total=6)) as r:
            prices, volumes = {}, {}
            for item in (await r.json()).get("data", {}).get("ticker", []):
                sym = item.get("symbol", "")
                if sym.endswith(f"-{QUOTE}"):
                    base = sym[:-len(f"-{QUOTE}")]
                    if base in SYMBOLS:
                        bid = float(item.get("buy",  0) or 0)
                        ask = float(item.get("sell", 0) or 0)
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
            timeout=aiohttp.ClientTimeout(total=6)) as r:
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
    res = await asyncio.gather(
        get_binance(session),
        get_kucoin(session),
        get_htx(session),
        return_exceptions=True
    )
    bn_p, bn_v = res[0] if not isinstance(res[0], Exception) else ({}, {})
    kc_p, kc_v = res[1] if not isinstance(res[1], Exception) else ({}, {})
    hx_p, hx_v = res[2] if not isinstance(res[2], Exception) else ({}, {})

    # Обновляем средние объёмы
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
    vol_ok = coin_volumes.get(symbol, 0) >= config["min_volume_usdt"]
    if not vol_ok:
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
        "volume_24h":  round(coin_volumes.get(symbol, 0) / 1e6, 2),
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


def format_signal(opp: dict) -> str:
    mode  = "🔵 СИМУЛЯЦИЯ" if config["simulation_mode"] else "🔴 РЕАЛЬНАЯ"
    p500  = round(opp["profit_usdt"] * 5,  4)
    p1000 = round(opp["profit_usdt"] * 10, 4)
    p3000 = round(opp["profit_usdt"] * 30, 4)
    return (
        f"🚨 *{opp['buy_ex']} → {opp['sell_ex']} | {opp['symbol']}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{mode}\n\n"
        f"📥 *КУПИТЬ на {opp['buy_ex']}*\n"
        f"   Цена ask: `{opp['buy_price']} USDT`\n"
        f"   Лот: `{opp['vol']} USDT`\n"
        f"   Получишь: `{opp['coins']} {opp['symbol']}`\n\n"
        f"📤 *ПРОДАТЬ на {opp['sell_ex']}*\n"
        f"   Цена bid: `{opp['sell_price']} USDT`\n\n"
        f"📊 *Расчёт:*\n"
        f"   Спред: `{opp['gross_pct']}%`\n"
        f"   После комиссий: `{opp['net_pct']}%`\n"
        f"   Объём монеты: `${opp['volume_24h']}М/сут`\n\n"
        f"💰 *Прибыль:*\n"
        f"   $100 → `~{opp['profit_usdt']} USDT`\n"
        f"   $500 → `~{p500} USDT`\n"
        f"   $1000 → `~{p1000} USDT`\n"
        f"   $3000 → `~{p3000} USDT`\n\n"
        f"⚠️ Цена актуальна только сейчас!\n"
        f"🕐 {opp['time']}"
    )


async def execute_sim(opp: dict):
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
    })

    stats["trades_sim"]            += 1
    stats["profit_sim"]            += profit
    stats["trades_this_minute"]    += 1
    stats["hourly_profit"][hour]   += profit

    if profit >= 0:
        config["daily_profit"] += profit
    else:
        config["daily_loss"] += abs(profit)
        if config["daily_loss"] >= config["stop_loss_usdt"]:
            config["trading_active"] = False

    update_sim_balances(opp)


# ═══════════════════════════════════════
# ГЕНЕРАЦИЯ ОТЧЁТОВ
# ═══════════════════════════════════════

def generate_daily_report() -> str:
    """Генерирует CSV отчёт за сегодня"""
    today = datetime.now().strftime("%Y-%m-%d")
    today_trades = [t for t in trade_history if t.get("date") == today]

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "ID", "Дата", "Время", "Монета",
        "Купить на", "Продать на",
        "Цена покупки", "Цена продажи",
        "Объём USDT", "Монет", "Спред%", "Чистая%", "Прибыль USDT"
    ])
    for t in today_trades:
        writer.writerow([
            t["id"], t["date"], t["time"], t["symbol"],
            t["buy_ex"], t["sell_ex"],
            t["buy_price"], t["sell_price"],
            t["vol"], t["coins"],
            t["gross_pct"], t["net_pct"], t["profit_usdt"]
        ])

    # Итоги
    writer.writerow([])
    writer.writerow(["ИТОГИ ЗА ДЕНЬ"])
    total_profit = sum(t["profit_usdt"] for t in today_trades)
    writer.writerow(["Сделок", len(today_trades)])
    writer.writerow(["Прибыль USDT", round(total_profit, 4)])
    avg = round(total_profit / len(today_trades), 4) if today_trades else 0
    writer.writerow(["Средняя прибыль", avg])

    return output.getvalue()


def generate_text_report() -> str:
    """Текстовый отчёт для Telegram"""
    today = datetime.now().strftime("%Y-%m-%d")
    today_trades = [t for t in trade_history if t.get("date") == today]
    total_profit = sum(t["profit_usdt"] for t in today_trades)
    wins  = sum(1 for t in today_trades if t["profit_usdt"] > 0)
    loses = len(today_trades) - wins

    # Топ монеты по прибыли
    sym_profit: Dict[str, float] = defaultdict(float)
    for t in today_trades:
        sym_profit[t["symbol"]] += t["profit_usdt"]
    top_syms = sorted(sym_profit.items(), key=lambda x: x[1], reverse=True)

    # Топ пары
    pair_profit: Dict[str, float] = defaultdict(float)
    for t in today_trades:
        pair_profit[f"{t['buy_ex']}→{t['sell_ex']}"] += t["profit_usdt"]
    top_pairs = sorted(pair_profit.items(), key=lambda x: x[1], reverse=True)

    report = (
        f"📋 *ДНЕВНОЙ ОТЧЁТ — {today}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"✅ Сделок: {len(today_trades)}\n"
        f"💰 Прибыль: {round(total_profit, 4)} USDT\n"
        f"📈 Прибыльных: {wins}\n"
        f"📉 Убыточных: {loses}\n"
        f"📊 Средняя: {round(total_profit/len(today_trades),4) if today_trades else 0} USDT\n\n"
        f"💱 *По монетам:*\n"
    )
    for sym, profit in top_syms:
        sign = "+" if profit >= 0 else ""
        report += f"   {sym}: {sign}{round(profit,4)} USDT\n"

    report += f"\n🔀 *По парам:*\n"
    for pair, profit in top_pairs:
        sign = "+" if profit >= 0 else ""
        report += f"   {pair}: {sign}{round(profit,4)} USDT\n"

    return report


# ═══════════════════════════════════════
# КОМАНДЫ
# ═══════════════════════════════════════

async def handle_command(session, text, chat_id):
    global CHAT_ID
    CHAT_ID = chat_id
    parts = text.strip().split()
    cmd = parts[0].lower()

    if cmd == "/start":
        mode = "🔵 СИМУЛЯЦИЯ" if config["simulation_mode"] else "🔴 РЕАЛЬНАЯ"
        sl   = "🟢 Активна"   if config["trading_active"]  else "🔴 СТОП-ЛОСС"
        await send_tg(session,
            f"✅ *TriangleArbBot*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Режим: {mode} | {sl}\n\n"
            f"🔀 Пары: KuCoin↔HTX | Binance→HTX\n"
            f"💱 Монеты: FET | NEAR | WIF | BONK | SEI\n\n"
            f"⚙️ Лот: `${config['trade_usdt']}` | "
            f"Мин.прибыль: `{config['min_profit_pct']}%`\n"
            f"⚙️ Стоп-лосс: `${config['stop_loss_usdt']}/день`\n"
            f"⚙️ Мин.объём: `${config['min_volume_usdt']/1e6:.1f}М`\n\n"
            f"*Команды:*\n"
            f"/scan — скан прямо сейчас\n"
            f"/top — все пары без порога\n"
            f"/stats — статистика\n"
            f"/hours — по каким часам сигналы\n"
            f"/report — дневной отчёт\n"
            f"/csv — скачать CSV отчёт\n"
            f"/rebalance — что нужно ребалансировать\n"
            f"/balances — текущие балансы\n"
            f"/deposit — план депозитов\n"
            f"/volumes — объёмы монет\n"
            f"/history — последние сделки\n"
            f"/mode — симуляция ↔ реал\n"
            f"/guide — инструкция\n"
            f"/setprofit 0.3 | /setstop 20 | /setminvol 500000\n"
            f"/resume — снять стоп-лосс\n"
        )

    elif cmd == "/scan":
        if not config["trading_active"] and not config["simulation_mode"]:
            await send_tg(session,
                f"🔴 *СТОП-ЛОСС*\nПотеря >${config['stop_loss_usdt']}/день.\n"
                f"/resume — возобновить.")
            return
        await send_tg(session, "🔍 Сканирую KuCoin / HTX / Binance...")
        signals, active = await scan_all(session)
        if not signals:
            await send_tg(session,
                f"😔 Нет сигналов (порог {config['min_profit_pct']}%).\n"
                f"Бирж онлайн: {', '.join(active)}\n"
                f"Напиши /top для спредов без порога."
            )
        else:
            await send_tg(session, f"✅ {len(signals)} сигналов! Топ-3:")
            for opp in signals[:3]:
                await send_tg(session, format_signal(opp))
                if config["simulation_mode"]:
                    await execute_sim(opp)

            # Проверка балансов после сделок
            warns = check_balance_health()
            if warns:
                await send_tg(session,
                    "⚠️ *ВНИМАНИЕ — БАЛАНСЫ:*\n" + "\n".join(warns)
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
        msg += f"Бирж: {', '.join(active)}\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        if not all_opps:
            msg += "Нет данных или объём монет слишком мал"
        for i, o in enumerate(all_opps, 1):
            icon = "🟢" if o["net_pct"] >= saved else "🔴"
            vol_str = f"${o['volume_24h']}М"
            msg += (
                f"{icon} *{i}. {o['symbol']}* "
                f"{o['buy_ex']}→{o['sell_ex']}\n"
                f"   Спред: `{o['gross_pct']}%` | "
                f"Чистая: `{o['net_pct']}%` | "
                f"Объём: {vol_str}\n\n"
            )
        msg += f"_Порог: {saved}%_"
        await send_tg(session, msg)

    elif cmd == "/hours":
        msg = f"⏰ *СИГНАЛЫ ПО ЧАСАМ (UTC)*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        hour_data = []
        for h in range(24):
            sigs   = stats["hourly_signals"].get(h, 0)
            profit = stats["hourly_profit"].get(h, 0.0)
            if sigs > 0:
                hour_data.append((h, sigs, profit))

        if not hour_data:
            msg += "Нет данных — бот работает меньше часа."
        else:
            hour_data.sort(key=lambda x: x[1], reverse=True)
            for h, sigs, profit in hour_data:
                bar_len = min(10, sigs // 5 + 1)
                bar = "█" * bar_len
                msg += (
                    f"*{h:02d}:00* {bar}\n"
                    f"   Сигналов: {sigs} | "
                    f"Прибыль: {round(profit,2)} USDT\n\n"
                )
            best_h = max(hour_data, key=lambda x: x[1])
            msg += f"\n🏆 Лучший час: *{best_h[0]:02d}:00 UTC*"
        await send_tg(session, msg)

    elif cmd == "/report":
        report = generate_text_report()
        await send_tg(session, report)

    elif cmd == "/csv":
        await send_tg(session, "📊 Генерирую CSV отчёт...")
        csv_content = generate_daily_report()
        today = datetime.now().strftime("%Y-%m-%d")
        filename = f"arb_report_{today}.csv"
        await send_document(
            session,
            filename,
            csv_content,
            f"📊 Отчёт за {today} | {stats['trades_sim']} сделок"
        )

    elif cmd == "/rebalance":
        msg = "⚖️ *РЕБАЛАНСИРОВКА*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        actions = []
        for ex in ["KuCoin", "HTX", "Binance"]:
            current = sim_balances.get(ex, {})
            target  = TARGET_BALANCE.get(ex, {})
            ex_actions = []
            for asset, target_val in target.items():
                current_val = current.get(asset, 0)
                diff = target_val - current_val
                if diff > 10:
                    ex_actions.append(f"   ➕ Купить {asset}: +${round(diff)}")
                elif diff < -10:
                    ex_actions.append(f"   ➖ Продать {asset}: ${round(abs(diff))}")
            if ex_actions:
                actions.append(f"*{ex}:*\n" + "\n".join(ex_actions))

        if not actions:
            msg += "✅ Все балансы в норме! Ребалансировка не нужна."
        else:
            msg += "Рекомендуемые действия:\n\n"
            msg += "\n\n".join(actions)
            msg += (
                "\n\n💡 *Как ребалансировать:*\n"
                "1. Продай лишнее на бирже где избыток\n"
                "2. Купи нужное на бирже где дефицит\n"
                "Или переведи USDT между биржами (TRC-20, ~$1)"
            )
        await send_tg(session, msg)

    elif cmd == "/balances":
        msg = "💰 *ТЕКУЩИЕ БАЛАНСЫ (СИМУЛЯЦИЯ)*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        total = 0.0
        for ex, assets in sim_balances.items():
            msg += f"🏦 *{ex}:*\n"
            ex_total = 0.0
            for asset, val in assets.items():
                ex_total += val
                bar = "🟢" if val >= 50 else "🔴"
                msg += f"   {bar} {asset}: ${round(val, 2)}\n"
            msg += f"   Итого: ${round(ex_total, 2)}\n\n"
            total += ex_total
        msg += f"💵 *Общий баланс: ${round(total, 2)}*\n"
        msg += f"Стартовый: ${TOTAL_DEPOSIT}\n"
        pnl = total - TOTAL_DEPOSIT
        sign = "+" if pnl >= 0 else ""
        msg += f"P&L: {sign}${round(pnl, 2)}"

        warns = check_balance_health()
        if warns:
            msg += "\n\n⚠️ *ПРЕДУПРЕЖДЕНИЯ:*\n" + "\n".join(warns)
        await send_tg(session, msg)

    elif cmd == "/volumes":
        msg = "📊 *ОБЪЁМЫ МОНЕТ (24ч)*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        if not coin_volumes:
            msg += "Нет данных — сделай /scan сначала."
        else:
            min_vol = config["min_volume_usdt"]
            for sym, vol in sorted(coin_volumes.items(), key=lambda x: x[1], reverse=True):
                icon = "✅" if vol >= min_vol else "❌"
                msg += f"{icon} *{sym}*: ${round(vol/1e6, 2)}М\n"
            msg += f"\n_Мин. объём для торговли: ${min_vol/1e6:.1f}М_"
        await send_tg(session, msg)

    elif cmd == "/stats":
        uptime = datetime.now() - stats["start_time"]
        h = int(uptime.total_seconds() // 3600)
        m = int((uptime.total_seconds() % 3600) // 60)
        mode = "Симуляция 🔵" if config["simulation_mode"] else "Реальная 🔴"
        sl   = "🟢 Активна"  if config["trading_active"]  else "🔴 СТОП-ЛОСС"

        pair_lines = "\n".join([
            f"   {p}: {c}"
            for p, c in sorted(stats["pair_stats"].items(),
                                key=lambda x: x[1], reverse=True)
        ])
        sym_lines = "\n".join([
            f"   {s}: {c}"
            for s, c in sorted(stats["symbol_stats"].items(),
                                key=lambda x: x[1], reverse=True)
        ])
        per_trade = round(
            stats["profit_sim"] / stats["trades_sim"], 4
        ) if stats["trades_sim"] else 0

        await send_tg(session,
            f"📈 *СТАТИСТИКА*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Режим: {mode} | {sl}\n"
            f"Аптайм: {h}ч {m}м\n\n"
            f"🔍 Сканов: {stats['scans']}\n"
            f"🎯 Сигналов: {stats['signals']}\n"
            f"✅ Сделок: {stats['trades_sim']}\n"
            f"💰 Прибыль: {round(stats['profit_sim'], 2)} USDT\n"
            f"📊 Прибыль/сделка: ~{per_trade} USDT\n"
            f"❌ Ошибок: {stats['errors']}\n\n"
            f"📅 *Сегодня:*\n"
            f"   +{round(config['daily_profit'], 2)} USDT\n"
            f"   -{round(config['daily_loss'], 2)} USDT\n"
            f"   Стоп: ${config['stop_loss_usdt']}\n\n"
            f"🔀 *Пары:*\n{pair_lines}\n\n"
            f"💱 *Монеты:*\n{sym_lines}\n\n"
            f"⚙️ Лот: ${config['trade_usdt']} | "
            f"Порог: {config['min_profit_pct']}%\n"
            f"⚙️ {stats['trades_this_minute']}/"
            f"{config['max_trades_per_min']} сделок/мин\n\n"
            f"/hours — по часам | /report — отчёт | /csv — скачать"
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

    elif cmd == "/deposit":
        await send_tg(session,
            f"💼 *ПЛАН ДЕПОЗИТОВ НА 24Ч*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Лот: $100 | ~5 сделок/ч = 120/сут\n\n"
            f"🏦 *KuCoin — $1200:*\n"
            f"   $600 USDT + $600 монет\n"
            f"   (FET $120 | NEAR $120 | WIF $120\n"
            f"    BONK $120 | SEI $120)\n\n"
            f"🏦 *HTX — $1200:*\n"
            f"   $600 USDT + $600 монет\n"
            f"   (те же монеты по $120)\n\n"
            f"🏦 *Binance — $600:*\n"
            f"   $600 USDT (только покупка)\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💵 *ИТОГО: $3000*\n\n"
            f"🔄 Ребалансировка каждые 24-48 ч"
        )

    elif cmd == "/guide":
        await send_tg(session,
            f"📖 *ИНСТРУКЦИЯ*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"1. Регистрация KYC: KuCoin, HTX, Binance\n"
            f"2. Пополни по /deposit ($3000 итого)\n"
            f"3. Купи монеты согласно плану\n"
            f"4. API ключи: только Spot Trading\n"
            f"   ❌ Без права вывода!\n"
            f"5. /mode → реальный режим\n"
            f"6. Начни с лота $20: /setstop 5\n"
            f"7. Наблюдай 48 часов\n"
            f"8. /stats каждые 3 часа\n"
            f"9. /rebalance раз в сутки\n"
            f"10. /csv для дневного отчёта\n\n"
            f"❌ Никогда: право вывода в API\n"
            f"❌ Никогда: отключать стоп-лосс\n"
            f"✅ Всегда: резерв $500 вне бирж"
        )

    elif cmd == "/mode":
        config["simulation_mode"] = not config["simulation_mode"]
        mode = "🔵 СИМУЛЯЦИЯ" if config["simulation_mode"] else "🔴 РЕАЛЬНАЯ"
        warn = "\n\n⚠️ Нужны API ключи!\nСмотри /guide" if not config["simulation_mode"] else ""
        await send_tg(session, f"Режим: {mode}{warn}")

    elif cmd == "/resume":
        config["trading_active"] = True
        config["daily_loss"] = 0.0
        await send_tg(session, "✅ Торговля возобновлена.")

    elif cmd == "/setprofit":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/setprofit 0.3`")
            return
        try:
            config["min_profit_pct"] = float(parts[1])
            await send_tg(session, f"✅ Мин. прибыль: `{config['min_profit_pct']}%`")
        except:
            await send_tg(session, "❌ Пример: `/setprofit 0.3`")

    elif cmd == "/setstop":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/setstop 20`")
            return
        try:
            config["stop_loss_usdt"] = float(parts[1])
            await send_tg(session, f"✅ Стоп-лосс: `${config['stop_loss_usdt']}/день`")
        except:
            await send_tg(session, "❌ Пример: `/setstop 20`")

    elif cmd == "/setminvol":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/setminvol 500000`")
            return
        try:
            config["min_volume_usdt"] = float(parts[1])
            await send_tg(session,
                f"✅ Мин. объём: `${config['min_volume_usdt']/1e6:.1f}М`")
        except:
            await send_tg(session, "❌ Пример: `/setminvol 500000`")

    else:
        await send_tg(session,
            "/start /scan /top /stats /hours\n"
            "/report /csv /rebalance /balances\n"
            "/volumes /history /deposit /guide\n"
            "/mode /resume\n"
            "/setprofit 0.3 /setstop 20 /setminvol 500000"
        )


# ═══════════════════════════════════════
# ЦИКЛЫ
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
    last_report_hour = -1

    while True:
        try:
            reset_daily()
            if not can_trade() and not config["simulation_mode"]:
                await asyncio.sleep(config["scan_interval"])
                continue

            signals, active = await scan_all(session)
            logger.info(
                f"Scan #{stats['scans']}: {len(active)} бирж | "
                f"{len(signals)} сигналов | "
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
                    await execute_sim(opp)

                    if not config["trading_active"] and CHAT_ID:
                        await send_tg(session,
                            f"🔴 *СТОП-ЛОСС*\n"
                            f"Потеря >${config['stop_loss_usdt']}/день.\n"
                            f"/resume — возобновить."
                        )
                        break

            # Проверка балансов каждые 30 минут
            if stats["scans"] % 300 == 0:
                warns = check_balance_health()
                if warns and CHAT_ID:
                    await send_tg(session,
                        "⚠️ *БАЛАНСЫ ТРЕБУЮТ ВНИМАНИЯ:*\n"
                        + "\n".join(warns)
                        + "\n\nНапиши /rebalance для рекомендаций."
                    )

            # Автоотчёт в 23:55
            now_dt = datetime.now()
            if now_dt.hour == 23 and now_dt.minute == 55:
                if last_report_hour != now_dt.date():
                    last_report_hour = now_dt.date()
                    if CHAT_ID:
                        report = generate_text_report()
                        await send_tg(session, report)
                        csv_content = generate_daily_report()
                        today = now_dt.strftime("%Y-%m-%d")
                        await send_document(
                            session,
                            f"arb_report_{today}.csv",
                            csv_content,
                            "📊 Автоматический ежедневный отчёт"
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
        f"TriangleArbBot | {len(SYMBOLS)} монет | "
        f"{len(PAIRS)} пар | Депозит ${TOTAL_DEPOSIT}"
    )
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        await asyncio.gather(polling_loop(session), scan_loop(session))


if __name__ == "__main__":
    asyncio.run(main())
