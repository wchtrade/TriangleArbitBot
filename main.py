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
    "empirical_spread_crossing_pct": 3.34,  # ОБНОВЛЕНО 17.08: старое значение
        # 0.34% было измерено 10.08 на КОНКРЕТНОЙ ситуации (Binance) и давно
        # устарело. Вечером 16.08 два свежих факт-замера на СПОКОЙНОМ рынке
        # RVN дали средний разрыв 1.84% сверх уже применённых 1.5% — то есть
        # реальная стоимость ближе к 3.34%. Теперь дополнительно есть
        # АВТОКАЛИБРОВКА (см. execute_trade) — это значение будет само
        # подстраиваться по ходу дела, это лишь безопасная стартовая точка.
    "threshold_safety_margin_pct": 0.05,  # запас сверху динамического порога
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
    "max_topup_spend_per_day": 100.0,  # ПОВЫШЕНО 10.08 (раунд 2, было 40.0):
        # докупка теперь восстанавливает СРАЗУ весь целевой резерв (не по
        # чуть-чуть под каждую сделку) — отдельные докупки стали крупнее
        # (~$19 за раз вместо ~$5-6), хоть и реже. Старый лимит $40 мог бы
        # заблокировать буквально 2 полных докупки за день.
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
    "periodic_rebalance_hours": 4.0,  # НОВОЕ 11.08: раз в сколько часов запускать
        # ФОНОВЫЙ, автоматический полный ребаланс — НЕ после каждой сделки (это
        # убрали 10.08, слишком часто пересекало спред биржи), а по таймеру,
        # независимо от торговых событий. Цель — периодически "подстригать"
        # резерв обратно к целевому значению: если цена выросла, излишек
        # продаётся в USDT, фиксируя часть роста; если упала — резерв
        # пополняется как обычно. Настраивается через /setperiodicrebalance,
        # 0 — выключить полностью.
    "profit_lock_interval_sec": 300,  # НОВОЕ 12.08: как часто (в секундах)
        # проверять курсовой плюс для немедленной фиксации в USDT — по
        # умолчанию раз в 5 минут. Отдельно и НЕЗАВИСИМО от
        # periodic_rebalance_hours (тот реже, но полный, эта чаще, но
        # только фиксирует плюс, никогда не докупает при минусе).
    "max_drawdown_pct": 5.0,  # НОВОЕ 14.08: предохранитель от чрезмерного
        # курсового минуса — по прямому запросу пользователя. Если общий
        # P&L (включая переоценку резерва) упадёт ниже этого % от
        # стартового капитала — бот САМ ставится на паузу и присылает
        # предупреждение, вместо того чтобы молча продолжать копить
        # непонятно насколько глубокий минус. Не устраняет сам курсовой
        # шум (это невозможно без потери чего-то другого — уже обсуждали),
        # но ограничивает МАКСИМАЛЬНУЮ глубину, прежде чем человек об этом
        # узнает и сможет решить, что делать. 0 — выключить полностью.
        # Настраивается через /setmaxdrawdown.
    "max_volatility_pct_15min": 3.0,  # НОВОЕ 16.08: по прямому запросу
        # пользователя после дня с диким ралли RVN (+9%/час) и ДВУМЯ
        # подряд фактически убыточными сделками, несмотря на красивый
        # спред на бумаге — оказалось, что при сильной волатильности
        # реальное исполнение расходится с расчётным спредом (цена уходит,
        # пока обе ноги сделки исполняются). Если цена монеты движется
        # больше этого % за последние 15 минут — бот САМ ставится на
        # паузу до тех пор, пока волатильность не спадёт обратно. Не
        # возобновляет торговлю автоматически (только предупреждает и
        # ставит на паузу/снимает предупреждение) — решение возобновить
        # остаётся за человеком. 0 — выключить. Настраивается через
        # /setmaxvolatility.
    "real_trades_today":    0,
    "real_start_capital":   None,  # фиксируется командой /setrealstart, для честного P&L в реальном режиме
    "max_real_trades_per_day": 200,  # поднято с 20 - для круглосуточной работы; /setmaxtrades меняет

    # ===== НОВОЕ 17.08 (по итогам анализа логов реальных сделок ONE) =====
    # Прямое измерение по логам показало: между обнаружением сигнала и
    # стартом реального исполнения проходило 5-9 секунд — почти всё это
    # время уходило на СИНХРОННУЮ докупку резерва ВНУТРИ execute_real_
    # arbitrage (на критическом пути самой сделки). На волатильной монете
    # за эти секунды цена успевает уйти, съедая спред, который был на
    # сигнале. reserve_watchdog_loop (см. ниже) докупает резерв ЗАРАНЕЕ,
    # в фоне, чтобы preflight-проверка в момент сделки в большинстве
    # случаев проходила мгновенно, без докупки на критическом пути.
    "reserve_watchdog_interval_sec": 90,   # как часто проверять резерв заранее
    "reserve_watchdog_trigger_frac": 0.6,  # докупать, если резерв ниже этой доли от цели

    # ===== НОВОЕ 18.08 (по итогам разбора логов реальных сделок ONE —
    # прямой запрос пользователя: "исправить чтобы работа давала + а не -") =====
    # Реактивная докупка резерва ВНУТРИ execute_real_arbitrage (на
    # критическом пути сделки) — главный подтверждённый источник мелких,
    # но регулярных потерь: 17.08-18.08 почти каждая реальная сделка
    # тянула за собой докупку, пересекающую собственный bid/ask спред
    # биржи, и по факту сделка почти всегда уходила в минус даже при
    # спреде на входе заметно выше честного порога. При True (по
    # умолчанию) — бот НЕ докупает резерв в момент сделки, а просто
    # ПРОПУСКАЕТ такую попытку (не тратя деньги на дорогую докупку в
    # моменте), полагаясь на reserve_watchdog_loop, который пополняет
    # резерв ЗАРАНЕЕ, в фоне, вне критического пути сделки. Из-за этого
    # сделок будет исполняться МЕНЬШЕ (некоторые сигналы будут пропущены,
    # если резерв в моменте недостаточен), но каждая исполненная сделка
    # не платит цену докупки — только комиссию и walk-the-book спред.
    "skip_reactive_topup": True,

    # НОВОЕ 17.08: порог волатильности ЗА ПОСЛЕДНЮЮ МИНУТУ, проверяется
    # прямо перед КАЖДОЙ попыткой реальной сделки (см. execute_trade) —
    # быстрее и точнее, чем фоновый предохранитель max_volatility_pct_15min
    # (тот проверяет раз в 2 минуты и реагирует постфактум). 0 — выключить.
    "pre_trade_max_volatility_pct_1min": 0.5,

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

# НОВОЕ 11.08: отдельный, НЕЗАВИСИМЫЙ список монет для треугольного
# арбитража (/triangle) — не пересекается с SYMBOLS (реальная торговля).
# Раньше /triangle проверял только то, что уже настроено для основной
# торговли — а это чистая идея-разведка, не должна случайно втягивать
# новую монету в реальные сделки через побочную дверь. Настраивается
# через /addtriangle и /removetriangle.
TRIANGLE_SYMBOLS: List[str] = []

QUOTE   = "USDT"
BRIDGE  = "BTC"   # мост для треугольного арбитража: USDT -> COIN -> BTC -> USDT
PAIRS   = [
    # ОТКЛЮЧЕНО 10.08 по решению: KuCoin→Binance убран из активной торговли.
    # Причина — по данным TrialArbBot этот маршрут в среднем даёт маржу
    # ~0.05%, почти всегда НИЖЕ честного порога (0.34%) — сделки там крайне
    # редки. При капитале ~$24 держать резерв на трёх биржах одновременно
    # означает распылять и без того скромный капитал на менее прибыльное
    # направление. Ключи/подключение Binance НЕ трогаем — при желании
    # вернуть просто раскомментируйте строку ниже.
    # ("KuCoin", "Binance"),
    #
    # НОВОЕ 10.08: KuCoin→MEXC — по данным TrialArbBot именно эта связка
    # стабильно показывает наибольшую и наиболее регулярную маржу (лучшая
    # маржа 3.35%, 711 сделок, $296 P&L — на порядок активнее и прибыльнее
    # KuCoin→Binance). Весь капитал сейчас сосредоточен именно здесь.
    ("KuCoin", "MEXC"),
    #
    # ИСПРАВЛЕНО 07.08 (было "Binance","KuCoin" — НАПРАВЛЕНИЕ БЫЛО ПЕРЕПУТАНО):
    # несколько дней подряд WorkerArbBot молчал (0 сигналов при ВСЕХ счётчиках
    # фильтров тоже на нуле — не тонкий стакан, не объём, не подозрительный
    # спред, а именно отсутствие положительного спреда в проверяемом
    # направлении). Тем временем монитор (TrialArbBot) на тех же IOST/YFI
    # стабильно подтверждал реальные сделки — но ВСЕГДА на маршруте
    # KuCoin→Binance (покупка на KuCoin, продажа на Binance), а не наоборот.
    # Бот честно считал правильно — просто проверял противоположное от
    # реально работающего направление. Актуально, если решите вернуть Binance.
    #
    # ИЗМЕНЕНО 05.08 по решению: HTX убрана из торговли полностью. За всю
    # сессию именно HTX была источником почти всех проблем — тонкие стаканы
    # на альткоинах (ZIL, ZK, RVN), цены, оторванные от реального рынка на
    # 15-45%, постоянная нехватка баланса из-за двойной роли на скромном
    # капитале. HTX остаётся подключена (ключи/баланс не трогаем), но
    # сделок через неё нет — при желании вернуть: добавить обратно строки
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


FEES = {"Binance": 0.10, "KuCoin": 0.10, "HTX": 0.20, "MEXC": 0.10}
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
    "depth_fail":   {"Binance": 0, "KuCoin": 0, "HTX": 0, "MEXC": 0},  # счётчик отказов стакана
    "insufficient_liquidity": 0,  # сколько раз стакана не хватило на объём
    "volume_fetch_fail": 0,  # НОВОЕ 04.08: сколько раз не удалось получить 24h-объём с Binance
                              # (раньше это было полностью невидимо и тихо блокировало ВСЕ сигналы)
    "insufficient_balance_skips": 0,  # сколько раз симуляция честно отказала из-за нехватки виртуального баланса
    "hourly_signals": defaultdict(int),
    "hourly_profit":  defaultdict(float),
    "topup_attempts": 0,   # НОВОЕ: сколько раз сработала точечная автодокупка
    "topup_success":  0,
    # ===== НОВОЕ (патч по итогам аудита 17.08): видимость неудачных ног сделки =====
    # Раньше stats["trades"] рос ТОЛЬКО при success=True — падения второй ноги
    # (продажа) и аварийные закрытия были полностью невидимы в /stats, из-за
    # чего "0 сделок исполнено" маскировало реальную, убыточную активность на
    # бирже (подтверждено выпиской KuCoin и скриншотами истории ордеров 17.08).
    "buy_leg_failures": 0,             # первая нога (покупка) не прошла
    "sell_leg_failures": 0,            # вторая нога (продажа) не прошла
    "emergency_closes_attempted": 0,   # сколько раз пришлось аварийно закрывать позицию
    "emergency_closes_succeeded": 0,   # из них — сколько реально закрылось
}
trade_history: List[dict] = []

# ═══════════════════════════════════════════════════════════════
# НОВОЕ 18.08 (по прямому запросу пользователя "сделать так, чтобы не
# было минуса"): по факту сегодняшних 8+ реальных сделок на ONE выяснили
# — теоретическая прибыль по формуле (net_pct × объём) почти всегда
# заметно превышает то, что реально получилось (реальный баланс до/
# после). Разрыв стабильно составлял ~$0.09-0.12 на лоте ~$3.5-4, то
# есть ~2.3-3.4% от объёма сделки — почти столько же, сколько сам спред
# на входе. Это НЕ стоимость докупки резерва (уже учтена отдельно) — это
# реальное расхождение между зафиксированной на сигнале ценой и тем, что
# фактически произошло к моменту полного исполнения обеих ног.
#
# Гарантировать отсутствие убытка невозможно в принципе (рыночный риск
# есть всегда) — но можно требовать спред, который заметно перекрывает
# ИЗМЕРЕННУЮ на практике эрозию, а не просто "честный" порог по комиссиям
# и ребалансу. execution_erosion_history копит последние измерения разрыва
# (net_pct на сигнале минус фактически реализованный %), и bar для входа
# автоматически поднимается, если реальная эрозия продолжает расти.
# ═══════════════════════════════════════════════════════════════
execution_erosion_history: List[float] = []
EXECUTION_EROSION_HISTORY_MAXLEN = 8

config["min_execution_erosion_estimate_pct"] = 2.7  # стартовая оценка — среднее
    # из сегодняшних измерений ($0.09-0.12 на лоте $3.5-4 ≈ 2.3-3.4%,
    # взято среднее). Используется, пока не накопится хотя бы 3 реальных
    # измерения — дальше расчёт идёт по факту, эта константа больше не нужна.

# НОВОЕ 23.08 (по прямому запросу пользователя, после разбора: эрозия НЕ
# пропорциональна размеру спреда — сделка с бОльшим ожидаемым спредом дала
# бОльшую эрозию в долларах). Раньше ВСЯ защита была в процентах, что
# вело к порогу 6.9%+ и почти полной остановке торговли, не решая саму
# проблему. Теперь защита разделена: половина — по-прежнему в процентах
# (erosion_pct_weight), половина — АБСОЛЮТНЫЙ минимум прибыли в долларах,
# рассчитанный по 3 реальным сделкам 22.08 (эрозия $0.07-0.26, средняя
# ~$0.15) с запасом. Сделка должна обещать прибыль БОЛЬШЕ этой суммы,
# независимо от того, насколько маленький или большой лот/спред.
config["min_absolute_profit_usd"] = 0.15
config["erosion_pct_weight"] = 0.5  # доля эрозийного буфера, остающаяся в % пороге (0.0-1.0)

# НОВОЕ 18.08: использовать ли KuCoin HF (High-Frequency) аккаунт вместо
# обычного trade — ПО УМОЛЧАНИЮ ВЫКЛЮЧЕНО. Реальное поведение бота не
# меняется, пока пользователь явно не проверит /testhfbalance (баланс
# читается верно) и не включит /setusehf on. Без этого — весь новый HF-код
# существует в файле, но НИКОГДА не вызывается в реальной торговле.
config["use_kucoin_hf"] = False

# НОВОЕ 18.08 (по прямому запросу — "дай 5 вещей, которые дадут +", №1):
# лимитные IOC-ордера вместо рыночных — строго безопаснее (худший исход:
# сделка не состоится, а не состоится ПЛОХО). ПО УМОЛЧАНИЮ ВКЛЮЧЕНО.
config["use_limit_ioc_orders"] = True


def record_execution_erosion(net_pct_at_signal: float, factual_delta: float, vol: float) -> None:
    """Вызывать после КАЖДОЙ реальной сделки с известным factual_delta.
    Копит разрыв между обещанным на сигнале % и тем, что реально
    получилось — та самая эрозия исполнения, найденная 18.08."""
    if vol <= 0:
        return
    factual_realized_pct = factual_delta / vol * 100
    gap_pct = net_pct_at_signal - factual_realized_pct
    execution_erosion_history.append(gap_pct)
    if len(execution_erosion_history) > EXECUTION_EROSION_HISTORY_MAXLEN:
        execution_erosion_history.pop(0)


def get_avg_execution_erosion_pct() -> float:
    """Скользящее среднее реальной эрозии исполнения. Пока данных мало
    (<3 сделки) — используем консервативную стартовую оценку 2.7%,
    откалиброванную по факту 18.08. Дальше — честное среднее по факту."""
    if len(execution_erosion_history) < 3:
        return config.get("min_execution_erosion_estimate_pct", 2.7)
    return round(sum(execution_erosion_history) / len(execution_erosion_history), 4)


# НОВОЕ 11.08: история P&L во времени — для скользящего среднего за час в
# /stats. Каждая проверка P&L (не чаще раз в минуту) добавляет точку
# (время, значение); старые точки (>1 часа) вычищаются, чтобы список не
# рос бесконечно. Цель — не устранить рыночный шум резерва (это его
# реальное свойство), а перестать показывать РЕЗКИЕ скачки одного снятого
# момента, вместо этого честно показывать усреднённый тренд за последний час.
pnl_history: List[Tuple[float, float]] = []  # (timestamp, pnl_real)
# НОВОЕ 15.08: история РЫНОЧНОЙ ЦЕНЫ монеты (не P&L) — по прямому запросу
# пользователя после обсуждения, что честное "прогнозирование" цены
# невозможно, но ОПИСАТЕЛЬНЫЙ тренд за последние часы — можно. Это НЕ
# предсказание будущего, а честный взгляд назад: росла или падала цена
# за последние 24 часа. Хранит точки за последние 24 часа, вычищает старые.
price_history: List[Tuple[float, float]] = []  # (timestamp, mid_price)


def record_price_and_get_trend() -> Optional[dict]:
    """Записывает текущую цену IOST (среднюю между best bid/ask на бирже
    продажи), вычищает точки старше 24 часов, возвращает честное описание
    тренда: было — сейчас — % изменения. НЕ предсказывает, куда пойдёт
    цена дальше — только показывает, что происходило до сих пор."""
    if not price_history:
        return None
    now_ts = time.time()
    cutoff = now_ts - 86400
    while price_history and price_history[0][0] < cutoff:
        price_history.pop(0)
    if len(price_history) < 2:
        return None
    oldest_price = price_history[0][1]
    newest_price = price_history[-1][1]
    change_pct = (newest_price - oldest_price) / oldest_price * 100
    hours_span = (price_history[-1][0] - price_history[0][0]) / 3600
    return {
        "oldest": oldest_price, "newest": newest_price,
        "change_pct": round(change_pct, 3), "hours_span": round(hours_span, 1),
    }

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
MEXC_KEY       = os.environ.get("MEXC_API_KEY", "")
MEXC_SECRET    = os.environ.get("MEXC_API_SECRET", "")
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

exchange_backoff_until: Dict[str, float] = {"Binance": 0.0, "KuCoin": 0.0, "HTX": 0.0, "MEXC": 0.0}

# ИСПРАВЛЕНИЕ 04.08 (раунд 2): раньше при отказе размещения реального ордера
# биржа возвращала подробный текст ошибки (например, точную причину отказа —
# insufficient balance / precision / min notional и т.п.), но бот его нигде
# не показывал пользователю — ни в Telegram, ни даже в сообщении об ошибке
# внутри execute_real_arbitrage. Наружу уходила только маска вида
# "buy_leg_failed_on_HTX" без единой цифры или слова причины, из-за чего
# невозможно было понять, что именно не так — баланс, шаг лота, минимальная
# сумма ордера или что-то ещё. Теперь текст ответа биржи сохраняется здесь
# и подставляется в сообщение об ошибке.
_last_exchange_error: Dict[str, str] = {"Binance": "", "KuCoin": "", "HTX": "", "MEXC": ""}
# НОВОЕ 09.08: цена последней реальной продажи по (биржа, монета) — нужна,
# чтобы честно посчитать реальную стоимость сдвига курса при последующей
# докупке резерва (не просто оценку по комиссии, как было раньше).
_last_real_sell_price: Dict[Tuple[str, str], float] = {}

# НОВОЕ 25.08 (по прямому запросу пользователя — "почему оценка не
# пересчитывается под конкретную докупку"): зеркальный трекер для
# top_up_usdt_via_coin_sale — раньше эта функция вообще НЕ считала свою
# реальную стоимость (в отличие от top_up_coin_reserve). Цена последней
# реальной ПОКУПКИ по (биржа, монета) — если реактивная продажа излишка
# монеты идёт по цене ХУЖЕ, чем мы только что за неё заплатили, это
# реальный, измеримый убыток сверх комиссии.
_last_real_buy_price: Dict[Tuple[str, str], float] = {}

# НОВОЕ 25.08 (по прямому запросу пользователя, найдено по логам Railway
# с точными временными метками): reserve_watchdog_loop — ФОНОВЫЙ цикл,
# который может сработать В ЛЮБОЙ момент, независимо от того, идёт ли
# прямо сейчас замер факта конкретной сделки (capital_before → sleep(1) →
# capital_after). Если watchdog купит/продаст монету ИМЕННО в эти
# секунды — эта реальная, но НЕ относящаяся к сделке операция попадает в
# "факт" этой сделки, искажая его (подтверждено логами: watchdog сделал
# докупку+продажу на $10 суммарно за 1 секунду, между двумя обычными
# сканами). Lock гарантирует: пока идёт замер факта сделки, watchdog
# ждёт своей очереди, а не работает параллельно.
_capital_measurement_lock = asyncio.Lock()


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


async def get_orderbook_mexc_rest(session, symbol: str) -> Optional[Dict]:
    """НОВОЕ 10.08: MEXC — REST-стакан, API идентичен по формату Binance
    (тот же /api/v3/depth). Отдельного WebSocket-подключения для MEXC пока
    нет (в отличие от Binance/KuCoin/HTX) — всегда работаем через REST.
    Для нашего интервала сканирования (3 сек) и лота ($5) задержка REST
    против WS несущественна."""
    if is_backed_off("MEXC"):
        return None
    url = "https://api.mexc.com/api/v3/depth"
    params = {"symbol": f"{symbol}{QUOTE}", "limit": config["depth_limit"]}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=6)) as r:
            if r.status in (429, 418):
                trigger_backoff("MEXC", r.status, r.headers.get("Retry-After"))
                return None
            if r.status != 200:
                stats["depth_fail"]["MEXC"] = stats["depth_fail"].get("MEXC", 0) + 1
                return None
            data = await r.json()
            bids = [(float(p), float(q)) for p, q in data.get("bids", [])]
            asks = [(float(p), float(q)) for p, q in data.get("asks", [])]
            if not bids or not asks:
                return None
            return {"bids": bids, "asks": asks}
    except Exception as e:
        stats["depth_fail"]["MEXC"] = stats["depth_fail"].get("MEXC", 0) + 1
        logger.error(f"MEXC depth {symbol}: {e}")
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
    for sym in TRIANGLE_SYMBOLS:
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

def compute_dynamic_min_profit_pct(buy_ex: str, sell_ex: str) -> float:
    """НОВОЕ 10.08: раньше min_profit_pct был фиксированным числом (0.18-0.3%),
    учитывающим только комиссии buy+sell ОДНОЙ ноги арбитража — но не
    стоимость последующего ребаланса резерва. Математически доказано
    (см. диалог 10.08): при лоте $5.5 честный порог безубыточности с учётом
    полного цикла — около 0.32%, а не 0.18%, из-за неизбежной периодической
    докупки резерва, пересекающей собственный bid/ask спред биржи.
    Порог теперь считается динамически из реальных комиссий и эмпирически
    измеренной стоимости пересечения спреда, амортизированной на размер
    резервного буфера (sell_reserve_lots) — чем больше буфер, тем реже
    нужен ребаланс, тем ниже честный порог на одну сделку.

    ИСПРАВЛЕНО 23.08 (по прямому запросу пользователя — "порог задрали до
    того, что не торгуется, а сделки всё равно в минус"): раньше буфер
    эрозии исполнения ПОЛНОСТЬЮ добавлялся сюда, в ПРОЦЕНТНЫЙ порог — и
    рос без остановки при каждой неудачной сделке (дошло до 6.9%). Но
    разбор 3 реальных сделок показал: эрозия НЕ пропорциональна размеру
    спреда (сделка с БОЛЬШИМ ожидаемым спредом дала БОЛЬШУЮ эрозию в
    долларах, не меньшую) — то есть повышение процентного порога решало
    не ту проблему, просто душило торговлю, не делая отдельные сделки
    надёжнее. Теперь здесь используется только ПОЛОВИНА эрозийного
    буфера (продолжает работать как мягкая защита), а вторая половина
    защиты переехала в АБСОЛЮТНЫЙ минимум прибыли в долларах — см.
    config['min_absolute_profit_usd'] и проверку в scan_all/execute."""
    buy_fee = FEES.get(buy_ex, 0.1)
    sell_fee = FEES.get(sell_ex, 0.1)
    spread_crossing_pct = config.get("empirical_spread_crossing_pct", 0.34)
    lots = max(config.get("sell_reserve_lots", 3), 1)
    amortized_rebalance = spread_crossing_pct / lots
    safety_margin = config.get("threshold_safety_margin_pct", 0.05)
    erosion_buffer = get_avg_execution_erosion_pct() * config.get("erosion_pct_weight", 0.5)
    return round(buy_fee + sell_fee + amortized_rebalance + safety_margin + erosion_buffer, 4)


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

    dynamic_min = compute_dynamic_min_profit_pct(buy_ex, sell_ex)
    if net < max(config["min_profit_pct"], dynamic_min):
        return None

    # ИСПРАВЛЕНО 24.08 (по прямому запросу пользователя, критическая находка):
    # раньше здесь проверялся "наивный" ожидаемый профит (net% × объём), не
    # учитывающий стоимость ребаланса вообще.
    #
    # ИСПРАВЛЕНО 25.08 (найден собственный баг вчерашней правки): использовал
    # ПОЛНУЮ (неамортизированную) стоимость crossing_pct здесь, а dynamic_min
    # выше уже использует АМОРТИЗИРОВАННУЮ (÷sell_reserve_lots). Это двойной
    # счёт — по факту требуемый спред получался ~7%+ при лоте $4, хотя /stats
    # показывал обманчивые "2.71% честный". Найдено по логам 25.08: реальный
    # устойчивый спред 3.66-3.86% (дважды подряд, не разовый выброс) не
    # генерировал НИ ОДНОГО сигнала за 5700+ сканов. Теперь обе проверки
    # используют ОДНУ И ТУ ЖЕ (амортизированную) модель стоимости — согласованно,
    # без задваивания.
    naive_profit_usd = trade_usdt * net / 100
    lots_for_cost = max(config.get("sell_reserve_lots", 3), 1)
    amortized_rebalance_cost_est = (trade_usdt * (buy_fee + sell_fee)
                                     + trade_usdt * config.get("empirical_spread_crossing_pct", 0.34)
                                     / 100 / lots_for_cost)
    honest_pretrade_profit_usd = round(naive_profit_usd - amortized_rebalance_cost_est, 4)

    min_abs = config.get("min_absolute_profit_usd", 0.15)
    if honest_pretrade_profit_usd < min_abs:
        stats["absolute_profit_too_low_rejected"] = stats.get("absolute_profit_too_low_rejected", 0) + 1
        return None

    # НОВОЕ 25.08 (по прямому запросу пользователя — "как сделать +, а не
    # -", на основе сегодняшних данных): проверка выше использует
    # АМОРТИЗИРОВАННУЮ (÷sell_reserve_lots) стоимость ребаланса — мягкую,
    # пропускающую сделки, которые сама же карточка чуть позже честно
    # покажет как убыточные (полная, неамортизированная формула).
    # Сегодня 25.08 подтверждено на 10 реальных сделках: КОГДА честная
    # оценка (полная формула) БЛИЗКА К НУЛЮ ИЛИ ОТРИЦАТЕЛЬНА — факт
    # почти всегда совпадает с этим прогнозом (разрыв всего $0.001-0.013,
    # против $0.05-0.26 раньше). Модель ТЕПЕРЬ ДОКАЗАННО ТОЧНА — значит,
    # можно и нужно гейтить именно по ней, не только по мягкой
    # амортизированной версии. Требуем строго ПОЛОЖИТЕЛЬНЫЙ прогноз по
    # полной формуле, с тем же абсолютным минимумом — а не просто "не
    # катастрофически отрицательный".
    full_crossing_cost_est = trade_usdt * config.get("empirical_spread_crossing_pct", 0.34) / 100
    full_rebalance_cost_est = trade_usdt * (buy_fee + sell_fee) + full_crossing_cost_est
    strict_honest_profit_usd = round(naive_profit_usd - full_rebalance_cost_est, 4)
    if strict_honest_profit_usd < 0:
        stats["strict_honest_negative_rejected"] = stats.get("strict_honest_negative_rejected", 0) + 1
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


async def fetch_all_orderbooks(session) -> Tuple[Dict, Dict, Dict, Dict, List[str]]:
    tasks = {}
    for ex, fn in [("Binance", get_orderbook_binance),
                    ("KuCoin", get_orderbook_kucoin),
                    ("HTX", get_orderbook_htx),
                    ("MEXC", get_orderbook_mexc_rest)]:
        for sym in SYMBOLS:
            tasks[(ex, sym)] = fn(session, sym)

    keys = list(tasks.keys())
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)

    books = {"Binance": {}, "KuCoin": {}, "HTX": {}, "MEXC": {}}
    for (ex, sym), res in zip(keys, results):
        if isinstance(res, Exception) or res is None:
            continue
        books[ex][sym] = res

    volumes = await get_24h_volume(session)
    for sym in SYMBOLS:
        if sym in volumes:
            coin_volumes[sym] = volumes[sym]

    active = [ex for ex, d in books.items() if d]
    return books["Binance"], books["KuCoin"], books["HTX"], books["MEXC"], active


async def scan_all(session) -> Tuple[List[dict], List[str]]:
    stats["scans"] += 1
    bn, kc, hx, mx, active = await fetch_all_orderbooks(session)
    ex_map = {"Binance": bn, "KuCoin": kc, "HTX": hx, "MEXC": mx}
    signals = []

    # ИСПРАВЛЕНИЕ 08.08 (найдено после 6 сделок подряд с растущим реальным
    # минусом, хотя каждая карточка обещала честную прибыль в плюс): здесь
    # ВСЕГДА использовался config["trade_usdt"] — старая, фиксированная на
    # $20 симуляционная переменная, — даже в реальном режиме, где реальный
    # размер ордера настраивается отдельно через /setreallot и может быть
    # МЕНЬШЕ (сейчас $7). Карточка сделки считала прибыль так, будто
    # торгуется $20, хотя реально исполнялось только $7 — прибыль на
    # карточке была примерно втрое завышена относительно того, что
    # происходило на бирже на самом деле.
    scan_lot = (config["max_real_order_usdt"] if not config["simulation_mode"]
                else config["trade_usdt"])

    hour = datetime.now().hour
    for sym in SYMBOLS:
        for buy_ex, sell_ex in pairs_for_symbol(sym):
            bob = ex_map.get(buy_ex, {}).get(sym)
            sob = ex_map.get(sell_ex, {}).get(sym)
            if not bob or not sob:
                continue
            # НОВОЕ 17.08: пишем цену на КАЖДОМ скане (не только когда
            # пользователь вызывает /stats или раз в 120 сек в volatility_
            # guard_loop) — нужна ПЛОТНАЯ история, чтобы честно проверять
            # мгновенную волатильность за последнюю минуту ПЕРЕД каждой
            # конкретной попыткой сделки (см. execute_trade), а не только
            # за 15 минут фоновым предохранителем, который реагирует
            # только постфактум.
            if sob.get("bids"):
                price_history.append((time.time(), sob["bids"][0][0]))
            opp = calc_arb_real(sym, buy_ex, bob, sell_ex, sob, scan_lot)
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


_mexc_lot_step_cache: Dict[str, float] = {}


async def get_mexc_lot_step(session, symbol: str) -> float:
    """НОВОЕ 10.08: формат exchangeInfo у MEXC идентичен Binance."""
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
        logger.error(f"MEXC lot step fetch {symbol}: {e}")
    return 1.0


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
    elif ex == "MEXC":
        step = await get_mexc_lot_step(session, symbol)
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


async def wait_for_mexc_fill(session, symbol: str, order_id, timeout: float = 3.0) -> Optional[float]:
    """НОВОЕ 10.08: идентично wait_for_binance_fill — тот же формат ответа."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        ts = int(time.time() * 1000)
        params = {"symbol": f"{symbol}{QUOTE}", "orderId": order_id, "timestamp": ts, "recvWindow": 5000}
        params["signature"] = sign_binance(params, MEXC_SECRET)
        headers = {"X-MEXC-APIKEY": MEXC_KEY, "Content-Type": "application/json"}
        try:
            async with session.get("https://api.mexc.com/api/v3/order", params=params,
                                    headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as r:
                data = await r.json()
                if data.get("status") == "FILLED":
                    return float(data.get("executedQty", 0))
                if data.get("status") in ("CANCELED", "REJECTED", "EXPIRED"):
                    return None
        except Exception as e:
            logger.error(f"MEXC fill check {symbol}: {e}")
        await asyncio.sleep(0.3)
    return None


async def wait_for_kucoin_fill(session, order_id: str, timeout: float = 6.0) -> Optional[float]:
    # ИЗМЕНЕНО 22.08 (по факту логов реальных сделок): было 3.0 сек — этого
    # оказалось недостаточно для надёжного подтверждения статуса лимитных
    # IOC-ордеров (введённых 18.08), из-за чего несколько раз подряд
    # confirm_fill_and_get_qty сдавался раньше, чем KuCoin API успевал
    # вернуть финальный статус ('buy_leg_not_confirmed_filled_on_KuCoin' в
    # логах 22.08, хотя сам ордер, скорее всего, уже давно закрылся —
    # просто не успели это увидеть за 10 попыток по 0.3 сек). Деньги в
    # этих случаях не терялись (вторая нога просто не открывалась), но
    # сделки терялись зря. Увеличено до 6 сек — тот же шаг 0.3 сек, просто
    # больше попыток (20 вместо 10) на случай кратковременной задержки API.
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
    elif ex == "MEXC":
        order_id = buy_result.get("orderId")
        if buy_result.get("status") == "FILLED":
            return float(buy_result.get("executedQty", 0))
        return await wait_for_mexc_fill(session, buy_result.get("symbol", "")[:-len(QUOTE)], order_id)
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


async def place_order_mexc(session, symbol: str, side: str, quote_usdt: float) -> Optional[dict]:
    """НОВОЕ 10.08: MARKET-ордер на MEXC. Формат идентичен Binance
    (тот же /api/v3/order, те же параметры quoteOrderQty/quantity).

    ИСПРАВЛЕНИЕ 11.08: первая же реальная попытка упала с
    {'code': 700013, 'msg': 'Invalid content Type.'} — с февраля 2024 MEXC
    ОБЯЗАТЕЛЬНО требует заголовок Content-Type: application/json на всех
    POST/PUT/DELETE запросах, даже когда сами параметры передаются в
    строке запроса (как у Binance), а не в теле. Без этого заголовка
    биржа отклоняет запрос ещё до проверки подписи/прав ключа."""
    if is_backed_off("MEXC"):
        logger.error("MEXC в бэкоффе — реальный ордер НЕ отправлен")
        return None
    url = "https://api.mexc.com/api/v3/order"
    ts = int(time.time() * 1000)
    params = {
        "symbol": f"{symbol}{QUOTE}", "side": side, "type": "MARKET",
        "timestamp": ts, "recvWindow": 5000,
    }
    if side == "BUY":
        params["quoteOrderQty"] = round(quote_usdt, 2)
    else:
        params["quantity"] = quote_usdt
    params["signature"] = sign_binance(params, MEXC_SECRET)
    headers = {"X-MEXC-APIKEY": MEXC_KEY, "Content-Type": "application/json"}
    try:
        async with session.post(url, params=params, headers=headers,
                                 timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status in (429, 418):
                trigger_backoff("MEXC", r.status, r.headers.get("Retry-After"))
                return None
            data = await r.json()
            if r.status != 200:
                logger.error(f"MEXC order failed: {data}")
                _remember_error("MEXC", data.get("msg", data))
                return None
            return data
    except Exception as e:
        logger.error(f"MEXC order exception: {e}")
        _remember_error("MEXC", e)
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


# ═══════════════════════════════════════════════════════════════
# НОВОЕ 18.08 (по прямому запросу пользователя — "дай 5 вещей, которые
# дадут +"; пункт №1, самый значимый): рыночный (MARKET) ордер исполняется
# по ЛЮБОЙ цене, какая есть на бирже в момент прихода запроса — не по той,
# что была зафиксирована на сигнале. Именно это, по нашим сегодняшним
# измерениям, съедало $0.09-0.12 почти на каждой сделке (~2.3-3.4% от
# объёма), даже когда сам спред на входе был честным и не аномальным.
#
# Лимитный ордер с IOC (Immediate-or-Cancel) — противоположная гарантия:
# биржа исполнит его ТОЛЬКО по указанной цене или лучше; всё, что не
# исполнилось мгновенно по этой цене — отменяется (частичное исполнение
# возможно, неисполненный остаток просто не размещается в стакан). Это
# превращает "гарантированный мелкий убыток от проскальзывания" в
# "либо сделка ровно по расчёту, либо сделка не состоялась вообще" —
# именно то, что нужно, когда каждая проигранная копейка на счету.
# ═══════════════════════════════════════════════════════════════

async def place_order_kucoin_limit_ioc(session, symbol: str, side: str,
                                         price: float, size: float) -> Optional[dict]:
    """Лимитный IOC-ордер на KuCoin. price — предельная цена (для BUY —
    не хуже этой, для SELL — не хуже этой), size — количество МОНЕТЫ
    (не USDT, в отличие от market-версии) — должно быть уже округлено
    под шаг лота биржи (round_quantity_for_exchange) ДО вызова."""
    if is_backed_off("KuCoin"):
        logger.error("KuCoin в бэкоффе — лимитный ордер НЕ отправлен")
        return None
    endpoint = "/api/v1/orders"
    url = f"https://api.kucoin.com{endpoint}"
    ts = str(int(time.time() * 1000))
    body_dict = {
        "clientOid": str(int(time.time() * 1000000)),
        "side": side.lower(), "symbol": f"{symbol}-{QUOTE}", "type": "limit",
        "price": str(price), "size": str(size), "timeInForce": "IOC",
    }
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
                logger.error(f"KuCoin limit-IOC order failed: {data}")
                _remember_error("KuCoin", data.get("msg", data))
                return None
            return data
    except Exception as e:
        logger.error(f"KuCoin limit-IOC order exception: {e}")
        _remember_error("KuCoin", e)
        return None


async def place_order_mexc_limit_ioc(session, symbol: str, side: str,
                                       price: float, quantity: float) -> Optional[dict]:
    """Лимитный IOC-ордер на MEXC. Формат идентичен Binance (type=LIMIT,
    timeInForce=IOC, price+quantity обязательны)."""
    if is_backed_off("MEXC"):
        logger.error("MEXC в бэкоффе — лимитный ордер НЕ отправлен")
        return None
    url = "https://api.mexc.com/api/v3/order"
    ts = int(time.time() * 1000)
    params = {
        "symbol": f"{symbol}{QUOTE}", "side": side, "type": "LIMIT",
        "timeInForce": "IOC", "quantity": quantity, "price": price,
        "timestamp": ts, "recvWindow": 5000,
    }
    params["signature"] = sign_binance(params, MEXC_SECRET)
    headers = {"X-MEXC-APIKEY": MEXC_KEY, "Content-Type": "application/json"}
    try:
        async with session.post(url, params=params, headers=headers,
                                 timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status in (429, 418):
                trigger_backoff("MEXC", r.status, r.headers.get("Retry-After"))
                return None
            data = await r.json()
            if r.status != 200:
                logger.error(f"MEXC limit-IOC order failed: {data}")
                _remember_error("MEXC", data.get("msg", data))
                return None
            return data
    except Exception as e:
        logger.error(f"MEXC limit-IOC order exception: {e}")
        _remember_error("MEXC", e)
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


MIN_ORDER_VALUE_USD = {"Binance": 5.0, "KuCoin": 1.0, "HTX": 10.0, "MEXC": 1.0}  # НАХОДКА 02.08:
# HTX отклоняет любой ордер дешевле $10 ("order-value-min-error") — именно
# поэтому ребаланс молча не мог докупить ZK на HTX, когда цель ($10) была
# впритык к минимуму: нужная докупка ($9.92) оказывалась ЧУТЬ ниже порога.


async def top_up_usdt_via_coin_sale(session, ex: str, symbol: str, usdt_needed: float,
                                     price_hint: float, cost_accumulator: Optional[list] = None) -> bool:
    """НОВОЕ 10.08: зеркальная функция к top_up_coin_reserve — но для
    ПРОТИВОПОЛОЖНОЙ проблемы. Найдено по логам и выгрузке Binance: биржа,
    которая только ПОКУПАЕТ монету (например, KuCoin в связке
    KuCoin→Binance), с каждым циклом накапливает саму монету (9422 IOST на
    KuCoin к утру 10.08!) и теряет USDT — а автодокупки для этого случая
    просто не было, только для противоположной стороны (Binance, которая
    теряет монету и накапливает USDT). Сделка попросту отклонялась с
    "insufficient_usdt_on_..." и требовала ручного /rebalance.

    ВАЖНО (пункт 4 запроса): перед продажей проверяем курс — если текущая
    цена ЗАМЕТНО хуже (>0.5%) от price_hint (ожидаемой цены сделки), это
    означает, что рынок только что резко дёрнулся, и продажа сейчас, скорее
    всего, зафиксирует убыток вместо покрытия нехватки. В этом случае лучше
    один раз пропустить попытку и подождать следующего скана, чем гарантированно
    продать в минус."""
    if is_backed_off(ex):
        logger.warning(f"⛔ Автодокупка USDT на {ex}/{symbol} пропущена: биржа в бэкоффе")
        return False
    if not price_hint or price_hint <= 0:
        logger.warning(f"⛔ Автодокупка USDT на {ex}/{symbol} пропущена: нет ориентировочной цены "
                         f"(price_hint={price_hint})")
        return False
    if stats.get("topup_cost_usdt", 0.0) >= config["max_topup_spend_per_day"]:
        logger.warning(f"⛔ Автодокупка USDT на {ex}/{symbol} пропущена: дневной лимит "
                         f"(${config['max_topup_spend_per_day']}) исчерпан")
        return False

    balances = await get_real_balances(session, ex)
    if not balances:
        logger.warning(f"⛔ Автодокупка USDT на {ex}/{symbol} пропущена: не удалось прочитать баланс")
        return False
    have_coin = balances.get(symbol, 0.0)
    if have_coin <= 0:
        logger.warning(f"⛔ Автодокупка USDT на {ex}/{symbol} пропущена: нет накопленной "
                         f"монеты для продажи (баланс {symbol}: {have_coin})")
        return False

    # +8% запас сверху, как и в зеркальной функции
    coin_to_sell = (usdt_needed * 1.08) / price_hint
    if coin_to_sell > have_coin:
        coin_to_sell = have_coin * 0.98  # не пытаемся продать больше, чем есть
    # ИСПРАВЛЕНИЕ 10.08: раньше округлял просто до 6 знаков после запятой —
    # для IOST/USDT на KuCoin это дало "Order size increment invalid"
    # (нужен другой шаг округления, обычно целые числа для дешёвых монет).
    # Используем ту же функцию правильного округления под конкретную биржу,
    # что уже применяется для аварийного закрытия позиции.
    coin_to_sell = await round_quantity_for_exchange(session, ex, symbol, coin_to_sell)
    if coin_to_sell <= 0:
        logger.warning(f"⛔ Автодокупка USDT на {ex}/{symbol} пропущена: после округления "
                         f"под правила биржи количество получилось нулевым")
        return False

    # ПРОВЕРКА КУРСА (пункт 4): берём самую свежую цену прямо перед продажей,
    # не полагаемся на price_hint, который мог устареть за секунды ожидания
    fresh_ob = None
    if ex == "Binance":
        fresh_ob = await get_orderbook_binance(session, symbol)
    elif ex == "MEXC":
        fresh_ob = await get_orderbook_mexc_rest(session, symbol)
    elif ex == "KuCoin":
        fresh_ob = await get_orderbook_kucoin(session, symbol)
    elif ex == "HTX":
        fresh_ob = await get_orderbook_htx(session, symbol)
    if fresh_ob and fresh_ob.get("bids"):
        fresh_bid = fresh_ob["bids"][0][0]
        if fresh_bid < price_hint * 0.995:
            # ИСПРАВЛЕНИЕ: раньше эта проверка только ЛОГИРОВАЛА
            # предупреждение, но всё равно продавала дальше — сам смысл
            # проверки курса терялся. Теперь реально пропускаем попытку.
            logger.warning(f"⚠️ Пропускаю докупку USDT на {ex}/{symbol}: текущая цена "
                             f"{fresh_bid:.8f} хуже ожидаемой {price_hint:.8f} более чем на 0.5% — "
                             f"продажа сейчас зафиксирует убыток, жду лучшего момента")
            return False
        price_hint = fresh_bid  # используем самую свежую (не худшую) цену для расчёта

    result = None
    if ex == "Binance":
        result = await place_order_binance(session, symbol, "SELL", coin_to_sell)
    elif ex == "MEXC":
        result = await place_order_mexc(session, symbol, "SELL", coin_to_sell)
    elif ex == "KuCoin":
        result = await place_order_kucoin(session, symbol, "sell", coin_to_sell, use_funds=False)
    elif ex == "HTX":
        global _htx_account_id_cache
        if not _htx_account_id_cache:
            _htx_account_id_cache = await get_htx_account_id(session)
        if _htx_account_id_cache:
            result = await place_order_htx(session, _htx_account_id_cache, symbol, "sell-market", coin_to_sell)

    if result:
        stats["topup_attempts"] = stats.get("topup_attempts", 0) + 1
        stats["topup_success"] = stats.get("topup_success", 0) + 1
        usd_gained = coin_to_sell * price_hint
        stats["topup_cost_usdt"] = stats.get("topup_cost_usdt", 0.0) + usd_gained
        # НОВОЕ 25.08 (по прямому запросу пользователя, критическая находка):
        # раньше эта функция вообще НЕ считала реальную стоимость сдвига
        # курса — в отличие от зеркальной top_up_coin_reserve. Если продаём
        # монету сейчас ДЕШЕВЛЕ, чем недавно за неё заплатили на этой же
        # бирже (_last_real_buy_price) — это реальный, измеримый убыток
        # сверх комиссии, и он должен учитываться так же честно.
        drift_cost = 0.0
        last_buy = _last_real_buy_price.get((ex, symbol))
        if last_buy and last_buy > 0:
            drift_cost = round((last_buy - price_hint) * coin_to_sell, 4)
            stats["price_drift_cost_usdt"] = round(stats.get("price_drift_cost_usdt", 0.0) + drift_cost, 4)
            stats["realized_trading_pnl"] = round(stats.get("realized_trading_pnl", 0.0) - drift_cost, 4)
            if cost_accumulator is not None:
                cost_accumulator[0] += drift_cost
            logger.info(f"💧 Реальная стоимость сдвига курса при продаже {symbol}/{ex}: "
                         f"{'+' if drift_cost < 0 else '-'}{abs(drift_cost):.4f} USDT "
                         f"(продажа по {price_hint:.8f} против последней покупки по {last_buy:.8f})")
        logger.info(f"✅ Докупка USDT на {ex} через продажу {coin_to_sell} {symbol} "
                     f"(~${usd_gained:.2f})")
        if CHAT_ID:
            drift_line = ""
            if last_buy and last_buy > 0:
                drift_line = (f"💧 Реальная стоимость сдвига курса: "
                               f"{'+' if drift_cost < 0 else '-'}{abs(drift_cost):.4f} USDT\n")
            await send_tg(session,
                f"🔧 *Автодокупка USDT*: не хватало ${usdt_needed:.2f} на {ex} "
                f"перед сделкой — продал {coin_to_sell} {symbol} (~${usd_gained:.2f}) "
                f"из накопленного избытка и продолжаю.\n{drift_line}")
        return True
    stats["topup_attempts"] = stats.get("topup_attempts", 0) + 1
    logger.error(f"❌ Автодокупка USDT на {ex}/{symbol} не удалась: ордер на продажу "
                  f"{coin_to_sell} {symbol} отклонён биржей "
                  f"({_last_exchange_error.get(ex) or 'нет деталей'})")
    if CHAT_ID:
        await send_tg(session,
            f"❌ *Автодокупка USDT не удалась*: пытался продать {coin_to_sell} {symbol} "
            f"на {ex}, биржа отклонила ордер "
            f"({_last_exchange_error.get(ex) or 'нет деталей'}). "
            f"Нужен ручной /rebalance или перевод на {ex}.")
    return False


async def top_up_coin_reserve(session, ex: str, symbol: str, shortfall_qty: float,
                               price_hint: float, cost_accumulator: Optional[list] = None) -> bool:
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

    # НОВОЕ 14.08: найден и воспроизведён замкнутый круг — если MEXC (или
    # любая продающая биржа) истощила резерв монеты, ей НЕЧЕМ торговать,
    # значит она не зарабатывает новый USDT от продаж, а докупить монету
    # не на что, потому что своего USDT тоже не хватает. Автодокупка
    # проваливалась 17 раз подряд именно по этой причине — биржа честно
    # отвечала отказом на BUY-ордер, для которого физически не было
    # средств, а не из-за проблем с API/подписью. Проверяем ЗАРАНЕЕ,
    # хватает ли своего USDT — если нет, предупреждаем явно (не молчим,
    # как раньше) и НЕ тратим время/лимит на заведомо обречённую попытку.
    own_balances = await get_real_balances(session, ex)
    own_usdt = (own_balances or {}).get("USDT", 0.0)
    est_cost = shortfall_qty * price_hint * 1.08
    if own_usdt < est_cost:
        logger.error(f"⛔ Докупка {symbol} на {ex} невозможна: своего USDT ${own_usdt:.2f}, "
                      f"нужно ~${est_cost:.2f} — на бирже физически не хватает средств "
                      f"(замкнутый круг: биржа не продаёт -> не зарабатывает USDT -> "
                      f"не может докупить монету). Нужен ручной /rebalance или перевод.")
        if CHAT_ID:
            await send_tg(session,
                f"🔴 *Замкнутый круг на {ex}*: не хватает {symbol} для торговли "
                f"({shortfall_qty:.0f} шт), но и своего USDT (${own_usdt:.2f}) не хватает, "
                f"чтобы это докупить (нужно ~${est_cost:.2f}). Биржа сама себя не вытащит — "
                f"нужен `/rebalance` или ручной перевод USDT на {ex} прямо сейчас.")
        return False

    stats["topup_attempts"] += 1
    # Берём не только сам shortfall, но и запас сверху (+8%), чтобы после
    # этой докупки следующая сделка не уткнулась в тот же порог снова.
    usd_needed = round(shortfall_qty * price_hint * 1.08, 2)
    usd_needed = max(usd_needed, MIN_ORDER_VALUE_USD.get(ex, 5.0))  # не меньше минимума биржи

    result = None
    if ex == "Binance":
        result = await place_order_binance(session, symbol, "BUY", usd_needed)
    elif ex == "MEXC":
        result = await place_order_mexc(session, symbol, "BUY", usd_needed)
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
        # НОВОЕ 09.08: реальная стоимость сдвига курса между продажей и
        # докупкой — вот что молчаливо съедало деньги. Раньше карточка
        # сделки показывала фиксированную оценку по комиссии (~$0.011),
        # а по факту докупка часто идёт ПОСЛЕ того, как курс монеты уже
        # успел подрасти со времени последней продажи на этой же бирже —
        # тогда докупка обходится дороже, чем принесла продажа. Это
        # подтверждено выгрузкой Binance: -$1.98 только от этого эффекта
        # за 2 дня, не считая комиссий.
        last_sell = _last_real_sell_price.get((ex, symbol))
        if last_sell and last_sell > 0:
            bought_qty = shortfall_qty * 1.08
            drift_cost = round((price_hint - last_sell) * bought_qty, 4)
            stats["price_drift_cost_usdt"] = round(stats.get("price_drift_cost_usdt", 0.0) + drift_cost, 4)
            stats["realized_trading_pnl"] = round(stats.get("realized_trading_pnl", 0.0) - drift_cost, 4)
            if cost_accumulator is not None:
                cost_accumulator[0] += drift_cost
            logger.info(f"💧 Реальная стоимость сдвига курса при докупке {symbol}/{ex}: "
                         f"{'+' if drift_cost < 0 else '-'}{abs(drift_cost):.4f} USDT "
                         f"(докупка по {price_hint:.8f} против последней продажи по {last_sell:.8f})")
        logger.info(f"✅ Точечная докупка {symbol} на {ex}: ~${usd_needed} размещена "
                     f"(нехватка была {shortfall_qty:.2f} {symbol})")
        if CHAT_ID:
            drift_line = ""
            if last_sell and last_sell > 0:
                drift_line = (f"💧 Реальная стоимость сдвига курса: "
                               f"{'+' if drift_cost < 0 else '-'}{abs(drift_cost):.4f} USDT\n")
            await send_tg(session,
                f"🔧 *Автодокупка*: не хватало {shortfall_qty:.2f} {symbol} на {ex} "
                f"перед сделкой — докупил на ~${usd_needed} и продолжаю. "
                f"(итого потрачено на автодокупки сегодня: ~${stats['topup_cost_usdt']:.2f} "
                f"из лимита ${config['max_topup_spend_per_day']})\n{drift_line}")
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

    # НОВОЕ 25.08 (по прямому запросу пользователя): накопитель РЕАЛЬНОЙ
    # стоимости всех докупок, произошедших ИМЕННО в этом цикле сделки (не
    # фоновых, из reserve_watchdog_loop — те используют вызовы БЕЗ этого
    # параметра, поэтому не попадают сюда). Список из одного элемента —
    # простой изменяемый "ящик" для накопления через несколько функций.
    cycle_topup_cost = [0.0]

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
    # НОВОЕ 18.08 (по итогам разбора реальной эрозии на каждой сделке —
    # прямой запрос пользователя "как это исправить"): раньше проверка
    # баланса buy_ex и sell_ex шла ПОСЛЕДОВАТЕЛЬНО — два отдельных
    # сетевых запроса подряд, каждый по 200-500мс. На волатильном рынке
    # это заметная доля времени между сигналом и реальной покупкой, за
    # которую цена успевает уйти. Запускаем оба запроса ОДНОВРЕМЕННО —
    # экономим один полный сетевой круг на каждой попытке сделки.
    buy_balances, sell_balances = await asyncio.gather(
        get_real_balances(session, buy_ex),
        get_real_balances(session, sell_ex),
    )
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
            # НОВОЕ 18.08: при skip_reactive_topup=True — не тратим деньги
            # на дорогую докупку в моменте сделки, просто пропускаем эту
            # попытку. reserve_watchdog_loop сам подтянет баланс в фоне
            # до следующей попытки — без пересечения спреда НА критическом
            # пути, где это стоило денег почти на каждой сделке 17-18.08.
            if config.get("skip_reactive_topup", True):
                return {"success": False,
                        "error": f"skipped_insufficient_usdt_on_{buy_ex}_waiting_for_watchdog: "
                                 f"нужно ~${vol}, свободно ${available_usdt_on_buy_ex:.2f} — "
                                 f"пропускаем, не докупаем в моменте (reserve_watchdog догонит сам)"}
            # НОВОЕ 10.08: раньше здесь был мгновенный отказ с просьбой сделать
            # /rebalance вручную. Найдено по логам: биржа-покупатель (buy_ex)
            # структурно НАКАПЛИВАЕТ монету с каждым циклом (она покупает, но
            # никогда не продаёт в этой схеме) и теряет USDT — а докупка
            # (top_up_coin_reserve) раньше умела чинить только противоположную
            # проблему (нехватку МОНЕТЫ на бирже-продавце). Пробуем зеркальную
            # автодокупку — продать часть накопленной монеты на buy_ex, чтобы
            # получить нужный USDT, с проверкой курса перед продажей.
            usdt_shortfall = vol * usdt_buffer_mult - available_usdt_on_buy_ex
            topped_up = await top_up_usdt_via_coin_sale(
                session, buy_ex, symbol, usdt_shortfall, opp.get("buy_price", 0),
                cost_accumulator=cycle_topup_cost)
            if topped_up:
                buy_balances = await get_real_balances(session, buy_ex)
                available_usdt_on_buy_ex = (buy_balances or {}).get("USDT", 0.0)
                if available_usdt_on_buy_ex < vol * usdt_buffer_mult:
                    vol = round(available_usdt_on_buy_ex / usdt_buffer_mult, 2)
                    if vol < required_min:
                        return {"success": False,
                                "error": f"insufficient_usdt_on_{buy_ex}_even_after_topup"}
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
    # (получен уже ВЫШЕ, параллельно с buy_balances — второй запрос не нужен)
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
        # НОВОЕ 18.08: skip_reactive_topup — не докупаем в моменте, просто
        # пропускаем попытку. Именно эта докупка (пересечение спреда
        # биржи продажи) была главным подтверждённым источником потерь
        # на почти каждой реальной сделке 17-18.08.
        if config.get("skip_reactive_topup", True):
            return {"success": False,
                    "error": f"skipped_insufficient_reserve_on_{sell_ex}_waiting_for_watchdog: "
                             f"нужно ~{required_with_buffer:.2f} {symbol}, есть {available_on_sell_ex:.2f} — "
                             f"пропускаем, не докупаем в моменте (reserve_watchdog догонит сам)"}
        # ИСПРАВЛЕНИЕ 10.08: раньше докупали ТОЛЬКО нехватку под ТЕКУЩУЮ
        # сделку — резерв никогда не восстанавливался до полного целевого
        # объёма (3 лота), только латался впритык каждый раз. Найдено по
        # выгрузке Binance: за день продано на 35544 IOST больше, чем
        # куплено обратно — резерв структурно "худел", несмотря на то что
        # докупка формально срабатывала. Теперь докупаем СРАЗУ до полного
        # целевого резерва (headroom_pct% сверху) — реже, но основательнее,
        # догоняя темп продаж, а не постоянно отставая от него на полшага.
        headroom_mult = 1 + config["rebalance_headroom_pct"] / 100
        full_target_qty = (config["max_real_order_usdt"] * config["sell_reserve_lots"]
                            / opp["sell_price"] * headroom_mult)
        shortfall = round(max(required_with_buffer, full_target_qty) - available_on_sell_ex, 4)
        topped = await top_up_coin_reserve(session, sell_ex, symbol, shortfall, opp["sell_price"], cost_accumulator=cycle_topup_cost)
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
    # НОВОЕ 18.08: лимитный IOC вместо рыночного, если включено (по
    # умолчанию ВКЛЮЧЕНО — это строго безопаснее рыночного: худший исход
    # лимитного IOC — сделка просто не состоится, а не состоится ПЛОХО).
    # Пока реализовано только для KuCoin (текущая буксирующая биржа
    # покупки) и MEXC (текущая биржа продажи) — остальные биржи по-прежнему
    # используют старые market-функции без изменений.
    buy_result = None
    use_limit_ioc = config.get("use_limit_ioc_orders", True)
    if buy_ex == "Binance":
        buy_result = await place_order_binance(session, symbol, "BUY", vol)
    elif buy_ex == "MEXC":
        buy_result = await place_order_mexc(session, symbol, "BUY", vol)
    elif buy_ex == "KuCoin":
        if use_limit_ioc and opp.get("buy_price"):
            coins_estimate = vol / opp["buy_price"]
            limit_size = await round_quantity_for_exchange(session, "KuCoin", symbol, coins_estimate)
            if limit_size > 0:
                buy_result = await place_order_kucoin_limit_ioc(
                    session, symbol, "buy", opp["buy_price"], limit_size)
            if not buy_result:
                # Лимитный IOC не прошёл (цена уже хуже расчётной) — это
                # ОЖИДАЕМОЕ, безопасное поведение самой защиты, не сбой.
                # НЕ откатываемся на market — если лимитный не прошёл,
                # значит цена реально ушла, и market только зафиксировал
                # бы тот самый убыток, которого мы стараемся избежать.
                stats["buy_leg_failures"] += 1
                stats["limit_ioc_not_filled"] = stats.get("limit_ioc_not_filled", 0) + 1
                return {"success": False,
                        "error": f"limit_ioc_buy_not_filled_on_{buy_ex}: "
                                 f"цена ушла хуже расчётной ({opp['buy_price']}) — "
                                 f"пропускаем вместо покупки по проскальзыванию"}
        else:
            buy_result = await place_order_kucoin(session, symbol, "buy", vol, use_funds=True)
    elif buy_ex == "HTX":
        if not _htx_account_id_cache:
            _htx_account_id_cache = await get_htx_account_id(session)
        if _htx_account_id_cache:
            buy_result = await place_order_htx(session, _htx_account_id_cache, symbol, "buy-market", vol)

    if not buy_result:
        stats["buy_leg_failures"] += 1  # НОВОЕ (патч 17.08): видимость в /stats
        return {"success": False,
                "error": f"buy_leg_failed_on_{buy_ex}: {_last_exchange_error.get(buy_ex) or 'нет деталей от биржи'}"}

    config["real_trades_today"] += 1

    # ТРЕБОВАНИЕ 1: не верим на слово, что покупка исполнилась — подтверждаем
    # реальным опросом биржи и берём ФАКТИЧЕСКОЕ количество, а не расчётное
    confirmed_qty = await confirm_fill_and_get_qty(session, buy_ex, buy_result)
    if not confirmed_qty or confirmed_qty <= 0:
        # ИСПРАВЛЕНО 23.08 (по факту логов реальных сделок): раньше текст
        # звучал как непонятный сбой ("not_confirmed_filled"), хотя по
        # факту это почти всегда — та же самая защита лимитного IOC
        # ордера, сработавшая в ДРУГОЙ точке кода (не в preflight-проверке
        # выше, а уже ПОСЛЕ размещения): биржа мгновенно отменила ордер
        # (0 исполнено), потому что цена успела уйти хуже расчётной за
        # доли секунды между вычислением цены и отправкой запроса. Деньги
        # НЕ потрачены — это ожидаемое, безопасное поведение защиты, не
        # техническая ошибка. Уточняем текст, чтобы это было понятно сразу
        # по сообщению, без разбора логов Railway каждый раз.
        stats["buy_leg_failures"] += 1
        if config.get("use_limit_ioc_orders", True):
            return {"success": False,
                    "error": f"limit_ioc_buy_cancelled_by_exchange_on_{buy_ex}: "
                             f"ордер размещён, но биржа исполнила 0 (цена ушла хуже "
                             f"расчётной за доли секунды) — деньги не потрачены, "
                             f"это защита сработала, не сбой"}
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
        # ИСПРАВЛЕНИЕ 10.08: та же логика, что и на первом рубеже — топим
        # до полного целевого резерва, а не только под текущую сделку.
        headroom_mult = 1 + config["rebalance_headroom_pct"] / 100
        full_target_qty = (config["max_real_order_usdt"] * config["sell_reserve_lots"]
                            / opp["sell_price"] * headroom_mult)
        shortfall = round(max(sell_qty, full_target_qty) - fresh_available, 4)
        topped = await top_up_coin_reserve(session, sell_ex, symbol, shortfall, opp["sell_price"], cost_accumulator=cycle_topup_cost)
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
    elif sell_ex == "MEXC":
        if use_limit_ioc and opp.get("sell_price"):
            sell_result = await place_order_mexc_limit_ioc(
                session, symbol, "SELL", opp["sell_price"], sell_qty)
            # ВАЖНО: если лимитный IOC на продажу не прошёл — здесь НЕЛЬЗЯ
            # просто "пропустить", как на покупке — первая нога УЖЕ куплена,
            # позиция открыта. Ниже по коду в любом случае сработает
            # аварийное закрытие market-ордером (это уже правильно и не
            # трогается) — лимитный IOC тут даёт только ШАНС продать без
            # проскальзывания, а не заменяет защиту от зависшей позиции.
        else:
            sell_result = await place_order_mexc(session, symbol, "SELL", sell_qty)
    elif sell_ex == "KuCoin":
        sell_result = await place_order_kucoin(session, symbol, "sell", sell_qty, use_funds=False)
    elif sell_ex == "HTX":
        if not _htx_account_id_cache:
            _htx_account_id_cache = await get_htx_account_id(session)
        if _htx_account_id_cache:
            sell_result = await place_order_htx(session, _htx_account_id_cache, symbol, "sell-market", sell_qty)

    if not sell_result:
        stats["sell_leg_failures"] += 1  # НОВОЕ (патч 17.08): видимость в /stats
        # АВАРИЙНОЕ ЗАКРЫТИЕ: продаём купленное обратно на бирже покупки,
        # чтобы не остаться с открытой направленной позицией. Округляем
        # под правила ИМЕННО buy_ex (это другая биржа с другим шагом лота).
        emergency_qty = await round_quantity_for_exchange(session, buy_ex, symbol, confirmed_qty)
        emergency = None
        stats["emergency_closes_attempted"] += 1  # НОВОЕ (патч 17.08)
        if emergency_qty > 0:
            if buy_ex == "Binance":
                emergency = await place_order_binance(session, symbol, "SELL", emergency_qty)
            elif buy_ex == "MEXC":
                emergency = await place_order_mexc(session, symbol, "SELL", emergency_qty)
            elif buy_ex == "KuCoin":
                emergency = await place_order_kucoin(session, symbol, "sell", emergency_qty, use_funds=False)
            elif buy_ex == "HTX":
                if _htx_account_id_cache:
                    emergency = await place_order_htx(session, _htx_account_id_cache, symbol, "sell-market", emergency_qty)
        if emergency:
            stats["emergency_closes_succeeded"] += 1  # НОВОЕ (патч 17.08)
        return {
            "success": False,
            "error": f"sell_leg_failed_on_{sell_ex}: {_last_exchange_error.get(sell_ex) or 'нет деталей от биржи'}",
            "emergency_close": bool(emergency),
            "buy_result": buy_result,
        }

    # НОВОЕ 09.08: запоминаем реальную цену продажи для этой биржи+монеты —
    # нужно, чтобы честно посчитать РЕАЛЬНУЮ (не оценочную по комиссии)
    # стоимость последующей докупки резерва. Найдено по выгрузке Binance:
    # между продажей и докупкой курс монеты успевает сдвинуться, и это
    # реальная, а не бумажная потеря — за 2 дня набежало -$1.98 только на
    # этом эффекте, и бот её никогда не видел и не показывал.
    _last_real_sell_price[(sell_ex, symbol)] = opp["sell_price"]
    # НОВОЕ 25.08: зеркальный трекер цены покупки — для честной стоимости
    # реактивной продажи излишка (top_up_usdt_via_coin_sale).
    _last_real_buy_price[(buy_ex, symbol)] = opp["buy_price"]

    return {"success": True, "buy_result": buy_result, "sell_result": sell_result, "vol": vol,
             "confirmed_qty": confirmed_qty, "real_topup_cost_usdt": round(cycle_topup_cost[0], 4)}


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


# ═══════════════════════════════════════════════════════════════
# НОВОЕ 18.08 (по прямому запросу пользователя — попытка ускорить ногу
# покупки на KuCoin через HF (High-Frequency) аккаунт, официально
# заявленный биржей как более быстрый путь исполнения при той же схеме
# аутентификации). ВАЖНО: формат запроса баланса НЕ подтверждён
# документацией со 100% уверенностью — использован тот же проверенный
# endpoint /api/v1/accounts (как для обычного "trade"), просто с другим
# ожидаемым значением type ("trade_hf"), это наиболее вероятный формат
# по структуре документации KuCoin, но НЕ гарантирован.
#
# БЕЗОПАСНОСТЬ: эта функция и её результат НЕ используются в реальной
# торговле, пока пользователь явно не проверит её командой /testhfbalance
# и не включит /setusehf on. По умолчанию (use_kucoin_hf=False) код ниже
# просто не вызывается вообще — реальное поведение бота НЕ меняется.
# ═══════════════════════════════════════════════════════════════

async def get_real_balances_kucoin_hf(session) -> Optional[Dict[str, float]]:
    """Баланс trade_hf аккаунта — тот же endpoint /api/v1/accounts, что и
    обычный trade, просто фильтруем по type=='trade_hf' вместо 'trade'.
    Возвращает None при любой ошибке или неожиданном формате — НИКОГДА не
    гадает и не додумывает данные, чтобы не исказить реальную торговлю."""
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
                logger.error(f"KuCoin HF balance fetch failed: {data}")
                return None
            result = {}
            found_hf_type = False
            for acc in data.get("data", []):
                if acc.get("type") == "trade_hf":
                    found_hf_type = True
                    result[acc["currency"]] = float(acc["available"])
            if not found_hf_type:
                logger.error("KuCoin HF balance: тип 'trade_hf' не найден в ответе — "
                              "либо ещё не переведены средства, либо формат отличается "
                              "от ожидаемого. Полный ответ: " + str(data)[:500])
                return None
            return result
    except Exception as e:
        logger.error(f"KuCoin HF balance exception: {e}")
        return None


async def place_order_kucoin_hf(session, symbol: str, side: str, funds_or_size: float,
                                  use_funds: bool = True) -> Optional[dict]:
    """Размещение ордера через HF-эндпоинт — /api/v1/hf/orders. Формат
    ЭТОГО эндпоинта (в отличие от баланса) подтверждён документацией
    напрямую. Та же схема подписи, что и у обычного place_order_kucoin."""
    if is_backed_off("KuCoin"):
        logger.error("KuCoin в бэкоффе — HF-ордер НЕ отправлен")
        return None
    endpoint = "/api/v1/hf/orders"
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
                logger.error(f"KuCoin HF order failed: {data}")
                _remember_error("KuCoin", data.get("msg", data))
                return None
            return data
    except Exception as e:
        logger.error(f"KuCoin HF order exception: {e}")
        _remember_error("KuCoin", e)
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


async def get_real_balances_mexc(session) -> Optional[Dict[str, float]]:
    """НОВОЕ 10.08: формат ответа MEXC идентичен Binance (/api/v3/account)."""
    if is_backed_off("MEXC"):
        return None
    url = "https://api.mexc.com/api/v3/account"
    ts = int(time.time() * 1000)
    params = {"timestamp": ts, "recvWindow": 5000}
    params["signature"] = sign_binance(params, MEXC_SECRET)
    headers = {"X-MEXC-APIKEY": MEXC_KEY, "Content-Type": "application/json"}
    try:
        async with session.get(url, params=params, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status in (429, 418):
                trigger_backoff("MEXC", r.status, r.headers.get("Retry-After"))
                return None
            data = await r.json()
            if r.status != 200:
                logger.error(f"MEXC balance fetch failed: {data}")
                _remember_error("MEXC", data)
                return None
            return {b["asset"]: float(b["free"]) for b in data.get("balances", [])}
    except Exception as e:
        logger.error(f"MEXC balance exception: {e}")
        return None


async def get_real_balances(session, ex: str) -> Optional[Dict[str, float]]:
    if ex == "Binance":
        return await get_real_balances_binance(session)
    elif ex == "KuCoin":
        return await get_real_balances_kucoin(session)
    elif ex == "HTX":
        return await get_real_balances_htx(session)
    elif ex == "MEXC":
        return await get_real_balances_mexc(session)
    return None


async def get_valuation_price(session, ex: str, symbol: str) -> Optional[float]:
    """Best bid как консервативная оценка стоимости позиции (если продавать)."""
    if ex == "Binance":
        ob = await get_orderbook_binance(session, symbol)
    elif ex == "MEXC":
        ob = await get_orderbook_mexc_rest(session, symbol)
    elif ex == "KuCoin":
        ob = await get_orderbook_kucoin(session, symbol)
    elif ex == "HTX":
        ob = await get_orderbook_htx(session, symbol)
    else:
        return None
    if not ob or not ob.get("bids"):
        return None
    return ob["bids"][0][0]


async def get_misc_asset_price_usdt(session, ex: str, asset: str) -> Optional[float]:
    """НОВОЕ 10.08: простая оценка цены ПОБОЧНОГО актива (BNB на Binance,
    KCS на KuCoin — топливо для скидки на комиссию, не торгуемая монета) в
    USDT через обычный REST-тикер. Найдено по прямому вопросу пользователя:
    BNB и KCS читались с биржи, но НИКОГДА не суммировались в общий
    капитал — вся потраченная на комиссию сумма была невидима для P&L.
    Не использует WebSocket-инфраструктуру (та настроена только под
    торгуемую монету) — вызывается редко, при подсчёте /stats, не в
    горячем цикле сканирования."""
    try:
        if ex == "Binance":
            async with session.get("https://api.binance.com/api/v3/ticker/price",
                                    params={"symbol": f"{asset}USDT"},
                                    timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status == 200:
                    data = await r.json()
                    return float(data.get("price", 0)) or None
        elif ex == "KuCoin":
            async with session.get("https://api.kucoin.com/api/v1/market/orderbook/level1",
                                    params={"symbol": f"{asset}-USDT"},
                                    timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status == 200:
                    data = await r.json()
                    price = data.get("data", {}).get("price")
                    return float(price) if price else None
        elif ex == "HTX":
            async with session.get("https://api.huobi.pro/market/detail/merged",
                                    params={"symbol": f"{asset.lower()}usdt"},
                                    timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status == 200:
                    data = await r.json()
                    tick = data.get("tick", {})
                    return float(tick.get("close", 0)) or None
        elif ex == "MEXC":
            async with session.get("https://api.mexc.com/api/v3/ticker/price",
                                    params={"symbol": f"{asset}USDT"},
                                    timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status == 200:
                    data = await r.json()
                    return float(data.get("price", 0)) or None
    except Exception as e:
        logger.warning(f"Не удалось оценить {asset} на {ex}: {e}")
    return None


def record_pnl_and_get_hour_avg(pnl_now: float) -> float:
    """НОВОЕ 11.08: добавляет текущую точку P&L в историю, вычищает точки
    старше часа, возвращает среднее за последний час. Не заменяет честный
    мгновенный P&L — просто даёт спокойный, менее дёргающийся ориентир
    рядом с ним."""
    now_ts = time.time()
    pnl_history.append((now_ts, pnl_now))
    cutoff = now_ts - 3600
    while pnl_history and pnl_history[0][0] < cutoff:
        pnl_history.pop(0)
    if not pnl_history:
        return pnl_now
    return sum(p for _, p in pnl_history) / len(pnl_history)


async def get_total_real_capital(session, fixed_prices: Optional[Dict[Tuple[str, str], float]] = None) -> Optional[dict]:
    """Реальный совокупный капитал на всех трёх биржах — используется в /stats
    вместо симуляционного SIM_START/sim_balances, когда бот в реальном режиме.
    ИСПРАВЛЕНИЕ 10.08: раньше считались ТОЛЬКО USDT и торгуемая монета
    (SYMBOLS) — любой другой актив (BNB на Binance, KCS на KuCoin — топливо
    для скидки на комиссию) был полностью невидим для этого расчёта. Вся
    сумма, потраченная на комиссию из этих запасов, никогда не появлялась
    ни в "Реальном балансе", ни в P&L — реальная, но невидимая утечка
    капитала. Теперь учитываем ЛЮБОЙ ненулевой актив на счету.

    ИСПРАВЛЕНО 25.08 (по прямому запросу пользователя — "почему даже
    честно положительные по прогнозу сделки дают факт -$0.01"): найдена
    корневая причина — эта функция запрашивает СВЕЖУЮ рыночную цену
    каждый раз при вызове. Когда её вызывают дважды подряд (до и после
    сделки, с разницей в несколько секунд из-за исполнения+докупок+
    паузы), любое естественное колебание цены ONE за эти секунды
    попадает в "фактический результат" как будто это потеря от сделки —
    хотя реально это просто рыночный шум на резерве (~$15), не имеющий
    отношения к тому, прибыльна сделка или нет. Теперь необязательный
    параметр fixed_prices позволяет замерить "после" ПО ТЕМ ЖЕ ценам,
    что и "до" — тогда разница отражает ТОЛЬКО реальное изменение
    количества монет/USDT от сделки, без искажения ценовым шумом."""
    per_exchange = {}
    total = 0.0
    misc_assets_value = {}
    prices_used: Dict[Tuple[str, str], float] = {}
    for ex in ["Binance", "KuCoin", "HTX", "MEXC"]:
        # MEXC — необязательна, пока не заданы ключи в Railway (иначе
        # авторизация 401 сломала бы ВЕСЬ /stats, а не только строку MEXC)
        if ex == "MEXC" and not MEXC_KEY:
            continue
        balances = await get_real_balances(session, ex)
        if balances is None:
            if ex == "MEXC":
                continue  # не валим весь расчёт из-за временного сбоя MEXC
            return None
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
        # НОВОЕ: любые другие ненулевые активы (BNB, KCS и т.п.)
        other_assets = {a: q for a, q in balances.items()
                         if a != "USDT" and a not in SYMBOLS and q > 0.00001}
        for asset, qty in other_assets.items():
            price = await get_misc_asset_price_usdt(session, ex, asset)
            if price:
                value = qty * price
                ex_total += value
                misc_assets_value[f"{ex}:{asset}"] = round(value, 4)
        per_exchange[ex] = round(ex_total, 2)
        total += ex_total
    return {"total": round(total, 2), "per_exchange": per_exchange,
            "misc_assets_value": misc_assets_value, "prices_used": prices_used}


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
                elif ex == "MEXC":
                    result = await place_order_mexc(session, sym, "SELL", qty_to_sell)
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
                elif ex == "MEXC":
                    result = await place_order_mexc(session, sym, "BUY", deficit_usd)
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
    for ex in ["Binance", "KuCoin", "HTX", "MEXC"]:
        if ex == "MEXC" and not MEXC_KEY:
            continue  # MEXC ещё не настроена — не участвует в ребалансе
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

    # НОВОЕ 17.08 (по итогам дня — 3 из 3 реальных сделок на ONE в факт-
    # минус, несмотря на то, что скорость исполнения и порог входа уже
    # чинили по отдельности): проверяем волатильность ЗА ПОСЛЕДНЮЮ МИНУТУ
    # прямо перед КАЖДОЙ попыткой реальной сделки — не только фоновым
    # предохранителем раз в 15 минут (тот реагирует постфактум, слишком
    # медленно для секундных движений). Если цена уже дёргалась за
    # последние 60 сек больше порога — очень вероятно, что она продолжит
    # двигаться, пока бот исполняет обе ноги, и спред на карточке уже не
    # будет соответствовать реальности к моменту сделки.
    if not config["simulation_mode"]:
        pre_trade_vol = get_recent_price_volatility_pct(1)
        pre_trade_threshold = config.get("pre_trade_max_volatility_pct_1min", 0.5)
        if pre_trade_vol is not None and pre_trade_threshold > 0 and pre_trade_vol > pre_trade_threshold:
            logger.info(f"⏭ Пропуск попытки: волатильность за 1 мин {pre_trade_vol}% "
                         f"выше порога {pre_trade_threshold}% — цена слишком дёргается прямо сейчас")
            return {"executed": False, "reason": "pre_trade_volatility_too_high"}

    real_result = None
    if not config["simulation_mode"] and is_real_trading_allowed():
        # НОВОЕ 16.08: по прямому запросу пользователя — вместо ТОЛЬКО оценки
        # (комиссии + предполагаемая стоимость спреда), берём РЕАЛЬНЫЙ баланс
        # ДО сделки, даём ей полностью исполниться (включая любые вызванные
        # ею докупки/ребалансы), и берём РЕАЛЬНЫЙ баланс ПОСЛЕ — разница между
        # ними это ФАКТИЧЕСКИЙ результат, без каких-либо предположений о
        # комиссиях или спреде. Не заменяет оценку в карточке (та приходит
        # мгновенно, до знания об исполнении) — дополняет её вторым,
        # безусловно точным числом чуть позже.
        capital_before = await get_total_real_capital(session)
        # НОВОЕ 25.08 (по прямому запросу пользователя, найдено по логам
        # Railway с точными метками времени): захватываем lock СРАЗУ после
        # capital_before — до самого исполнения и до snapshot "после".
        # Это гарантирует, что reserve_watchdog_loop (см. его код) не
        # сможет выполнить параллельную докупку/продажу именно в эти
        # секунды, искажая факт-результат ЭТОЙ сделки чужой операцией.
        # Освобождается ниже — либо после снимка "после" (успех), либо
        # сразу же (неудача, снимок "после" не нужен).
        await _capital_measurement_lock.acquire()
        real_result = await execute_real_arbitrage(session, opp)
        # НОВОЕ 10.08 (раунд 2): раньше диагностика шла только в Railway-логи
        # (logger.info) — тишина в Telegram при этом сохранялась, а до
        # логов Railway на практике трудно добраться быстро. Теперь то же
        # самое сообщение уходит НАПРЯМУЮ в Telegram при КАЖДОЙ попытке,
        # без вариантов пропустить — либо увидим success=True (и тогда
        # ищем баг дальше, в коде ПОСЛЕ этой точки), либо success=False
        # с конкретной причиной (и тогда сама сделка не проходит, а не
        # только счётчик врёт).
        if CHAT_ID:
            await send_tg(session,
                f"🔍 *Диагностика попытки сделки*\n"
                f"success={real_result.get('success')}\n"
                f"error={real_result.get('error')}\n"
                f"vol={real_result.get('vol')}")
        logger.info(f"🔍 ДИАГНОСТИКА execute_real_arbitrage: success={real_result.get('success')} "
                     f"error={real_result.get('error')} symbol={opp.get('symbol')} "
                     f"buy_ex={opp.get('buy_ex')} sell_ex={opp.get('sell_ex')}")
        if real_result.get("success"):
            # НОВОЕ 08.08: раньше единственная метрика P&L была "Реальный
            # баланс" целиком — а это ОБЩАЯ рыночная стоимость портфеля,
            # включая переоценку резерва монеты по текущей цене. Колебание
            # цены IOST на резерве (~$21) всего на десятые доли процента
            # маскирует или искажает результат нескольких сделок подряд —
            # после 3 сделок с честной расчётной прибылью +$0.0046 каждая
            # (~$0.014 суммарно) общий баланс показывал -$0.15, и было
            # невозможно понять, торговля ли в минусе или просто цена
            # резерва просела. Теперь считаем РЕАЛИЗОВАННУЮ прибыль отдельно,
            # независимо от рыночной переоценки резерва.
            # НОВОЕ 16.08: раньше rebalance_cost_est учитывал ТОЛЬКО торговые
            # комиссии (buy_fee+sell_fee) — но реальная стоимость ПОСЛЕДУЮЩЕГО
            # ребаланса (докупки резерва) складывается из комиссии ПЛЮС
            # пересечения bid-ask спреда биржи, которое РАСТЁТ при волатильности
            # (найдено 16.08: во время быстрого ралли RVN разрыв между
            # заявленной и реальной прибылью составил ~\$1.47 за сессию —
            # карточка попросту не учитывала расширение спреда при волатильном
            # рынке). Теперь явно добавляем настраиваемую оценку пересечения
            # спреда (/setcrossingcost) поверх комиссий.
            spread_crossing_est = opp["vol"] * config.get("empirical_spread_crossing_pct", 0.34) / 100
            fees_only = opp["vol"] * (FEES.get(opp["buy_ex"], 0.1) + FEES.get(opp["sell_ex"], 0.1)) / 100

            # НОВОЕ 25.08 (по прямому запросу пользователя — "почему оценка
            # не пересчитывается под конкретную докупку"): если в ЭТОМ
            # конкретном цикле реально произошла докупка — её реальная
            # стоимость (drift_cost) уже была вычтена из stats["realized_
            # trading_pnl"] ПРЯМО ВНУТРИ top_up_coin_reserve/top_up_usdt_
            # via_coin_sale, в момент докупки. Если добавить сюда ЕЩЁ и
            # общую оценку (spread_crossing_est) — получится двойной счёт
            # одной и той же стоимости. Поэтому: есть реальные данные —
            # используем ТОЛЬКО комиссии здесь (стоимость уже учтена
            # отдельно), нет — как раньше, используем оценку.
            #
            # ВАЖНО: pretrade_card_estimate — это ТА ЖЕ формула (оценка),
            # что уже была отправлена пользователю в карточке ДО сделки —
            # она НЕ меняется задним числом, иначе строка "оценка в
            # карточке была: X" станет враньём (покажет не то число, что
            # реально видел пользователь в предыдущем сообщении).
            # honest_cycle_profit — ОТДЕЛЬНОЕ, более точное число для
            # внутренней бухгалтерии (stats["realized_trading_pnl"]),
            # использующее реальные данные о докупке, когда они есть.
            pretrade_card_estimate = round(opp["profit_usdt"] - round(fees_only + spread_crossing_est, 4), 4)

            real_topup_cost_this_cycle = real_result.get("real_topup_cost_usdt", 0.0)
            if real_topup_cost_this_cycle != 0.0:
                rebalance_cost_est = round(fees_only, 4)
                cost_source_note = f" (скорректировано по реальной докупке: {real_topup_cost_this_cycle:+.4f} USDT)"
            else:
                rebalance_cost_est = round(fees_only + spread_crossing_est, 4)
                cost_source_note = ""
            honest_cycle_profit = round(opp["profit_usdt"] - rebalance_cost_est, 4)
            stats["realized_trading_pnl"] = round(stats.get("realized_trading_pnl", 0.0) + honest_cycle_profit, 4)
            stats["realized_trades_count"] = stats.get("realized_trades_count", 0) + 1

            # НОВОЕ 16.08: ФАКТИЧЕСКИЙ результат — не оценка. Сравниваем
            # реальный баланс до и после (включая любые докупки, случившиеся
            # ВНУТРИ execute_real_arbitrage — они уже произошли к этому моменту).
            # Небольшая задержка перед снимком "после" — даём биржам полностью
            # отразить исполнение на балансе (та же логика, что и в других
            # местах кода после докупок).
            try:
                capital_after = None
                if capital_before is not None:
                    await asyncio.sleep(1.0)
                    # ИСПРАВЛЕНО 25.08: используем ТЕ ЖЕ цены, что были в
                    # capital_before — иначе рыночный шум за эти секунды
                    # искажает "факт" сделки (см. комментарий в
                    # get_total_real_capital). Теперь разница отражает ТОЛЬКО
                    # реальное изменение количества монет/USDT.
                    capital_after = await get_total_real_capital(
                        session, fixed_prices=capital_before.get("prices_used"))
            finally:
                # НОВОЕ 25.08: освобождаем lock ЗДЕСЬ — после снимка "после",
                # что бы ни случилось внутри (даже исключение) — иначе
                # watchdog навсегда останется заблокирован при ошибке.
                if _capital_measurement_lock.locked():
                    _capital_measurement_lock.release()
            if capital_after is not None:
                    factual_delta = round(capital_after["total"] - capital_before["total"], 4)
                    stats["factual_realized_pnl"] = round(stats.get("factual_realized_pnl", 0.0) + factual_delta, 4)
                    stats["factual_trades_count"] = stats.get("factual_trades_count", 0) + 1
                    diff_from_estimate = round(factual_delta - honest_cycle_profit, 4)

                    # НОВОЕ 18.08: записываем реальную эрозию исполнения (разница
                    # между % на сигнале и фактически реализованным %) — питает
                    # get_avg_execution_erosion_pct(), которая теперь напрямую
                    # входит в честный порог compute_dynamic_min_profit_pct.
                    record_execution_erosion(opp["net_pct"], factual_delta, opp["vol"])

                    # НОВОЕ 17.08: САМОКАЛИБРОВКА — по прямому запросу пользователя
                    # после вечера, когда даже "спокойный" рынок (движение цены <1%)
                    # дал 2 сделки подряд с реальным минусом при оценке crossingcost=1.5%.
                    # Вместо ручного гадания — копим ПОСЛЕДНИЕ N расхождений между
                    # оценкой и фактом и СРЕДНЕЕ добавляем к текущему crossingcost.
                    # Пересчитывается каждые CALIBRATION_WINDOW сделок, не на каждой —
                    # чтобы не дёргаться от одного шумного результата.
                    gap_history = stats.setdefault("crossing_gap_history_pct", [])
                    gap_pct = diff_from_estimate / opp["vol"] * 100 if opp["vol"] else 0
                    gap_history.append(gap_pct)
                    CALIBRATION_WINDOW = 3
                    if len(gap_history) >= CALIBRATION_WINDOW:
                        avg_gap = sum(gap_history[-CALIBRATION_WINDOW:]) / CALIBRATION_WINDOW
                        if avg_gap < 0:  # систематически хуже оценки — поднимаем crossingcost
                            old_cc = config.get("empirical_spread_crossing_pct", 0.34)
                            new_cc = round(old_cc + abs(avg_gap), 2)
                            config["empirical_spread_crossing_pct"] = new_cc
                            if CHAT_ID:
                                await send_tg(session,
                                    f"🎛 *Автокалибровка*: последние {CALIBRATION_WINDOW} сделки в среднем "
                                    f"на {avg_gap:.2f}% хуже оценки — crossingcost поднят с "
                                    f"{old_cc}% до {new_cc}%. Честный порог автоматически станет строже.")
                            gap_history.clear()

                    # НОВОЕ 17.08: автопауза, если ДАЖЕ ПОСЛЕ калибровки продолжаем
                    # терять — значит проблема не в оценке, а в чём-то более глубоком,
                    # человеку нужно разобраться, а не продолжать пытаться вслепую.
                    consecutive_losses = stats.get("consecutive_factual_losses", 0)
                    if factual_delta < 0:
                        consecutive_losses += 1
                    else:
                        consecutive_losses = 0
                    stats["consecutive_factual_losses"] = consecutive_losses
                    if consecutive_losses >= 5:
                        config["paused"] = True
                        if CHAT_ID:
                            await send_tg(session,
                                f"🛑 *5 сделок подряд в реальном минусе* (даже после "
                                f"автокалибровки) — торговля остановлена. Это не шум, "
                                f"нужен ручной разбор, прежде чем продолжать `/go`.")

                    if CHAT_ID:
                        await send_tg(session,
                            f"📐 *ФАКТИЧЕСКИЙ результат цикла* (реальный баланс до/после, "
                            f"не оценка):\n"
                            f"   До: ${capital_before['total']} → После: ${capital_after['total']}\n"
                            f"   Фактически: `{factual_delta:+.4f} USDT`\n"
                            f"   (оценка в карточке была: `{pretrade_card_estimate:+.4f}` — "
                            f"разница `{round(factual_delta - pretrade_card_estimate, 4):+.4f}`)"
                            f"{cost_source_note}")
        if not real_result.get("success"):
            # НОВОЕ 25.08: если сделка не удалась — снимок "после" не нужен,
            # но lock ВСЁ РАВНО был захвачен выше и должен быть освобождён
            # здесь, иначе watchdog навсегда останется заблокирован после
            # любой неудачной попытки сделки.
            if _capital_measurement_lock.locked():
                _capital_measurement_lock.release()
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

    # НОВОЕ 10.08: "Реальных сделок исполнено" (stats["trades"]) много дней
    # подряд стоял на нуле, несмотря на подтверждённую реальную активность.
    # Раз обычное логирование не помогло найти причину — оборачиваем именно
    # этот участок (запись истории + инкремент счётчиков) в защиту с прямым
    # уведомлением в Telegram при ЛЮБОМ исключении, чтобы поймать проблему
    # на первой же реальной сделке, без похода в Railway.
    try:
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
    except Exception as e:
        logger.error(f"❌ ИСКЛЮЧЕНИЕ при записи истории сделки: {e}")
        if CHAT_ID:
            await send_tg(session,
                f"❌ *Найдена причина бага со счётчиком!*\n"
                f"Исключение при записи сделки в историю: `{type(e).__name__}: {e}`\n"
                f"opp keys: `{list(opp.keys())}`")
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
    "pre_trade_volatility_too_high": "🌪 цена дёргается прямо сейчас (за последнюю минуту) — пропущено на всякий случай",
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
        # НОВОЕ 16.08: та же поправка, что и в execute_trade — добавляем
        # оценку пересечения спреда (настраивается /setcrossingcost),
        # не только комиссии.
        spread_crossing_est = opp["vol"] * config.get("empirical_spread_crossing_pct", 0.34) / 100
        rebalance_cost = round(opp["vol"] * (FEES.get(opp["buy_ex"], 0.1) +
                                              FEES.get(opp["sell_ex"], 0.1)) / 100
                                + spread_crossing_est, 4)
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
        await send_tg(session, f"🔍 Запрашиваю реальный стакан {sym} с четырёх бирж...")
        bn, kc, hx, mx, active = await fetch_all_orderbooks(session)
        msg = f"📖 *Стакан {sym}USDT*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for ex, books in [("Binance", bn), ("KuCoin", kc), ("HTX", hx), ("MEXC", mx)]:
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
                f"KuCoin={stats['depth_fail']['KuCoin']} HTX={stats['depth_fail']['HTX']} "
                f"MEXC={stats['depth_fail'].get('MEXC', 0)}\n"
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
                # НОВОЕ 10.08: показываем стоимость "топливных" активов
                # (BNB/KCS) отдельной строкой — раньше она была невидима.
                misc = real.get("misc_assets_value", {})
                misc_line = ""
                if misc:
                    misc_total = sum(misc.values())
                    misc_parts = ", ".join(f"{k}: ${v:.2f}" for k, v in misc.items())
                    misc_line = (f"⛽ Топливо на комиссию (учтено в балансе выше): "
                                 f"${misc_total:.2f} ({misc_parts})\n")
                realized_pnl = stats.get("realized_trading_pnl", 0.0)
                realized_n = stats.get("realized_trades_count", 0)
                drift_cost = stats.get("price_drift_cost_usdt", 0.0)
                # НОВОЕ 16.08: ФАКТИЧЕСКИЙ результат (реальный баланс до/после
                # каждой сделки) — не оценка, единственное безусловно точное
                # число. Показываем рядом с оценочным для сравнения.
                factual_pnl = stats.get("factual_realized_pnl", 0.0)
                factual_n = stats.get("factual_trades_count", 0)
                factual_line = ""
                if factual_n > 0:
                    factual_line = (
                        f"📐 Фактический результат (реальный баланс до/после, "
                        f"не оценка): {factual_pnl:+.4f} USDT за {factual_n} сделок\n"
                    )
                realized_line = (
                    f"📊 Реализованная торговая прибыль (без учёта переоценки резерва, "
                    f"НО с учётом реальной стоимости докупки): "
                    f"{realized_pnl:+.4f} USDT за {realized_n} сделок\n"
                    f"💧 Из них реальная стоимость сдвига курса при докупках: "
                    f"{'+' if drift_cost < 0 else '-'}{abs(drift_cost):.4f} USDT\n"
                    f"{factual_line}"
                    f"{misc_line}"
                )
                if config["real_start_capital"]:
                    pnl_real = round(real["total"] - config["real_start_capital"], 2)
                    hour_avg = round(record_pnl_and_get_hour_avg(pnl_real), 2)
                    # НОВОЕ 15.08: записываем текущую рыночную цену монеты
                    # (не P&L) — честный тренд за 24ч, не предсказание.
                    trend_line = ""
                    if SYMBOLS:
                        price_now = await get_valuation_price(session, "MEXC", SYMBOLS[0])
                        if price_now:
                            price_history.append((time.time(), price_now))
                            trend = record_price_and_get_trend()
                            if trend:
                                icon = "📈" if trend["change_pct"] > 0 else "📉" if trend["change_pct"] < 0 else "➡️"
                                trend_line = (
                                    f"   {icon} Цена {SYMBOLS[0]} за последние {trend['hours_span']}ч: "
                                    f"`{trend['oldest']}` → `{trend['newest']}` "
                                    f"({trend['change_pct']:+.2f}%) — это ОПИСАНИЕ прошлого, "
                                    f"не прогноз на будущее\n"
                                )
                    balance_block = (
                        f"💵 Реальный баланс: ${real['total']} ({per_ex})\n"
                        f"   Старт (зафиксирован): ${config['real_start_capital']} | "
                        f"P&L: {pnl_real:+.2f} (включает переоценку резерва по рынку)\n"
                        f"   📉 Среднее P&L за последний час: {hour_avg:+.2f} "
                        f"(спокойнее, чем моментальное число — меньше рыночного шума)\n"
                        f"{trend_line}"
                        f"{realized_line}"
                    )
                else:
                    balance_block = (
                        f"💵 Реальный баланс: ${real['total']} ({per_ex})\n"
                        f"   💡 Стартовая точка не зафиксирована — `/setrealstart` "
                        f"чтобы считать P&L честно\n"
                        f"{realized_line}"
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
                f"   MEXC: {stats['depth_fail'].get('MEXC', 0)}\n"
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
                f"⚠️ Неудачных первых ног (buy): {stats.get('buy_leg_failures', 0)}\n"
                f"⚠️ Неудачных вторых ног (sell): {stats.get('sell_leg_failures', 0)}\n"
                f"🚨 Аварийных закрытий: {stats.get('emergency_closes_succeeded', 0)}/"
                f"{stats.get('emergency_closes_attempted', 0)} (успех/попытка)\n\n"
                f"{balance_block}\n"
                f"⚙️ Реальный лимит ордера: ${config['max_real_order_usdt']} | "
                f"Порог: {config['min_profit_pct']}% (ручной) / "
                f"{compute_dynamic_min_profit_pct('KuCoin', 'Binance')}% (честный, с учётом ребаланса) | "
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
        if ex_name not in ("Binance", "KuCoin", "HTX", "MEXC"):
            await send_tg(session, "❌ Биржа должна быть одной из: Binance, KuCoin, HTX, MEXC")
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

    elif cmd == "/setperiodicrebalance":
        if len(parts) < 2:
            await send_tg(session,
                f"Текущий интервал фонового ребаланса: каждые "
                f"{config['periodic_rebalance_hours']}ч (0 = выключено)\n\n"
                f"Это ОТДЕЛЬНЫЙ механизм от ребаланса после сделки (тот убран 10.08) — "
                f"срабатывает по таймеру, не по событию, специально чтобы периодически "
                f"фиксировать рост резерва в USDT, не пересекая спред слишком часто.\n\n"
                f"Пример: `/setperiodicrebalance 4`"
            )
            return
        try:
            val = float(parts[1])
            if val < 0:
                await send_tg(session, "❌ Не может быть отрицательным.")
                return
            config["periodic_rebalance_hours"] = val
            if val == 0:
                await send_tg(session, "✅ Периодический ребаланс выключен.")
            else:
                await send_tg(session, f"✅ Периодический ребаланс: каждые {val}ч.")
        except ValueError:
            await send_tg(session, "❌ Пример: `/setperiodicrebalance 4`")

    elif cmd == "/setprofitlock":
        if len(parts) < 2:
            cur = config.get("profit_lock_interval_sec", 300)
            await send_tg(session,
                f"Текущий интервал фиксации курсового плюса: каждые {cur} сек "
                f"(0 = выключено)\n\n"
                f"При ЛЮБОМ реальном плюсе на резерве (стоит дороже цели) — "
                f"сразу продаёт излишек в USDT. При минусе НИЧЕГО не делает, "
                f"просто ждёт (докупка дефицита — отдельный, реактивный механизм).\n\n"
                f"Пример: `/setprofitlock 300` (раз в 5 минут)"
            )
            return
        try:
            val = int(parts[1])
            if val < 0:
                await send_tg(session, "❌ Не может быть отрицательным.")
                return
            config["profit_lock_interval_sec"] = val
            if val == 0:
                await send_tg(session, "✅ Фиксация курсового плюса выключена.")
            else:
                await send_tg(session, f"✅ Фиксация курсового плюса: проверка каждые {val} сек.")
        except ValueError:
            await send_tg(session, "❌ Пример: `/setprofitlock 300`")

    elif cmd == "/setmaxdrawdown":
        if len(parts) < 2:
            cur = config.get("max_drawdown_pct", 0)
            await send_tg(session,
                f"Текущий порог предохранителя: -{cur}% от старта (0 = выключено)\n\n"
                f"Если общий P&L упадёт ниже этого порога — бот САМ ставится на "
                f"паузу и присылает предупреждение, вместо того чтобы молча "
                f"продолжать при уже болезненном минусе. Не устраняет курсовой "
                f"шум резерва (это невозможно без потери чего-то другого) — "
                f"только ограничивает, насколько глубоко он может утянуть "
                f"капитал, прежде чем вы об этом узнаете.\n\n"
                f"Пример: `/setmaxdrawdown 5` (пауза при -5% от старта)"
            )
            return
        try:
            val = float(parts[1])
            if val < 0:
                await send_tg(session, "❌ Не может быть отрицательным.")
                return
            config["max_drawdown_pct"] = val
            if val == 0:
                await send_tg(session, "✅ Предохранитель от минуса выключен.")
            else:
                await send_tg(session, f"✅ Предохранитель: пауза при общем P&L ниже -{val}% от старта.")
        except ValueError:
            await send_tg(session, "❌ Пример: `/setmaxdrawdown 5`")

    elif cmd == "/setmaxvolatility":
        if len(parts) < 2:
            cur = config.get("max_volatility_pct_15min", 0)
            await send_tg(session,
                f"Текущий порог волатильности: {cur}% за 15 минут (0 = выключено)\n\n"
                f"Если цена монеты сдвинется больше этого % за последние 15 минут — "
                f"бот САМ ставится на паузу. При сильной волатильности расчётный "
                f"спред перестаёт отражать реальность (цена уходит, пока обе ноги "
                f"сделки исполняются) — именно так 16.08 карточка дважды подряд "
                f"обещала плюс, а по факту вышел минус.\n\n"
                f"Не возобновляет торговлю сам — только предупреждает, когда "
                f"волатильность спадает, решение о `/go` остаётся за вами.\n\n"
                f"Пример: `/setmaxvolatility 3` (пауза при движении >3% за 15 мин)"
            )
            return
        try:
            val = float(parts[1])
            if val < 0:
                await send_tg(session, "❌ Не может быть отрицательным.")
                return
            config["max_volatility_pct_15min"] = val
            if val == 0:
                await send_tg(session, "✅ Предохранитель волатильности выключен.")
            else:
                await send_tg(session, f"✅ Предохранитель волатильности: пауза при движении "
                                        f"цены больше {val}% за 15 минут.")
        except ValueError:
            await send_tg(session, "❌ Пример: `/setmaxvolatility 3`")

    elif cmd == "/setwatchdoginterval":
        # НОВОЕ 17.08: как часто reserve_watchdog_loop заранее проверяет и
        # докупает резерв, ДО того как он понадобится для сделки — чтобы
        # избежать 5-9 сек задержки на докупку в момент самой сделки
        # (найдено по логам реальных сделок ONE 17.08).
        if len(parts) < 2:
            await send_tg(session,
                f"Текущий интервал проверки резерва заранее: "
                f"{config.get('reserve_watchdog_interval_sec', 90)} сек\n\n"
                f"Раз в этот интервал бот проверяет резерв монеты на бирже "
                f"продажи и докупает его ЗАРАНЕЕ, если он опускается ниже "
                f"порога (см. `/setwatchdogtrigger`) — чтобы в момент самой "
                f"сделки НЕ пришлось докупать синхронно (это и создавало "
                f"задержку 5-9 сек, найденную в логах 17.08).\n\n"
                f"Пример: `/setwatchdoginterval 60`"
            )
            return
        try:
            val = int(parts[1])
            if val < 10:
                await send_tg(session, "❌ Не меньше 10 сек — иначе слишком часто дёргаем API бирж.")
                return
            config["reserve_watchdog_interval_sec"] = val
            await send_tg(session, f"✅ Интервал проверки резерва заранее: {val} сек")
        except ValueError:
            await send_tg(session, "❌ Пример: `/setwatchdoginterval 60`")

    elif cmd == "/setwatchdogtrigger":
        if len(parts) < 2:
            cur = config.get("reserve_watchdog_trigger_frac", 0.6)
            await send_tg(session,
                f"Текущий порог срабатывания: {cur*100:.0f}% от целевого резерва\n\n"
                f"Если резерв монеты на бирже продажи опускается ниже этой доли "
                f"от цели (`max_real_order_usdt × sell_reserve_lots`) — бот "
                f"докупает его ЗАРАНЕЕ, в фоне, не дожидаясь попытки сделки.\n\n"
                f"Пример: `/setwatchdogtrigger 70` (докупать раньше, при 70% от цели)"
            )
            return
        try:
            val = float(parts[1])
            if val <= 0 or val > 100:
                await send_tg(session, "❌ Разумный диапазон: 1-100%.")
                return
            config["reserve_watchdog_trigger_frac"] = val / 100
            await send_tg(session, f"✅ Порог срабатывания: {val:.0f}% от целевого резерва")
        except ValueError:
            await send_tg(session, "❌ Пример: `/setwatchdogtrigger 70`")

    elif cmd == "/setskiptopup":
        # НОВОЕ 18.08: главный переключатель фикса "чтобы работа давала +
        # а не -". on (по умолчанию) — бот НЕ докупает резерв в момент
        # сделки (до покупки первой ноги), а пропускает такую попытку,
        # полагаясь на reserve_watchdog_loop. off — возврат к старому
        # поведению (докупка в моменте, дороже, но сделок больше).
        if len(parts) < 2:
            cur = config.get("skip_reactive_topup", True)
            await send_tg(session,
                f"Текущий режим: {'✅ ON — сделки БЕЗ докупки в моменте' if cur else '❌ OFF — старое поведение с докупкой в моменте'}\n\n"
                f"ON (рекомендуется): если резерва не хватает прямо перед сделкой — "
                f"бот НЕ докупает его на месте (это стоило денег почти на "
                f"каждой сделке 17-18.08), а просто пропускает попытку. "
                f"Резерв пополняется ТОЛЬКО фоново, через reserve_watchdog_loop, "
                f"вне критического пути сделки. Сделок будет меньше, но каждая "
                f"исполненная — без цены докупки.\n\n"
                f"OFF: возврат к прежнему поведению (докупка прямо в момент "
                f"сделки, если не хватает).\n\n"
                f"Пример: `/setskiptopup on` или `/setskiptopup off`"
            )
            return
        val = parts[1].lower()
        if val in ("on", "1", "true", "вкл"):
            config["skip_reactive_topup"] = True
            await send_tg(session, "✅ Реактивная докупка в момент сделки ВЫКЛЮЧЕНА — "
                                     "сделки без достаточного резерва будут пропускаться, "
                                     "не докупаться на месте.")
        elif val in ("off", "0", "false", "выкл"):
            config["skip_reactive_topup"] = False
            await send_tg(session, "⚠️ Реактивная докупка в момент сделки ВКЛЮЧЕНА обратно — "
                                     "старое поведение, докупка на месте при нехватке резерва.")
        else:
            await send_tg(session, "❌ Пример: `/setskiptopup on` или `/setskiptopup off`")

    elif cmd == "/erosionstats":
        # НОВОЕ 18.08: показать накопленную историю измеренной эрозии
        # исполнения (разница между обещанным % на сигнале и фактически
        # реализованным %) — тот самый буфер, который теперь автоматически
        # входит в честный порог входа, чтобы снизить частоту убыточных
        # сделок (полностью исключить их код не может — рыночный риск
        # есть всегда).
        avg = get_avg_execution_erosion_pct()
        hist_str = ", ".join(f"{v:+.2f}%" for v in execution_erosion_history) or "(пусто)"
        source = "по факту сделок" if len(execution_erosion_history) >= 3 else "стартовая оценка (мало данных)"
        await send_tg(session,
            f"📉 *ЭРОЗИЯ ИСПОЛНЕНИЯ*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Текущий буфер: *{avg:.2f}%* ({source})\n"
            f"История последних сделок: {hist_str}\n\n"
            f"Этот буфер автоматически добавляется к честному порогу входа "
            f"(`/stats` → «Порог... честный»). Чем выше реально измеренная "
            f"эрозия — тем строже порог, тем реже (но надёжнее) сделки.\n\n"
            f"Стартовая оценка (пока данных <3 сделок): "
            f"{config.get('min_execution_erosion_estimate_pct', 2.7)}%, "
            f"откалибрована по фактам 18.08. Изменить: "
            f"`/setminerosion N`"
        )

    elif cmd == "/setminerosion":
        if len(parts) < 2:
            await send_tg(session,
                f"Текущая стартовая оценка эрозии (пока мало реальных данных): "
                f"{config.get('min_execution_erosion_estimate_pct', 2.7)}%\n\n"
                f"Пример: `/setminerosion 2.5`"
            )
            return
        try:
            val = float(parts[1])
            if val < 0 or val > 20:
                await send_tg(session, "❌ Разумный диапазон: 0-20%.")
                return
            config["min_execution_erosion_estimate_pct"] = val
            await send_tg(session, f"✅ Стартовая оценка эрозии: {val}% "
                                     f"(будет использоваться, пока не накопится 3+ реальных сделки)")
        except ValueError:
            await send_tg(session, "❌ Пример: `/setminerosion 2.5`")

    elif cmd == "/setminabsprofit":
        # НОВОЕ 23.08: абсолютный минимум ожидаемой прибыли сделки в
        # долларах — независимая от процентов защита, добавленная после
        # находки: эрозия исполнения не пропорциональна % спреда.
        if len(parts) < 2:
            val = config.get("min_absolute_profit_usd", 0.15)
            lot = config.get("max_real_order_usdt", 4.0)
            required_pct = round(val / lot * 100, 2) if lot > 0 else 0
            await send_tg(session,
                f"Текущий абсолютный минимум прибыли: ${val}\n"
                f"При текущем лоте (${lot}) это требует спред минимум ~{required_pct}% "
                f"после комиссий — независимо от процентного порога.\n\n"
                f"Откалибровано по 3 реальным сделкам 22.08 (эрозия была "
                f"$0.07-0.26, среднее ~$0.15 с запасом).\n\n"
                f"Пример: `/setminabsprofit 0.15`"
            )
            return
        try:
            val = float(parts[1])
            if val < 0:
                await send_tg(session, "❌ Не может быть отрицательным.")
                return
            config["min_absolute_profit_usd"] = val
            await send_tg(session, f"✅ Абсолютный минимум прибыли: ${val}")
        except ValueError:
            await send_tg(session, "❌ Пример: `/setminabsprofit 0.15`")

    elif cmd == "/seterosionweight":
        # НОВОЕ 23.08: доля процентного эрозийного буфера, остающаяся в
        # честном % пороге (остальное теперь покрывает абсолютный $ фильтр).
        if len(parts) < 2:
            val = config.get("erosion_pct_weight", 0.5)
            await send_tg(session,
                f"Текущий вес % эрозии в честном пороге: {val} (0.0-1.0)\n\n"
                f"1.0 = как было раньше (вся защита в %, порог мог достигать 6.9%+)\n"
                f"0.5 = половина защиты в %, половина — в абсолютном $ фильтре (сейчас)\n"
                f"0.0 = вся защита только в $ (компонент эрозии исключён из % порога)\n\n"
                f"Пример: `/seterosionweight 0.5`"
            )
            return
        try:
            val = float(parts[1])
            if val < 0 or val > 1:
                await send_tg(session, "❌ Диапазон: 0.0-1.0.")
                return
            config["erosion_pct_weight"] = val
            await send_tg(session, f"✅ Вес % эрозии: {val}")
        except ValueError:
            await send_tg(session, "❌ Пример: `/seterosionweight 0.5`")

    elif cmd == "/setlimitioc":
        # НОВОЕ 18.08: главный переключатель фикса эрозии исполнения —
        # лимитные IOC ордера вместо рыночных на KuCoin (покупка) и MEXC
        # (продажа). on (по умолчанию) — безопаснее: сделка либо ровно по
        # расчётной цене, либо не состоится вовсе. off — старое поведение
        # (market, гарантированное исполнение, но с проскальзыванием).
        if len(parts) < 2:
            cur = config.get("use_limit_ioc_orders", True)
            not_filled = stats.get("limit_ioc_not_filled", 0)
            await send_tg(session,
                f"Текущий режим: {'✅ ON — лимитные IOC (KuCoin покупка + MEXC продажа)' if cur else '❌ OFF — рыночные ордера (старое поведение)'}\n\n"
                f"Не исполнено из-за ушедшей цены (защита сработала): {not_filled}\n\n"
                f"ON (рекомендуется): цена фиксируется на уровне сигнала — либо "
                f"исполнится ровно по расчёту, либо не исполнится вовсе (без "
                f"проскальзывания). OFF: старое поведение — гарантированное "
                f"исполнение market-ордером, но по любой цене, включая худшую.\n\n"
                f"Пример: `/setlimitioc on` или `/setlimitioc off`"
            )
            return
        val = parts[1].lower()
        if val in ("on", "1", "true", "вкл"):
            config["use_limit_ioc_orders"] = True
            await send_tg(session, "✅ Лимитные IOC-ордера ВКЛЮЧЕНЫ — цена фиксируется на "
                                     "уровне сигнала, проскальзывание больше невозможно.")
        elif val in ("off", "0", "false", "выкл"):
            config["use_limit_ioc_orders"] = False
            await send_tg(session, "⚠️ Лимитные IOC-ордера ВЫКЛЮЧЕНЫ — возврат к рыночным "
                                     "ордерам (гарантированное исполнение, но с проскальзыванием).")
        else:
            await send_tg(session, "❌ Пример: `/setlimitioc on` или `/setlimitioc off`")

    elif cmd == "/setprofittarget":
        # НОВОЕ 18.08: целевой ОБЩИЙ плюс, при достижении которого бот сам
        # запускает ребаланс и поднимает стартовую точку — фиксируя плюс
        # как новую базу отсчёта P&L. 0 — выключить полностью.
        if len(parts) < 2:
            target = config.get("profit_target_usdt", 0.30)
            enabled = config.get("profit_target_enabled", True)
            await send_tg(session,
                f"Текущая цель фиксации плюса: {'выключено' if not enabled or target <= 0 else f'{target} USDT'}\n\n"
                f"При достижении ОБЩЕГО P&L этой суммы — бот запускает ребаланс "
                f"(продажа излишка монеты в USDT на всех биржах) и поднимает "
                f"стартовую точку до нового баланса — плюс становится новым "
                f"нулём отсчёта.\n\n"
                f"Пример: `/setprofittarget 0.3` или `/setprofittarget 0` (выключить)"
            )
            return
        try:
            val = float(parts[1])
            if val < 0:
                await send_tg(session, "❌ Не может быть отрицательным.")
                return
            config["profit_target_usdt"] = val
            config["profit_target_enabled"] = val > 0
            if val > 0:
                await send_tg(session, f"✅ Цель фиксации плюса: {val} USDT")
            else:
                await send_tg(session, "✅ Автофиксация плюса выключена.")
        except ValueError:
            await send_tg(session, "❌ Пример: `/setprofittarget 0.3`")

    elif cmd == "/setidlealert":
        # НОВОЕ 21.08: порог, при котором бот уведомляет о простаивающем
        # USDT на биржах-продавцах (которые никогда не покупают в своей
        # роли) — только уведомление, деньги сам не трогает.
        if len(parts) < 2:
            thr = config.get("idle_usdt_alert_threshold_usdt", 2.0)
            await send_tg(session,
                f"Текущий порог уведомления о простаивающем USDT: {thr} USDT\n\n"
                f"Раз в час бот проверяет биржи, которые по своей роли НИКОГДА "
                f"не покупают (только продают) — если там скопился USDT сверх "
                f"этого порога, присылает уведомление с рекомендацией, куда "
                f"его перевести вручную. Бот НЕ переводит деньги сам.\n\n"
                f"Пример: `/setidlealert 2.0` или `/setidlealert 0` (выключить)"
            )
            return
        try:
            val = float(parts[1])
            if val < 0:
                await send_tg(session, "❌ Не может быть отрицательным.")
                return
            config["idle_usdt_alert_threshold_usdt"] = val
            if val > 0:
                await send_tg(session, f"✅ Порог уведомления о простаивающем USDT: {val} USDT")
            else:
                await send_tg(session, "✅ Уведомления о простаивающем USDT выключены "
                                         "(порог 0 — эффективно никогда не сработает).")
        except ValueError:
            await send_tg(session, "❌ Пример: `/setidlealert 2.0`")

    elif cmd == "/testhfbalance":
        # НОВОЕ 18.08: БЕЗОПАСНАЯ диагностическая команда — только ЧИТАЕТ
        # баланс trade_hf аккаунта на KuCoin, НИЧЕГО не меняет и не
        # торгует. Нужна, чтобы проверить, что формат ответа биржи
        # действительно содержит type=='trade_hf' (это не подтверждено
        # документацией на 100%), ПРЕЖДЕ чем что-либо реально включать в
        # торговую логику. Полное подключение HF-аккаунта в реальную
        # торговлю ЕЩЁ НЕ РЕАЛИЗОВАНО — сначала нужно подтвердить формат
        # здесь, потом отдельно переносить логику ребаланса/watchdog.
        await send_tg(session, "📡 Проверяю баланс trade_hf аккаунта на KuCoin (только чтение, ничего не меняю)...")
        hf_balance = await get_real_balances_kucoin_hf(session)
        if hf_balance is None:
            await send_tg(session,
                "🔴 *Не удалось прочитать баланс trade_hf.*\n\n"
                "Либо на HF-аккаунте ещё нет средств (переведите хотя бы "
                "$1 через приложение KuCoin: Активы → Перевод → из Spot в "
                "Pro/HF аккаунт), либо формат ответа биржи отличается от "
                "ожидаемого. Смотри детали в логах Railway (поиск "
                "\"KuCoin HF balance\") — там будет полный сырой ответ биржи."
            )
            return
        if not hf_balance:
            await send_tg(session,
                "✅ Формат ответа распознан правильно (type='trade_hf' найден), "
                "но баланс пустой — переведите средства на HF-аккаунт через "
                "приложение KuCoin, потом проверьте снова."
            )
            return
        balance_str = ", ".join(f"{k}: {v}" for k, v in hf_balance.items())
        await send_tg(session,
            f"✅ *Баланс trade_hf распознан успешно:*\n{balance_str}\n\n"
            f"Формат подтверждён. Полное подключение HF в реальную торговлю "
            f"(ребаланс, watchdog, докупки) — отдельный шаг, ещё не сделан, "
            f"чтобы не создать рассинхрон между обычным и HF балансом."
        )

    elif cmd == "/setpretradevolatility":
        # НОВОЕ 17.08: порог мгновенной волатильности (за 1 минуту),
        # проверяется прямо перед КАЖДОЙ попыткой реальной сделки — не
        # только фоновым 15-минутным предохранителем. Найдено по факту:
        # 3 из 3 сделок на ONE 17.08 ушли в реальный минус именно из-за
        # того, что цена продолжала двигаться в момент исполнения.
        if len(parts) < 2:
            cur = config.get("pre_trade_max_volatility_pct_1min", 0.5)
            await send_tg(session,
                f"Текущий порог мгновенной волатильности: {cur}% за 1 минуту "
                f"(0 = выключено)\n\n"
                f"Проверяется прямо ПЕРЕД каждой попыткой реальной сделки — если "
                f"цена уже дёргалась за последнюю минуту больше этого % — "
                f"попытка пропускается (в /stats и логах это видно как "
                f"pre_trade_volatility_too_high).\n\n"
                f"Это БЫСТРЕЕ, чем `/setmaxvolatility` (тот проверяет раз в "
                f"2 минуты и ставит на паузу ВСЮ торговлю за 15-минутное "
                f"окно) — этот же порог просто пропускает ОДНУ конкретную "
                f"попытку, не останавливая бота целиком.\n\n"
                f"Пример: `/setpretradevolatility 0.5`"
            )
            return
        try:
            val = float(parts[1])
            if val < 0:
                await send_tg(session, "❌ Не может быть отрицательным.")
                return
            config["pre_trade_max_volatility_pct_1min"] = val
            if val == 0:
                await send_tg(session, "✅ Проверка мгновенной волатильности выключена.")
            else:
                await send_tg(session, f"✅ Порог мгновенной волатильности: {val}% за 1 минуту "
                                        f"(проверяется перед каждой попыткой сделки)")
        except ValueError:
            await send_tg(session, "❌ Пример: `/setpretradevolatility 0.5`")

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

    elif cmd == "/setcrossingcost":
        if len(parts) < 2:
            cur = config.get("empirical_spread_crossing_pct", 0.34)
            await send_tg(session,
                f"Текущая оценка стоимости пересечения спреда при докупках: {cur}%\n\n"
                f"Используется для: (1) честного порога прибыльности сделки, "
                f"(2) оценки \"стоимости ребаланса\" в карточке каждой сделки.\n\n"
                f"⚠️ НАЙДЕНО 16.08: эта оценка была откалибрована на СПОКОЙНОМ рынке. "
                f"Во время сильной волатильности (движение цены на несколько % в час) "
                f"биржи расширяют bid-ask спред, и реальная стоимость докупки может "
                f"быть в разы выше — карточка сделки в такие моменты завышает "
                f"показанную прибыль.\n\n"
                f"Пример: `/setcrossingcost 1.5` (повысить во время волатильности)"
            )
            return
        try:
            val = float(parts[1])
            if val < 0 or val > 20:
                await send_tg(session, "❌ Разумный диапазон: 0-20%.")
                return
            config["empirical_spread_crossing_pct"] = val
            await send_tg(session, f"✅ Оценка стоимости пересечения спреда: {val}%\n"
                                    f"Честный порог и карточки сделок теперь пересчитываются с этим значением.")
        except ValueError:
            await send_tg(session, "❌ Пример: `/setcrossingcost 1.5`")

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
        # НОВОЕ 16.08 (найдено при формальном аудите): очищаем историю цены —
        # иначе после смены монеты индикатор тренда сравнивает цену НОВОЙ
        # монеты со старой точкой от УДАЛЁННОЙ (как случилось при IOST->STORJ:
        # "+7646%" — абсурд, старая цена IOST осталась в истории и
        # сравнивалась с ценой STORJ).
        price_history.clear()
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
            for ex in ["Binance", "KuCoin", "HTX", "MEXC"]:
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
                elif ex == "MEXC":
                    result = await place_order_mexc(session, sym, "SELL", sell_qty)
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
        await send_tg(session, f"📡 Проверяю реальный остаток {sym} на четырёх биржах...")
        sold, failed, none_found = {}, [], []
        for ex in ["Binance", "KuCoin", "HTX", "MEXC"]:
            if ex == "MEXC" and not MEXC_KEY:
                continue
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
            elif ex == "MEXC":
                result = await place_order_mexc(session, sym, "SELL", sell_qty)
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
        if ex_name not in ("Binance", "KuCoin", "HTX", "MEXC"):
            await send_tg(session, "❌ Биржа должна быть одной из: Binance, KuCoin, HTX, MEXC")
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
        for ex in ["Binance", "KuCoin", "HTX", "MEXC"]:
            if ex == "MEXC" and not MEXC_KEY:
                continue
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

    elif cmd == "/addtriangle":
        if len(parts) < 2:
            await send_tg(session,
                f"Добавляет монету в список для /triangle (НЕ влияет на "
                f"реальную торговлю — отдельный список).\n"
                f"Сейчас: {', '.join(TRIANGLE_SYMBOLS) if TRIANGLE_SYMBOLS else '(пусто)'}\n"
                f"Пример: `/addtriangle ETH`")
            return
        sym = parts[1].upper()
        if sym in TRIANGLE_SYMBOLS:
            await send_tg(session, f"⚠️ {sym} уже в списке треугольника.")
            return
        TRIANGLE_SYMBOLS.append(sym)
        await send_tg(session, f"✅ Добавлено в треугольник: {sym}\n"
                                 f"Текущий список: {', '.join(TRIANGLE_SYMBOLS)}")

    elif cmd == "/removetriangle":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/removetriangle ETH`")
            return
        sym = parts[1].upper()
        if sym not in TRIANGLE_SYMBOLS:
            await send_tg(session, f"⚠️ {sym} не в списке треугольника.")
            return
        TRIANGLE_SYMBOLS.remove(sym)
        await send_tg(session, f"✅ Удалено из треугольника: {sym}\n"
                                 f"Текущий список: {', '.join(TRIANGLE_SYMBOLS) if TRIANGLE_SYMBOLS else '(пусто)'}")

    elif cmd == "/triangle":
        if not TRIANGLE_SYMBOLS:
            await send_tg(session,
                "⚠️ Список монет для треугольника пуст.\n"
                "Добавьте хотя бы одну: `/addtriangle ETH`\n"
                "Это отдельный список, не влияет на реальную торговлю.")
            return
        await send_tg(session, f"🔺 Сканирую треугольный арбитраж на Binance "
                                 f"({', '.join(TRIANGLE_SYMBOLS)})...")
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
        bn, kc, hx, mx, active = await fetch_all_orderbooks(session)
        ex_map = {"Binance": bn, "KuCoin": kc, "HTX": hx, "MEXC": mx}
        saved = config["min_profit_pct"]
        config["min_profit_pct"] = -999
        # ИСПРАВЛЕНИЕ 08.08: тот же лот, что использует фоновый цикл реальной
        # торговли — иначе /top показывает картину для другого объёма сделки,
        # чем то, что реально сканирует и исполняет бот, и результаты не
        # совпадают между собой (именно так и было замечено — /top нашёл
        # сигнал, а /stats фонового цикла — нет).
        top_lot = (config["max_real_order_usdt"] if not config["simulation_mode"]
                   else config["trade_usdt"])
        all_opps = []
        for sym in SYMBOLS:
            for buy_ex, sell_ex in pairs_for_symbol(sym):
                bob = ex_map.get(buy_ex, {}).get(sym)
                sob = ex_map.get(sell_ex, {}).get(sym)
                if bob and sob:
                    opp = calc_arb_real(sym, buy_ex, bob, sell_ex, sob, top_lot)
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
                # ИСПРАВЛЕНИЕ 10.08: раньше это выглядело так, будто команда
                # меняет реальный размер сделки — на деле она меняет ТОЛЬКО
                # симуляционную переменную, реальный лот управляется отдельно
                # через /setreallot. Из-за этого несколько раз подряд /setlot
                # 5-6 "срабатывал" (бот подтверждал), но /realbalance
                # продолжал требовать резерв под старый реальный лот — явное
                # предупреждение теперь показывается всегда, чтобы не гадать.
                warn = ""
                if not config["simulation_mode"]:
                    warn = (f"\n⚠️ Вы в РЕАЛЬНОМ режиме — эта команда меняет лот "
                             f"только для симуляции и НЕ влияет на реальные сделки. "
                             f"Для реального лота используйте `/setreallot` "
                             f"(сейчас: ${config['max_real_order_usdt']})")
                await send_tg(session, f"✅ Лот (симуляция): ${config['trade_usdt']}{warn}")
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


async def periodic_rebalance_loop(session):
    """НОВОЕ 11.08: фоновый, привязанный к ВРЕМЕНИ (не к сделкам) полный
    ребаланс — раз в config['periodic_rebalance_hours'] часов. Отличие от
    убранного 10.08 механизма "ребаланс после каждой сделки": там триггер
    был СОБЫТИЕМ (каждая сделка, слишком часто — раз в ~2 минуты), здесь
    триггер ВРЕМЯ (по умолчанию раз в 4 часа) — достаточно редко, чтобы не
    пересекать спред биржи зря, но достаточно часто, чтобы периодически
    "подстригать" резерв обратно к цели: если цена резерва выросла —
    излишек продаётся в USDT, фиксируя часть роста в стабильной форме."""
    await asyncio.sleep(300)  # даём боту 5 минут на старте устаканиться, прежде чем первый прогон
    while True:
        hours = config.get("periodic_rebalance_hours", 0)
        if hours <= 0:
            await asyncio.sleep(600)
            continue
        try:
            if not config["simulation_mode"] and not config["paused"]:
                logger.info(f"Периодический ребаланс (раз в {hours}ч): запускаю...")
                rb_result = await real_auto_rebalance_all(session)
                if CHAT_ID and (rb_result.get("applied") or rb_result.get("cross_exchange_needed")):
                    await send_tg(session, "⏰ *Периодический ребаланс* (по таймеру, не от сделки):\n\n" +
                                   format_real_rebalance_result(rb_result))
        except Exception as e:
            logger.error(f"Periodic rebalance loop error: {e}")
        await asyncio.sleep(hours * 3600)


async def reserve_watchdog_loop(session):
    """НОВОЕ 17.08 (по итогам анализа логов реальных сделок ONE, прямой
    запрос пользователя): прямое измерение по логам Railway показало
    задержку 5-9 секунд между обнаружением сигнала (лог "Скан #N:
    сигналов=1") и стартом реального исполнения — почти всё это время
    уходило на СИНХРОННУЮ докупку резерва ВНУТРИ execute_real_arbitrage
    ("Уменьшаю объём сделки...", "Точечная докупка..." в логе), то есть
    НА КРИТИЧЕСКОМ ПУТИ самой сделки. На волатильной монете (та же ONE
    двигалась на ~1% за минуты) за эти секунды цена успевает уйти,
    съедая спред, который был зафиксирован на сигнале — отсюда
    систематический факт-минус даже когда карточка обещала плюс.

    Этот цикл проверяет резерв монеты на КАЖДОЙ бирже-продавце ЗАРАНЕЕ,
    часто (по умолчанию раз в 90 сек — гораздо чаще, чем
    periodic_rebalance_loop раз в 4 часа), и докупает его ДО того, как
    он понадобится для сделки. Цель — чтобы preflight-проверка внутри
    execute_real_arbitrage в подавляющем большинстве случаев видела уже
    достаточный резерв и НЕ запускала синхронную докупку в момент сделки,
    сокращая задержку критического пути до долей секунды.

    Использует ТУ ЖЕ функцию top_up_coin_reserve, что и реактивная
    докупка внутри сделки — просто вызывает её ЗАРАНЕЕ, а не по факту
    нехватки в момент попытки. Дневной лимит max_topup_spend_per_day
    общий для обоих механизмов — реактивная докупка внутри сделки
    сработает реже, но общие траты на докупки не увеличатся бесконтрольно.

    ДОБАВЛЕНО 18.08 (найдено сразу после включения /setskiptopup on):
    раньше этот цикл проверял ТОЛЬКО резерв монеты на бирже ПРОДАЖИ
    (sell_ex) — но биржа ПОКУПКИ (buy_ex, напр. KuCoin) тоже структурно
    накапливает лишнюю монету и теряет USDT (она покупает, но не
    продаёт в этой схеме). Реактивный механизм top_up_usdt_via_coin_sale
    чинил это в моменте сделки — но при skip_reactive_topup=on сделки с
    такой нехваткой теперь просто пропускаются, и БЕЗ этого дополнения
    остались бы пропущены НАВСЕГДА (watchdog про buy_ex вообще не знал).
    Теперь цикл заранее продаёт излишек монеты на buy_ex в USDT, тем же
    способом, каким это раньше делала реактивная докупка — просто заранее."""
    await asyncio.sleep(60)
    while True:
        interval = config.get("reserve_watchdog_interval_sec", 90)
        try:
            if not config["simulation_mode"]:
                # НОВОЕ 25.08 (по прямому запросу пользователя, найдено по
                # логам с точными метками времени): раньше watchdog мог
                # сработать В ЛЮБОЙ момент, включая ровно те секунды, пока
                # идёт замер факта конкретной сделки (capital_before →
                # capital_after) — подтверждено логами: watchdog выполнил
                # реальную докупку+продажу на $10 за 1 секунду, попав прямо
                # в окно замера чужой сделки и исказив её факт-результат.
                # Теперь watchdog СНАЧАЛА получает и сразу отпускает lock —
                # если сделка сейчас в процессе замера, watchdog подождёт её
                # завершения, а не будет работать параллельно.
                async with _capital_measurement_lock:
                    pass
                for sym in list(SYMBOLS):
                    for buy_ex, sell_ex in pairs_for_symbol(sym):
                        # --- Резерв МОНЕТЫ на бирже ПРОДАЖИ (как раньше) ---
                        try:
                            balances = await get_real_balances(session, sell_ex)
                            if balances is None:
                                continue
                            have = balances.get(sym, 0.0)
                            price = await get_valuation_price(session, sell_ex, sym)
                            if not price or price <= 0:
                                continue
                            headroom_mult = 1 + get_headroom_pct(sell_ex) / 100
                            target_qty = (config["max_real_order_usdt"] * config.get("sell_reserve_lots", 3)
                                          / price * headroom_mult)
                            trigger_frac = config.get("reserve_watchdog_trigger_frac", 0.6)
                            if target_qty > 0 and have < target_qty * trigger_frac:
                                shortfall = round(target_qty - have, 4)
                                if shortfall > 0:
                                    logger.info(f"🛡 Reserve watchdog: {sell_ex}/{sym} резерв {have:.2f} "
                                                 f"ниже {trigger_frac*100:.0f}% от цели {target_qty:.2f} — "
                                                 f"докупаю ЗАРАНЕЕ, не дожидаясь попытки сделки")
                                    await top_up_coin_reserve(session, sell_ex, sym, shortfall, price)
                        except Exception as e:
                            logger.error(f"Reserve watchdog {sell_ex}/{sym}: {e}")

                        # --- НОВОЕ 18.08: резерв USDT на бирже ПОКУПКИ ---
                        try:
                            buy_balances = await get_real_balances(session, buy_ex)
                            if buy_balances is None:
                                continue
                            usdt_have = buy_balances.get("USDT", 0.0)
                            usdt_target = config["max_real_order_usdt"] * max(config.get("rebalance_target_lots", 1), 1)
                            trigger_frac = config.get("reserve_watchdog_trigger_frac", 0.6)
                            if usdt_target > 0 and usdt_have < usdt_target * trigger_frac:
                                have_coin_on_buy_ex = buy_balances.get(sym, 0.0)
                                if have_coin_on_buy_ex > 0:
                                    usdt_shortfall = round(usdt_target - usdt_have, 4)
                                    price_on_buy_ex = await get_valuation_price(session, buy_ex, sym)
                                    if price_on_buy_ex and price_on_buy_ex > 0:
                                        logger.info(f"🛡 Reserve watchdog: {buy_ex} USDT {usdt_have:.2f} "
                                                     f"ниже {trigger_frac*100:.0f}% от цели {usdt_target:.2f} — "
                                                     f"продаю излишек {sym} ЗАРАНЕЕ")
                                        await top_up_usdt_via_coin_sale(
                                            session, buy_ex, sym, usdt_shortfall, price_on_buy_ex)
                        except Exception as e:
                            logger.error(f"Reserve watchdog USDT {buy_ex}/{sym}: {e}")
        except Exception as e:
            logger.error(f"Reserve watchdog loop error: {e}")
        await asyncio.sleep(interval)


# ═══════════════════════════════════════════════════════════════
# НОВОЕ 21.08 (по прямому запросу пользователя, после разбора логов
# /realbalance): найдено, что на бирже-продавце (напр. MEXC в связке
# KuCoin→MEXC) со временем накапливается простаивающий USDT — выручка от
# успешных продаж, которая физически не помогает торговле (MEXC никогда
# не покупает в этой схеме, ей не нужен большой запас USDT). Ни один
# существующий механизм (profit_lock, periodic_rebalance, reserve_
# watchdog) не перераспределяет ЭТИ конкретные деньги — они не "излишек
# монеты" (для profit_lock) и не "нехватка USDT" (для watchdog), они
# просто лежат.
#
# ВАЖНО: этот цикл ТОЛЬКО УВЕДОМЛЯЕТ, никогда не переводит деньги сам —
# переводы МЕЖДУ биржами всегда оставались ручным решением пользователя
# (это сознательный, уже сложившийся принцип во всём проекте — бот не
# трогает межбиржевые переводы автоматически).
# ═══════════════════════════════════════════════════════════════
config["idle_usdt_alert_threshold_usdt"] = 2.0   # уведомлять, если простаивает больше этой суммы
config["idle_usdt_alert_interval_sec"] = 3600    # как часто проверять (по умолчанию раз в час)
_last_idle_usdt_alert: Dict[str, float] = {}      # ex -> timestamp последнего уведомления
IDLE_USDT_ALERT_COOLDOWN_SEC = 6 * 3600            # не спамить чаще раза в 6 часов на одну биржу


async def idle_usdt_alert_loop(session):
    """Раз в час (настраивается) проверяет: если на бирже, которая
    НИКОГДА не покупает по своей роли (только продаёт), скопился USDT
    сверх небольшого буфера — присылает уведомление с конкретной
    рекомендацией, куда его вручную перевести (на биржу с дефицитом).
    Кулдаун 6 часов на одну биржу, чтобы не спамить одним и тем же."""
    await asyncio.sleep(300)
    while True:
        interval = config.get("idle_usdt_alert_interval_sec", 3600)
        try:
            if not config["simulation_mode"]:
                threshold = config.get("idle_usdt_alert_threshold_usdt", 2.0)
                # Собираем дефициты USDT по покупающим биржам — чтобы
                # рекомендация была конкретной ("переведи на KuCoin"),
                # а не абстрактной.
                deficits = {}
                for buy_ex in {b for sym in SYMBOLS for b, _ in pairs_for_symbol(sym)}:
                    buy_balances = await get_real_balances(session, buy_ex)
                    if buy_balances is None:
                        continue
                    usdt_have = buy_balances.get("USDT", 0.0)
                    usdt_target = config["max_real_order_usdt"] * max(config.get("rebalance_target_lots", 1), 1)
                    if usdt_have < usdt_target:
                        deficits[buy_ex] = round(usdt_target - usdt_have, 2)

                for sell_ex in {s for sym in SYMBOLS for _, s in pairs_for_symbol(sym)}:
                    # Биржа считается "чистым продавцом" (никогда не покупает),
                    # только если она не встречается как buy_ex ни для одной монеты.
                    is_also_buyer = any(sell_ex == b for sym in SYMBOLS for b, _ in pairs_for_symbol(sym))
                    if is_also_buyer:
                        continue
                    balances = await get_real_balances(session, sell_ex)
                    if balances is None:
                        continue
                    idle_usdt = balances.get("USDT", 0.0)
                    if idle_usdt < threshold:
                        continue
                    now_ts = time.time()
                    if now_ts - _last_idle_usdt_alert.get(sell_ex, 0) < IDLE_USDT_ALERT_COOLDOWN_SEC:
                        continue
                    _last_idle_usdt_alert[sell_ex] = now_ts

                    if deficits:
                        target_ex, target_deficit = max(deficits.items(), key=lambda kv: kv[1])
                        recommendation = (f"Рекомендация: переведи ~${min(idle_usdt, target_deficit):.2f} "
                                           f"с {sell_ex} на {target_ex} вручную (там сейчас дефицит "
                                           f"${target_deficit:.2f}).")
                    else:
                        recommendation = ("Все биржи-покупатели сейчас с достаточным резервом — "
                                           "можно просто оставить как есть или перевести в свой кошелёк.")

                    if CHAT_ID:
                        await send_tg(session,
                            f"💤 *Простаивающий USDT на {sell_ex}: ${idle_usdt:.2f}*\n\n"
                            f"{sell_ex} по своей роли никогда не покупает — этот USDT, "
                            f"скорее всего, выручка от прошлых продаж, физически не "
                            f"участвующая в торговле.\n\n"
                            f"{recommendation}\n\n"
                            f"⚠️ Бот НЕ переводит деньги между биржами сам — это твоё "
                            f"решение, как и всегда."
                        )
        except Exception as e:
            logger.error(f"Idle USDT alert loop error: {e}")
        await asyncio.sleep(interval)



async def lock_in_profit_only(session, ex: str, plan: dict) -> List[dict]:
    """НОВОЕ 12.08: облегчённая версия apply_real_intra_exchange_rebalance —
    ТОЛЬКО продажа излишка (фиксация курсового плюса в USDT), НИКОГДА не
    докупает при дефиците. По прямому запросу пользователя: "при минусе
    просто ждать" — эта функция реализует именно половину логики, не
    трогая вторую.

    ИСПРАВЛЕНИЕ 12.08 (раунд 2, найдено по факту — потеряно ~$0.80): у
    KuCoin (биржа ПОКУПКИ в связке KuCoin→MEXC) целевой резерв монеты
    формально равен НУЛЮ — KuCoin не считается "продавцом" в конфигурации
    PAIRS. Но физически KuCoin с каждой сделкой РЕАЛЬНО покупает и держит
    IOST (продажа идёт из ОТДЕЛЬНОГО резерва на MEXC, монета между
    биржами не переезжает мгновенно) — это давно известный побочный
    эффект архитектуры. Функция ошибочно принимала эту только что
    купленную монету за "курсовой плюс" и тут же продавала её обратно НА
    ТОЙ ЖЕ БИРЖЕ (купили по ask, продали по bid через несколько минут) —
    именно тот эффект пересечения собственного спреда, который убрали
    10.08 для другого механизма. Теперь профит-лок трогает ТОЛЬКО биржи,
    которые реально назначены "продавцом" (coin_target > 0) — настоящий
    резерв на MEXC. Побочное накопление монеты на KuCoin по-прежнему
    обслуживает существующий, более аккуратный механизм
    top_up_usdt_via_coin_sale — он продаёт её РЕАКТИВНО, только когда
    реально не хватает USDT для следующей покупки, а не превентивно
    каждые 5 минут."""
    dry_run = config["real_rebalance_dry_run"]
    actions = []
    coin_targets = plan["coin_targets"]
    min_order = MIN_ORDER_VALUE_USD.get(ex, 5.0)

    for sym, value in plan["coin_values"].items():
        coin_target = coin_targets.get(sym, 0.0)
        if coin_target <= 0:
            continue  # эта биржа НЕ назначена резервом для этой монеты —
                       # любая монета тут просто побочный продукт покупки,
                       # не курсовой плюс. Не трогаем, пусть работает
                       # существующий top_up_usdt_via_coin_sale.
        if value <= coin_target:
            continue  # плюса нет вообще — ничего не делаем, просто ждём
        qty = plan["balances_qty"].get(sym, 0)
        price = value / qty if qty else None
        if not price:
            continue
        excess_usd = value - coin_target
        max_single_sell_usd = config["max_real_order_usdt"] * 3
        if excess_usd > max_single_sell_usd:
            excess_usd = max_single_sell_usd
        if excess_usd < min_order:
            # ИСПРАВЛЕНИЕ 12.08: НЕ продаём весь резерв целиком, если излишек
            # меньше минимума биржи (в отличие от редкого 4-часового
            # ребаланса, где это уместно) — здесь это разрушило бы сам смысл
            # готового резерва для мгновенной торговли, срабатывая каждые
            # 5 минут. Просто ждём, пока излишек сам не подрастёт достаточно.
            continue
        qty_to_sell_raw = min(excess_usd / price, qty * 0.98)
        qty_to_sell = qty_to_sell_raw if dry_run else \
            await round_quantity_for_exchange(session, ex, sym, qty_to_sell_raw)
        if not dry_run and qty_to_sell <= 0:
            continue
        result = "DRY_RUN" if dry_run else None
        if not dry_run:
            if ex == "Binance":
                result = await place_order_binance(session, sym, "SELL", qty_to_sell)
            elif ex == "MEXC":
                result = await place_order_mexc(session, sym, "SELL", qty_to_sell)
            elif ex == "KuCoin":
                result = await place_order_kucoin(session, sym, "sell", qty_to_sell, use_funds=False)
            elif ex == "HTX":
                if _htx_account_id_cache:
                    result = await place_order_htx(session, _htx_account_id_cache, sym, "sell-market", qty_to_sell)
        actions.append({"ex": ex, "symbol": sym, "usd_estimate": round(excess_usd, 2),
                         "success": bool(result), "dry_run": dry_run})
    return actions


async def profit_lock_loop(session):
    """НОВОЕ 12.08: по прямому запросу пользователя — следит за P&L ЧАСТО
    (не раз в 4 часа, как periodic_rebalance_loop) и, как только на КАКОЙ-
    ЛИБО бирже резерв стоит дороже цели (реальный курсовой плюс), сразу
    продаёт излишек в USDT, фиксируя прибыль. При минусе — ничего не
    делает, просто ждёт (докупка дефицита остаётся на существующих
    реактивных механизмах внутри самой сделки, не здесь). Работает НЕЗАВИСИМО
    от periodic_rebalance_loop (тот по-прежнему держит редкий, полный
    ребаланс как подстраховку на случай, если эта проверка что-то пропустит)."""
    await asyncio.sleep(180)  # 3 минуты на старте, чтобы бот успел прочитать первый баланс
    while True:
        try:
            if not config["simulation_mode"] and not config["paused"]:
                all_actions = []
                for ex in ["Binance", "KuCoin", "HTX", "MEXC"]:
                    if ex == "MEXC" and not MEXC_KEY:
                        continue
                    plan = await real_exchange_rebalance_plan(session, ex)
                    if plan is None:
                        continue
                    actions = await lock_in_profit_only(session, ex, plan)
                    all_actions.extend(actions)
                if all_actions and CHAT_ID:
                    lines = "\n".join(
                        f"  {'✅' if a['success'] else '❌'} {a['ex']}/{a['symbol']}: "
                        f"зафиксировано ~${a['usd_estimate']}"
                        + (" (DRY RUN — не реальный ордер)" if a["dry_run"] else "")
                        for a in all_actions
                    )
                    await send_tg(session, f"💰 *Курсовой плюс зафиксирован в USDT:*\n\n{lines}")
        except Exception as e:
            logger.error(f"Profit lock loop error: {e}")
        await asyncio.sleep(config.get("profit_lock_interval_sec", 300))  # по умолчанию раз в 5 минут


async def drawdown_guard_loop(session):
    """НОВОЕ 14.08: предохранитель от чрезмерного курсового минуса — по
    прямому запросу пользователя. Не устраняет сам курсовой шум резерва
    (мы честно обсудили — это невозможно без потери чего-то другого), но
    ограничивает МАКСИМАЛЬНУЮ глубину, прежде чем человек об этом узнает.
    Проверяет каждые 5 минут (та же частота, что и profit_lock_loop) —
    если общий P&L (реальный баланс минус зафиксированный старт) упадёт
    ниже config['max_drawdown_pct']% от стартового капитала — бот САМ
    ставится на паузу и явно предупреждает, вместо того чтобы молча
    продолжать торговать при уже болезненном минусе. Не срабатывает
    повторно на каждой проверке подряд — только один раз при пересечении
    порога, чтобы не спамить одним и тем же предупреждением."""
    await asyncio.sleep(200)
    already_warned = False
    while True:
        try:
            pct = config.get("max_drawdown_pct", 0)
            if (pct > 0 and not config["simulation_mode"] and not config["paused"]
                    and config.get("real_start_capital")):
                real = await get_total_real_capital(session)
                if real:
                    pnl = real["total"] - config["real_start_capital"]
                    pnl_pct = pnl / config["real_start_capital"] * 100
                    if pnl_pct <= -pct:
                        if not already_warned:
                            config["paused"] = True
                            already_warned = True
                            if CHAT_ID:
                                await send_tg(session,
                                    f"🛑 *ПРЕДОХРАНИТЕЛЬ СРАБОТАЛ — торговля поставлена на паузу*\n\n"
                                    f"Общий P&L: {pnl:+.2f} USDT ({pnl_pct:+.1f}% от старта "
                                    f"${config['real_start_capital']}) — превышен порог "
                                    f"-{pct}%.\n\n"
                                    f"Это может быть как реальная торговая потеря, так и "
                                    f"обычный курсовой шум резерва — проверьте `/stats` и "
                                    f"`/realbalance`, прежде чем решать, что делать дальше. "
                                    f"`/go` возобновит торговлю вручную, когда будете готовы.\n\n"
                                    f"Настройка порога: `/setmaxdrawdown` (сейчас {pct}%)")
                    else:
                        already_warned = False  # P&L восстановился выше порога — снимаем флаг,
                                                  # чтобы предупреждение могло сработать снова,
                                                  # если минус повторится в будущем
        except Exception as e:
            logger.error(f"Drawdown guard loop error: {e}")
        await asyncio.sleep(300)


# НОВОЕ 18.08 (по прямому запросу пользователя, Вариант 1): при достижении
# заданного ОБЩЕГО плюса ($0.30 по умолчанию) — бот сам запускает полный
# реальный ребаланс (продажа излишка монеты в USDT на всех биржах, как
# делает /rebalance), а затем ПОДНИМАЕТ стартовую точку (real_start_capital)
# до нового, уже зафиксированного баланса — то есть этот плюс становится
# новым "нулём отсчёта" для дальнейшего P&L, а не просто разовым
# уведомлением. Работает НЕЗАВИСИМО от lock_in_profit_only (тот проверяет
# КАЖДУЮ биржу отдельно раз в 5 минут по избытку резерва; этот — ОБЩИЙ
# P&L всего счёта раз в 5 минут).
config["profit_target_usdt"] = 0.30      # общий плюс, при котором фиксируем
config["profit_target_enabled"] = True   # можно выключить /setprofittarget 0


async def profit_target_lock_loop(session):
    """Проверяет общий P&L раз в 5 минут; при достижении config
    ['profit_target_usdt'] — РЕАЛЬНО продаёт излишек монеты в USDT на
    каждой бирже-продавце (через lock_in_profit_only, БЕЗОПАСНУЮ версию —
    она никогда не продаёт больше излишка и никогда не сливает весь
    резерв целиком, в отличие от полного /rebalance), затем поднимает
    real_start_capital до нового баланса.

    ИСПРАВЛЕНО 21.08 (Вариант Б, по прямому запросу пользователя после
    того как он заметил: "я думал продаём монеты в USDT, а тут по-другому"):
    раньше здесь вызывался real_auto_rebalance_all() — полный ребаланс,
    который у apply_real_intra_exchange_rebalance имеет ОПАСНЫЙ fallback
    "если излишек меньше минимума биржи — продать ВЕСЬ резерв целиком"
    (там это уместно раз в 4 часа для крупной переоценки, но НЕ для
    частой, мелкой фиксации плюса). Из-за этого на практике реальной
    продажи не происходило вообще (излишек $0.2 не проходил порог) —
    "фиксация" была только переносом стартовой точки без изменения
    состава портфеля, ровно то, что пользователь справедливо не ожидал.
    Теперь используется lock_in_profit_only — та же функция, что работает
    в profit_lock_loop, у неё НЕТ опасного fallback, она либо продаёт
    именно излишек (если он больше реального минимума ордера биржи),
    либо честно ничего не делает — не трогая резерв целиком."""
    await asyncio.sleep(220)
    while True:
        try:
            target = config.get("profit_target_usdt", 0)
            if (target > 0 and config.get("profit_target_enabled", True)
                    and not config["simulation_mode"] and not config["paused"]
                    and config.get("real_start_capital")):
                real = await get_total_real_capital(session)
                if real:
                    pnl = real["total"] - config["real_start_capital"]
                    if pnl >= target:
                        if CHAT_ID:
                            await send_tg(session,
                                f"🎯 *Достигнут целевой плюс {target} USDT* "
                                f"(факт: {pnl:+.2f}) — продаю реальный излишек монеты "
                                f"в USDT на биржах-продавцах...")
                        config["paused"] = True  # на время операции, как и везде в коде
                        all_actions = []
                        sold_anything = False
                        for ex in ALL_EXCHANGES:
                            try:
                                plan = await real_exchange_rebalance_plan(session, ex)
                                if not plan:
                                    continue
                                actions = await lock_in_profit_only(session, ex, plan)
                                if actions:
                                    all_actions.extend(actions)
                                    if any(a.get("success") for a in actions):
                                        sold_anything = True
                            except Exception as e:
                                logger.error(f"Profit target lock: ошибка на {ex}: {e}")

                        if CHAT_ID:
                            if all_actions:
                                lines = "\n".join(
                                    f"  {a['ex']}/{a['symbol']}: продано ~${a['usd_estimate']} "
                                    f"({'✅' if a['success'] else '❌'})"
                                    for a in all_actions
                                )
                                await send_tg(session, f"📋 *Действия по фиксации:*\n{lines}")
                            else:
                                await send_tg(session,
                                    "ℹ️ Реальный излишек монеты на всех биржах сейчас меньше "
                                    "технического минимума ордера — физически нечего продать "
                                    "прямо сейчас (это ограничение биржи, не бота). Плюс "
                                    "останется в монете до следующей проверки, когда излишек "
                                    "подрастёт достаточно.")

                        # Поднимаем стартовую точку ПОСЛЕ попытки продажи, по
                        # СВЕЖЕМУ балансу — фиксирует то, что реально удалось
                        # продать (или просто текущий уровень, если продать
                        # было физически нечего).
                        fresh = await get_total_real_capital(session)
                        if fresh:
                            old_start = config["real_start_capital"]
                            config["real_start_capital"] = fresh["total"]
                            if CHAT_ID:
                                sold_note = "" if sold_anything else " (без реальной продажи — см. выше)"
                                await send_tg(session,
                                    f"✅ *Точка отсчёта обновлена{sold_note}.* Новая стартовая точка: "
                                    f"${fresh['total']} (была ${old_start}). Дальше P&L "
                                    f"снова считается от этой суммы, с нуля.")
                        config["paused"] = False
        except Exception as e:
            logger.error(f"Profit target lock loop error: {e}")
        await asyncio.sleep(300)


def get_recent_price_volatility_pct(minutes: int = 15) -> Optional[float]:
    """НОВОЕ 16.08: считает МАКСИМАЛЬНОЕ движение цены (в любую сторону)
    за последние N минут из уже существующей price_history — та же
    инфраструктура, что и честный индикатор тренда в /stats. Возвращает
    None, если данных ещё недостаточно (например, только что запустились)."""
    if len(price_history) < 2:
        return None
    now_ts = time.time()
    cutoff = now_ts - minutes * 60
    recent = [p for ts, p in price_history if ts >= cutoff]
    if len(recent) < 2:
        return None
    lo, hi = min(recent), max(recent)
    if lo <= 0:
        return None
    return round((hi - lo) / lo * 100, 3)


async def volatility_guard_loop(session):
    """НОВОЕ 16.08: по прямому запросу пользователя, после дня с диким
    ралли RVN (+9%/час) и ДВУМЯ подряд фактически убыточными сделками,
    несмотря на красивый спред на бумаге. Идея: при сильной волатильности
    цена уходит, пока обе ноги сделки исполняются — расчётный спред
    перестаёт отражать реальность. Проверяет каждые 2 минуты (чаще, чем
    drawdown_guard — волатильность может измениться быстро); если
    движение цены за последние 15 минут превышает порог — ставит
    торговлю на паузу и предупреждает. НЕ возобновляет автоматически —
    только явно сообщает, когда волатильность спадает обратно, оставляя
    решение о `/go` за человеком."""
    await asyncio.sleep(120)
    already_warned = False
    while True:
        try:
            pct_threshold = config.get("max_volatility_pct_15min", 0)
            if pct_threshold > 0 and not config["simulation_mode"]:
                # НОВОЕ 16.08: записываем цену САМИ, не полагаясь на то,
                # что пользователь вызовет /stats — иначе предохранитель
                # был бы слеп, пока никто не смотрит в чат.
                if SYMBOLS:
                    price_now = await get_valuation_price(session, "MEXC", SYMBOLS[0])
                    if price_now:
                        price_history.append((time.time(), price_now))
                vol = get_recent_price_volatility_pct(15)
                if vol is not None:
                    if vol > pct_threshold:
                        if not already_warned:
                            was_already_paused = config["paused"]
                            config["paused"] = True
                            already_warned = True
                            if CHAT_ID:
                                await send_tg(session,
                                    f"🌪 *ПРЕДОХРАНИТЕЛЬ ВОЛАТИЛЬНОСТИ СРАБОТАЛ — "
                                    f"торговля поставлена на паузу*\n\n"
                                    f"Цена монеты сдвинулась на {vol}% за последние 15 минут "
                                    f"(порог: {pct_threshold}%).\n\n"
                                    f"При такой скорости движения расчётный спред может не "
                                    f"отражать то, что реально достанется при исполнении — "
                                    f"именно это уже дважды подряд случилось 16.08 (карточка "
                                    f"обещала плюс, по факту вышел минус).\n\n"
                                    f"Бот сам напишет, когда волатильность спадёт. `/go` "
                                    f"возобновит вручную, если решите не ждать.\n\n"
                                    f"Настройка порога: `/setmaxvolatility` (сейчас {pct_threshold}%)")
                    else:
                        if already_warned and CHAT_ID:
                            await send_tg(session,
                                f"✅ *Волатильность успокоилась*: {vol}% за 15 минут "
                                f"(было выше {pct_threshold}%).\n\n"
                                f"Можно рассмотреть `/go`, если готовы возобновить торговлю — "
                                f"решение за вами, автоматически бот не возобновляет.")
                        already_warned = False
        except Exception as e:
            logger.error(f"Volatility guard loop error: {e}")
        await asyncio.sleep(120)


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
                    f"K:{stats['depth_fail']['KuCoin']}/H:{stats['depth_fail']['HTX']}/"
                    f"M:{stats['depth_fail'].get('MEXC', 0)}"
                    # ИСПРАВЛЕНО 25.08 (по факту находки): счётчик MEXC уже
                    # существовал в коде (stats["depth_fail"]["MEXC"]), но
                    # НИКОГДА не выводился — ни здесь, ни в /stats, ни в
                    # /wsstatus. Найдено после смены региона Railway на
                    # Singapore: 5733 скана, 0 сигналов, при этом /depthcheck
                    # вручную честно показал реальный спред 3.75% (выше
                    # порога). MEXC не участвует в WS (только Binance/KuCoin/
                    # HTX её используют), значит сканирование берёт MEXC
                    # через отдельный путь — если ОН тихо отказывает при
                    # реальном скане (не при разовой ручной /depthcheck), это
                    # объясняет полное отсутствие сигналов без единой видимой
                    # ошибки где-либо в интерфейсе.
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
                            # УБРАНО 10.08: раньше здесь после КАЖДОЙ успешной
                            # сделки запускался полный авто-ребаланс — задумывался
                            # как подстраховка от нехватки баланса на следующей
                            # попытке. На практике (подтверждено выгрузкой Binance)
                            # это заставляло Binance немедленно выкупать обратно
                            # монету на всю полученную от продажи выручку — то
                            # есть пересекать собственный bid/ask спред биржи на
                            # КАЖДОМ цикле (~0.34% за раз, сопоставимо со всей
                            # маржой сделки!). Кулдаун в 30 сек не спасал, так как
                            # сделки идут раз в ~2 минуты — защита не срабатывала
                            # НИ РАЗУ. Теперь полагаемся на точечные докупки
                            # (top_up_coin_reserve / top_up_usdt_via_coin_sale),
                            # которые срабатывают только когда РЕАЛЬНО не хватает,
                            # а не "на всякий случай" после каждой сделки.
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
        await asyncio.gather(polling_loop(session), scan_loop(session),
                              periodic_rebalance_loop(session), profit_lock_loop(session),
                              drawdown_guard_loop(session), volatility_guard_loop(session),
                              reserve_watchdog_loop(session), profit_target_lock_loop(session),
                              idle_usdt_alert_loop(session))


if __name__ == "__main__":
    asyncio.run(main())
