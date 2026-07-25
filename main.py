"""
ЭТАП: WebSocket order book для Binance (первая из трёх бирж).
Тестируемый модуль. Не подключён к основному боту.

Задача: поддерживать локальную копию стакана через WebSocket diff-события,
вместо REST-запроса /depth каждые 10 секунд. REST используется только один
раз при старте (snapshot) и при пересинхронизации, если что-то разошлось.

Логика синхронизации — точно по документации Binance:
https://binance-docs.github.io/apidocs/spot/en/#how-to-manage-a-local-order-book-correctly
1. Открываем WS-стрим на <symbol>@depth, начинаем буферизовать события
2. Забираем REST snapshot (/api/v3/depth?limit=1000) с lastUpdateId
3. Отбрасываем события из буфера, где u <= lastUpdateId
4. Первое применяемое событие должно иметь U <= lastUpdateId+1 <= u
5. Дальше каждое следующее событие: его U должно быть равно (предыдущий u)+1
   — если не совпало, стакан рассинхронизирован, нужна пересинхронизация
6. Количество в событии — абсолютное, не дельта. qty=0 значит уровень убрать.
"""

import asyncio
import json
import time
import logging
from typing import Dict, Optional, List, Tuple
import aiohttp

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

WS_BASE = "wss://stream.binance.com:9443/ws"
REST_SNAPSHOT_URL = "https://api.binance.com/api/v3/depth"


class BinanceLocalOrderBook:
    """Живой локальный стакан для ОДНОГО символа (напр. 'BONKUSDT').
    Использование:
        book = BinanceLocalOrderBook("BONKUSDT")
        asyncio.create_task(book.run())
        ... где-то в другом месте ...
        snapshot = book.get_book(depth=50)  # мгновенно, без сетевого запроса
    """

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

    # -----------------------------------------------------------------
    # ПУБЛИЧНЫЙ API — то, чем будет пользоваться остальной бот
    # -----------------------------------------------------------------

    def get_book(self, depth: int = 50) -> Optional[Dict[str, List[Tuple[float, float]]]]:
        """Мгновенный снимок локального стакана. None, если ещё не синхронизирован
        или данные устарели (нет обновлений >10 сек — соединение могло умереть)."""
        if not self.synced:
            return None
        if time.time() - self.last_event_time > 10:
            logger.warning(f"{self.symbol}: нет обновлений >10 сек, данные могут быть устаревшими")
            return None
        bids_sorted = sorted(self.bids.items(), key=lambda x: -x[0])[:depth]
        asks_sorted = sorted(self.asks.items(), key=lambda x: x[0])[:depth]
        return {"bids": bids_sorted, "asks": asks_sorted}

    def is_healthy(self) -> bool:
        return self.synced and (time.time() - self.last_event_time) < 10

    async def stop(self):
        self._stop = True

    # -----------------------------------------------------------------
    # ВНУТРЕННЯЯ ЛОГИКА
    # -----------------------------------------------------------------

    async def _get_snapshot(self, session: aiohttp.ClientSession) -> Optional[dict]:
        params = {"symbol": self.symbol, "limit": self.depth_limit}
        try:
            async with session.get(REST_SNAPSHOT_URL, params=params,
                                    timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    logger.error(f"{self.symbol}: snapshot HTTP {r.status}")
                    return None
                return await r.json()
        except Exception as e:
            logger.error(f"{self.symbol}: snapshot exception {e}")
            return None

    async def _resync(self, session: aiohttp.ClientSession):
        """Полная пересинхронизация: чистим стакан, берём новый snapshot,
        дальше применяем накопленный буфер событий заново."""
        self.resync_count += 1
        logger.info(f"{self.symbol}: пересинхронизация #{self.resync_count}")
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

        # Применяем то, что накопилось в буфере, пока брали snapshot
        applied_first = False
        for event in list(self._buffer):
            if event["u"] <= self.last_update_id:
                continue  # событие старше snapshot — не нужно
            if not applied_first:
                if not (event["U"] <= self.last_update_id + 1 <= event["u"]):
                    # snapshot не попадает в этот диапазон событий — буфер
                    # ещё не догнал snapshot по времени, ждём следующих событий
                    continue
                applied_first = True
            self._apply_event(event)
        self._buffer.clear()
        if applied_first or self.last_update_id:
            self.synced = True
            self.last_event_time = time.time()
            logger.info(f"{self.symbol}: синхронизирован, "
                        f"bids={len(self.bids)} asks={len(self.asks)}")

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

    async def run(self):
        """Основной цикл: подключается, поддерживает стакан, переподключается
        при разрыве. Предполагается запуск как asyncio.create_task(book.run())."""
        stream_url = f"{WS_BASE}/{self.symbol.lower()}@depth"
        backoff = 1

        async with aiohttp.ClientSession() as session:
            while not self._stop:
                try:
                    async with session.ws_connect(stream_url, heartbeat=20) as ws:
                        logger.info(f"{self.symbol}: WS подключен")
                        backoff = 1  # сброс backoff при успешном подключении
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
                                # После пары событий в буфере — берём snapshot
                                if len(self._buffer) >= 2:
                                    await self._resync(session)
                                    need_snapshot = False
                                continue

                            # Проверка непрерывности последовательности
                            if self.last_update_id and event["U"] != self.last_update_id + 1:
                                logger.warning(f"{self.symbol}: разрыв последовательности "
                                                f"(ожидали U={self.last_update_id+1}, "
                                                f"получили U={event['U']}) — пересинхронизация")
                                self._buffer = [event]
                                need_snapshot = True
                                continue

                            self._apply_event(event)

                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"{self.symbol}: WS ошибка {e}, реконнект через {backoff} сек")
                    self.synced = False

                if self._stop:
                    break
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)  # экспоненциальный backoff, потолок 60 сек


# =====================================================================
# ТЕСТ — запускается напрямую: python binance_ws_orderbook.py
# =====================================================================

async def _test():
    symbols = ["BONKUSDT", "SEIUSDT"]
    books = {s: BinanceLocalOrderBook(s) for s in symbols}
    tasks = [asyncio.create_task(b.run()) for b in books.values()]

    print("Жду синхронизации (до 15 сек)...")
    await asyncio.sleep(15)

    for _ in range(6):  # 6 проверок с интервалом 5 сек = 30 сек теста
        print(f"\n{'='*60}")
        for sym, book in books.items():
            snap = book.get_book(depth=5)
            print(f"{sym}: synced={book.synced} healthy={book.is_healthy()} "
                  f"events={book.event_count} resyncs={book.resync_count}")
            if snap:
                print(f"  best bid: {snap['bids'][0] if snap['bids'] else None}")
                print(f"  best ask: {snap['asks'][0] if snap['asks'] else None}")
        await asyncio.sleep(5)

    for t in tasks:
        t.cancel()


if __name__ == "__main__":
    asyncio.run(_test())
