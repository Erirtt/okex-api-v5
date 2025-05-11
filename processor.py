# processor.py
import asyncio
from collections import deque
import asyncpg
import os

class KlineProcessor:
    def __init__(self, batch_size=50, batch_interval=0.5):
        self.queue = deque()
        self.batch_size = batch_size
        self.batch_interval = batch_interval
        self._pool = None
        asyncio.create_task(self._batch_writer())

    async def init_db(self):
        self._pool = await asyncpg.create_pool(
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            database=os.getenv("PG_DB"),
            host=os.getenv("PG_HOST"),
            port=int(os.getenv("PG_PORT")),
            min_size=1, max_size=5
        )

    async def process(self, raw_msg):
        # 解析 OKX 返回的 kline 数据
        # 假设 raw_msg = {"arg": {...}, "data":[{"ts":..., "o":..., "h":..., "l":..., "c":..., "v":...}, ...]}
        recs = raw_msg.get("data") or []
        for d in recs:
            symbol = raw_msg["arg"]["instId"]
            ts = d["ts"]       # 毫秒时间戳
            o, h, l, c = d["o"], d["h"], d["l"], d["c"]
            v = d["v"]
            self.queue.append((symbol, ts, o, h, l, c, v))

    async def _batch_writer(self):
        # 等待数据库连接池就绪
        while self._pool is None:
            try:
                await self.init_db()
            except Exception:
                await asyncio.sleep(1)
        while True:
            await asyncio.sleep(self.batch_interval)
            batch = []
            while self.queue and len(batch) < self.batch_size:
                batch.append(self.queue.popleft())
            if not batch:
                continue
            # 批量写入
            async with self._pool.acquire() as conn:
                await conn.executemany(
                    """
                    INSERT INTO kline_1m(symbol, ts, open, high, low, close, volume)
                    VALUES($1, to_timestamp($2/1000.0), $3, $4, $5, $6, $7)
                    ON CONFLICT (symbol, ts) DO UPDATE
                      SET open=EXCLUDED.open,
                          high=EXCLUDED.high,
                          low=EXCLUDED.low,
                          close=EXCLUDED.close,
                          volume=EXCLUDED.volume
                    """,
                    batch
                )
