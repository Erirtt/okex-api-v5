import asyncio
from collections import deque, defaultdict
from datetime import datetime, timezone
import asyncpg
import logging

logger = logging.getLogger("kline_processor")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(message)s"))
logger.addHandler(handler)

class KlineProcessor:
    def __init__(self, table: str = "kline_1m_agg",
                 batch_size: int = 200, batch_interval: float = 2.0):
        self.table = table
        self.batch_size = batch_size
        self.batch_interval = batch_interval
        self._pool = None
        self.queue = deque()
        self.current: dict[str, dict] = defaultdict(lambda: None)

    @classmethod
    async def create(cls, pg_conf: dict, **kwargs):
        """
        异步工厂：初始化连接池、建表、启动后台写任务
        """
        self = cls(**kwargs)
        self._pool = await asyncpg.create_pool(
            user=pg_conf["user"],
            password=pg_conf["password"],
            database=pg_conf["database"],
            host=pg_conf["host"],
            port=pg_conf["port"],
            min_size=1,
            max_size=3,
        )
        async with self._pool.acquire() as conn:
            # 建表
            await conn.execute(f"""
              CREATE TABLE IF NOT EXISTS {self.table} (
                symbol TEXT,
                ts TIMESTAMPTZ,
                open DOUBLE PRECISION,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                close DOUBLE PRECISION,
                volume DOUBLE PRECISION
              );
            """)
            # 唯一约束
            await conn.execute(f"""
            DO $$
            BEGIN
              IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conrelid = '{self.table}'::regclass
                  AND contype = 'u'
              ) THEN
                ALTER TABLE {self.table}
                ADD CONSTRAINT uniq_symbol_ts UNIQUE(symbol, ts);
              END IF;
            END
            $$;
            """)
        # 启动后台批量写入
        asyncio.create_task(self._batch_writer())
        logger.info("KlineProcessor initialized, writing into '%s'", self.table)
        return self

    async def process(self, raw: dict):
        arg = raw.get("arg", {})
        chan = arg.get("channel", "")
        if "candle1m" not in chan or "data" not in raw:
            return

        symbol = arg["instId"]
        for item in raw["data"]:
            try:
                ts_ms = int(item[0])
                ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
                o, h, l, c, v = map(float, item[1:6])
            except Exception as e:
                logger.warning("Invalid kline data %s: %s", item, e)
                continue

            bucket = ts.replace(second=0, microsecond=0)
            bar = self.current.get(symbol)
            if not bar or bucket != bar["bucket"]:
                if bar:
                    self.queue.append((
                        symbol, bar["bucket"],
                        bar["open"], bar["high"],
                        bar["low"], bar["close"], bar["vol"]
                    ))
                bar = {"bucket": bucket, "open": o, "high": h, "low": l, "close": c, "vol": v}
            else:
                bar["high"]  = max(bar["high"],  h)
                bar["low"]   = min(bar["low"],   l)
                bar["close"] = c
                bar["vol"]   = v

            self.current[symbol] = bar

    async def _batch_writer(self):
        """
        后台批量写入：每 batch_interval 检查一次队列，逐条写入并忽略重复
        """
        insert_sql = (
            f"INSERT INTO {self.table}"
            "(symbol, ts, open, high, low, close, volume) "
            "VALUES($1, $2, $3, $4, $5, $6, $7) "
            "ON CONFLICT (symbol, ts) DO NOTHING"
        )
        while True:
            await asyncio.sleep(self.batch_interval)
            if not self.queue:
                continue

            batch = []
            while self.queue and len(batch) < self.batch_size:
                batch.append(self.queue.popleft())

            async with self._pool.acquire() as conn:
                for rec in batch:
                    try:
                        await conn.execute(insert_sql, *rec)
                    except asyncpg.exceptions.UniqueViolationError:
                        # 已存在，忽略
                        continue
                    except Exception as e:
                        logger.exception("Failed to insert record %s: %s", rec, e)

            logger.info("Processed batch of %d records", len(batch))

    async def close(self):
        """优雅关闭连接池"""
        if self._pool:
            await self._pool.close()
            logger.info("Closed PostgreSQL connection pool")
