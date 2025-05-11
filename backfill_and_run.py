#!/usr/bin/env python3
# backfill_and_run.py

import asyncio
import logging
from datetime import datetime
import ssl
import certifi
import aiohttp
import asyncpg

from config import settings
from ws_manager import WSManager
from kline_processor import KlineProcessor

# ———— 日志配置 ————
logging.basicConfig(
    format="[%(asctime)s] %(levelname)s %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("backfill_and_run")

# ———— 常量 ————
OKX_REST = "https://www.okx.com"
CG_REST  = "https://api.coingecko.com/api/v3"
SSL_CTX  = ssl.create_default_context(cafile=certifi.where())

class UnifiedClient:
    def __init__(self, rate_limit_per_sec: float = 10):
        self._sema = asyncio.Semaphore(rate_limit_per_sec)

    async def fetch_top_market_caps(self, n: int = 30) -> list[str]:
        """
        通过 CoinGecko API 获取市值前 n 名的币种 symbol，
        并拼成 OKX 格式 instId（如 "BTC-USDT"）。
        """
        url    = f"{CG_REST}/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": n,
            "page": 1,
            "sparkline": "false"
        }
        await self._sema.acquire()
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.get(url, params=params, ssl=SSL_CTX) as resp:
                    data = await resp.json()
        finally:
            self._sema.release()

        if not isinstance(data, list):
            logger.error("CoinGecko fetch error: %s", data)
            return []

        return [f"{coin['symbol'].upper()}-USDT" for coin in data if coin.get("symbol")]

    async def fetch_candles(self, instId: str, limit: int = 200) -> list:
        """
        OKX 历史分钟线：
        GET /api/v5/market/candles?instId=...&bar=1m&limit=...
        返回 list of [ts, open, high, low, close, volume]
        """
        url    = f"{OKX_REST}/api/v5/market/candles"
        params = {"instId": instId, "bar":"1m", "limit": limit}

        await self._sema.acquire()
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.get(url, params=params, ssl=SSL_CTX) as resp:
                    j = await resp.json()
        finally:
            self._sema.release()

        if j.get("code") != "0":
            logger.error("fetch_candles %s error: %s", instId, j)
            return []
        return j["data"]


async def backfill_symbols(pool: asyncpg.Pool, client: UnifiedClient, symbols: list, limit: int):
    """
    批量回填历史分钟线
    """
    insert_sql = """
    INSERT INTO kline_1m_agg(symbol, ts, open, high, low, close, volume)
    VALUES($1,$2,$3,$4,$5,$6,$7)
    ON CONFLICT (symbol, ts) DO NOTHING
    """
    for inst in symbols:
        data = await client.fetch_candles(inst, limit)
        records = []
        for item in data:
            ts = datetime.fromtimestamp(int(item[0]) / 1000.0)
            o, h, l, c, v = map(float, item[1:6])
            records.append((inst, ts, o, h, l, c, v))
        if not records:
            continue
        async with pool.acquire() as conn:
            await conn.executemany(insert_sql, records)
        logger.info("Backfilled %d bars for %s", len(records), inst)


async def main():
    # 1. 获取 top30
    client = UnifiedClient(rate_limit_per_sec=10)
    symbols = await client.fetch_top_market_caps(30)
    logger.info("Top symbols: %s", symbols)

    # 2. 建立 DB 池
    pool = await asyncpg.create_pool(
        user=settings.PG_USER, password=settings.PG_PASSWORD,
        database=settings.PG_DB, host=settings.PG_HOST,
        port=settings.PG_PORT, min_size=1, max_size=5
    )

    # 2.1 确保唯一约束存在
    await pool.execute(f"""
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid = 'kline_1m_agg'::regclass
              AND contype = 'u'
          ) THEN
            ALTER TABLE kline_1m_agg
            ADD CONSTRAINT uniq_symbol_ts UNIQUE(symbol, ts);
          END IF;
        END
        $$;
    """)

    # 3. 回填历史数据
    await backfill_symbols(pool, client, symbols, limit=200)

    # 4. 启动实时订阅
    processor = await KlineProcessor.create(
        {"user":settings.PG_USER,
         "password":settings.PG_PASSWORD,
         "database":settings.PG_DB,
         "host":settings.PG_HOST,
         "port":settings.PG_PORT},
        table="kline_1m_agg"
    )

    channels = [{"channel":"candle1m","instId":sym} for sym in symbols]
    async def handler(msg):
        await processor.process(msg)

    WSManager(
        url=settings.OKX_PUBLIC_URL,
        channels=channels,
        handler=handler
    )

    # 5. 持续运行
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
