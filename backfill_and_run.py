#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backfill_and_run.py

1. 从 CoinGecko 获取市值前 50 的币种 symbol（如 "btc"、"eth" …），转为大写 base_ccy 列表。
2. 从 PostgreSQL 的 instruments 表中筛选出 base_ccy 在列表中且 inst_id 以 "-USDT" 结尾的合约 inst_id。
3. 使用筛选后的 inst_id 列表去回填历史分钟线并启动实时订阅。
"""
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

# ——— 日志配置 —————————————————————————
logging.basicConfig(
    format="[%(asctime)s] %(levelname)s %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("backfill_and_run")

# ——— 常量 —————————————————————————————
OKX_REST = "https://www.okx.com"
CG_REST  = "https://api.coingecko.com/api/v3"
SSL_CTX  = ssl.create_default_context(cafile=certifi.where())

class UnifiedClient:
    def __init__(self, rate_limit_per_sec: float = 10):
        self._sema = asyncio.Semaphore(rate_limit_per_sec)

    async def fetch_top_market_caps(self, n: int = 50) -> list[str]:
        """
        通过 CoinGecko API 获取市值前 n 名的币种 symbol 小写，
        返回大写列表 ["BTC","ETH",…]
        """
        url    = f"{CG_REST}/coins/markets"
        params = {
            "vs_currency": "usd",
            "order":       "market_cap_desc",
            "per_page":    n,
            "page":        1,
            "sparkline":   "false"
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
        # 提取 symbol、去重、转大写
        return [coin["symbol"].upper() for coin in data if coin.get("symbol")]

    async def fetch_candles(self, instId: str, limit: int = 200) -> list:
        """
        OKX 历史分钟线：GET /api/v5/market/candles?instId=...&bar=1m&limit=...
        返回 list of [ts, open, high, low, close, volume]
        """
        url    = f"{OKX_REST}/api/v5/market/candles"
        params = {"instId": instId, "bar": "1m", "limit": limit}

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
    client = UnifiedClient(rate_limit_per_sec=10)

    # 1. 取市值前50的 base_ccy
    base_ccys = await client.fetch_top_market_caps(50)
    logger.info("Top50 base_ccy: %s", base_ccys)

    # 2. 建立 DB 池
    pool = await asyncpg.create_pool(
        user=settings.PG_USER, password=settings.PG_PASSWORD,
        database=settings.PG_DB, host=settings.PG_HOST,
        port=settings.PG_PORT, min_size=1, max_size=5
    )

    # 3. 从 instruments 表筛选 inst_id
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT inst_id
            FROM instruments
            WHERE base_ccy = ANY($1::text[])
              AND inst_id LIKE '%-USDT'
            """,
            base_ccys
        )
    inst_ids = [r["inst_id"] for r in rows]
    logger.info("Filtered USDT inst_ids: %s", inst_ids)

    if not inst_ids:
        logger.error("No matching USDT inst_ids found; exiting.")
        await pool.close()
        return

    # 4. 回填历史
    await backfill_symbols(pool, client, inst_ids, limit=200)

    # 5. 启动实时订阅
    processor = await KlineProcessor.create(
        {"user":settings.PG_USER,
         "password":settings.PG_PASSWORD,
         "database":settings.PG_DB,
         "host":settings.PG_HOST,
         "port":settings.PG_PORT},
        table="kline_1m_agg"
    )

    channels = [{"channel":"candle1m","instId":sym} for sym in inst_ids]
    async def handler(msg):
        await processor.process(msg)

    WSManager(
        url=settings.OKX_PUBLIC_URL,
        channels=channels,
        handler=handler
    )

    # 6. 持续运行
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
