#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backfill_mark_price_3m.py

使用 OKX mark-price-candles 接口并分页回填最近 3 个月的 1m K 线：
- endpoint: GET /api/v5/market/mark-price-candles
- 每次最多拉 100 条（limit 上限）
- 只保存已完结 K 线 (confirm=="1")
- 并发 5 个交易对，重试限流/超时
- 写入 kline_1m_agg(symbol, ts, open, high, low, close, volume)
"""

import asyncio
import aiohttp
import asyncpg
import logging
import ssl
import certifi
from datetime import datetime, timedelta

from config import settings

# ——— 配置 ———
OKX_MARKPRICE_URL = "https://www.okx.com/api/v5/market/mark-price-candles"
MAX_LIMIT         = 100    # 官方最大值
MAX_RETRIES       = 5
CONCURRENCY       = 5

logging.basicConfig(format="[%(asctime)s] %(levelname)s %(message)s",
                    level=logging.INFO)
logger = logging.getLogger("backfill")

# SSL context to avoid cert errors
SSL_CTX = ssl.create_default_context(cafile=certifi.where())


async def fetch_chunk(session: aiohttp.ClientSession, instId: str, after: int) -> list:
    """
    分页拉取更旧的 mark-price-candles。
    :param session: aiohttp session
    :param instId: 合约
    :param after: 毫秒时间戳，仅返回 ts < after 的数据
    """
    params = {
        "instId": instId,
        "bar":    "1m",
        "after":  after,
        "limit":  MAX_LIMIT
    }
    backoff = 1
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(OKX_MARKPRICE_URL, params=params, timeout=10) as resp:
                j = await resp.json()
        except Exception as e:
            logger.warning("%s attempt %d exception: %s", instId, attempt, e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue

        code = j.get("code")
        if code == "0" and j.get("data"):
            return j["data"]

        # 限流
        if code == "50011":
            wait = min(backoff * 5, 60)
            logger.warning("%s attempt %d Too Many Requests, sleeping %ds",
                           instId, attempt, wait)
            await asyncio.sleep(wait)
            backoff = min(backoff * 2, 60)
            continue

        # 其他错误也退避
        logger.warning("%s attempt %d failed: %s", instId, attempt, j)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 30)

    logger.error("%s failed after %d attempts, skipping chunk", instId, MAX_RETRIES)
    return []


async def backfill_symbol(pool: asyncpg.Pool, instId: str, since_ts: int):
    """
    回填单个交易对。
    :param pool: asyncpg pool
    :param instId: 合约标识
    :param since_ts: 三个月前的毫秒时间戳
    """
    total = 0
    connector = aiohttp.TCPConnector(ssl=SSL_CTX)
    async with aiohttp.ClientSession(connector=connector) as session:
        # 从最新开始，向前 paginate
        after = int(datetime.utcnow().timestamp() * 1000)
        insert_sql = """
            INSERT INTO kline_1m_agg(symbol, ts, open, high, low, close, volume)
            VALUES($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT(symbol, ts) DO NOTHING;
        """
        while after > since_ts:
            data = await fetch_chunk(session, instId, after)
            if not data:
                break

            records = []
            for item in data:
                ts_ms   = int(item[0])
                # confirm== "1" 表示已完结
                confirm = item[5]
                if confirm != "1":
                    continue
                if ts_ms <= since_ts:
                    after = 0
                    break
                ts = datetime.fromtimestamp(ts_ms / 1000.0)
                o, h, l, c = map(float, item[1:5])
                # mark-price K 线没有 volume，写 0
                records.append((instId, ts, o, h, l, c, 0.0))

            if not records:
                break

            # 批量写库
            await pool.executemany(insert_sql, records)
            total += len(records)
            # 下一页 after = 本页最老一条的 ts_ms - 1
            oldest = int(data[-1][0])
            after = oldest - 1

    logger.info("%s ▶️ backfilled %d bars", instId, total)


async def main():
    # 1) since_ts = 三个月前
    since_dt = datetime.utcnow() - timedelta(days=90)
    since_ts = int(since_dt.timestamp() * 1000)
    logger.info("Backfill mark-price since: %s", since_dt.isoformat())

    # 2) 建 asyncpg 池
    pool = await asyncpg.create_pool(
        user=settings.PG_USER, password=settings.PG_PASSWORD,
        database=settings.PG_DB, host=settings.PG_HOST,
        port=settings.PG_PORT, min_size=1, max_size=CONCURRENCY
    )

    # 3) 读所有 USDT 合约
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT inst_id FROM instruments WHERE inst_id LIKE '%-USDT'"
        )
    symbols = [r['inst_id'] for r in rows]
    logger.info("Found %d symbols", len(symbols))

    # 4) 并发回填
    sem = asyncio.Semaphore(CONCURRENCY)
    async def worker(sym):
        async with sem:
            try:
                await backfill_symbol(pool, sym, since_ts)
            except Exception as e:
                logger.error("%s ▶️ backfill error: %s", sym, e)

    tasks = [asyncio.create_task(worker(sym)) for sym in symbols]
    await asyncio.gather(*tasks)

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
