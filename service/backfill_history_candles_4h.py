#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backfill_history_candles_4h.py

使用 OKX REST API `/api/v5/market/history-candles` 回填最近三个月的 4H K 线，
并在请求层面严格遵守 20 次／2 秒的 IP 限速规则，避免频繁被限流。

- bar=4H
- 分页参数 after（请求此时间戳之前的数据）
- 只保存已完结（confirm=="1"）的 K 线
- 并发最多 CONCURRENCY 个交易对
- 全局令牌桶限流：20 次／2 秒
- 支持重试 & 指数退避
- 写入 PostgreSQL 表 kline_4h
"""

import asyncio
import aiohttp
import asyncpg
import logging
import ssl
import certifi
from datetime import datetime, timedelta
from aiolimiter import AsyncLimiter

from config import settings

# ——— 配置 ———
OKX_URL      = "https://www.okx.com/api/v5/market/history-candles"
BAR          = "4H"
LIMIT        = 100
MAX_RETRIES  = 5
CONCURRENCY  = 5

# 全局 20 次／2 秒 令牌桶
rate_limiter = AsyncLimiter(20, 2)

# SSL 上下文，使用 certifi 避免证书验证失败
SSL_CTX = ssl.create_default_context(cafile=certifi.where())

logging.basicConfig(
    format="[%(asctime)s] %(levelname)s %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("backfill_4h")


async def fetch_chunk(session: aiohttp.ClientSession, inst_id: str, after_ts: int) -> list:
    params = {"instId": inst_id, "bar": BAR, "after": after_ts, "limit": LIMIT}
    backoff = 1

    for attempt in range(1, MAX_RETRIES + 1):
        # —— 全局限速 —— 
        await rate_limiter.acquire()

        try:
            async with session.get(OKX_URL, params=params, timeout=10) as resp:
                j = await resp.json()
        except Exception as e:
            logger.warning(f"{inst_id} try#{attempt} network error: {e}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue

        code = j.get("code")
        data = j.get("data", [])
        if code == "0" and data:
            return data

        if code == "50011":  # Too Many Requests
            wait = min(backoff * 5, 60)
            logger.warning(f"{inst_id} rate limited by server, sleeping {wait}s")
            await asyncio.sleep(wait)
            backoff = min(backoff * 2, 60)
            continue

        logger.info(f"{inst_id} try#{attempt} got code={code}, stop paging")
        return []

    logger.error(f"{inst_id} failed after {MAX_RETRIES} attempts")
    return []


async def backfill_inst(pool: asyncpg.Pool, inst_id: str, since_ts: int):
    logger.info(f">>> [{inst_id}] backfill 4H start")
    connector = aiohttp.TCPConnector(ssl=SSL_CTX)
    async with aiohttp.ClientSession(connector=connector) as session:
        # 从当前时间向前翻页
        after = int(datetime.utcnow().timestamp() * 1000) - 1
        total = 0

        insert_sql = """
        INSERT INTO kline_4h(
            symbol, ts, open, high, low, close,
            vol, vol_ccy, vol_ccy_quote, confirm
        ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        ON CONFLICT(symbol, ts) DO NOTHING;
        """

        while after > since_ts:
            batch = await fetch_chunk(session, inst_id, after)
            if not batch:
                break

            records = []
            for ts_s, o, h, l, c, vol, volCcy, volCcyQuote, confirm in batch:
                ts = int(ts_s)
                if ts <= since_ts or confirm != "1":
                    continue
                dt = datetime.fromtimestamp(ts / 1000.0)
                records.append((
                    inst_id, dt,
                    float(o), float(h), float(l), float(c),
                    float(vol), float(volCcy), float(volCcyQuote),
                    True
                ))

            if not records:
                break

            await pool.executemany(insert_sql, records)
            total += len(records)
            after = int(batch[-1][0]) - 1

    logger.info(f"<<< [{inst_id}] backfill 4H done, wrote {total} bars")


async def make_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        user     = settings.PG_USER,
        password = settings.PG_PASSWORD,
        database = settings.PG_DB,
        host     = settings.PG_HOST,
        port     = settings.PG_PORT,
        min_size = 1,
        max_size = CONCURRENCY,
        command_timeout = 60
    )


async def main():
    # 回填起点：3 个月前 0 点
    since_dt = (datetime.utcnow() - timedelta(days=90)) \
               .replace(hour=0, minute=0, second=0, microsecond=0)
    since_ts = int(since_dt.timestamp() * 1000)
    logger.info(f"== Backfill history-candles 4H since {since_dt.isoformat()} ==")

    pool = await make_pool()
    try:
        rows    = await pool.fetch("SELECT inst_id FROM instruments WHERE inst_id LIKE '%-USDT'")
        symbols = [r["inst_id"] for r in rows]
        logger.info(f"Found {len(symbols)} USDT symbols to backfill 4H")

        sem = asyncio.Semaphore(CONCURRENCY)
        async def worker(sym: str):
            async with sem:
                try:
                    await backfill_inst(pool, sym, since_ts)
                except Exception as e:
                    logger.error(f"{sym} ▶️ backfill 4H error: {e}")

        await asyncio.gather(*(worker(sym) for sym in symbols))
    finally:
        await pool.close()
        logger.info("== All 4H backfill tasks completed ==")


if __name__ == "__main__":
    asyncio.run(main())
