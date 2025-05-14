#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backfill_and_incremental_4h.py

一个脚本同时完成：
1) 回填过去 90 天的 4H K 线（history-candles & `after` 分页）
2) 拉取自上次存储后新增的 4H K 线（history-candles & `before` 分页）

- bar=4H
- OKX REST 接口 /api/v5/market/history-candles
- 只保存 confirm=="1" 的已完结 K 线
- 并发最多 CONCURRENCY 个交易对
- 全局 20 次/2 秒 令牌桶限速
- 支持重试 & 指数退避
- 写入 TimescaleDB 表 kline_4h
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

# ——— 常量 ———
OKX_URL      = "https://www.okx.com/api/v5/market/history-candles"
BAR          = "4H"
LIMIT        = 100
MAX_RETRIES  = 5
CONCURRENCY  = 5

# 全局令牌桶：20 次／2 秒
rate_limiter = AsyncLimiter(20, 2)

# SSL 上下文，使用 certifi 避免证书验证失败
SSL_CTX = ssl.create_default_context(cafile=certifi.where())

# 日志
logging.basicConfig(format="[%(asctime)s] %(levelname)s %(message)s",
                    level=logging.INFO)
logger = logging.getLogger("backfill_inc_4h")


async def fetch_backfill_chunk(session: aiohttp.ClientSession, inst: str, after_ts: int) -> list:
    """
    回填用分页：after=请求此时间戳之前（更旧）的数据
    """
    params = {"instId": inst, "bar": BAR, "after": after_ts, "limit": LIMIT}
    backoff = 1
    for attempt in range(1, MAX_RETRIES + 1):
        await rate_limiter.acquire()
        try:
            async with session.get(OKX_URL, params=params, timeout=10) as resp:
                j = await resp.json()
        except Exception as e:
            logger.warning(f"{inst} backfill try#{attempt} network error: {e}")
            await asyncio.sleep(backoff)
            backoff = min(backoff*2, 60)
            continue

        code = j.get("code")
        data = j.get("data", []) or []
        if code == "0" and data:
            return data
        if code == "50011":
            wait = min(backoff*5, 60)
            logger.warning(f"{inst} backfill rate limited, sleep {wait}s")
            await asyncio.sleep(wait)
            backoff = min(backoff*2, 60)
            continue

        logger.info(f"{inst} backfill try#{attempt} got code={code}, stop")
        return []
    logger.error(f"{inst} backfill failed after {MAX_RETRIES} attempts")
    return []


async def backfill_inst(pool: asyncpg.Pool, inst: str, since_ts: int):
    """
    回填过去 90 天历史 4H
    """
    logger.info(f">>> [{inst}] backfill history 4H start")
    connector = aiohttp.TCPConnector(ssl=SSL_CTX)
    async with aiohttp.ClientSession(connector=connector) as session:
        after = int(datetime.utcnow().timestamp()*1000) - 1
        total = 0
        insert_sql = """
        INSERT INTO kline_4h(
            symbol, ts, open, high, low, close,
            vol, vol_ccy, vol_ccy_quote, confirm
        ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        ON CONFLICT(symbol, ts) DO NOTHING;
        """
        while after > since_ts:
            batch = await fetch_backfill_chunk(session, inst, after)
            if not batch:
                break
            records = []
            for ts_s, o, h, l, c, vol, volCcy, volCcyQuote, confirm in batch:
                ts = int(ts_s)
                if ts <= since_ts or confirm != "1":
                    continue
                dt = datetime.fromtimestamp(ts/1000.0)
                records.append((
                    inst, dt,
                    float(o), float(h), float(l), float(c),
                    float(vol), float(volCcy), float(volCcyQuote),
                    True
                ))
            if not records:
                break
            await pool.executemany(insert_sql, records)
            total += len(records)
            after = int(batch[-1][0]) - 1
    logger.info(f"<<< [{inst}] backfill 4H done, wrote {total} bars")


async def fetch_incremental_inst(pool: asyncpg.Pool, inst: str, last_ms: int):
    """
    拉增量 4H：before=请求此时间戳之后的新数据
    """
    logger.info(f">>> [{inst}] incremental 4H fetch since {last_ms}")
    connector = aiohttp.TCPConnector(ssl=SSL_CTX)
    async with aiohttp.ClientSession(connector=connector) as session:
        before = last_ms
        total = 0
        insert_sql = """
        INSERT INTO kline_4h(
            symbol, ts, open, high, low, close,
            vol, vol_ccy, vol_ccy_quote, confirm
        ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        ON CONFLICT(symbol, ts) DO NOTHING;
        """
        while True:
            await rate_limiter.acquire()
            params = {"instId": inst, "bar": BAR, "before": before, "limit": LIMIT}
            try:
                async with session.get(OKX_URL, params=params, timeout=10) as resp:
                    j = await resp.json()
            except Exception as e:
                logger.warning(f"{inst} inc fetch network error: {e}")
                break

            code = j.get("code")
            data = j.get("data", []) or []
            if code != "0" or not data:
                break

            records = []
            for ts_s, o, h, l, c, vol, volCcy, volCcyQuote, confirm in data:
                ts = int(ts_s)
                if ts <= last_ms or confirm != "1":
                    continue
                dt = datetime.fromtimestamp(ts/1000.0)
                records.append((
                    inst, dt,
                    float(o), float(h), float(l), float(c),
                    float(vol), float(volCcy), float(volCcyQuote),
                    True
                ))
            if not records:
                break
            await pool.executemany(insert_sql, records)
            total += len(records)
            before = int(data[-1][0])
        logger.info(f"<<< [{inst}] incremental 4H done, wrote {total} bars")


async def make_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        user        = settings.PG_USER,
        password    = settings.PG_PASSWORD,
        database    = settings.PG_DB,
        host        = settings.PG_HOST,
        port        = settings.PG_PORT,
        min_size    = 1,
        max_size    = CONCURRENCY,
        command_timeout = 60
    )


async def main():
    # 回填起点：3 个月前零点
    since_dt = (datetime.utcnow() - timedelta(days=90)) \
               .replace(hour=0, minute=0, second=0, microsecond=0)
    since_ts = int(since_dt.timestamp() * 1000)
    logger.info(f"== Backfill & Incremental 4H since {since_dt.isoformat()} ==")

    pool = await make_pool()
    try:
        # 1) 加载所有 USDT 合约
        rows    = await pool.fetch("SELECT inst_id FROM instruments WHERE inst_id LIKE '%-USDT'")
        symbols = [r["inst_id"] for r in rows]
        logger.info(f"Found {len(symbols)} USDT symbols")

        # 2) 并发补历史
        sem = asyncio.Semaphore(CONCURRENCY)
        async def wf(inst):
            async with sem:
                await backfill_inst(pool, inst, since_ts)
        await asyncio.gather(*(wf(s) for s in symbols))

        # 3) 查每合约最新 ts
        rows = await pool.fetch("""
            SELECT symbol, MAX(EXTRACT(EPOCH FROM ts)*1000::BIGINT) AS last_ms
              FROM kline_4h GROUP BY symbol
        """)
        # 4) 并发拉增量
        async def wi(r):
            await fetch_incremental_inst(pool, r['symbol'], int(r['last_ms'] or 0))
        await asyncio.gather(*(wi(r) for r in rows))

    finally:
        await pool.close()
        logger.info("== All 4H tasks completed ==")


if __name__=="__main__":
    asyncio.run(main())
