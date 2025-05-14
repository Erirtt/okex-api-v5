#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backfill_and_incremental_4h.py

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
from asyncpg.exceptions import ConnectionDoesNotExistError

from config import settings

# —— 常量 —— 
OKX_URL      = "https://www.okx.com/api/v5/market/history-candles"
BAR          = "4H"
LIMIT        = 100
MAX_RETRIES  = 5
CONCURRENCY  = 5

# 全局令牌桶：20 次／2 秒
rate_limiter = AsyncLimiter(20, 2)

# SSL 上下文，使用 certifi 避免证书验证失败
SSL_CTX = ssl.create_default_context(cafile=certifi.where())

# 日志配置
logging.basicConfig(format="[%(asctime)s] %(levelname)s %(message)s",
                    level=logging.INFO)
logger = logging.getLogger("backfill_inc_4h")


async def fetch_chunk(session: aiohttp.ClientSession, inst: str, param: str, ts: int) -> list:
    """
    通用分页请求：param 可为 "after"（回填）或 "before"（增量）
    """
    params = {"instId": inst, "bar": BAR, "limit": LIMIT, param: ts}
    backoff = 1
    for attempt in range(1, MAX_RETRIES + 1):
        await rate_limiter.acquire()
        try:
            async with session.get(OKX_URL, params=params, timeout=10) as resp:
                j = await resp.json()
        except Exception as e:
            logger.warning(f"{inst} {param}#{attempt} 网络错误: {e}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue

        code = j.get("code")
        data = j.get("data", []) or []
        if code == "0" and data:
            return data

        if code == "50011":  # Too Many Requests
            wait = min(backoff * 5, 60)
            logger.warning(f"{inst} {param}#{attempt} 限流, sleep {wait}s")
            await asyncio.sleep(wait)
            backoff = min(backoff * 2, 60)
            continue

        # 其他 code 或空 data，停止分页
        return []
    logger.error(f"{inst} {param} 重试 {MAX_RETRIES} 次后失败")
    return []


async def backfill_inst(conn: asyncpg.Connection, inst: str, since_ts: int):
    """回填过去 90 天历史 4H"""
    logger.info(f">>> [{inst}] 回填历史 4H 开始 (since {since_ts})")
    async with conn.transaction():
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=SSL_CTX)) as session:
            after = int(datetime.utcnow().timestamp() * 1000) - 1
            insert_sql = """
                INSERT INTO kline_4h(
                  symbol, ts, open, high, low, close,
                  vol, vol_ccy, vol_ccy_quote, confirm
                ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                ON CONFLICT(symbol, ts) DO NOTHING;
            """
            total = 0
            while after > since_ts:
                batch = await fetch_chunk(session, inst, "after", after)
                if not batch:
                    break

                records = []
                for ts_s, o, h, l, c, vol, volCcy, volCcyQuote, confirm in batch:
                    ts = int(ts_s)
                    if ts <= since_ts or confirm != "1":
                        continue
                    dt = datetime.fromtimestamp(ts / 1000.0)
                    records.append((
                        inst, dt,
                        float(o), float(h), float(l), float(c),
                        float(vol), float(volCcy), float(volCcyQuote),
                        True
                    ))
                if not records:
                    break

                try:
                    await conn.executemany(insert_sql, records)
                except ConnectionDoesNotExistError as e:
                    logger.warning(f"{inst} 回填写库断开，重试本批: {e}")
                    raise

                total += len(records)
                after = int(batch[-1][0]) - 1

    logger.info(f"<<< [{inst}] 回填历史 4H 完成，共写入 {total} 条")


async def incr_inst(conn: asyncpg.Connection, inst: str, last_ms: int):
    """拉取自上次后续的增量 4H"""
    logger.info(f">>> [{inst}] 增量拉取 4H 开始 (since {last_ms})")
    async with conn.transaction():
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=SSL_CTX)) as session:
            before = last_ms
            insert_sql = """
                INSERT INTO kline_4h(
                  symbol, ts, open, high, low, close,
                  vol, vol_ccy, vol_ccy_quote, confirm
                ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                ON CONFLICT(symbol, ts) DO NOTHING;
            """
            total = 0
            while True:
                batch = await fetch_chunk(session, inst, "before", before)
                if not batch:
                    break

                records = []
                for ts_s, o, h, l, c, vol, volCcy, volCcyQuote, confirm in batch:
                    ts = int(ts_s)
                    if ts <= last_ms or confirm != "1":
                        continue
                    dt = datetime.fromtimestamp(ts / 1000.0)
                    records.append((
                        inst, dt,
                        float(o), float(h), float(l), float(c),
                        float(vol), float(volCcy), float(volCcyQuote),
                        True
                    ))
                if not records:
                    break

                try:
                    await conn.executemany(insert_sql, records)
                except ConnectionDoesNotExistError as e:
                    logger.warning(f"{inst} 增量写库断开，重试本批: {e}")
                    raise

                total += len(records)
                before = int(batch[-1][0])

    logger.info(f"<<< [{inst}] 增量 4H 完成，共写入 {total} 条")


async def make_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        user=settings.PG_USER, password=settings.PG_PASSWORD,
        database=settings.PG_DB, host=settings.PG_HOST,
        port=settings.PG_PORT, min_size=1, max_size=CONCURRENCY,
        command_timeout=60,
        max_inactive_connection_lifetime=300
    )


async def main():
    # 回填起点：3 个月前零点
    since_dt = (datetime.utcnow() - timedelta(days=90))\
               .replace(hour=0, minute=0, second=0, microsecond=0)
    since_ts = int(since_dt.timestamp() * 1000)
    logger.info(f"== 回填 + 增量 4H K 线 (since {since_dt.isoformat()}) ==")

    pool = await make_pool()
    try:
        # 1) 取所有 USDT 合约
        rows = await pool.fetch("SELECT inst_id FROM instruments WHERE inst_id LIKE '%-USDT'")
        symbols = [r["inst_id"] for r in rows]
        logger.info(f"Found {len(symbols)} USDT 合约")

        sem = asyncio.Semaphore(CONCURRENCY)

        # 2) 并发补历史
        async def wf(sym):
            async with sem:
                async with pool.acquire() as conn:
                    await backfill_inst(conn, sym, since_ts)
        await asyncio.gather(*(wf(s) for s in symbols))

        # 3) 查询每个合约最新时间戳
        rows = await pool.fetch("""
            SELECT symbol, MAX(EXTRACT(EPOCH FROM ts)*1000::BIGINT) AS last_ms
              FROM kline_4h GROUP BY symbol
        """)
        # 4) 并发拉增量
        async def wi(r):
            async with sem:
                async with pool.acquire() as conn:
                    await incr_inst(conn, r['symbol'], int(r['last_ms'] or 0))
        await asyncio.gather(*(wi(r) for r in rows))

    finally:
        await pool.close()
        logger.info("== 全部 4H 任务完成，连接池已关闭 ==")


if __name__ == "__main__":
    asyncio.run(main())
