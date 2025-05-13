#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backfill_markprice_3m_1h.py

从“今天”0点 UTC 开始，向前 3 个月回填 OKX 标记价格 1H K 线到 kline_markprice_1h 表，
并在每个合约开始/结束时打印日志，支持连接重建和写入重试。

- 接口：GET /api/v5/market/history-mark-price-candles
- bar=1H（1 小时级别）
- 分页使用 after 参数（获取更旧的数据）
- limit=100，最多 100 条/次
- 只保存 confirm=="1" 的完整 K 线
- 并发 5 个交易对，限速/超时重试
- 回填所有 inst_id 以 “-USDT” 结尾的合约
"""

import asyncio
import aiohttp
import asyncpg
import logging
import ssl
import certifi
import time
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

from config import settings

# ——— 常量 ———
OKX_URL      = "https://www.okx.com/api/v5/market/history-mark-price-candles"
BAR          = "1H"
MAX_LIMIT    = 100
MAX_RETRIES  = 5
CONCURRENCY  = 5

# ——— 日志配置 ———
logging.basicConfig(
    format="[%(asctime)s] %(levelname)s %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("backfill_markprice_1h")

# SSL 上下文，避免证书验证失败
SSL_CTX = ssl.create_default_context(cafile=certifi.where())


async def make_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        user=settings.PG_USER,
        password=settings.PG_PASSWORD,
        database=settings.PG_DB,
        host=settings.PG_HOST,
        port=settings.PG_PORT,
        min_size=1,
        max_size=CONCURRENCY,
        command_timeout=60,
        max_inactive_connection_lifetime=300,
    )


async def write_batch(pool: asyncpg.Pool, sql: str, records: list) -> asyncpg.Pool:
    for attempt in (1, 2):
        try:
            await pool.executemany(sql, records)
            return pool
        except (ConnectionError, asyncpg.exceptions._base.InterfaceError) as e:
            logger.warning("写库第 %d 次失败，重建连接池… %s", attempt, e)
            try:
                await pool.close()
            except:
                pass
            pool = await make_pool()
    logger.error("写库连续失败，跳过 %d 条", len(records))
    return pool


async def fetch_markprice(session: aiohttp.ClientSession, instId: str, after: int) -> list:
    params = {
        "instId": instId,
        "bar":    BAR,
        "after":  after,
        "limit":  MAX_LIMIT
    }
    backoff = 1
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(OKX_URL, params=params, timeout=10) as resp:
                j = await resp.json()
        except Exception as e:
            logger.warning("%s try#%d 网络错误: %s", instId, attempt, e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue

        code = j.get("code")
        data = j.get("data", []) or []
        if code == "0":
            return data

        if code == "50011":
            wait = min(backoff * 5, 60)
            logger.warning("%s try#%d 被限流, 等待 %ds", instId, attempt, wait)
            await asyncio.sleep(wait)
            backoff = min(backoff * 2, 60)
            continue

        logger.info("%s try#%d 返回 code=%s, 停止", instId, attempt, code)
        return []
    logger.error("%s 在 %d 次尝试后失败", instId, MAX_RETRIES)
    return []


async def backfill_inst(pool: asyncpg.Pool, instId: str, since_ts: int) -> asyncpg.Pool:
    start = time.time()
    logger.info(">>> [%s] 开始回填 1H K 线", instId)

    connector = aiohttp.TCPConnector(ssl=SSL_CTX)
    async with aiohttp.ClientSession(connector=connector) as session:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        first = await fetch_markprice(session, instId, now_ms)
        if not first:
            logger.info("<<< [%s] 首次无数据，跳过", instId)
            return pool

        insert_sql = """
            INSERT INTO kline_markprice_1h(symbol, ts, open, high, low, close, confirm)
            VALUES($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT(symbol, ts) DO NOTHING;
        """
        total = 0

        # 写入第一批
        recs = []
        for ts_str, o, h, l, c, confirm_str in first:
            ts_ms = int(ts_str)
            if ts_ms <= since_ts or confirm_str != "1":
                continue
            ts_dt = datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc)
            recs.append((instId, ts_dt, float(o), float(h), float(l), float(c), True))
        if recs:
            pool = await write_batch(pool, insert_sql, recs)
            total += len(recs)

        # 翻页：after = 最旧 ts -1
        after = int(first[-1][0]) - 1
        while after > since_ts:
            batch = await fetch_markprice(session, instId, after)
            if not batch:
                break
            recs = []
            for ts_str, o, h, l, c, confirm_str in batch:
                ts_ms = int(ts_str)
                if ts_ms <= since_ts or confirm_str != "1":
                    continue
                ts_dt = datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc)
                recs.append((instId, ts_dt, float(o), float(h), float(l), float(c), True))
            if not recs:
                break
            pool = await write_batch(pool, insert_sql, recs)
            total += len(recs)
            after = int(batch[-1][0]) - 1

    dur = time.time() - start
    logger.info("<<< [%s] 完成回填: %d 条, 耗时 %.1f 秒", instId, total, dur)
    return pool


async def do_backfill(pool: asyncpg.Pool, since_ts: int):
    rows = await pool.fetch("SELECT inst_id FROM instruments WHERE inst_id LIKE '%-USDT'")
    syms = [r["inst_id"] for r in rows]
    logger.info("共 %d 个合约待回填", len(syms))

    sem = asyncio.Semaphore(CONCURRENCY)
    tasks = []
    for sym in syms:
        async def worker(inst=sym):
            async with sem:
                nonlocal pool
                pool = await backfill_inst(pool, inst, since_ts)
        tasks.append(asyncio.create_task(worker()))
    await asyncio.gather(*tasks)


async def main():
    today_utc = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    since_dt  = today_utc - relativedelta(months=3)
    since_ts  = int(since_dt.timestamp() * 1000)
    logger.info("== 从 %s UTC 开始回填 3 个月 1H 标记价 ==", since_dt.isoformat())

    pool = await make_pool()
    try:
        await do_backfill(pool, since_ts)
    finally:
        await pool.close()
        logger.info("== 回填完成，连接池关闭 ==")

if __name__ == "__main__":
    asyncio.run(main())
