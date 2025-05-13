#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
load_history.py

提供异步函数 load_history_async，用于从 kline_1m_agg 表中
读取指定交易对的最近 N 根 1 分钟 K 线，返回 pandas.DataFrame。
现在支持传入已创建的 asyncpg.Pool，以便外部复用连接池。
"""

import asyncpg
import pandas as pd
from config import settings

async def load_history_async(symbol: str,
                             limit: int = 200,
                             pool: asyncpg.Pool = None) -> pd.DataFrame:
    """
    异步从 PostgreSQL 读历史 K 线。
    如果传入了 pool，则复用该池；否则内部会创建一个，然后使用完毕关闭。

    Params:
      symbol: 交易对符号，如 "BTC-USDT"
      limit:  读取的 K 线条数
      pool:   可选的 asyncpg.Pool

    Returns:
      pandas.DataFrame，index 为 ts (datetime)，含列 open,high,low,close,volume。
      如果无数据返回 None。
    """
    internal_pool = False
    if pool is None:
        internal_pool = True
        pool = await asyncpg.create_pool(
            user=settings.PG_USER,
            password=settings.PG_PASSWORD,
            database=settings.PG_DB,
            host=settings.PG_HOST,
            port=settings.PG_PORT,
            min_size=1,
            max_size=2
        )

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT ts, open, high, low, close, volume
              FROM kline_1m_agg
             WHERE symbol = $1
             ORDER BY ts DESC
             LIMIT $2
            """,
            symbol, limit
        )

    if internal_pool:
        await pool.close()

    if not rows:
        return None

    # 转为 DataFrame
    df = pd.DataFrame([dict(r) for r in rows])
    df['ts'] = pd.to_datetime(df['ts'])
    df.set_index('ts', inplace=True)
    # 返回升序
    return df.sort_index()
