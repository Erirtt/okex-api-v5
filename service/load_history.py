# load_history.py

import asyncpg
import pandas as pd
from config import settings

async def load_history_async(symbol: str,
                             limit: int = 200,
                             pool: asyncpg.Pool = None) -> pd.DataFrame:
    if pool is None:
        raise RuntimeError("load_history_async: 必须传入已创建的 asyncpg.Pool")

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT ts, open, high, low, close, vol AS volume
              FROM kline_1h
             WHERE symbol = $1
             ORDER BY ts DESC
             LIMIT $2
            """,
            symbol, limit
        )

    if not rows:
        return None

    df = pd.DataFrame([dict(r) for r in rows])
    df['ts'] = pd.to_datetime(df['ts'])
    df.set_index('ts', inplace=True)
    return df.sort_index()
