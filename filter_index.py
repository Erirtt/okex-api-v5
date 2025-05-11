# filter_index.py
import asyncio
import asyncpg
import pandas as pd
from config import settings
from market_client import MarketClient
from filter_engine import (
    cond_change_ge, cond_vol_ge,
    combined_filter, full_entry_filter
)

async def load_history(pool: asyncpg.Pool, instId: str, limit: int = 200) -> pd.DataFrame:
    """
    从 kline_1m_agg 表中按时间倒序取最近 `limit` 条数据，
    返回 DataFrame，index 为 ts（UTC datetime），列 open, high, low, close, volume
    """
    rows = await pool.fetch(f"""
        SELECT ts, open, high, low, close, volume
        FROM kline_1m_agg
        WHERE symbol = $1
        ORDER BY ts DESC
        LIMIT $2
    """, instId, limit)

    if not rows:
        return None

    # 先把 asyncpg Record 转成普通 dict
    records = [dict(r) for r in rows]
    df = pd.DataFrame.from_records(records)
    # 确认列名
    if "ts" not in df.columns:
        raise ValueError(f"'ts' column missing in loaded data for {instId}: cols={df.columns.tolist()}")
    df = df.set_index("ts").sort_index()
    return df

async def run_entry_workflow():
    # 1. 拉取现货行情
    client = MarketClient(rate_limit_per_sec=10)
    tickers = await client.fetch_tickers(instType="SPOT")
    print(f"Fetched {len(tickers)} SPOT tickers")

    # 2. 基础量价筛选
    basic_rules = [
        cond_change_ge(3.0),
        cond_vol_ge(5000),
    ]
    prelim = combined_filter(tickers, basic_rules)
    print(f"{len(prelim)} symbols after basic filtering")

    # 3. 创建一个共享的 asyncpg 连接池
    pool = await asyncpg.create_pool(
        user=settings.PG_USER,
        password=settings.PG_PASSWORD,
        database=settings.PG_DB,
        host=settings.PG_HOST,
        port=settings.PG_PORT,
        min_size=1,
        max_size=5,
    )

    # 4. 加载历史数据
    price_histories = {}
    for t in prelim:
        inst = t["instId"]
        df = await load_history(pool, inst, limit=200)
        if df is not None and len(df) >= 60:
            price_histories[inst] = df
    print(f"Loaded history for {len(price_histories)} symbols")

    # 5. 最终入场判断
    entries = full_entry_filter(
        tickers=prelim,
        price_histories=price_histories,
        basic_rules=basic_rules,
        entry_params={"onchain_min": 200}
    )
    print("Final entry candidates:", entries)

    # 6. 关闭连接池
    await pool.close()

if __name__ == "__main__":
    asyncio.run(run_entry_workflow())
