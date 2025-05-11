#!/usr/bin/env python3
# select_candidates.py

import sys
import asyncio
import asyncpg
import pandas as pd
from tabulate import tabulate

from config import settings
from market_client import MarketClient
from filter_engine import cond_change_ge, cond_vol_ge, combined_filter, entry_signal

async def load_history(pool: asyncpg.Pool, instId: str, limit: int = 200) -> pd.DataFrame:
    rows = await pool.fetch("""
        SELECT ts, open, high, low, close, volume
        FROM kline_1m_agg
        WHERE symbol = $1
        ORDER BY ts DESC
        LIMIT $2
    """, instId, limit)
    if not rows:
        return None
    df = pd.DataFrame([dict(r) for r in rows])
    return df.set_index("ts").sort_index()

async def main():
    client = MarketClient(rate_limit_per_sec=10)

    # 1. 市值前 30
    symbols = await client.fetch_top_market_caps(30)
    if not symbols:
        print("❌ 无法获取市值前30的币对，退出。")
        sys.exit(1)
    print("\n市值前 30：")
    print(tabulate([[s] for s in symbols], headers=["Symbol"], tablefmt="grid"))

    # 2. 拉最新行情并过滤
    all_tickers = await client.fetch_tickers(instType="SPOT") or []
    tickers = [t for t in all_tickers if t.get("instId") in symbols]

    # 3. 基础筛选
    basic_rules = [cond_change_ge(3.0), cond_vol_ge(5000)]
    prelim = combined_filter(tickers, basic_rules)
    prelim_table = [
        [t["instId"], f"{t['_change24h']:.2f}%", f"{t['_vol24h']:.0f}"]
        for t in prelim
    ]
    print(f"\n基础筛选后候选（{len(prelim)}）：")
    print(tabulate(prelim_table, headers=["Symbol", "Change24h", "Vol24h"], tablefmt="fancy_grid"))

    # 4. 加载历史
    pool = await asyncpg.create_pool(
        user=settings.PG_USER, password=settings.PG_PASSWORD,
        database=settings.PG_DB, host=settings.PG_HOST,
        port=settings.PG_PORT, min_size=1, max_size=5
    )
    price_histories = {}
    for t in prelim:
        inst = t["instId"]
        df = await load_history(pool, inst, limit=200)
        if df is not None and len(df) >= 60:
            price_histories[inst] = df
    await pool.close()

    # 5. 最终入场判断
    final_entries = []
    for inst, df in price_histories.items():
        if entry_signal(df, inst):
            final_entries.append(inst)

    if final_entries:
        print("\n🏁 最终入场候选：")
        print(tabulate([[s] for s in final_entries], headers=["Symbol"], tablefmt="pipe"))
    else:
        print("\n🏁 无符合入场信号的标的。")

if __name__ == "__main__":
    asyncio.run(main())
