#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
service/select_candidates.py

新的筛选逻辑 + 详细调试打印：
1) 趋势：EMA20 > EMA50
2) 支撑：当前价 ≤ 20m 低点 × (1 + support_thresh)
3) 动量：RSI14 > 50 且 MACD_hist > 0

每个步骤都会打印中间值和是否通过该步。
"""

import asyncpg
import pandas as pd

from config import settings
from market_client import MarketClient
from filter_engine import cond_change_ge, cond_vol_ge, combined_filter
from load_history import load_history_async

def compute_emas(df, fast=20, slow=50):
    e1 = df['close'].ewm(span=fast, adjust=False).mean()
    e2 = df['close'].ewm(span=slow, adjust=False).mean()
    return e1, e2

def compute_rsi(s, n=14):
    d = s.diff()
    u = d.where(d>0, 0)
    v = -d.where(d<0, 0)
    avg_u = u.rolling(n).mean()
    avg_v = v.rolling(n).mean()
    rs = avg_u / avg_v
    return 100 - 100/(1+rs)

def compute_macd(s, f=12, l=26, m=9):
    e_f = s.ewm(span=f, adjust=False).mean()
    e_l = s.ewm(span=l, adjust=False).mean()
    macd_line = e_f - e_l
    sig       = macd_line.ewm(span=m, adjust=False).mean()
    hist      = macd_line - sig
    return macd_line, sig, hist

async def get_candidates() -> dict:
    client = MarketClient(rate_limit_per_sec=10)
    symbols = await client.fetch_top_market_caps(n=50)

    # 初步 24h-change & vol 过滤
    all_tks = await client.fetch_tickers(instType="SPOT") or []
    tickers = [t for t in all_tks if t["instId"] in symbols]
    prelim_rules = [cond_change_ge(3.0), cond_vol_ge(5000)]
    prelim_tks = combined_filter(tickers, prelim_rules)
    prelim = [
        {"symbol": t["instId"],
         "change24h": round(t["_change24h"],2),
         "vol24h":    round(t["_vol24h"],2)}
        for t in prelim_tks
    ]

    # 建连接池
    pool = await asyncpg.create_pool(
        user=settings.PG_USER, password=settings.PG_PASSWORD,
        database=settings.PG_DB, host=settings.PG_HOST,
        port=settings.PG_PORT, min_size=1, max_size=3
    )

    final = []
    print("开始逐 symbol 筛选并打印过程：")
    for t in prelim_tks:
        sym = t["instId"]
        print(f"\n>>> 检查 {sym}")
        df = await load_history_async(sym, limit=100)
        if df is None or len(df) < 60:
            print("  ❌ K 线数据不足，跳过")
            continue

        close = df['close']
        ema20, ema50 = compute_emas(df)
        v20, v50 = ema20.iat[-1], ema50.iat[-1]
        trend_ok = v20 > v50
        print(f"  EMA20={v20:.2f}, EMA50={v50:.2f} → 趋势 {'OK' if trend_ok else 'NG'}")
        if not trend_ok:
            continue

        low20 = df['low'].rolling(20).min().iat[-1]
        price = close.iat[-1]
        support_ok = (price - low20) / low20 <= 0.01
        print(f"  recent_low={low20:.2f}, close={price:.2f}, "
              f"distance={(price-low20)/low20:.4f} → 支撑 {'OK' if support_ok else 'NG'}")
        if not support_ok:
            continue

        rsi = compute_rsi(close).iat[-1]
        macd_hist = compute_macd(close)[2].iat[-1]
        mom_ok = (rsi > 50) and (macd_hist > 0)
        print(f"  RSI={rsi:.1f} (>50? {'Y' if rsi>50 else 'N'}), "
              f"MACD_hist={macd_hist:.3f} (>0? {'Y' if macd_hist>0 else 'N'}) "
              f"→ 动量 {'OK' if mom_ok else 'NG'}")
        if not mom_ok:
            continue

        reason = (
            f"趋势↑: EMA20={v20:.2f}>EMA50={v50:.2f}; "
            f"支撑: close={price:.2f}/low20={low20:.2f}; "
            f"RSI={rsi:.1f}; MACD_hist={macd_hist:.3f}"
        )
        final.append({
            "symbol":     sym,
            "ema20":      round(v20,2),
            "ema50":      round(v50,2),
            "recent_low": round(low20,2),
            "close":      round(price,2),
            "rsi":        round(rsi,1),
            "macd_hist":  round(macd_hist,3),
            "reason":     reason
        })
        print(f"  ✅ {sym} 符合条件，加入候选")

    await pool.close()
    print("\n筛选完成，总计入选：", len(final), "个")
    return {"prelim": prelim, "final": final}


# 调试
if __name__ == "__main__":
    import asyncio, json
    res = asyncio.run(get_candidates())
    print(json.dumps(res, indent=2, ensure_ascii=False))
