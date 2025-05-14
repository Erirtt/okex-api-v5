#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
select_candidates_with_plan.py

在市值前 50 的标的中选出“多周期趋势对齐 + 支撑回踩 + 动量过滤”候选，
并为每个候选生成：入场点位、止损点、止盈点及选中理由。
"""

import asyncpg
import pandas as pd
from typing import List, Dict

from config import settings
from market_client import MarketClient
from load_history import load_history_async

# ——— 指标计算 ———
def compute_emas(df: pd.DataFrame, fast: int = 20, slow: int = 50):
    ema_f = df['close'].ewm(span=fast, adjust=False).mean()
    ema_s = df['close'].ewm(span=slow, adjust=False).mean()
    return ema_f, ema_s

def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain  = delta.where(delta > 0, 0)
    loss  = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()
    rs = avg_gain / avg_loss
    return 100 - 100/(1+rs)

def compute_macd(series: pd.Series, fast=12, slow=26, sig=9):
    ema_f = series.ewm(span=fast, adjust=False).mean()
    ema_s = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_f - ema_s
    macd_sig  = macd_line.ewm(span=sig, adjust=False).mean()
    hist      = macd_line - macd_sig
    return macd_line, macd_sig, hist

# ——— 核心逻辑 ———
async def get_candidates_with_plan() -> List[Dict]:
    # 1) 拿市值前 50
    client = MarketClient(rate_limit_per_sec=10)
    symbols = await client.fetch_top_market_caps(n=50)

    # 2) 建 DB 连接池
    pool = await asyncpg.create_pool(
        user=settings.PG_USER, password=settings.PG_PASSWORD,
        database=settings.PG_DB, host=settings.PG_HOST, port=settings.PG_PORT,
        min_size=1, max_size=3
    )

    final = []
    support_thresh = 0.01  # 回踩支撑阈值 1%
    rr             = 2.0   # 止盈倍数

    for sym in symbols:
        # 3) 加载近 200 根 1H 历史
        df = await load_history_async(sym, limit=200,pool=pool)
        if df is None or len(df) < 60:
            continue

        close = df['close']

        # ——— 趋势对齐 (1H EMA20>EMA50) ———
        ema20, ema50 = compute_emas(df, 20, 50)
        if ema20.iat[-1] <= ema50.iat[-1]:
            continue

        # ——— 支撑回踩位 ———
        recent_low = df['low'].rolling(20).min().iat[-1]
        entry_price = recent_low * (1 + support_thresh)
        stop_loss   = recent_low

        # ——— 动量过滤 ———
        rsi = compute_rsi(close).iat[-1]
        if rsi <= 50:
            continue
        macd_hist = compute_macd(close)[2].iat[-1]
        if macd_hist <= 0:
            continue

        # ——— 计算止盈 ———
        take_profit = entry_price + (entry_price - stop_loss) * rr

        # ——— 组装输出 ———
        reason = (
            f"EMA20={ema20.iat[-1]:.2f}>EMA50={ema50.iat[-1]:.2f}; "
            f"支撑低={recent_low:.2f}, 回踩阈={support_thresh*100:.0f}%; "
            f"RSI={rsi:.1f}; MACD柱={macd_hist:.3f}"
        )
        final.append({
            "symbol":       sym,
            "entry_price":  round(entry_price, 4),
            "stop_loss":    round(stop_loss,   4),
            "take_profit":  round(take_profit, 4),
            "ema20":        round(ema20.iat[-1], 2),
            "ema50":        round(ema50.iat[-1], 2),
            "rsi":          round(rsi, 1),
            "macd_hist":    round(macd_hist, 3),
            "reason":       reason
        })

    await pool.close()
    return final

# ——— 调试 / CLI ———
if __name__ == "__main__":
    import asyncio, json
    candidates = asyncio.run(get_candidates_with_plan())
    print("🏁 候选列表：")
    print(json.dumps(candidates, ensure_ascii=False, indent=2))
