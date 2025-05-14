#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
select_candidates.py

策略：
- Vegas 隧道：EMA144/EMA169 + 过滤线 EMA12，基于 4H K 线判断多空趋势
- 动量过滤：RSI14 & MACD 直方图，在 1H K 线上进一步筛选
- 入场/止损/止盈：
    * entry       = 最新 1H 收盘价
    * stop_loss   = 最近 20 根 1H K 线低点
    * take_profit = entry + (entry - stop_loss) * 2    （空头相反）
"""

import asyncpg
import pandas as pd
from config import settings
from market_client import MarketClient
from load_history import load_history_async

def compute_vegas_tunnel(series: pd.Series,
                         span_fast=144, span_slow=169, span_filter=12):
    """返回 (EMA_fast, EMA_slow, EMA_filter)"""
    ema_fast   = series.ewm(span=span_fast, adjust=False).mean()
    ema_slow   = series.ewm(span=span_slow, adjust=False).mean()
    ema_filter = series.ewm(span=span_filter, adjust=False).mean()
    return ema_fast, ema_slow, ema_filter

def compute_rsi(series: pd.Series, period=14) -> pd.Series:
    delta = series.diff()
    gain  = delta.where(delta>0, 0)
    loss  = -delta.where(delta<0, 0)
    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()
    rs = avg_gain / avg_loss
    return 100 - 100 / (1 + rs)

def compute_macd_hist(series: pd.Series,
                      fast=12, slow=26, sig=9) -> pd.Series:
    ema_f    = series.ewm(span=fast, adjust=False).mean()
    ema_s    = series.ewm(span=slow, adjust=False).mean()
    macd_line= ema_f - ema_s
    signal   = macd_line.ewm(span=sig, adjust=False).mean()
    hist     = macd_line - signal
    return hist

async def get_candidates(n: int = 50) -> list[dict]:
    """
    返回最终候选列表，格式：
    [
      {
        symbol, trend,
        price4h, ema144_4h, ema169_4h, ema12_4h,
        rsi1h, macd_hist_1h,
        entry, stop_loss, take_profit,
        reason
      },
      …
    ]
    """
    # 1) 拿市值前 n
    client  = MarketClient(rate_limit_per_sec=10)
    symbols = await client.fetch_top_market_caps(n=n)

    # 2) 建连接池
    pool = await asyncpg.create_pool(
        user=settings.PG_USER,
        password=settings.PG_PASSWORD,
        database=settings.PG_DB,
        host=settings.PG_HOST,
        port=settings.PG_PORT,
        min_size=1,
        max_size=3
    )

    final = []
    for sym in symbols:
        # —— 2.1 4H 隧道趋势判断 —— 
        df4h = await load_history_async(sym, limit=200, pool=pool)
        if df4h is None or len(df4h) < 169:
            continue
        close4h = df4h['close']
        ema144, ema169, ema12 = compute_vegas_tunnel(close4h)
        price4h = close4h.iat[-1]
        e144    = ema144.iat[-1]
        e169    = ema169.iat[-1]
        e12     = ema12.iat[-1]

        is_long  = price4h>e144 and price4h>e169 and e12>e144 and e12>e169
        is_short = price4h<e144 and price4h<e169 and e12<e144 and e12<e169
        if not (is_long or is_short):
            continue

        # —— 2.2 1H 动量 & 支撑判断 —— 
        df1h = await load_history_async(sym, limit=50, pool=pool)
        if df1h is None or len(df1h) < 20:
            continue
        close1h = df1h['close']
        low20   = df1h['low'].rolling(20).min().iat[-1]
        rsi1h   = compute_rsi(close1h).iat[-1]
        macd_hist1h = compute_macd_hist(close1h).iat[-1]

        if is_long and (rsi1h <= 50 or macd_hist1h <= 0):
            continue
        if is_short and (rsi1h >= 50 or macd_hist1h >= 0):
            continue

        # —— 3. 计算入场/止损/止盈 —— 
        entry      = float(close1h.iat[-1])
        stop_loss  = float(low20)
        # 多头：利润目标 = 风险*2，空头相反
        if is_long:
            take_profit = entry + (entry - stop_loss) * 2
        else:
            take_profit = entry - (stop_loss - entry) * 2

        # —— 4. 拼装理由 —— 
        reason = (
            f"{'多头' if is_long else '空头'}趋势(4H)："
            f"price={price4h:.2f}, EMA144={e144:.2f}, EMA169={e169:.2f}, EMA12={e12:.2f}; "
            f"动量(1H)：RSI={rsi1h:.1f}, MACD_hist={macd_hist1h:.3f}"
        )

        final.append({
            "symbol":         sym,
            "trend":          "Long" if is_long else "Short",
            "price4h":        round(price4h,2),
            "ema144_4h":      round(e144,2),
            "ema169_4h":      round(e169,2),
            "ema12_4h":       round(e12,2),
            "rsi1h":          round(rsi1h,1),
            "macd_hist_1h":   round(macd_hist1h,3),
            "entry":          round(entry,4),
            "stop_loss":      round(stop_loss,4),
            "take_profit":    round(take_profit,4),
            "reason":         reason
        })

    await pool.close()
    return final

# 本地调试
if __name__ == "__main__":
    import asyncio, json
    cands = asyncio.run(get_candidates(n=50))
    print(json.dumps(cands, ensure_ascii=False, indent=2))
