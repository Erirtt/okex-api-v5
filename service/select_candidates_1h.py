#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
select_candidates_1h.py

基于 1H K 线数据的筛选逻辑（不依赖 volume）：
1) 趋势：EMA20 > EMA50
2) 支撑：当前价 ≤ 最近 20 根 1H 低点 × (1 + support_thresh)
3) 动量：RSI14 > 50 且 MACD_hist > 0

从 kline_markprice_1h 表中加载历史 1H K 线，返回最终候选列表及理由。
"""

import asyncio
import asyncpg
import pandas as pd

from config import settings

# ——— 参数 ———
HISTORY_LIMIT  = 36    # 每个合约加载最近 100 根 1H
EMA_FAST       = 20
EMA_SLOW       = 50
RSI_PERIOD     = 14
SUPPORT_BARS   = 20
SUPPORT_THRESH = 0.01   # 1%

# ——— 指标函数 ———
def compute_emas(close: pd.Series, fast: int, slow: int):
    ema_f = close.ewm(span=fast, adjust=False).mean()
    ema_s = close.ewm(span=slow, adjust=False).mean()
    return ema_f, ema_s

def compute_rsi(close: pd.Series, period: int):
    delta = close.diff()
    gain  = delta.where(delta > 0, 0)
    loss  = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()
    rs = avg_gain / avg_loss
    return 100 - 100 / (1 + rs)

def compute_macd(close: pd.Series, fast: int=12, slow: int=26, sig: int=9):
    ema_f = close.ewm(span=fast, adjust=False).mean()
    ema_s = close.ewm(span=slow, adjust=False).mean()
    macd_line = ema_f - ema_s
    macd_sig  = macd_line.ewm(span=sig, adjust=False).mean()
    hist      = macd_line - macd_sig
    return macd_line, macd_sig, hist

# ——— 加载历史 1H K 线 ———
async def load_history_1h(pool: asyncpg.Pool, symbol: str, limit: int) -> pd.DataFrame:
    rows = await pool.fetch(
        """
        SELECT ts, open, high, low, close
        FROM kline_markprice_1h
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

# ——— 最终筛选 ———
async def get_candidates_1h() -> list[dict]:
    # 1) 建立连接池
    pool = await asyncpg.create_pool(
        user=settings.PG_USER,
        password=settings.PG_PASSWORD,
        database=settings.PG_DB,
        host=settings.PG_HOST,
        port=settings.PG_PORT,
        min_size=1, max_size=3
    )

    # 2) 获取所有 USDT 合约
    rows = await pool.fetch("SELECT inst_id FROM instruments WHERE inst_id LIKE '%-USDT'")
    symbols = [r['inst_id'] for r in rows]

    candidates = []
    for sym in symbols:
        df = await load_history_1h(pool, sym, HISTORY_LIMIT)
        if df is None or len(df) < max(EMA_SLOW, RSI_PERIOD, SUPPORT_BARS):
            continue

        close = df['close']

        # ——— 趋势判断 ———
        ema_f, ema_s = compute_emas(close, EMA_FAST, EMA_SLOW)
        if ema_f.iat[-1] <= ema_s.iat[-1]:
            continue

        # ——— 支撑位判断 ———
        recent_low = df['low'].rolling(SUPPORT_BARS).min().iat[-1]
        price_now  = close.iat[-1]
        if (price_now - recent_low) / recent_low > SUPPORT_THRESH:
            continue

        # ——— RSI & MACD 动量过滤 ———
        rsi = compute_rsi(close, RSI_PERIOD).iat[-1]
        if rsi <= 50:
            continue
        macd_hist = compute_macd(close)[2].iat[-1]
        if macd_hist <= 0:
            continue

        # 如果都满足，构造理由并加入列表
        reason = (
            f"EMA{EMA_FAST}>{EMA_SLOW}: {ema_f.iat[-1]:.2f}>{ema_s.iat[-1]:.2f}; "
            f"支撑: 价{price_now:.2f} vs 20h低{recent_low:.2f}; "
            f"RSI{RSI_PERIOD}={rsi:.1f}>50; MACD柱={macd_hist:.3f}>0"
        )
        candidates.append({
            "symbol":      sym,
            "ema_fast":    round(ema_f.iat[-1], 2),
            "ema_slow":    round(ema_s.iat[-1], 2),
            "recent_low":  round(recent_low, 2),
            "close":       round(price_now, 2),
            "rsi":         round(rsi, 1),
            "macd_hist":   round(macd_hist, 3),
            "reason":      reason
        })

    await pool.close()
    return candidates

# ——— 调试主函数 ———
if __name__ == "__main__":
    import json
    import asyncio

    recs = asyncio.run(get_candidates_1h())
    print("🏁 1H 最终入场候选：")
    print(json.dumps(recs, ensure_ascii=False, indent=2))
