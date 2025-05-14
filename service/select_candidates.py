#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
select_candidates.py

基于 4H 趋势 + 1H 动量的候选筛选并生成交易计划

流程：
1. 取市值前 50 （CoinGecko）
2. 从 TimescaleDB 读取各标的最新 100 条 4H K 线，计算 EMA20/EMA50 判趋势
3. 从 TimescaleDB 读取各标的最新 200 条 1H K 线，计算 RSI14 & MACD_hist 判动量
4. 针对符合条件的标的，给出 entry、stop_loss、take_profit 以及一个“健康度”score
5. 按 score 从高到低排序，返回前 N 个

函数：
    async def get_candidates_by_4h1h(n: int = 10) -> List[dict]
"""

import asyncio
import asyncpg
import pandas as pd
from datetime import datetime
from config import settings
from market_client import MarketClient

# —— 指标函数 —— 
def compute_emas(df: pd.DataFrame, fast: int = 20, slow: int = 50):
    """返回 (EMA_fast, EMA_slow) 两条序列"""
    e_fast = df['close'].ewm(span=fast, adjust=False).mean()
    e_slow = df['close'].ewm(span=slow, adjust=False).mean()
    return e_fast, e_slow

def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """简单 RSI 计算"""
    delta = series.diff()
    up    = delta.where(delta > 0, 0.0)
    down  = -delta.where(delta < 0, 0.0)
    ma_up   = up.rolling(period).mean()
    ma_down = down.rolling(period).mean()
    rs = ma_up / ma_down
    return 100 - 100 / (1 + rs)

def compute_macd(series: pd.Series, fast: int = 12, slow: int = 26, sig: int = 9):
    """返回 (MACD_line, Signal, Histogram)"""
    e_fast = series.ewm(span=fast, adjust=False).mean()
    e_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = e_fast - e_slow
    signal    = macd_line.ewm(span=sig, adjust=False).mean()
    hist      = macd_line - signal
    return macd_line, signal, hist

# —— 从指定表读取历史 K 线 —— 
async def load_history(pool: asyncpg.Pool,
                       symbol: str,
                       table: str,
                       limit: int) -> pd.DataFrame | None:
    """
    从 table（kline_4h 或 kline_1h）读最近 limit 根 K 线，
    返回升序 DataFrame，columns = [open,high,low,close,volume]
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT ts, open, high, low, close, vol AS volume
              FROM {table}
             WHERE symbol = $1
             ORDER BY ts DESC
             LIMIT $2
        """, symbol, limit)

    if not rows:
        return None

    df = pd.DataFrame([dict(r) for r in rows])
    # ts field 已经是 timestamptz 类型，可直接转换
    df['ts'] = pd.to_datetime(df['ts'])
    df.set_index('ts', inplace=True)
    return df.sort_index()

# —— 主函数 —— 
async def get_candidates_by_4h1h(n: int = 10) -> list[dict]:
    """
    返回 top-n 个候选：
    [
      {
        symbol, score,
        ema20_4h, ema50_4h,
        rsi14_1h, macd_hist_1h,
        entry, stop_loss, take_profit
      },
      …
    ]
    """
    # 1) 获取市值前 50
    client = MarketClient(rate_limit_per_sec=10)
    symbols = await client.fetch_top_market_caps(n=50)

    # 2) 建 DB 连接池
    pool = await asyncpg.create_pool(
        user=settings.PG_USER,
        password=settings.PG_PASSWORD,
        database=settings.PG_DB,
        host=settings.PG_HOST,
        port=settings.PG_PORT,
        min_size=1, max_size=5
    )

    candidates = []
    for sym in symbols:
        # 3) 读 4H 历史
        df4h = await load_history(pool, sym, 'kline_4h', limit=100)
        if df4h is None or len(df4h) < 20:
            continue
        ema20_4h, ema50_4h = compute_emas(df4h)
        # 趋势：EMA20 > EMA50
        if ema20_4h.iat[-1] <= ema50_4h.iat[-1]:
            continue

        # 4) 读 1H 历史
        df1h = await load_history(pool, sym, 'kline_1h', limit=200)
        if df1h is None or len(df1h) < 20:
            continue
        rsi1h = compute_rsi(df1h['close'])
        macd_hist1h = compute_macd(df1h['close'])[2]
        # 动量：RSI14 > 50 且 MACD_hist > 0
        if rsi1h.iat[-1] <= 50 or macd_hist1h.iat[-1] <= 0:
            continue

        # 5) 生成交易计划
        price_now  = df1h['close'].iat[-1]
        recent_low = df1h['low'].rolling(20).min().iat[-1]
        stop_loss  = recent_low * 0.995          # 止损：20 根 1H 低点下 0.5%
        entry      = price_now
        take_profit= entry + (entry - stop_loss) * 2  # 2:1

        # 6) 计算健康度 score（EMA 差值百分比）
        score = (ema20_4h.iat[-1] - ema50_4h.iat[-1]) / ema50_4h.iat[-1]

        candidates.append({
            "symbol":       sym,
            "score":        round(score * 100, 2),
            "ema20_4h":     round(ema20_4h.iat[-1], 2),
            "ema50_4h":     round(ema50_4h.iat[-1], 2),
            "rsi14_1h":     round(rsi1h.iat[-1], 1),
            "macd_hist_1h": round(macd_hist1h.iat[-1], 3),
            "entry":        round(entry, 4),
            "stop_loss":    round(stop_loss, 4),
            "take_profit":  round(take_profit, 4)
        })

    await pool.close()

    # 7) 按 score 排序，取前 n
    candidates.sort(key=lambda x: x['score'], reverse=True)
    return candidates[:n]


# —— 供调试 —— 
if __name__ == "__main__":
    import json
    recs = asyncio.run(get_candidates_by_4h1h(n=10))
    print("🏁 Final Candidates:", json.dumps(recs, ensure_ascii=False, indent=2))
