import asyncio
import asyncpg
import pandas as pd
from datetime import datetime
from config import settings

async def get_candidates_by_4h1h(n: int = 5) -> list[dict]:
    """
    返回前 n 个“健康度”标的及其交易计划：
      1) 4H EMA20>EMA50
      2) 计算 1H RSI14 与 MACD_hist，归一化得 Score
      3) Entry=最新收盘，Stop=20 根 1H 最低，TP=Entry+(Entry-Stop)*2
    """
    # 1）市值前 50
    from market_client import MarketClient
    client = MarketClient(rate_limit_per_sec=10)
    symbols = await client.fetch_top_market_caps(n=50)

    # 2）建 DB 池
    pool = await asyncpg.create_pool(
        user=settings.PG_USER, password=settings.PG_PASSWORD,
        database=settings.PG_DB, host=settings.PG_HOST, port=settings.PG_PORT,
        min_size=1, max_size=3
    )

    candidates = []
    for sym in symbols:
        # —————— 4H 初筛 ——————
        rows4h = await pool.fetch(
            "SELECT ts, close FROM kline_4h WHERE symbol=$1 ORDER BY ts DESC LIMIT $2",
            sym, 200
        )
        if len(rows4h) < 50:
            continue
        df4h = pd.DataFrame([dict(r) for r in rows4h])
        df4h['ts']    = pd.to_datetime(df4h['ts'])
        df4h.set_index('ts', inplace=True)
        close4h = df4h['close'].sort_index()
        ema20_4h = close4h.ewm(span=20, adjust=False).mean().iat[-1]
        ema50_4h = close4h.ewm(span=50, adjust=False).mean().iat[-1]
        if ema20_4h <= ema50_4h:
            continue

        # —————— 1H 打分 ——————
        rows1h = await pool.fetch(
            "SELECT ts, open, high, low, close FROM kline_1h WHERE symbol=$1 ORDER BY ts DESC LIMIT $2",
            sym, 200
        )
        if len(rows1h) < 60:
            continue
        df1h = pd.DataFrame([dict(r) for r in rows1h])
        df1h['ts']    = pd.to_datetime(df1h['ts'])
        df1h.set_index('ts', inplace=True)
        close1h = df1h['close'].sort_index()
        # RSI14
        delta = close1h.diff()
        gain  = delta.where(delta>0, 0)
        loss  = -delta.where(delta<0, 0)
        rsi14 = 100 - 100/(1 + gain.rolling(14).mean()/loss.rolling(14).mean())
        rsi   = rsi14.iat[-1]
        # MACD_hist
        ema12 = close1h.ewm(span=12, adjust=False).mean()
        ema26 = close1h.ewm(span=26, adjust=False).mean()
        macd_hist = (ema12-ema26 - (ema12-ema26).ewm(span=9, adjust=False).mean()).iat[-1]

        # 归一化简单打分
        score = rsi/100 + macd_hist/close1h.iat[-1]

        # —————— 交易计划 ——————
        entry = close1h.iat[-1]
        stop  = df1h['low'].rolling(20).min().iat[-1]
        tp    = entry + (entry - stop) * 2

        candidates.append({
            "symbol":      sym,
            "score":       round(score, 3),
            "entry":       round(entry, 4),
            "stop_loss":   round(stop, 4),
            "take_profit": round(tp, 4),
            "ema20_4h":    round(ema20_4h, 4),
            "ema50_4h":    round(ema50_4h, 4),
            "rsi14":       round(rsi, 1),
            "macd_hist":   round(macd_hist, 4),
        })

    await pool.close()
    # 按 score 排序并取前 N
    return sorted(candidates, key=lambda x: x['score'], reverse=True)[:n]


# —— 测试 ——  
if __name__ == "__main__":
    import json
    res = asyncio.run(get_candidates_by_4h1h(n=5))
    print("🎯 最终健康度排行及交易计划：")
    print(json.dumps(res, ensure_ascii=False, indent=2))
