import asyncio
from market_client import MarketClient
from filter_engine import cond_change_ge, cond_vol_ge, cond_spread_le, combined_filter, score

async def run():
    client = MarketClient(rate_limit_per_sec=10)

    # 1. 拉取所有以 USDT 计价的现货标的
    tickers = await client.fetch_tickers(instType="SPOT")
    print(f"Fetched {len(tickers)} SPOT tickers")

    # 2. 定义筛选规则
    rules = [
        cond_change_ge(3.0),   # 24h 涨幅 ≥ 3%
        cond_vol_ge(5000),     # 24h 成交量 ≥ 5000
        cond_spread_le(0.2)    # 买一卖一价差 ≤ 0.2%
    ]

    # 3. 执行过滤
    candidates = combined_filter(tickers, rules)
    print(f"{len(candidates)} symbols after filtering:")
    for t in candidates:
        print(f"  {t['instId']}: change24h={t['_change24h']:.2f}%, vol24h={t['_vol24h']:.1f}, spread={t['_spread']:.3f}%")

    # 4. 打分并取 Top 10
    scored = [(t["instId"], score(t)) for t in candidates]
    scored.sort(key=lambda x: x[1], reverse=True)
    top10 = scored[:10]
    print("\nTop 10 by score:")
    for sym, sc in top10:
        print(f"  {sym}: score={sc:.3f}")

if __name__ == "__main__":
    asyncio.run(run())
