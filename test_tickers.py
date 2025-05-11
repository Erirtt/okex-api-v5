import asyncio
from market_client import MarketClient
from filter_engine import filter_by_ticker

async def ticker_workflow():
    client = MarketClient(rate_limit_per_sec=10)

    # 1. 拉取所有 SPOT 标的
    tickers = await client.fetch_tickers(instType="SPOT")
    print(f"Fetched {len(tickers)} SPOT tickers")

    # 2. 简单筛选：24h 成交量 ≥ 5000，24h 涨跌幅 ≥ 3%
    filtered = filter_by_ticker(
        tickers,
        min_vol24h=5000,
        min_change24h=3.0
    )

    print(f"Found {len(filtered)} symbols meeting criteria:")
    for t in filtered:
        print(f"{t['instId']}: vol24h={t['_vol24h']:.1f}, change24h={t['_change24h']:.2f}%")

if __name__ == "__main__":
    asyncio.run(ticker_workflow())
