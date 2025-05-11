import asyncio
from config import settings
from ws_manager import WSManager
from kline_processor import KlineProcessor
from market_client import MarketClient

async def main():
    # 1. 初始化 KlineProcessor
    pg_conf = {
        "user": settings.PG_USER,
        "password": settings.PG_PASSWORD,
        "database": settings.PG_DB,
        "host": settings.PG_HOST,
        "port": settings.PG_PORT,
    }
    processor = await KlineProcessor.create(pg_conf, table="kline_1m_agg")

    # 2. 获取市值前30的 instId 列表
    client = MarketClient(rate_limit_per_sec=10)
    top30 = await client.fetch_top_market_caps(30)

    # 3. 构造订阅频道
    public_channels = [{"channel": "candle1m", "instId": sym} for sym in top30]

    async def handler(msg):
        await processor.process(msg)

    # 4. 启动订阅
    WSManager(
        url=settings.OKX_PUBLIC_URL,
        channels=public_channels,
        handler=handler
    )

    # 5. 永不退出
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
