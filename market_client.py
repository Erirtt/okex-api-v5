import aiohttp
import asyncio
import logging
import ssl
import certifi

logger = logging.getLogger("market_client")
logger.setLevel(logging.INFO)
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(message)s"))
logger.addHandler(_handler)

OKX_REST = "https://www.okx.com"
CG_REST  = "https://api.coingecko.com/api/v3"
SSL_CTX  = ssl.create_default_context(cafile=certifi.where())

class MarketClient:
    def __init__(self, rate_limit_per_sec: float = 10):
        """
        OKX/API 限速：20 次/2s
        """
        self._sema = asyncio.Semaphore(rate_limit_per_sec)

    async def fetch_tickers(self,
                            instType: str = "SPOT",
                            uly: str = None,
                            instFamily: str = None) -> list:
        """
        拉取 OKX /api/v5/market/tickers
        :return: 列表，如果失败返回 []
        """
        url    = f"{OKX_REST}/api/v5/market/tickers"
        params = {"instType": instType}
        if uly:
            params["uly"] = uly
        if instFamily:
            params["instFamily"] = instFamily

        await self._sema.acquire()
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.get(url, params=params, ssl=SSL_CTX) as resp:
                    data = await resp.json()
        except Exception as e:
            logger.error("fetch_tickers exception: %s", e)
            return []
        finally:
            self._sema.release()

        if not isinstance(data, dict) or data.get("code") != "0":
            logger.error("fetch_tickers API error: %s", data)
            return []

        return data.get("data", [])

    async def fetch_top_market_caps(self, n: int = 30) -> list[str]:
        """
        通过 CoinGecko API 获取市值前 n 名的币种 symbol，
        并拼成 OKX 格式 instId（如 "BTC-USDT"）。
        """
        url    = f"{CG_REST}/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": n,
            "page": 1,
            "sparkline": "false"
        }
        await self._sema.acquire()
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.get(url, params=params, ssl=SSL_CTX) as resp:
                    data = await resp.json()
        except Exception as e:
            logger.error("fetch_top_market_caps exception: %s", e)
            return []
        finally:
            self._sema.release()

        if not isinstance(data, list):
            logger.error("fetch_top_market_caps API error: %s", data)
            return []

        return [f"{coin.get('symbol','').upper()}-USDT" for coin in data if coin.get("symbol")]


# 单元测试示例
if __name__ == "__main__":
    import asyncio

    async def test():
        client = MarketClient()
        tickers = await client.fetch_tickers()
        print("Tickers count:", len(tickers))
        tops = await client.fetch_top_market_caps(10)
        print("Top10:", tops)

    asyncio.run(test())
