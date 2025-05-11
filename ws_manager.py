import asyncio
import logging
import json
import ssl
import certifi
import websockets
from websockets.exceptions import ConnectionClosed, InvalidMessage

logger = logging.getLogger("ws_manager")
logger.setLevel(logging.INFO)
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(message)s"))
logger.addHandler(_handler)

# SSL 上下文，使用 certifi 根证书避免 SSL 认证问题
SSL_CTX = ssl.create_default_context(cafile=certifi.where())

class WSManager:
    def __init__(
        self,
        url: str,
        channels: list = None,
        handler: callable = None,
        auth: dict = None
    ):
        """
        :param url: WebSocket 地址
        :param channels: 订阅参数列表，示例: [{"channel":"candle1m","instId":"BTC-USDT"}]
        :param handler: async 回调函数，签名 async def handler(msg: dict)
        :param auth: None 或 {"apiKey":..., "secretKey":..., "passphrase":...}
        """
        self.url = url
        self.channels = channels or []
        self.handler = handler
        self.auth = auth or {}
        # 如果初始化时就传入了 channels+handler，则自动启动
        if self.channels and self.handler:
            asyncio.create_task(self._run())

    async def _run(self):
        backoff = 1
        subscribe_msg = json.dumps({"op": "subscribe", "args": self.channels})

        while True:
            try:
                logger.info("Connecting to %s …", self.url)
                async with websockets.connect(self.url, ssl=SSL_CTX) as ws:
                    logger.info("Connected, subscribing %d channels", len(self.channels))

                    # 私有频道需要先登录
                    if self.auth:
                        await self._authenticate(ws)

                    await ws.send(subscribe_msg)
                    logger.info("Subscribed: %s", self.channels)
                    backoff = 1  # 重置退避

                    while True:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            logger.debug("No data for 30s, sending ping")
                            try:
                                pong = await ws.ping()
                                await asyncio.wait_for(pong, timeout=10)
                                continue
                            except Exception:
                                logger.warning("Ping failed, reconnecting…")
                                break
                        except ConnectionClosed as e:
                            logger.warning("Connection closed (%s), reconnecting…", e)
                            break

                        # 解析并过滤
                        try:
                            data = json.loads(msg)
                        except (json.JSONDecodeError, InvalidMessage):
                            logger.warning("Invalid JSON, skipping")
                            continue

                        if data.get("event") in ("subscribe", "pong"):
                            continue

                        # 转给业务处理
                        try:
                            await self.handler(data)
                        except Exception:
                            logger.exception("Handler raised exception")

            except Exception as e:
                logger.error("WebSocket error: %s", e)

            # 指数退避重连
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 32)
            logger.info("Reconnecting in %ds…", backoff)

    async def _authenticate(self, ws):
        """
        OKX v5 私有频道认证
        """
        import time, hmac, hashlib, base64
        ts = str(time.time())
        prehash = ts + "GET" + "/users/self/verify"
        sign = base64.b64encode(
            hmac.new(
                self.auth["secretKey"].encode(),
                prehash.encode(),
                hashlib.sha256
            ).digest()
        ).decode()
        auth_msg = {
            "op": "login",
            "args": [{
                "apiKey": self.auth["apiKey"],
                "passphrase": self.auth["passphrase"],
                "timestamp": ts,
                "sign": sign
            }]
        }
        await ws.send(json.dumps(auth_msg))
        logger.info("Sent auth message")
