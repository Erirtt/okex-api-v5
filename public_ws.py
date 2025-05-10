"""
公共 WebSocket 客户端模块
提供无需登录即可订阅 OKX 公共频道的工具
"""
import asyncio
import websockets
import json
import ssl
import certifi

# SSL 上下文，使用 certifi 根证书避免 SSL 认证问题
SSL_CTX = ssl.create_default_context(cafile=certifi.where())

async def subscribe_public(url: str, channels: list, on_message):
    """
    订阅公共频道，无需登录
    :param url: WebSocket 地址，例如 wss://ws.okx.com:8443/ws/v5/public
    :param channels: 订阅参数列表，示例列表见 main.py 注释
    :param on_message: 回调函数，签名 async def on_message(data: dict)
    """
    while True:
        try:
            async with websockets.connect(url, ssl=SSL_CTX) as ws:
                await ws.send(json.dumps({"op":"subscribe","args":channels}))
                print(f"Subscribed public: {channels}")
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=25)
                    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
                        # 心跳
                        try:
                            await ws.send(json.dumps({"op":"ping"}))
                            await ws.recv()
                            continue
                        except:
                            print("Public WS disconnected, reconnecting...")
                            break
                    data = json.loads(msg)
                    # 过滤订阅确认与心跳回复
                    if 'event' in data:
                        continue
                    await on_message(data)
        except Exception as e:
            print("Public WS error, retrying...", e)
            await asyncio.sleep(1)