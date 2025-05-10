"""
私有 WebSocket 客户端模块
提供登录并订阅私有频道的工具
"""
import asyncio
import websockets
import json
import ssl
import certifi
import hmac
import base64
import time

# SSL 上下文
SSL_CTX = ssl.create_default_context(cafile=certifi.where())


def get_local_timestamp():
    return int(time.time())


def login_params(timestamp, api_key, passphrase, secret_key):
    message = timestamp + 'GET' + '/users/self/verify'
    print(api_key)
    mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
    d = mac.digest()
    sign = base64.b64encode(d)

    login_param = {"op": "login", "args": [{"apiKey": api_key,
                                            "passphrase": passphrase,
                                            "timestamp": timestamp,
                                            "sign": sign.decode("utf-8")}]}
    login_str = json.dumps(login_param)
    return login_str

async def subscribe_private(url: str, api_key: str, passphrase: str, secret_key: str, channels: list, on_message):
    """
    登录后订阅私有频道
    :param url: 私有 WS 地址，例如 wss://ws.okx.com:8443/ws/v5/private
    :param api_key, passphrase, secret_key: OKX API 凭证
    :param channels: 私有频道列表，示例见 main.py 注释
    :param on_message: 回调函数，签名 async def on_message(data: dict)
    """
    while True:
        try:
            async with websockets.connect(url, ssl=SSL_CTX) as ws:
                # 登录
                timestamp = str(get_local_timestamp())
                print("Login...")
                login_str = login_params(timestamp, api_key, passphrase, secret_key)
                await ws.send(login_str)
                # print(f"send: {login_str}")
                res = await ws.recv()
                print(res)

                # 订阅私有频道
                await ws.send(json.dumps({"op": "subscribe", "args": channels}))
                print(f"Subscribed private: {channels}")
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
                            print("Private WS disconnected, reconnecting...")
                            break
                    data = json.loads(msg)
                    await on_message(data)
        except Exception as e:
            print("Private WS error, retrying...", e)
            await asyncio.sleep(1)