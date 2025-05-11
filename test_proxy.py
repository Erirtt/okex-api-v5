import asyncio
from aiohttp import ClientSession, TCPConnector
from aiohttp_socks import ProxyConnector

async def test_via_socks():
    # 这是你本地运行的 SOCKS5 服务，假设 127.0.0.1:44321 直接连到目标节点
    proxy_url = "socks5://ff942fe9-9d68-4821-b212-b9f012d5ddc4@high.ab.he.defehaita.xyz:44221"
    # 如果是用户名/密码模式，aiohttp-socks 会自动拆出 creds

    # 构造一个 SOCKS5 Connector
    connector = ProxyConnector.from_url(proxy_url, rdns=True)

    # HTTP 请求示例
    async with ClientSession(connector=connector) as sess:
        async with sess.get("https://www.google.com") as resp:
            print("Status:", resp.status)
            print("Body snippet:", (await resp.text())[:200])

    # WebSocket 示例
    import websockets
    async with websockets.connect(
        "wss://ws.okx.com:8443/ws/v5/public",
        ssl=True,
        sock=await connector.connect(None, None)  # 让 connector 建立底层 socket
    ) as ws:
        await ws.send('{"op":"ping"}')
        print("Pong:", await ws.recv())

if __name__ == "__main__":
    asyncio.run(test_via_socks())
