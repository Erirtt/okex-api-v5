"""
主模块：根据配置选择调用 公共/私有 订阅，并自定义频道与回调
可在此模块中注释/取消注释来选择需要的频道和功能
"""
import os
import asyncio
from public_ws import subscribe_public
from private_ws import subscribe_private
from dotenv import load_dotenv
# 
async def public_handler(data):
    print("Public data:", data)

async def private_handler(data):
    print("Private data:", data)

if __name__ == '__main__':
    load_dotenv()  # 自动搜索当前目录及父目录的 .env

    loop = asyncio.get_event_loop()

    # --------- 公共频道示例（无需登录） ---------
    public_url = "wss://wspap.okx.com:8443/ws/v5/public"
   
    # 取消注释以下行，选择要订阅的公共频道：
    public_channels = [{"channel": "mark-price","instId": ""}]
    loop.create_task(subscribe_public(public_url, public_channels, public_handler))

    # --------- 私有频道示例（需登录） ---------
    private_url = "wss://wspap.okx.com:8443/ws/v5/private"
    api_key    = os.getenv('OKX_API_KEY')
    secret_key = os.getenv('OKX_API_SECRET')
    passphrase = os.getenv('OKX_PASSPHRASE')
  

    # 取消注释以下行，选择要订阅的私有频道：
    # private_channels = [
    #     {"channel":"account","ccy":"USDT"}
    # ]
    # loop.create_task(subscribe_private(private_url, api_key, passphrase, secret_key, private_channels, private_handler))

    loop.run_forever()





# WebSocket公共频道 public channels
# 实盘 real trading
# url = "wss://ws.okx.com:8443/ws/v5/business"
# 模拟盘 demo trading
# url = "wss://ws.okx.com:8443/ws/v5/public?brokerId=9999"

# WebSocket私有频道 private channels
# 实盘 real trading
# url = "wss://ws.okx.com:8443/ws/v5/private"
# 模拟盘 demo trading
url = "wss://wspap.okx.com:8443/ws/v5/private"

'''
公共频道 public channel
:param channel: 频道名
:param instType: 产品类型
:param instId: 产品ID
:param uly: 合约标的指数

'''

# 产品频道
# channels = [{"channel": "instruments", "instType": "FUTURES"}]
# 行情频道 tickers channel
# channels = [{"channel": "tickers", "instId": "BTC-USD-210326"}]
# 持仓总量频道 
# channels = [{"channel": "open-interest", "instId": "BTC-USD-210326"}]
# K线频道
# channels = [{"channel": "mark-price-candle1D","instId": "BTC-USDT"}]
# 交易频道
# channels = [{"channel": "trades", "instId": "BTC-USD-201225"}]
# 预估交割/行权价格频道
# channels = [{"channel": "estimated-price", "instType": "FUTURES", "uly": "BTC-USD"}]
# 标记价格频道
# channels = [{"channel": "mark-price", "instId": "BTC-USDT-210326"}]
# 标记价格K线频道
# channels = [{"channel": "mark-price-candle1D", "instId": "BTC-USD-201225"}]
# 限价频道
# channels = [{"channel": "price-limit", "instId": "BTC-USD-201225"}]
# 深度频道
# channels = [{"channel": "books", "instId": "BTC-USD-SWAP"}]
# 期权定价频道
# channels = [{"channel": "opt-summary", "uly": "BTC-USD"}]
# 资金费率频道
# channels = [{"channel": "funding-rate", "instId": "BTC-USD-SWAP"}]
# 指数K线频道
# channels = [{"channel": "index-candle1m", "instId": "BTC-USDT"}]
# 指数行情频道
# channels = [{"channel": "index-tickers", "instId": "BTC-USDT"}]
# status频道
# channels = [{"channel": "status"}]

'''
私有频道 private channel
:param channel: 频道名
:param ccy: 币种
:param instType: 产品类型
:param uly: 合约标的指数
:param instId: 产品ID

'''

# 账户频道
# channels = [{"channel": "account", "ccy": "USDT"}]
# 持仓频道
# channels = [{"channel": "positions", "instType": "SWAP"}]
# 订单频道
# channels = [{"channel": "orders", "instType": "FUTURES", "uly": "BTC-USD", "instId": "BTC-USD-201225"}]
# 策略委托订单频道
# channels = [{"channel": "orders-algo", "instType": "FUTURES", "uly": "BTC-USD", "instId": "BTC-USD-201225"}]

'''
交易 trade
'''

# 下单
# trade_param = {"id": "1512", "op": "order", "args": [{"side": "buy", "instId": "BTC-USDT", "tdMode": "isolated", "ordType": "limit", "px": "19777", "sz": "1"}]}
# 批量下单
# trade_param = {"id": "1512", "op": "batch-orders", "args": [
#         {"side": "buy", "instId": "BTC-USDT", "tdMode": "isolated", "ordType": "limit", "px": "19666", "sz": "1"},
#         {"side": "buy", "instId": "BTC-USDT", "tdMode": "isolated", "ordType": "limit", "px": "19633", "sz": "1"}
#     ]}
# 撤单
# trade_param = {"id": "1512", "op": "cancel-order", "args": [{"instId": "BTC-USDT", "ordId": "259424589042823169"}]}
# 批量撤单
# trade_param = {"id": "1512", "op": "batch-cancel-orders", "args": [
#         {"instId": "BTC-USDT", "ordId": "259432098826694656"},
#         {"instId": "BTC-USDT", "ordId": "259432098826694658"}
#     ]}
# 改单
# trade_param = {"id": "1512", "op": "amend-order", "args": [{"instId": "BTC-USDT", "ordId": "259432767558135808", "newSz": "2"}]}
# 批量改单
# trade_param = {"id": "1512", "op": "batch-amend-orders", "args": [
#         {"instId": "BTC-USDT", "ordId": "259435442492289024", "newSz": "2"},
#         {"instId": "BTC-USDT", "ordId": "259435442496483328", "newSz": "3"}
#     ]}