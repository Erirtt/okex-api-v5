#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OKX mark-price-candle1m 实时订阅 + 多周期聚合存库示例
使用原生 websockets + 重连逻辑（方案 A）
"""

import os
import ssl
import certifi
import json
import time
import asyncio
import websockets
from dotenv import load_dotenv

from data_collector import DataCollector

# 加载环境变量
load_dotenv()

# SSL 上下文，避免证书验证错误
SSL_CTX = ssl.create_default_context(cafile=certifi.where())

# 打印聚合结果的策略
class PrintStrategy:
    def on_new_bar(self, period: str, bar: dict):
        ts = bar['ts'].strftime('%Y-%m-%d %H:%M:%S')
        print(f"[聚合{period}] {ts}  O:{bar['open']}  H:{bar['high']}  "
              f"L:{bar['low']}  C:{bar['close']}  V:{bar['volume']}")

async def run_subscription():
    # 初始化策略和数据聚合器
    strategy  = PrintStrategy()
    collector = DataCollector(strategy=strategy)

    url       = "wss://wspap.okx.com:8443/ws/v5/business"
    args      = [{"channel": "mark-price-candle1m", "instId": "ETH-USDT"}]
    run_seconds = 2 * 3600  # 运行 2 小时示例
    start_time  = time.time()

    subscribe_msg   = json.dumps({"op": "subscribe",   "args": args})
    unsubscribe_msg = json.dumps({"op": "unsubscribe", "args": args})

    while True:
        try:
            async with websockets.connect(
                url,
                ssl=SSL_CTX,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5
            ) as ws:
                # 1) 发送订阅请求
                await ws.send(subscribe_msg)
                print(f">>> 已发送订阅: {args}")

                # 2) 接收并处理数据
                while True:
                    # 检查是否到达正常退出时间
                    if time.time() - start_time >= run_seconds:
                        # 主动退订、关闭并退出
                        await ws.send(unsubscribe_msg)
                        print(f">>> 已发送退订: {args}")
                        collector.close()
                        return

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30)
                    except asyncio.TimeoutError:
                        # 发送心跳
                        await ws.ping()
                        continue

                    data = None
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    # 先处理事件消息
                    if 'event' in data:
                        ev   = data['event']
                        arg  = data.get('arg', {}) or {}
                        ch   = arg.get('channel')
                        inst = arg.get('instId')
                        if ev == 'subscribe':
                            print(f"✅ 订阅成功: channel={ch}, instId={inst}")
                        elif ev == 'unsubscribe':
                            print(f"✅ 退订成功: channel={ch}, instId={inst}")
                        elif ev == 'error':
                            print(f"❌ 错误: code={data.get('code')}, msg={data.get('msg')}")
                        continue

                    # 处理 mark-price-candle1m 数据
                    if data.get("arg", {}).get("channel") != "mark-price-candle1m":
                        continue

                    for item in data.get("data", []):
                        ts_ms = int(item[0])
                        bar = {
                            "ts":     ts_ms,
                            "open":   float(item[1]),
                            "high":   float(item[2]),
                            "low":    float(item[3]),
                            "close":  float(item[4]),
                            "volume": float(item[5])
                        }
                        # 打印原始 1m K 线
                        print(
                            f"[原始1m(mark)] {ts_ms}  "
                            f"O:{bar['open']}  H:{bar['high']}  "
                            f"L:{bar['low']}  C:{bar['close']}  "
                            f"V:{bar['volume']}"
                        )
                        # 交给 DataCollector 聚合并写入 DB
                        try:
                            collector.on_1m_bar(bar)
                        except Exception as db_err:
                            # 只打印，不影响 WebSocket 连续性
                            print(f"⚠️ 数据库写入异常，已忽略：{db_err}")
        except (websockets.exceptions.InvalidHandshake,
                websockets.exceptions.InvalidMessage,
                ssl.SSLError,
                ConnectionError) as e:
            # 明确 WebSocket/网络层面的错误才重连
            print("❗ WebSocket/网络错误，重连中...", e)
            await asyncio.sleep(5)
            continue
        except Exception as e:
            print("❗ 未知错误，准备重连...", e)
            await asyncio.sleep(5)
            continue

async def main():
    await run_subscription()

if __name__ == "__main__":
    asyncio.run(main())
