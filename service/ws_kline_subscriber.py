#!/usr/bin/env python3
# ws_kline_subscriber_psycopg2.py
# -*- coding: utf-8 -*-
"""
OKX 1H/4H K 线 WebSocket 实时订阅服务（使用 psycopg2）
- 普通 K 线走 Public 接入点 (candle1H, candle4H)
- 自动断线重连 & 重订阅
- 只写入 confirm=="1" 的已完结 K 线
- 幂等插入 TimescaleDB 表 kline_1h / kline_4h
- 数据库访问由 psycopg2 ThreadedConnectionPool 负责，同步写入在线程池中执行
"""

import os
import ssl
import json
import asyncio
import logging
from datetime import datetime

import certifi
import websockets
import psycopg2
from psycopg2.pool import ThreadedConnectionPool

from config import settings

# ——— 常量配置 ———
WS_URL          = settings.OKX_KLINE_URL
PING_INTERVAL   = 20
RECONNECT_DELAY = 5

TABLE_1H = "kline_1h"
TABLE_4H = "kline_4h"

# SQL 模板
SQL_INSERT_1H = f"""
INSERT INTO {TABLE_1H} (
  symbol, ts, open, high, low, close,
  vol, vol_ccy, vol_ccy_quote, confirm
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (symbol, ts) DO NOTHING;
"""
SQL_INSERT_4H = SQL_INSERT_1H.replace(TABLE_1H, TABLE_4H)

# SSL 上下文，避免证书验证错误
SSL_CTX = ssl.create_default_context(cafile=certifi.where())

# 日志配置
logging.basicConfig(format="[%(asctime)s] %(levelname)s %(message)s",
                    level=logging.INFO)
logger = logging.getLogger("ws_kline")

# 全局写入计数
cnt_1h = 0
cnt_4h = 0


def init_db_pool():
    """初始化 psycopg2 线程连接池"""
    return ThreadedConnectionPool(
        minconn=1,
        maxconn=10,
        user=settings.PG_USER,
        password=settings.PG_PASSWORD,
        host=settings.PG_HOST,
        port=settings.PG_PORT,
        database=settings.PG_DB
    )


def load_symbols_sync(db_pool):
    """同步：从 instruments 表读取所有 USDT 合约 inst_id 列表"""
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT inst_id FROM instruments WHERE inst_id LIKE '%-USDT'")
            rows = cur.fetchall()
        symbols = [r[0] for r in rows]
    finally:
        db_pool.putconn(conn)
    logger.info(f"Loaded {len(symbols)} symbols from instruments")
    return symbols


def insert_records_sync(db_pool, sql, records):
    """同步写库函数，放到线程池里执行"""
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, records)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)


async def monitor_loop():
    """后台监控：每分钟输出写库计数，并清零"""
    global cnt_1h, cnt_4h
    while True:
        await asyncio.sleep(60)
        logger.info(f"[Monitor] last 60s inserts → 1H:{cnt_1h} bars, 4H:{cnt_4h} bars")
        cnt_1h = cnt_4h = 0


async def subscribe_and_consume(db_pool):
    """
    核心订阅 & 写库循环（1H/4H）
    - 先同步加载 symbols
    - 构造订阅消息，订阅 candle1H + candle4H
    - 不断读取 ws 消息并写库
    - 出错重连
    """
    # 1) 同步加载所有 symbol
    symbols = await asyncio.get_event_loop().run_in_executor(
        None, load_symbols_sync, db_pool
    )

    # 2) 构造订阅 args
    args = []
    for sym in symbols:
        args.append({"channel": "candle1H", "instId": sym})
        args.append({"channel": "candle4H", "instId": sym})
    sub_msg = json.dumps({"op": "subscribe", "args": args})

    # 3) 启动监控后台任务
    asyncio.create_task(monitor_loop())

    loop = asyncio.get_event_loop()

    # 4) 订阅 + 消费循环
    while True:
        try:
            async with websockets.connect(
                WS_URL, ssl=SSL_CTX, ping_interval=PING_INTERVAL
            ) as ws:
                # 订阅
                await ws.send(sub_msg)
                logger.info(f"✅ Subscribed to {len(args)} channels")

                async for raw in ws:
                    data = json.loads(raw)

                    # —— 处理 event 消息 —— 
                    if "event" in data:
                        ev = data["event"]
                        if ev == "error":
                            # 打印完整 code+msg
                            logger.error(f"WS error code={data.get('code')} msg={data.get('msg')}")
                        else:
                            arg = data.get("arg", {}) or {}
                            logger.info(f"WS event={ev} channel={arg.get('channel')} inst={arg.get('instId')}")
                        continue

                    # —— 正常 K 线消息 —— 
                    chan = data["arg"]["channel"]
                    if chan not in ("candle1H", "candle4H"):
                        continue

                    records = []
                    for it in data["data"]:
                        ts_ms   = int(it[0])
                        confirm = it[8] if len(it) > 8 else "1"
                        if confirm != "1":
                            continue
                        ts      = datetime.fromtimestamp(ts_ms/1000.0)
                        o, h, l, c = map(float, it[1:5])
                        vol       = float(it[5]) if len(it)>5 else 0.0
                        vol_ccy   = float(it[6]) if len(it)>6 else 0.0
                        vol_quote = float(it[7]) if len(it)>7 else 0.0

                        records.append((
                            data["arg"]["instId"],
                            ts, o, h, l, c,
                            vol, vol_ccy, vol_quote,
                            True
                        ))

                    if records:
                        # 挑选对应 SQL
                        sql = SQL_INSERT_1H if chan == "candle1H" else SQL_INSERT_4H
                        # 委托线程池写库
                        await loop.run_in_executor(
                            None, insert_records_sync, db_pool, sql, records
                        )
                        # 更新计数器
                        global cnt_1h, cnt_4h
                        if chan == "candle1H":
                            cnt_1h += len(records)
                        else:
                            cnt_4h += len(records)

        except Exception as e:
            logger.error(f"❗ WS exception, reconnecting in {RECONNECT_DELAY}s: {e}")
            await asyncio.sleep(RECONNECT_DELAY)


async def main():
    # 1) 初始化 psycopg2 线程池
    db_pool = init_db_pool()

    # 2) 启动订阅消费
    await subscribe_and_consume(db_pool)

    # 3) 退出时关闭所有连接
    db_pool.closeall()


if __name__ == "__main__":
    asyncio.run(main())
