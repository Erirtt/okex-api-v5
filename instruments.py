#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
instruments.py

异步版：拉取 OKX SPOT 产品 instId + lever，
并把“当前北京时间”存入 PostgreSQL 的 TIMESTAMP （无时区）列，
实现写入和查询时都能看到同样的北京时间。
"""

import os
import asyncio
import logging
from datetime import datetime, timedelta

import asyncpg
from dotenv import load_dotenv
from okx.PublicData import PublicAPI

# ——— 日志配置 —————————————————————————
logging.basicConfig(
    format="[%(asctime)s] %(levelname)s %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("instruments")

# ——— 环境变量 —————————————————————————
load_dotenv()
OKX_FLAG    = os.getenv("OKX_FLAG", "0")  # "0" 实盘, "1" 模拟
PG_HOST     = os.getenv("PG_HOST")
PG_PORT     = os.getenv("PG_PORT")
PG_DB       = os.getenv("PG_DB")
PG_USER     = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

PUBLIC_API = PublicAPI(flag=OKX_FLAG)
SOURCE     = "OKX_SPOT"

async def ensure_table(pool: asyncpg.Pool):
    # 这里 updated_at 使用无时区 TIMESTAMP
    await pool.execute("""
    CREATE TABLE IF NOT EXISTS instruments (
      inst_id      TEXT PRIMARY KEY,
      lever        INT,
      updated_at   TIMESTAMP,   -- 无时区存本地时间
      source       TEXT
    );
    """)

async def upsert_instruments(pool: asyncpg.Pool, records: list):
    sql = """
    INSERT INTO instruments(inst_id, lever, updated_at, source)
    VALUES($1,$2,$3,$4)
    ON CONFLICT(inst_id) DO UPDATE
      SET lever      = EXCLUDED.lever,
          updated_at = EXCLUDED.updated_at,
          source     = EXCLUDED.source;
    """
    await pool.executemany(sql, records)

async def delete_stale(pool: asyncpg.Pool, cutoff: datetime):
    # cutoff 也是 naive 本地时间
    await pool.execute(
        "DELETE FROM instruments WHERE updated_at < $1;",
        cutoff
    )

def fetch_spot_instruments() -> list[tuple[str,int]]:
    resp = PUBLIC_API.get_instruments(instType="SPOT")
    if resp.get("code") != "0":
        raise RuntimeError(f"OKX get_instruments failed: {resp}")
    out = []
    for item in resp.get("data", []):
        inst_id = item.get("instId","").strip()
        lever_s = item.get("lever","").strip()
        try:
            lever = int(float(lever_s)) if lever_s else 0
        except:
            lever = 0
        if inst_id:
            out.append((inst_id, lever))
    return out

async def main():
    # 1) 建连接池
    pool = await asyncpg.create_pool(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASSWORD,
        database=PG_DB,
        min_size=1, max_size=3,
        command_timeout=60
    )

    # 2) 确保表存在
    await ensure_table(pool)

    # 3) 拉取产品并构造记录
    instruments = fetch_spot_instruments()
    # 生成“当前北京时间”的 naive datetime
    bj_now = datetime.utcnow() + timedelta(hours=8)
    records = [(iid, lev, bj_now, SOURCE) for iid, lev in instruments]

    # 4) UPSERT + 删除过期
    await upsert_instruments(pool, records)
    await delete_stale(pool, bj_now)
    logger.info("✅ 已写入 %d 条 instruments，时间（本地北京）: %s",
                len(records), bj_now.strftime("%Y-%m-%d %H:%M:%S"))

    # 5) 关闭连接池
    await pool.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.exception("❌ 执行失败：%s", e)
        exit(1)
