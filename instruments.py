#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
instruments.py

异步版本：每次全量拉取 OKX SPOT 产品（instId + baseCcy + lever），
并写入 PostgreSQL 表（instruments），
使用 asyncpg 连接池，保证异步和重连能力。
updated_at 字段已改为 TEXT，无需调整。
"""
import pytz
import os
import asyncio
import logging
from datetime import datetime, timedelta

import asyncpg
from dotenv import load_dotenv
from okx.PublicData import PublicAPI

# ——— 日志配置 ———
logging.basicConfig(
    format="[%(asctime)s] %(levelname)s %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("instruments")

# ——— 加载环境变量 ———
load_dotenv()
OKX_FLAG    = os.getenv("OKX_FLAG", "0")  # "0"=实盘, "1"=模拟
PG_HOST     = os.getenv("PG_HOST")
PG_PORT     = os.getenv("PG_PORT")
PG_DB       = os.getenv("PG_DB")
PG_USER     = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

PUBLIC_API = PublicAPI(flag=OKX_FLAG)
SOURCE     = "OKX_SPOT"

# ——— 异步表结构与写入 ———
async def ensure_table(pool: asyncpg.Pool):
    # updated_at 已经是 TEXT 类型
    await pool.execute("""
    CREATE TABLE IF NOT EXISTS instruments (
        inst_id      TEXT      PRIMARY KEY,
        base_ccy     TEXT      NOT NULL,
        lever        INT       NOT NULL,
        updated_at   TEXT,
        source       TEXT      NOT NULL
    );
    """)

async def upsert_instruments(pool: asyncpg.Pool, records: list):
    """
    批量 UPSERT：inst_id, base_ccy, lever, updated_at, source
    """
    sql = """
    INSERT INTO instruments(inst_id, base_ccy, lever, updated_at, source)
    VALUES($1,$2,$3,$4,$5)
    ON CONFLICT (inst_id) DO UPDATE
      SET base_ccy   = EXCLUDED.base_ccy,
          lever      = EXCLUDED.lever,
          updated_at = EXCLUDED.updated_at,
          source     = EXCLUDED.source;
    """
    async with pool.acquire() as conn:
        await conn.executemany(sql, records)

async def delete_stale(pool: asyncpg.Pool, cutoff: str):
    """
    删除所有 updated_at < cutoff 的旧记录，实现“全覆盖”效果。
    cutoff: 字符串，格式 "YYYY-MM-DD HH:MM:SS"
    """
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM instruments WHERE updated_at < $1;",
            cutoff
        )

# ——— 拉取 OKX 产品 ———
def fetch_spot_instruments() -> list[tuple[str,str,int]]:
    """
    同步调用 OKX SDK，返回 [(instId, baseCcy, lever), ...]
    """
    resp = PUBLIC_API.get_instruments(instType="SPOT")
    if resp.get("code") != "0":
        raise RuntimeError(f"OKX get_instruments failed: {resp}")
    out = []
    for item in resp.get("data", []):
        inst_id  = item.get("instId", "").strip()
        base_ccy = item.get("baseCcy", "").strip()
        lever_s  = item.get("lever", "").strip()
        try:
            lever = int(float(lever_s)) if lever_s else 0
        except:
            lever = 0
        if inst_id and base_ccy:
            out.append((inst_id, base_ccy, lever))
    return out

# ——— 主流程 ———
async def main():
    # 1) 建立 asyncpg 连接池
    pool = await asyncpg.create_pool(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASSWORD,
        database=PG_DB,
        min_size=1, max_size=3,
        command_timeout=60
    )

    # 2) 确保表存在
    await ensure_table(pool)

    # 3) 拉取产品并构造写入记录
    instruments = fetch_spot_instruments()

    # 生成北京时间字符串
    bj_now = datetime.utcnow() + timedelta(hours=8)
    bj_str = bj_now.strftime("%Y-%m-%d %H:%M:%S")
    print(f"更新时间（北京时间）：{bj_str}")

    # 构造 records：inst_id, base_ccy, lever, updated_at, source
    records = [
        (inst_id, base_ccy, lever, bj_str, SOURCE)
        for inst_id, base_ccy, lever in instruments
    ]

    # 4) UPSERT + 删除过期
    await upsert_instruments(pool, records)
    await delete_stale(pool, bj_str)
    logger.info("✅ 共写入/更新 %d 条 instruments", len(records))

    # 5) 关闭连接池
    await pool.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.exception("❌ 执行失败：%s", e)
        exit(1)
