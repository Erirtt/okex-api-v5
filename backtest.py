#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_markprice_fetch.py

最简测试：同步发 HTTP 请求到 OKX history-mark-price-candles，
拉取最近 5 根 1m 标记价格已完结 K 线并打印。
"""

import requests
import certifi
import json

def fetch_markprice(instId: str, limit: int = 5):
    url = "https://www.okx.com/api/v5/market/history-mark-price-candles"
    params = {
        "instId": instId,  # 例如 "BTC-USD-SWAP"
        "bar":     "1m",
        "limit":   limit
    }
    # 强制使用 certifi 的根证书以避免本地证书问题
    resp = requests.get(url, params=params, timeout=10, verify=certifi.where())
    resp.raise_for_status()
    return resp.json()

if __name__ == "__main__":
    # —— 注意：标记价格仅对永续合约（SWAP）有返回 —— 
    # 如果你用 SPOT 合约（如 "BTC-USDT"）会拿到空 data。
    inst = "S-USDT"

    print(f"请求 instId={inst} 最近 5 根 1m 标记价格 K 线...")
    result = fetch_markprice(inst, limit=5)
    print(json.dumps(result, indent=2, ensure_ascii=False))

    # 如果需要分页拿更老的数据，可以接着用 before 参数：
    if result.get("data"):
        oldest_ts = int(result["data"][-1][0])
        print(f"\n-- 再次用 before={oldest_ts-1} 翻页 --")
        more = fetch_markprice(inst, limit=5)
        print(json.dumps(more, indent=2, ensure_ascii=False))
