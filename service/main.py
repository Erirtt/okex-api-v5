#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
main.py

FastAPI 服务，提供：
1. 页面索引（渲染 templates/index.html）
2. /api/candidates —— 调用 select_candidates.get_candidates
3. /api/kline      —— 查询指定交易对的最近 60 根 1 分钟 K 线
"""

import asyncio
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from select_candidates import get_candidates
from load_history import load_history_async

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """
    渲染首页
    """
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/candidates", response_class=JSONResponse)
async def api_candidates():
    """
    返回：
      {
        prelim: [...],  # 初步筛选列表（字典数组）
        final:  [...]   # 最终入场候选（symbol 字符串数组）
      }
    """
    try:
        data = await get_candidates()
        return JSONResponse(content=data)
    except Exception as e:
        # 失败时返回空结构
        return JSONResponse(content={"prelim": [], "final": []}, status_code=500)


@app.get("/api/kline", response_class=JSONResponse)
async def api_kline(symbol: str):
    """
    返回最近 60 根 1 分钟 K 线 JSON：
    {
      ts:     ["2025-05-13T12:00:00", ...],
      open:   [...],
      high:   [...],
      low:    [...],
      close:  [...],
      volume: [...],
    }
    """
    try:
        df = await load_history_async(symbol, limit=60)
        if df is None or df.empty:
            raise HTTPException(status_code=404, detail="未找到 K 线数据")
        df = df.sort_index()
        return JSONResponse({
            "ts":     [t.isoformat() for t in df.index],
            "open":   df["open"].tolist(),
            "high":   df["high"].tolist(),
            "low":    df["low"].tolist(),
            "close":  df["close"].tolist(),
            "volume": df["volume"].tolist(),
        })
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
