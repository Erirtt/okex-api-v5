# app.py
import asyncio
import json

import asyncpg
import pandas as pd
import plotly.graph_objs as go
from flask import Flask, render_template

from config import settings
from service.select_candidates_with_plan import get_candidates_with_plan
from service.load_history import load_history_async

app = Flask(__name__)

async def gather_data():
    # 1) 建池
    pool = await asyncpg.create_pool(
        user=settings.PG_USER, password=settings.PG_PASSWORD,
        database=settings.PG_DB, host=settings.PG_HOST,
        port=settings.PG_PORT, min_size=1, max_size=10
    )
    try:
        # 2) 拿候选
        data = await get_candidates_with_plan(pool=pool)
        # 3) 计算入场/止损/止盈，并拉 1H K 线
        charts = {}
        for it in data['final']:
            sym = it['symbol']
            price = it['close']
            low20 = it['recent_low']
            # 风险收益比 1:2
            entry = price
            stop  = low20
            tp    = price + (price - low20) * 2
            it.update(entry=round(entry,2),
                      stop=round(stop,2),
                      tp=round(tp,2))
            # 拉最近 50 根 1H
            df = await load_history_async(sym, limit=50, pool=pool)
            if df is None:
                continue
            fig = go.Figure(data=[go.Candlestick(
                x=df.index,
                open=df['open'], high=df['high'],
                low=df['low'],   close=df['close']
            )])
            fig.update_layout(
                margin=dict(l=0,r=0,t=20,b=0),
                height=300,
                xaxis_title="时间",
                yaxis_title="价格"
            )
            charts[sym] = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        return data['prelim'], data['final'], charts
    finally:
        await pool.close()

@app.route("/")
def index():
    prelim, final, charts = asyncio.run(gather_data())
    return render_template("index.html",
                           prelim=prelim,
                           final=final,
                           charts=charts)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
