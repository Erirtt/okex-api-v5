# app.py
from flask import Flask, render_template
import json
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

from service.select_candidates import get_candidates  # 你的异步候选函数
from config import settings

app = Flask(__name__)

# 用 SQLAlchemy 创建 engine，给 pandas.read_sql_query 用
DB_URL = (
    f"postgresql://{settings.PG_USER}:"
    f"{settings.PG_PASSWORD}@"
    f"{settings.PG_HOST}:"
    f"{settings.PG_PORT}/"
    f"{settings.PG_DB}"
)
engine = create_engine(DB_URL, pool_pre_ping=True)

def fetch_candidates(n=10):
    import asyncio
    return asyncio.run(get_candidates(n=n))

def fetch_last_n_1h(symbol: str, n: int = 50):
    """
    从 kline_1h 拉最近 n 根 1H K 线，用 pandas + SQLAlchemy
    """
    sql = """
        SELECT ts, open, high, low, close
          FROM kline_1h
         WHERE symbol = %s
         ORDER BY ts DESC
         LIMIT %s
    """
    df = pd.read_sql_query(sql, engine, params=(symbol, n))
    # 格式化时间为 'YYYY-MM-DD HH:mm'
    df['ts'] = pd.to_datetime(df['ts']).dt.strftime('%Y-%m-%d %H:%M')
    return df.sort_values('ts').to_dict('records')

@app.route("/")
def index():
    # 1. 拿到候选列表
    cands = fetch_candidates(n=10)

    # 2. 对每个候选同步地拉取历史 K 线
    chart_data = {}
    for c in cands:
        chart_data[c['symbol']] = fetch_last_n_1h(c['symbol'], n=50)
    print(len(chart_data))
    print(chart_data)
    # 3. 渲染模板 (we used to do json.dumps here – don’t)
    return render_template(
        "index.html",
        candidates=cands,
        chart_data=chart_data  # 直接传 Python dict/list，不用先 `json.dumps`
    )

if __name__ == "__main__":
    app.run(debug=True)
