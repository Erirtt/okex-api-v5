# app.py

from flask import Flask, render_template
import json
import psycopg2
import pandas as pd
from datetime import datetime

from service.select_candidates import get_candidates_by_4h1h  # 你的候选函数
from config import settings

app = Flask(__name__)

def fetch_candidates(n=10):
    # 因为 get_candidates_by_4h1h 是 async 的，我们在这里 run 一次 event loop
    import asyncio
    return asyncio.run(get_candidates_by_4h1h(n=n))

def fetch_last_n_1h(symbol: str, n: int = 50):
    """
    同步地从 kline_1h 表拉取最近 n 根 1H K 线，
    返回 pandas.DataFrame.to_dict('records') 格式。
    """
    conn = psycopg2.connect(
        dbname=settings.PG_DB,
        user=settings.PG_USER,
        password=settings.PG_PASSWORD,
        host=settings.PG_HOST,
        port=settings.PG_PORT
    )
    sql = """
        SELECT ts, open, high, low, close
          FROM kline_1h
         WHERE symbol = %s
         ORDER BY ts DESC
         LIMIT %s
    """
    df = pd.read_sql_query(sql, conn, params=(symbol, n))
    conn.close()
    # 格式化时间，按升序
    df['ts'] = pd.to_datetime(df['ts']).dt.strftime('%Y-%m-%d %H:%M')
    return df.sort_values('ts').to_dict('records')

@app.route("/")
def index():
    # 1. 拿到候选列表
    cands = fetch_candidates(n=10)

    # 2. 对每个候选异步地拉取历史 K 线
    chart_data = {}
    for c in cands:
        chart_data[c['symbol']] = fetch_last_n_1h(c['symbol'], n=50)

    # 3. 渲染模板
    return render_template(
        "index.html",
        candidates=cands,
        chart_data=json.dumps(chart_data, ensure_ascii=False)
    )

if __name__ == "__main__":
    app.run(debug=True)
