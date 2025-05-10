# data_collector.py
"""
数据收集与 K 线聚合模块
* 接收来自 WebSocket 的 1m K 线消息
* 按 5 根 1m 自动聚合成 5m，按 12 根 5m 聚合成 1H，按 4 根 1H 聚合成 4H
* 将聚合后的 5m/1h/4h 数据写入 TimescaleDB
"""
import os
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from collections import deque
from dotenv import load_dotenv, find_dotenv


def get_db_conn():
    dotenv_path = find_dotenv()
    load_dotenv(dotenv_path)

    # 从环境变量读取连接信息
    return psycopg2.connect(
        dbname=os.getenv('PG_DB'),
        user=os.getenv('PG_USER'),
        password=os.getenv('PG_PASSWORD'),
        host=os.getenv('PG_HOST'),
        port=os.getenv('PG_PORT')
    )

class DataCollector:
    def __init__(self, strategy=None):
        # 缓存最新 bars
        self.buf_1m = deque(maxlen=5000)
        self.buf_5m = deque(maxlen=1000)
        self.buf_1h = deque(maxlen=500)
        self.buf_4h = deque(maxlen=200)
        self.strategy = strategy
        # DB 连接
        self.conn = get_db_conn()
        self._ensure_tables()

    def _ensure_tables(self):
        # 创建 TimescaleDB 表（若不存在）
        with self.conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS ohlcv_5m (
                ts TIMESTAMPTZ PRIMARY KEY,
                open DOUBLE PRECISION, high DOUBLE PRECISION,
                low DOUBLE PRECISION, close DOUBLE PRECISION,
                volume DOUBLE PRECISION
            );
            SELECT create_hypertable('ohlcv_5m','ts',if_not_exists=>TRUE);
            CREATE TABLE IF NOT EXISTS ohlcv_1h (
                ts TIMESTAMPTZ PRIMARY KEY,
                open DOUBLE PRECISION, high DOUBLE PRECISION,
                low DOUBLE PRECISION, close DOUBLE PRECISION,
                volume DOUBLE PRECISION
            );
            SELECT create_hypertable('ohlcv_1h','ts',if_not_exists=>TRUE);
            CREATE TABLE IF NOT EXISTS ohlcv_4h (
                ts TIMESTAMPTZ PRIMARY KEY,
                open DOUBLE PRECISION, high DOUBLE PRECISION,
                low DOUBLE PRECISION, close DOUBLE PRECISION,
                volume DOUBLE PRECISION
            );
            SELECT create_hypertable('ohlcv_4h','ts',if_not_exists=>TRUE);
            """)
        self.conn.commit()

    def on_1m_bar(self, bar: dict):
        # 接收原始 1m bar，缓存并按 5 根聚合
        t = pd.to_datetime(bar['ts'], unit='ms')
        self.buf_1m.append((t, bar))
        if len(self.buf_1m) >= 5 and len(self.buf_1m) % 5 == 0:
            self._agg_5m()

    def _agg_5m(self):
        # 聚合最后 5 个 1m
        last5 = list(self.buf_1m)[-5:]
        times = [t for (t, _) in last5]
        bars  = [b for (_, b) in last5]
        df = pd.DataFrame(bars, index=times)
        t0 = times[0].floor('5T')
        bar5m = {
            'ts': t0,
            'open': df['open'].iloc[0],
            'high': df['high'].max(),
            'low': df['low'].min(),
            'close': df['close'].iloc[-1],
            'volume': df['volume'].sum()
        }
        print(f"[聚合5m] {t0}  O:{bar5m['open']} H:{bar5m['high']} L:{bar5m['low']} C:{bar5m['close']} V:{bar5m['volume']}")
        self.buf_5m.append((t0, bar5m))
        self._save_5m([bar5m])
        if self.strategy:
            self.strategy.on_new_bar('5M', bar5m)
        # 每 12 根 5m 聚合成 1h
        if len(self.buf_5m) >= 12 and len(self.buf_5m) % 12 == 0:
            self._agg_1h()

    def _agg_1h(self):
        # 聚合最后 12 个 5m
        last12 = list(self.buf_5m)[-12:]
        times  = [t for (t, _) in last12]
        bars   = [b for (_, b) in last12]
        df = pd.DataFrame(bars, index=times)
        t0 = times[0].floor('H')
        bar1h = {
            'ts': t0,
            'open': df['open'].iloc[0],
            'high': df['high'].max(),
            'low': df['low'].min(),
            'close': df['close'].iloc[-1],
            'volume': df['volume'].sum()
        }
        print(f"[聚合1H] {t0}  O:{bar1h['open']} H:{bar1h['high']} L:{bar1h['low']} C:{bar1h['close']} V:{bar1h['volume']}")
        self.buf_1h.append((t0, bar1h))
        self._save_1h([bar1h])
        if self.strategy:
            self.strategy.on_new_bar('1H', bar1h)
        # 每 4 根 1h 聚合成 4h
        if len(self.buf_1h) >= 4 and len(self.buf_1h) % 4 == 0:
            self._agg_4h()

    def _agg_4h(self):
        # 聚合最后 4 个 1h
        last4 = list(self.buf_1h)[-4:]
        times = [t for (t, _) in last4]
        bars  = [b for (_, b) in last4]
        df4 = pd.DataFrame(bars, index=times)
        t0 = times[0].floor('4H')
        bar4h = {
            'ts': t0,
            'open': df4['open'].iloc[0],
            'high': df4['high'].max(),
            'low': df4['low'].min(),
            'close': df4['close'].iloc[-1],
            'volume': df4['volume'].sum()
        }
        print(f"[聚合4H] {t0}  O:{bar4h['open']} H:{bar4h['high']} L:{bar4h['low']} C:{bar4h['close']} V:{bar4h['volume']}")
        self.buf_4h.append((t0, bar4h))
        self._save_4h([bar4h])
        if self.strategy:
            self.strategy.on_new_bar('4H', bar4h)

    def _save_5m(self, bars: list):
        vals = [(b['ts'], b['open'], b['high'], b['low'], b['close'], b['volume']) for b in bars]
        with self.conn.cursor() as cur:
            execute_values(cur,
                "INSERT INTO ohlcv_5m (ts, open, high, low, close, volume) VALUES %s "
                "ON CONFLICT (ts) DO NOTHING;",
                vals
            )
        self.conn.commit()

    def _save_1h(self, bars: list):
        vals = [(b['ts'], b['open'], b['high'], b['low'], b['close'], b['volume']) for b in bars]
        with self.conn.cursor() as cur:
            execute_values(cur,
                "INSERT INTO ohlcv_1h (ts, open, high, low, close, volume) VALUES %s "
                "ON CONFLICT (ts) DO NOTHING;",
                vals
            )
        self.conn.commit()

    def _save_4h(self, bars: list):
        vals = [(b['ts'], b['open'], b['high'], b['low'], b['close'], b['volume']) for b in bars]
        with self.conn.cursor() as cur:
            execute_values(cur,
                "INSERT INTO ohlcv_4h (ts, open, high, low, close, volume) VALUES %s "
                "ON CONFLICT (ts) DO NOTHING;",
                vals
            )
        self.conn.commit()

    def close(self):
        self.conn.close()
