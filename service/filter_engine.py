# filter_engine.py
import pandas as pd
import numpy as np
from typing import Dict, List

# —— 链上流入量的占位函数 ——  
def get_onchain_inflow(instId: str) -> float:
    """
    返回最近 24h 的“鲸鱼”净流入量（基础币），
    真实场景应调用链上数据服务。
    """
    # TODO: 替换为真实链上数据抓取逻辑
    return np.random.uniform(0, 1000)

# 基础硬筛选条件
def cond_change_ge(threshold: float):
    """24h 涨跌幅 ≥ threshold (%)"""
    def fn(t: Dict) -> bool:
        try:
            last = float(t["last"])
            open24 = float(t["open24h"])
            return (last - open24) / open24 * 100 >= threshold
        except:
            return False
    return fn

def cond_vol_ge(threshold: float):
    """24h 成交量（基础币） ≥ threshold"""
    def fn(t: Dict) -> bool:
        try:
            return float(t["volCcy24h"]) >= threshold
        except:
            return False
    return fn

def cond_spread_le(threshold: float):
    """买一卖一价差 ≤ threshold (%)"""
    def fn(t: Dict) -> bool:
        try:
            ask = float(t["askPx"])
            bid = float(t["bidPx"])
            return (ask - bid) / bid * 100 <= threshold
        except:
            return False
    return fn

def combined_filter(tickers: List[Dict], rules: List) -> List[Dict]:
    """
    用一组 rule 函数对 tickers 过滤，所有规则都要满足
    """
    out = []
    for t in tickers:
        if all(rule(t) for rule in rules):
            # 把计算好的中间量存回，供后续打分使用
            t["_change24h"] = (float(t["last"]) - float(t["open24h"])) / float(t["open24h"]) * 100
            t["_vol24h"]    = float(t["volCcy24h"])
            ask = float(t["askPx"])
            bid = float(t["bidPx"])
            t["_spread"]    = (ask - bid) / bid * 100
            out.append(t)
    return out

# ————————————————————————————————————————————————
# 以下各函数都接收一个 pandas.DataFrame，index 为 ts, 含列 open, high, low, close, volume
# ————————————————————————————————————————————————
def cond_near_sma(df: pd.DataFrame, window: int = 50, tol: float = 0.005) -> bool:
    sma   = df["close"].rolling(window).mean().iloc[-1]
    price = df["close"].iloc[-1]
    return abs(price - sma) / sma <= tol

def cond_rsi_rebound(df: pd.DataFrame, window: int = 14) -> bool:
    delta    = df["close"].diff()
    up       = delta.clip(lower=0)
    down     = -delta.clip(upper=0)
    ema_up   = up.ewm(com=window-1, adjust=False).mean()
    ema_down = down.ewm(com=window-1, adjust=False).mean()
    rsi      = 100 - 100 / (1 + ema_up / ema_down)
    last, prev = rsi.iloc[-1], rsi.iloc[-3]
    return (prev < 30) and (30 <= last <= 50) and (last > prev)

def cond_macd_golden(df: pd.DataFrame, 
                    fast: int = 12, slow: int = 26, signal: int = 9) -> bool:
    ema_fast   = df["close"].ewm(span=fast, adjust=False).mean()
    ema_slow   = df["close"].ewm(span=slow, adjust=False).mean()
    macd_line  = ema_fast - ema_slow
    sig_line   = macd_line.ewm(span=signal, adjust=False).mean()
    hist       = macd_line - sig_line
    return ((hist.iloc[-2] < 0) and (hist.iloc[-1] > 0)) \
        or (macd_line.iloc[-1] > 0 and (hist.iloc[-1] > hist.iloc[-2]))

def cond_volume_spike(df: pd.DataFrame, window: int = 20, mult: float = 2.0) -> bool:
    avg_vol  = df["volume"].rolling(window).mean().iloc[-1]
    curr_vol = df["volume"].iloc[-1]
    return curr_vol >= avg_vol * mult

def entry_signal(
    df: pd.DataFrame,
    instId: str,
    params: Dict = None
) -> bool:
    p = {
        "sma_window":   50,   "sma_tol":   0.005,
        "rsi_window":   14,
        "macd_fast":    12,   "macd_slow": 26, "macd_signal": 9,
        "vol_window":   20,   "vol_mult":  2.0,
        "onchain_min": 100
    }
    if params:
        p.update(params)

    if not cond_near_sma(df, p["sma_window"], p["sma_tol"]):
        return False
    if not cond_rsi_rebound(df, p["rsi_window"]):
        return False
    if not cond_macd_golden(df, p["macd_fast"], p["macd_slow"], p["macd_signal"]):
        return False
    if not cond_volume_spike(df, p["vol_window"], p["vol_mult"]):
        return False
    if get_onchain_inflow(instId) < p["onchain_min"]:
        return False
    return True

def full_entry_filter(
    tickers: List[Dict],
    price_histories: Dict[str, pd.DataFrame],
    basic_rules: List,
    entry_params: Dict = None
) -> List[str]:
    prelim = combined_filter(tickers, basic_rules)
    entries = []
    for t in prelim:
        inst = t["instId"]
        df = price_histories.get(inst)
        if df is None:
            continue
        if entry_signal(df, inst, entry_params):
            entries.append(inst)
    return entries
