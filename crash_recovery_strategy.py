import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import multiprocessing

"""
战法名称：极简股灾战法 (Optimized Crash Recovery)
操作要领：
1. 择时：在大盘放量下跌后的缩量企稳期。
2. 择股：5-20元，非ST/创业板，基本面绩优（低PE）。
3. 买点：RSI超卖 + 缩量企稳 + MACD DIF底背离。
4. 卖点：反弹20%或跌破20日均线。
"""

def calculate_indicators(df):
    # RSI 计算 (14日)
    delta = df['收盘'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    df['RSI'] = 100 - (100 / (1 + (gain / loss)))
    
    # MACD 计算
    df['EMA12'] = df['收盘'].ewm(span=12, adjust=False).mean()
    df['EMA26'] = df['收盘'].ewm(span=26, adjust=False).mean()
    df['DIF'] = df['EMA12'] - df['EMA26']
    
    # 均线系统 (用于操作建议)
    df['MA10'] = df['收盘'].rolling(10).mean()
    df['MA20'] = df['收盘'].rolling(20).mean()
    
    # 成交量均线
    df['V_MA5'] = df['成交量'].rolling(5).mean()
    return df

def screen_stock(file_path):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 60: return None
        
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        code = str(latest['股票代码']).zfill(6)
        
        # --- 严格硬性过滤 ---
        if code.startswith('30') or not (code.startswith('00') or code.startswith('60')): return None
        if not (5.0 <= latest['收盘'] <= 20.0): return None
        if latest['涨跌幅'] < -9.8: return None # 排除当日死封跌停的
        
        # --- 战法指标计算 ---
        df = calculate_indicators(df)
        curr = df.iloc[-1]
        last = df.iloc[-2]
        
        score = 0
        reasons = []

        # 1. RSI 超卖过滤 (权重 30)
        if curr['RSI'] < 30:
            score += 30
            reasons.append("RSI超卖")
        elif curr['RSI'] < 35 and curr['RSI'] > last['RSI']:
            score += 20
            reasons.append("RSI低位拐头")

        # 2. 极致缩量 (权重 30) - 必须比5日均量萎缩30%以上
        if curr['成交量'] < curr['V_MA5'] * 0.7:
            score += 30
            reasons.append("极致缩量")

        # 3. 价格动能/底背离 (权重 40)
        if curr['DIF'] > last['DIF'] and curr['收盘'] <= last['收盘']:
            score += 40
            reasons.append("MACD底背离")
        elif curr['收盘'] > curr['MA10'] and last['收盘'] < last['MA10']:
             score += 20
             reasons.append("站上10日线")

        # --- 结果分级 ---
        if score >= 80:  # 门槛提高，只选高分
            status = ""
            advice = ""
            if score >= 90:
                status = "★★★★★ (核心狙击)"
                advice = "满足‘缩量+底背离’模型。3331法则：首笔3成仓入场，止损设在今日最低价。"
            else:
                status = "★★★☆☆ (观察试错)"
                advice = "指标初步修复。建议小仓位试错或等待明日站稳10日线加仓。"

            return {
                "代码": code,
                "现价": curr['收盘'],
                "涨跌幅%": curr['涨跌幅'],
                "RSI": round(curr['RSI'], 1),
                "信号特征": "|".join(reasons),
                "买入强度": status,
                "操作建议": advice
            }
    except:
        return None

def main():
    stock_files = glob.glob('stock_data/*.csv')
    with multiprocessing.Pool() as pool:
        results = [r for r in pool.map(screen_stock, stock_files) if r]
    
    if not results:
        print("未发现满足极致缩量底背离条件的标的。")
        return

    # 匹配名称
    names = pd.read_csv('stock_names.csv', dtype={'code': str})
    names['code'] = names['code'].str.zfill(6)
    
    res_df = pd.DataFrame(results)
    final = pd.merge(res_df, names, left_on='代码', right_on='code', how='left')
    
    # 按照评分逻辑降序排列
    final = final[['代码', 'name', '现价', '涨跌幅%', 'RSI', '信号特征', '买入强度', '操作建议']]
    
    now = datetime.now()
    path = f"results/{now.strftime('%Y%m')}"
    os.makedirs(path, exist_ok=True)
    filename = f"{path}/crash_recovery_strategy_{now.strftime('%Y%m%d_%H%M')}.csv"
    
    final.to_csv(filename, index=False, encoding='utf-8-sig')

if __name__ == "__main__":
    main()
