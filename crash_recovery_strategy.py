import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import multiprocessing

"""
战法名称：股灾战法-阶梯筛选版 (Crash Recovery Tiered Selection)
备注：如果市场极度低迷，脚本会降低非核心条件的硬性门槛，但保持 MACD 金叉为底线。
逻辑：
1. 核心底线：MACD金叉 (DIF >= DEA) + 深沪A股 + 5-20元。
2. 加分项：RSI < 35 (超卖)、成交量 < 5日均量 (缩量)。
3. 操作：根据总分给出“狙击”、“试错”或“观察”建议。
"""

def calculate_indicators(df):
    # RSI (14日)
    delta = df['收盘'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    df['RSI'] = 100 - (100 / (1 + (gain / loss)))
    
    # MACD
    df['EMA12'] = df['收盘'].ewm(span=12, adjust=False).mean()
    df['EMA26'] = df['收盘'].ewm(span=26, adjust=False).mean()
    df['DIF'] = df['EMA12'] - df['EMA26']
    df['DEA'] = df['DIF'].ewm(span=9, adjust=False).mean()
    
    # 均线与成交量
    df['MA10'] = df['收盘'].rolling(10).mean()
    df['MA20'] = df['收盘'].rolling(20).mean()
    df['V_MA5'] = df['成交量'].rolling(5).mean()
    return df

def screen_stock(file_path):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 60: return None
        
        latest = df.iloc[-1]
        code = str(latest['股票代码']).zfill(6)
        
        # 1. 硬性准入 (排除创业板, 价格区间)
        if code.startswith('30') or not (code.startswith('00') or code.startswith('60')): return None
        if not (5.0 <= latest['收盘'] <= 20.0): return None
        
        df = calculate_indicators(df)
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
        # --- 阶梯评分逻辑 ---
        score = 0
        
        # 条件A: MACD 金叉 (核心底线，不满足直接排除)
        if curr['DIF'] >= curr['DEA']:
            score += 40
        else:
            return None # 连金叉都没有，绝对不碰
            
        # 条件B: RSI 超卖加分
        if curr['RSI'] < 30: score += 30
        elif curr['RSI'] < 40: score += 15
        
        # 条件C: 缩量加分 (代表抛压衰竭)
        if curr['成交量'] < curr['V_MA5'] * 0.9: score += 30
        elif curr['成交量'] < curr['V_MA5']: score += 10
        
        # 条件D: 站上10日线 (代表短期走强)
        if curr['收盘'] > curr['MA10']: score += 10

        # --- 回测数据 (简单模拟) ---
        # 计算该股近1年类似信号后的表现
        signals = (df['DIF'] >= df['DEA']) & (df['RSI'] < 40)
        win_rate = 0
        if signals.sum() > 0:
            profits = []
            for idx in df.index[signals]:
                if idx + 5 < len(df):
                    profits.append(df.loc[idx+5, '收盘'] > df.loc[idx, '收盘'])
            win_rate = sum(profits) / len(profits) if profits else 0

        # --- 结果分级 ---
        if score >= 50: # 只要及格就输出，但分等级
            if score >= 85:
                intensity = "★★★★★ (核心狙击)"
                advice = "极端超跌+缩量金叉，反转概率极高。3331法则建立底仓。"
            elif score >= 70:
                intensity = "★★★★☆ (优选试错)"
                advice = "已止跌并金叉，缩量稍显不足。可轻仓试错。"
            else:
                intensity = "★★★☆☆ (观察待定)"
                advice = "仅满足金叉底线，RSI尚未杀透，建议先放入自选股观察。"

            return {
                "代码": code,
                "现价": curr['收盘'],
                "涨跌幅%": curr['涨跌幅'],
                "RSI": round(curr['RSI'], 2),
                "历史胜率%": round(win_rate * 100, 2),
                "信号强度": intensity,
                "操作建议": advice
            }
    except:
        return None

def main():
    stock_files = glob.glob('stock_data/*.csv')
    with multiprocessing.Pool() as pool:
        results = [r for r in pool.map(screen_stock, stock_files) if r]
    
    if not results:
        print("当前市场环境下，无任何个股满足MACD金叉底线。")
        return

    names = pd.read_csv('stock_names.csv', dtype={'code': str})
    names['code'] = names['code'].str.zfill(6)
    
    res_df = pd.DataFrame(results)
    final = pd.merge(res_df, names, left_on='代码', right_on='code', how='left')
    final = final.sort_values(by='RSI', ascending=True) # 越超卖越靠前
    
    final = final[['代码', 'name', '现价', '涨跌幅%', 'RSI', '历史胜率%', '信号强度', '操作建议']]
    
    now = datetime.now()
    path = f"results/{now.strftime('%Y%m')}"
    os.makedirs(path, exist_ok=True)
    filename = f"{path}/crash_recovery_strategy_{now.strftime('%Y%m%d_%H%M')}.csv"
    
    final.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"分析完成。找到 {len(final)} 只标的，已按 RSI 排序。")

if __name__ == "__main__":
    main()
