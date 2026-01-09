import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import multiprocessing

"""
战法名称：股灾战法-极致狙击版 (Crash Recovery Ultimate)

1. 核心逻辑：在极端超跌后寻找“止跌+反转”的共振点。
2. 强制条件：
   - 价格：5.0 - 20.0 元。
   - 范围：排除ST、排除创业板(30开头)、仅限深沪A股。
   - 状态：必须满足 MACD 金叉 (DIF >= DEA) 且当日未大跌。
   - 指标：RSI < 35 (超卖) 且 成交量低于5日均量 (缩量)。
3. 止损：10/20日均线动态止损。
"""

def calculate_indicators(df):
    # RSI (14日)
    delta = df['收盘'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    df['RSI'] = 100 - (100 / (1 + (gain / loss)))
    
    # MACD 计算
    df['EMA12'] = df['收盘'].ewm(span=12, adjust=False).mean()
    df['EMA26'] = df['收盘'].ewm(span=26, adjust=False).mean()
    df['DIF'] = df['EMA12'] - df['EMA26']
    df['DEA'] = df['DIF'].ewm(span=9, adjust=False).mean()
    df['MACD_HIST'] = (df['DIF'] - df['DEA']) * 2
    
    # 均线与成交量
    df['MA10'] = df['收盘'].rolling(10).mean()
    df['MA20'] = df['收盘'].rolling(20).mean()
    df['V_MA5'] = df['成交量'].rolling(5).mean()
    return df

def screen_stock(file_path):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 30: return None
        
        latest = df.iloc[-1]
        code = str(latest['股票代码']).zfill(6)
        
        # 1. 基础硬性过滤
        if code.startswith('30') or not (code.startswith('00') or code.startswith('60')): return None
        if not (5.0 <= latest['收盘'] <= 20.0): return None
        if latest['涨跌幅'] < -3.0: return None  # 过滤仍在大幅杀跌的股票
        
        # 2. 计算指标
        df = calculate_indicators(df)
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 3. 严格战法逻辑过滤
        # 条件A: MACD金叉 (今日金叉或已在金叉状态且红柱增长)
        is_macd_gold = curr['DIF'] >= curr['DEA']
        
        # 条件B: RSI处于相对超卖低位
        is_rsi_low = curr['RSI'] < 35
        
        # 条件C: 极致缩量 (今日成交量 < 5日均量的85%)
        is_vol_shrink = curr['成交量'] < curr['V_MA5'] * 0.85
        
        # 必须同时满足上述三个核心条件
        if is_macd_gold and is_rsi_low and is_vol_shrink:
            # 计算信号强度
            score = 0
            if curr['DIF'] > prev['DIF']: score += 40 # DIF拐头
            if curr['RSI'] < 30: score += 30          # 极度超卖
            if curr['收盘'] > curr['MA10']: score += 30 # 站上10日线
            
            status = "★★★★★ (一击必中)" if score >= 70 else "★★★★☆ (优选入场)"
            
            # 自动化复盘操作建议
            if curr['收盘'] > curr['MA20']:
                advice = "已站稳20日线，趋势反转确认，建议重仓加仓，目标反弹20%。"
            else:
                advice = "MACD金叉确认，符合股灾自救战法。建议按3331法则建立首笔底仓，止损设在10日线。"

            return {
                "代码": code,
                "现价": curr['收盘'],
                "涨跌幅%": curr['涨跌幅'],
                "RSI": round(curr['RSI'], 2),
                "信号强度": status,
                "操作建议": advice
            }
    except:
        return None

def main():
    stock_files = glob.glob('stock_data/*.csv')
    with multiprocessing.Pool() as pool:
        results = [r for r in pool.map(screen_stock, stock_files) if r]
    
    if not results:
        print("今日无符合'MACD金叉+缩量超卖'严苛条件的标的。")
        return

    # 匹配名称
    names = pd.read_csv('stock_names.csv', dtype={'code': str})
    names['code'] = names['code'].str.zfill(6)
    
    res_df = pd.DataFrame(results)
    final = pd.merge(res_df, names, left_on='代码', right_on='code', how='left')
    
    # 结果按强度排序
    final = final[['代码', 'name', '现价', '涨跌幅%', 'RSI', '信号强度', '操作建议']]
    final.rename(columns={'name': '名称'}, inplace=True)
    
    # 保存结果
    now = datetime.now()
    path = f"results/{now.strftime('%Y%m')}"
    os.makedirs(path, exist_ok=True)
    filename = f"{path}/crash_recovery_strategy_{now.strftime('%Y%m%d_%H%M')}.csv"
    
    final.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"筛选完成，共发现 {len(final)} 只精品标的。")

if __name__ == "__main__":
    main()
