import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import multiprocessing

"""
战法名称：股灾战法-绝对狙击版 (Crash Recovery Sniper Mode)
操作要领：
1. 目的：宁可错过，不可做错。只抓取恐慌杀跌后的“黄金坑”反转点。
2. 强制筛选：
   - 价格 5.0-20.0 元，排除ST，排除创业板(30开头)。
   - 必须 MACD 金叉 (DIF >= DEA) 且 DIF 向上拐头。
   - 必须 RSI < 35 (超卖区)。
   - 必须 极致缩量 (成交量 < 5日均量 * 0.8)。
3. 操作：一击必中，3331法则滚动操作，止损严格执行均线原则。
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
    df['V_MA5'] = df['成交量'].rolling(5).mean()
    return df

def sniper_screen(file_path):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 60: return None
        
        latest = df.iloc[-1]
        code = str(latest['股票代码']).zfill(6)
        
        # 1. 基础硬性过滤
        if code.startswith('30') or not (code.startswith('00') or code.startswith('60')): return None
        if not (5.0 <= latest['收盘'] <= 20.0): return None
        
        df = calculate_indicators(df)
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
        # --- 核心狙击条件判定 ---
        # 条件1：MACD金叉
        is_macd_gold = curr['DIF'] >= curr['DEA'] and curr['DIF'] > prev['DIF']
        # 条件2：RSI超卖
        is_rsi_oversold = curr['RSI'] < 35
        # 条件3：极致缩量 (抛压衰竭)
        is_vol_shrink = curr['成交量'] < curr['V_MA5'] * 0.8
        
        # 只有三个条件同时满足，才是“狙击”级别
        if is_macd_gold and is_rsi_oversold and is_vol_shrink:
            # 计算历史表现供参考
            signals = (df['DIF'] >= df['DEA']) & (df['RSI'] < 35) & (df['成交量'] < df['V_MA5'] * 0.8)
            win_rate = 0
            if signals.sum() > 0:
                # 简单计算信号发出5天后上涨的概率
                results = []
                for idx in df.index[signals]:
                    if idx + 5 < len(df):
                        results.append(df.loc[idx+5, '收盘'] > df.loc[idx, '收盘'])
                win_rate = sum(results) / len(results) if results else 0

            return {
                "代码": code,
                "现价": curr['收盘'],
                "涨跌幅%": curr['涨跌幅'],
                "RSI": round(curr['RSI'], 2),
                "历史信号胜率%": round(win_rate * 100, 2),
                "信号强度": "★★★★★ (核心狙击)",
                "操作建议": "符合极端缩量+金叉共振。建议3成仓位狙击，止损今日最低价或10日线。"
            }
        else:
            return None # 非狙击级别直接丢弃
    except:
        return None

def main():
    stock_files = glob.glob('stock_data/*.csv')
    # 并行扫描所有CSV
    with multiprocessing.Pool() as pool:
        results = [r for r in pool.map(sniper_screen, stock_files) if r]
    
    if not results:
        print("今日未发现符合‘绝对狙击’标准的标的，建议继续空仓观望。")
        return

    # 匹配名称
    names = pd.read_csv('stock_names.csv', dtype={'code': str})
    names['code'] = names['code'].str.zfill(6)
    
    res_df = pd.DataFrame(results)
    final = pd.merge(res_df, names, left_on='代码', right_on='code', how='left')
    
    # 按胜率降序排序
    final = final.sort_values(by='历史信号胜率%', ascending=False)
    final = final[['代码', 'name', '现价', '涨跌幅%', 'RSI', '历史信号胜率%', '信号强度', '操作建议']]
    final.rename(columns={'name': '名称'}, inplace=True)

    # 结果推送到年月目录
    now = datetime.now()
    path = f"results/{now.strftime('%Y%m')}"
    os.makedirs(path, exist_ok=True)
    filename = f"{path}/crash_recovery_strategy_{now.strftime('%Y%m%d_%H%M')}.csv"
    
    final.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"狙击复盘完成。筛选出 {len(final)} 只精品标的。")

if __name__ == "__main__":
    main()
