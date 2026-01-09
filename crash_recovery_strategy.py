import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import multiprocessing

"""
战法名称：股灾战法-极致回测狙击版
战法逻辑说明：
1. 择时：RSI < 35 (超卖) 且 成交量 < 5日均量*0.85 (缩量企稳)。
2. 买点：必须满足 MACD 金叉 (DIF >= DEA) 且 DIF 拐头向上。
3. 选股：5-20元，排除30开头(创业板)及ST，仅限沪深A股。
4. 仓位：3331法则（3成短线、3成趋势、3成备用、1成机动）。
5. 止损：跌破10日线减半，20日线清仓；止盈目标 20%。
"""

def calculate_indicators(df):
    """计算战法核心技术指标"""
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
    
    # 均线与成交量
    df['MA10'] = df['收盘'].rolling(10).mean()
    df['MA20'] = df['收盘'].rolling(20).mean()
    df['V_MA5'] = df['成交量'].rolling(5).mean()
    return df

def backtest_and_screen(file_path):
    """单只股票的回测与实时筛选"""
    try:
        df = pd.read_csv(file_path)
        if len(df) < 60: return None
        
        code = str(df.iloc[-1]['股票代码']).zfill(6)
        # 严格过滤：非30开头(创业板)，仅限00和60
        if code.startswith('30') or not (code.startswith('00') or code.startswith('60')): return None
        
        df = calculate_indicators(df)
        
        # --- 历史回测逻辑 ---
        # 寻找历史上符合条件的买入点：RSI<35 & 缩量 & MACD金叉
        buy_signals = (df['RSI'] < 35) & \
                      (df['成交量'] < df['V_MA5'] * 0.85) & \
                      (df['DIF'] >= df['DEA']) & \
                      (df['DIF'] > df['DIF'].shift(1))
        
        history_profit = 0
        win_rate = 0
        signal_count = buy_signals.sum()
        
        if signal_count > 0:
            profits = []
            for idx in df.index[buy_signals]:
                if idx + 10 < len(df): # 模拟持有10个交易日
                    profit = (df.loc[idx+10, '收盘'] - df.loc[idx, '收盘']) / df.loc[idx, '收盘']
                    profits.append(profit)
            if profits:
                history_profit = np.mean(profits)
                win_rate = len([p for p in profits if p > 0]) / len(profits)

        # --- 实时筛选逻辑 (最新交易日) ---
        latest = df.iloc[-1]
        if not (5.0 <= latest['收盘'] <= 20.0): return None
        
        # 必须满足买入信号且当日未大跌
        if buy_signals.iloc[-1] and latest['涨跌幅'] >= -3.0:
            score = 70
            if latest['收盘'] > latest['MA10']: score += 15
            if latest['RSI'] < 30: score += 15
            
            status = "★★★★★ (一击必中)" if score >= 85 else "★★★★☆ (优选试错)"
            
            return {
                "代码": code,
                "现价": latest['收盘'],
                "涨跌幅%": latest['涨跌幅'],
                "RSI": round(latest['RSI'], 2),
                "历史胜率%": round(win_rate * 100, 2),
                "历史平均收益%": round(history_profit * 100, 2),
                "信号强度": status,
                "操作建议": "满足‘缩量+MACD金叉’狙击点。3331法则分批入场，止损设在MA20。"
            }
    except Exception as e:
        return None

def main():
    stock_files = glob.glob('stock_data/*.csv')
    print(f"开始并行分析 {len(stock_files)} 只股票...")
    
    # 并行计算
    with multiprocessing.Pool() as pool:
        results = [r for r in pool.map(backtest_and_screen, stock_files) if r]
    
    if not results:
        print("今日无符合战法要求的精品标的。")
        return

    # 加载名称
    names = pd.read_csv('stock_names.csv', dtype={'code': str})
    names['code'] = names['code'].str.zfill(6)
    
    res_df = pd.DataFrame(results)
    final = pd.merge(res_df, names, left_on='代码', right_on='code', how='left')
    
    # 按照历史胜率和信号强度排序，确保排在前面的是最稳的
    final = final.sort_values(by=['历史胜率%', '现价'], ascending=[False, True])
    
    final = final[['代码', 'name', '现价', '涨跌幅%', 'RSI', '历史胜率%', '历史平均收益%', '信号强度', '操作建议']]
    final.rename(columns={'name': '名称'}, inplace=True)
    
    # 保存结果
    now = datetime.now()
    path = f"results/{now.strftime('%Y%m')}"
    os.makedirs(path, exist_ok=True)
    filename = f"{path}/crash_recovery_strategy_{now.strftime('%Y%m%d_%H%M')}.csv"
    
    final.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"分析完成！结果已存至 {filename}")

if __name__ == "__main__":
    main()
