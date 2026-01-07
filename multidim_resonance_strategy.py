import pandas as pd
import numpy as np
import os
from datetime import datetime
import multiprocessing

# ==========================================
# 战法名称：乾坤一击·多维共振突破战法
# 操作要领：
# 1. 均线走平抬头 + 站稳60日长线 (趋势反转)
# 2. MACD & KDJ & 均线三金叉共振 (动能确认)
# 3. 5-20元低价股 + 非创业板 + 非ST (筹码优势)
# 4. 放量突破前期整理平台 (一击必中)
# ==========================================

def calculate_kdj(df, n=9, m1=3, m2=3):
    low_list = df['最低'].rolling(window=n).min()
    high_list = df['最高'].rolling(window=n).max()
    rsv = (df['收盘'] - low_list) / (high_list - low_list) * 100
    df['K'] = rsv.ewm(com=m1-1).mean()
    df['D'] = df['K'].ewm(com=m2-1).mean()
    df['J'] = 3 * df['K'] - 2 * df['D']
    return df

def calculate_macd(df):
    exp1 = df['收盘'].ewm(span=12, adjust=False).mean()
    exp2 = df['收盘'].ewm(span=26, adjust=False).mean()
    df['DIF'] = exp1 - exp2
    df['DEA'] = df['DIF'].ewm(span=9, adjust=False).mean()
    df['MACD'] = (df['DIF'] - df['DEA']) * 2
    return df

def analyze_stock(file_path):
    try:
        code = os.path.basename(file_path).replace('.csv', '')
        # 排除 ST 和 创业板(300)
        if 'ST' in code or code.startswith('300'):
            return None
        
        df = pd.read_csv(file_path)
        if len(df) < 60: return None
        
        # 基础筛选：最新收盘价在 5-20 元
        last_close = df.iloc[-1]['收盘']
        if not (5.0 <= last_close <= 20.0):
            return None

        # 计算技术指标
        df['MA5'] = df['收盘'].rolling(5).mean()
        df['MA10'] = df['收盘'].rolling(10).mean()
        df['MA20'] = df['收盘'].rolling(20).mean()
        df['MA60'] = df['收盘'].rolling(60).mean()
        df = calculate_macd(df)
        df = calculate_kdj(df)
        
        # 逻辑判断
        last = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 1. 均线多头与趋势逻辑：收盘在60日线上方，且60日线走平或微升
        trend_ok = last['收盘'] > last['MA60'] and last['MA60'] >= prev['MA60'] * 0.998
        
        # 2. 三金叉共振逻辑
        # MACD金叉
        macd_cross = (prev['DIF'] <= prev['DEA'] and last['DIF'] > last['DEA']) or (last['DIF'] > last['DEA'] and last['MACD'] > prev['MACD'])
        # KDJ金叉/向上
        kdj_up = last['K'] > last['D'] and last['J'] > prev['J']
        # 均线共振 (5日线上穿10日或呈多头)
        ma_up = last['MA5'] >= last['MA10']
        
        # 3. 优中选优：量价复盘
        vol_ratio = last['成交量'] / df['成交量'].tail(5).mean() # 量比
        
        if trend_ok and macd_cross and kdj_up and ma_up:
            # 计算信号强度 (1-100)
            strength = 70
            if vol_ratio > 1.5: strength += 15 # 放量突破
            if last['涨跌幅'] > 3: strength += 15 # 实体坚决
            
            # 操作建议逻辑
            suggestion = "观察待机"
            if strength >= 90: suggestion = "【极强】重仓一击必中，主升浪起点"
            elif strength >= 80: suggestion = "【走强】建议适量试错，关注5日线支撑"
            else: suggestion = "【初步转强】轻仓观察，确认站稳长线"
            
            return {
                'code': code,
                '最新价': last_close,
                '涨跌幅': f"{last['涨跌幅']}%",
                '强度': min(strength, 100),
                '建议': suggestion,
                '战法理由': "长线走平+三位一体金叉"
            }
    except Exception:
        return None

def main():
    stock_dir = 'stock_data'
    name_file = 'stock_names.csv'
    
    # 扫描文件
    files = [os.path.join(stock_dir, f) for f in os.listdir(stock_dir) if f.endswith('.csv')]
    
    # 并行处理
    with multiprocessing.Pool() as pool:
        results = pool.map(analyze_stock, files)
    
    results = [r for r in results if r is not None]
    
    # 匹配名称
    if os.path.exists(name_file):
        names_df = pd.read_csv(name_file)
        # 确保 code 是字符串以便匹配
        names_df['code'] = names_df['code'].astype(str).str.zfill(6)
        final_list = []
        for r in results:
            name = names_df[names_df['code'] == r['code']]['name'].values
            r['名称'] = name[0] if len(name) > 0 else "未知"
            final_list.append(r)
        results = final_list

    # 结果排序 (强度优先)
    report_df = pd.DataFrame(results)
    if not report_df.empty:
        report_df = report_df.sort_values(by='强度', ascending=False)
    
    # 保存结果
    now = datetime.now()
    dir_path = now.strftime('%Y%m')
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    
    file_name = f"multidim_resonance_strategy_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    save_path = os.path.join(dir_path, file_name)
    
    report_df.to_csv(save_path, index=False, encoding='utf-8-sig')
    print(f"复盘完成，找到 {len(report_df)} 只符合战法标的。")

if __name__ == "__main__":
    main()
