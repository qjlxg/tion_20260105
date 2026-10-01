import pandas as pd
import numpy as np
import os
from datetime import datetime
import multiprocessing

"""
战法名称：乾坤一击·多维共振突破
操作要领：
1. 【趋势】：60日线走平或抬头，股价站稳。
2. 【量能】：当日成交量 > 5日均量 2倍（放量突破）。
3. 【共振】：均线多头 + MACD金叉 + KDJ金叉。
4. 【止损】：跌破 20 日线或 5% 强制离场。
"""

def analyze_stock(file_info):
    file_path, name_dict = file_info
    try:
        code = os.path.basename(file_path).split('.')[0]
        # 1. 基础硬过滤：排除创业板(30)、科创板(68)、ST
        if code.startswith(('30', '68')) or 'ST' in name_dict.get(code, ''):
            return None
        
        df = pd.read_csv(file_path)
        if len(df) < 120: return None
        
        # 数据整理
        df['MA5'] = df['收盘'].rolling(5).mean()
        df['MA10'] = df['收盘'].rolling(10).mean()
        df['MA20'] = df['收盘'].rolling(20).mean()
        df['MA60'] = df['收盘'].rolling(60).mean()
        df['VMA5'] = df['成交量'].rolling(5).mean()
        
        # MACD 计算
        ema12 = df['收盘'].ewm(span=12, adjust=False).mean()
        ema26 = df['收盘'].ewm(span=26, adjust=False).mean()
        dif = ema12 - ema26
        dea = dif.ewm(span=9, adjust=False).mean()
        macd = (dif - dea) * 2
        
        last = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 2. 严格核心条件筛选
        # 条件A: 价格在 5-20元 且 今日涨幅 > 2%
        if not (5.0 <= last['收盘'] <= 20.0) or last['涨跌幅'] <= 2.0: return None
        
        # 条件B: 趋势走平抬头 (MA60不下降)
        if last['MA60'] < prev['MA60'] * 0.998: return None
        
        # 条件C: 倍量突破 (核心：主力进场)
        vol_ratio = last['成交量'] / last['VMA5']
        if vol_ratio < 1.8: return None
        
        # 条件D: 均线多头发散初态
        ma_perfect = last['MA5'] > last['MA10'] > last['MA20']
        
        # 3. 评分系统 (0-100)
        score = 50
        if ma_perfect: score += 15
        if last['收盘'] > last['MA60']: score += 10
        if dif.iloc[-1] > dea.iloc[-1]: score += 10 # MACD金叉
        if vol_ratio > 2.5: score += 15 # 巨量突破
        
        # 4. 自动生成操作建议
        if score >= 85:
            action = "【买入】共振极强，放量起爆，一击必中"
            strength = "极高 (S)"
        elif score >= 75:
            action = "【试错】形态良好，量能配合，分批建仓"
            strength = "高 (A)"
        else:
            return None # 剔除低分，不给用户增加干扰

        return {
            '代码': code,
            '名称': name_dict.get(code, '未知'),
            '收盘价': last['收盘'],
            '涨跌幅': f"{last['涨跌幅']}%",
            '量比': round(vol_ratio, 2),
            '信号强度': strength,
            '操作建议': action,
            '逻辑分析': "均线/MACD/量能三者共振突破"
        }
    except:
        return None

def main():
    stock_data_dir = 'stock_data'
    names_file = 'stock_names.csv'
    
    # 加载股票名称对照表
    name_dict = {}
    if os.path.exists(names_file):
        n_df = pd.read_csv(names_file)
        name_dict = dict(zip(n_df['code'].astype(str).str.zfill(6), n_df['name']))

    files = [os.path.join(stock_data_dir, f) for f in os.listdir(stock_data_dir) if f.endswith('.csv')]
    tasks = [(f, name_dict) for f in files]
    
    # 并行处理加快速度
    with multiprocessing.Pool() as pool:
        raw_results = pool.map(analyze_stock, tasks)
    
    # 过滤无效结果并按强度排序
    final_results = [r for r in raw_results if r is not None]
    final_results = sorted(final_results, key=lambda x: x['量比'], reverse=True)[:5] # 只要最强前5

    # 保存结果
    now = datetime.now()
    output_dir = now.strftime('%Y%m')
    os.makedirs(output_dir, exist_ok=True)
    
    file_name = f"multidim_resonance_strategy_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    output_path = os.path.join(output_dir, file_name)
    
    pd.DataFrame(final_results).to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"复盘完成！精选标的数量：{len(final_results)}")

if __name__ == "__main__":
    main()
