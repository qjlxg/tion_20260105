import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

# ==========================================
# 战法名称：量价乾坤优选战法 (Volume-Price Mastery)
# 操作要领：
# 1. 核心看量：量比是关键，无量不入。
# 2. 价格区间：5-20元，剔除垃圾股与创业板。
# 3. 信号分级：结合量价趋势，给出明确强度与建议。
# ==========================================

DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
OUTPUT_BASE_DIR = 'results'

def analyze_stock(file_path, stock_names):
    try:
        df = pd.read_csv(file_path)
        if df.empty or len(df) < 20:
            return None
        
        # 基础过滤：代码格式与板块排除
        code = os.path.basename(file_path).replace('.csv', '')
        # 排除 30 (创业板), 688 (科创板), 排除 ST (通过名称判断)
        if code.startswith('30') or code.startswith('688'):
            return None
            
        stock_name = stock_names.get(code, "未知")
        if "ST" in stock_name or "*ST" in stock_name:
            return None

        # 提取最近数据
        last_row = df.iloc[-1]
        prev_row = df.iloc[-2]
        
        curr_price = last_row['收盘']
        curr_vol = last_row['成交量']
        avg_vol_5 = df['成交量'].tail(5).mean()
        
        # 基础条件筛选：价格区间 5.0 - 20.0
        if not (5.0 <= curr_price <= 20.0):
            return None

        # --- 核心量价逻辑计算 ---
        price_change = last_row['涨跌幅']
        vol_ratio = curr_vol / avg_vol_5 if avg_vol_5 > 0 else 0
        
        signal = ""
        strength = 0
        advice = ""
        
        # 1. 量增价涨 - 攻击型 (优选)
        if price_change > 2 and vol_ratio > 1.5:
            signal = "量增价涨 (主升)"
            strength = 90
            advice = "主力强力介入，建议积极跟进或加仓。"
            
        # 2. 低位缩量 - 观察型
        elif abs(price_change) < 1 and vol_ratio < 0.6:
            signal = "低位锁仓 (筑底)"
            strength = 60
            advice = "缩量止跌，建议小仓位试错或重点观察。"
            
        # 3. 量缩价涨 - 警惕型
        elif price_change > 1 and vol_ratio < 0.8:
            signal = "量缩价涨 (背离)"
            strength = 40
            advice = "上涨动能不足，不宜追高，谨防诱多。"
            
        # 优中选优过滤：只保留强度大于50的精选结果
        if strength < 70: 
            return None

        return {
            '日期': last_row['日期'],
            '代码': code,
            '名称': stock_name,
            '收盘价': curr_price,
            '涨跌幅%': price_change,
            '量比': round(vol_ratio, 2),
            '战法信号': signal,
            '买入信号强度': f"{strength}%",
            '操作建议': advice
        }
    except Exception as e:
        return None

def main():
    # 加载名称映射
    names_df = pd.read_csv(NAMES_FILE)
    stock_names = dict(zip(names_df['code'].astype(str), names_df['name']))
    
    # 扫描目录
    csv_files = glob.glob(os.path.join(DATA_DIR, '*.csv'))
    
    # 并行处理
    results = []
    with ProcessPoolExecutor() as executor:
        # 预加载参数
        from functools import partial
        func = partial(analyze_stock, stock_names=stock_names)
        results = list(executor.map(func, csv_files))
    
    # 过滤空值并汇总
    final_list = [res for res in results if res is not None]
    
    if final_list:
        output_df = pd.DataFrame(final_list)
        
        # 按照强度和量比排序，确保“一击必中”
        output_df = output_df.sort_values(by=['买入信号强度', '量比'], ascending=False)
        
        # 创建年月目录
        now = datetime.now()
        dir_name = now.strftime('%Y-%m')
        os.makedirs(dir_name, exist_ok=True)
        
        # 保存结果
        timestamp = now.strftime('%Y%m%d_%H%M%S')
        file_name = f"volume_price_strategy_{timestamp}.csv"
        save_path = os.path.join(dir_name, file_name)
        
        output_df.to_csv(save_path, index=False, encoding='utf-8-sig')
        print(f"筛选完成，结果已保存至: {save_path}")
    else:
        print("今日未筛选出符合“优中选优”条件的股票。")

if __name__ == "__main__":
    main()
