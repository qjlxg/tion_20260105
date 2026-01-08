import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

# ==========================================
# 战法名称：量价乾坤·一击必中 (Precision Vol-Price Strategy)
# 战法备注：极致缩圈，只做上升趋势中的温和放量突破点。
# ==========================================

DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'

def analyze_stock(file_path, stock_names):
    try:
        df = pd.read_csv(file_path)
        if df.empty or len(df) < 30:
            return None
        
        # 1. 严格代码过滤
        code = os.path.basename(file_path).replace('.csv', '')
        if code.startswith(('30', '688', '8', '4')):
            return None
            
        stock_name = stock_names.get(code, "未知")
        if any(x in stock_name for x in ["ST", "退", "PT"]):
            return None

        # --- 静态基础条件 ---
        last_row = df.iloc[-1]
        curr_price = last_row['收盘']
        if not (5.0 <= curr_price <= 20.0):
            return None

        # --- 趋势与空间计算 ---
        ma20 = df['收盘'].rolling(window=20).mean()
        ma20_curr = ma20.iloc[-1]
        ma20_prev = ma20.iloc[-2]
        
        # 必须在MA20之上，且MA20正在向上运行
        if curr_price < ma20_curr or ma20_curr <= ma20_prev:
            return None
            
        # 突破确认：今日收盘价必须是过去5天的最高收盘价
        if curr_price < df['收盘'].tail(5).max():
            return None

        # --- 量比与质量过滤 ---
        avg_vol_5 = df['成交量'].iloc[-6:-1].mean()
        curr_vol = last_row['成交量']
        vol_ratio = curr_vol / avg_vol_5 if avg_vol_5 > 0 else 0
        turnover = last_row['换手率']
        amplitude = last_row['振幅']
        
        # K线实体比例
        high, low = last_row['最高'], last_row['最低']
        entity_ratio = (curr_price - low) / (high - low) if high != low else 0

        # --- 极严筛选逻辑 ---
        price_change = last_row['涨跌幅']
        
        # 核心：量价齐升启动 (条件收窄)
        # 涨幅限制在 3-6%，量比 1.8-3.5，换手 3-8% (主力洗盘后的标准启动指标)
        if (3.0 <= price_change <= 6.5) and (1.8 <= vol_ratio <= 3.5) and (3.0 <= turnover <= 8.0):
            if entity_ratio > 0.85 and amplitude < 9.0:
                signal = "一击必中：启动点"
                strength = 98
                advice = "上升通道温和放量突破，筹码锁定良好，建议重点关注。"
            else:
                return None
        else:
            return None

        return {
            '日期': last_row['日期'],
            '代码': code, # 修复：移除引号
            '名称': stock_name,
            '收盘': curr_price,
            '涨跌幅%': price_change,
            '换手率%': turnover,
            '量比': round(vol_ratio, 2),
            '战法信号': signal,
            '信号强度': f"{strength}%",
            '操作建议': advice
        }
    except:
        return None

def main():
    names_df = pd.read_csv(NAMES_FILE)
    # 确保代码是6位字符串
    stock_names = dict(zip(names_df['code'].astype(str).str.zfill(6), names_df['name']))
    
    csv_files = glob.glob(os.path.join(DATA_DIR, '*.csv'))
    
    with ProcessPoolExecutor() as executor:
        from functools import partial
        func = partial(analyze_stock, stock_names=stock_names)
        results = list(executor.map(func, csv_files))
    
    final_list = [res for res in results if res is not None]
    
    if final_list:
        output_df = pd.DataFrame(final_list)
        # 再次优选：按量比和实体比例二次排序
        output_df = output_df.sort_values(by=['信号强度', '量比'], ascending=False).head(5) # 强制限额，只留最精锐的5只
        
        now = datetime.now()
        dir_name = now.strftime('%Y-%m')
        os.makedirs(dir_name, exist_ok=True)
        
        file_path = os.path.join(dir_name, f"volume_price_strategy_{now.strftime('%Y%m%d')}.csv")
        output_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"精选复盘完成。")
    else:
        print("今日无极高概率信号，空仓休息。")

if __name__ == "__main__":
    main()
