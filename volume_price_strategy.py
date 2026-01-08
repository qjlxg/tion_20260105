import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

# ==========================================
# 战法名称：量价乾坤·绝杀优选 (Enhanced Volume-Price Alpha)
# 战法备注：只做上升趋势中的放量启动点，严控换手与位置。
# ==========================================

DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'

def analyze_stock(file_path, stock_names):
    try:
        df = pd.read_csv(file_path)
        # 增加数据量要求，确保能计算均线
        if df.empty or len(df) < 30:
            return None
        
        code = os.path.basename(file_path).replace('.csv', '')
        # 严格排除：创业板(30)、科创板(688)、北交所(8/4)
        if code.startswith(('30', '688', '8', '4')):
            return None
            
        stock_name = stock_names.get(code, "未知")
        if any(x in stock_name for x in ["ST", "退", "PT"]):
            return None

        # --- 基础条件 ---
        last_row = df.iloc[-1]
        curr_price = last_row['收盘']
        if not (5.0 <= curr_price <= 20.0):
            return None

        # --- 计算技术指标 ---
        # 1. 均线过滤 (MA20)
        ma20 = df['收盘'].rolling(window=20).mean().iloc[-1]
        if curr_price < ma20: # 趋势向下，排除
            return None
        
        # 2. 量比与换手率
        avg_vol_5 = df['成交量'].iloc[-6:-1].mean()
        curr_vol = last_row['成交量']
        vol_ratio = curr_vol / avg_vol_5 if avg_vol_5 > 0 else 0
        turnover = last_row['换手率']
        
        # 3. K线实体比例 (排除长上影线)
        high = last_row['最高']
        low = last_row['最低']
        entity_ratio = (curr_price - low) / (high - low) if high != low else 0

        # --- 绝杀逻辑 (严苛筛选) ---
        price_change = last_row['涨跌幅']
        
        signal = ""
        strength = 0
        advice = ""

        # 【优中选优：量价齐升绝杀点】
        # 条件：涨幅在3%-7%之间(不过分亢奋)，量比1.5-4倍，换手3%-10%，收盘价接近最高点
        if (3.0 <= price_change <= 7.5) and (1.5 <= vol_ratio <= 4.0) and (3.0 <= turnover <= 10.0):
            if entity_ratio > 0.8: # K线实体饱满
                signal = "绝杀：量价齐升启动"
                strength = 95
                advice = "主力介入痕迹明显且筹码稳定，一击必中概率高，建议建仓。"
            else:
                return None # 上影线太长，抛压大，弃选

        # 【次优选：缩量洗盘结束点】
        elif (-1.5 <= price_change <= 1.5) and (vol_ratio < 0.5) and (curr_price > ma20):
            signal = "优选：缩量洗盘末端"
            strength = 80
            advice = "极度缩量暗示洗盘结束，可轻仓试错，等待放量拉升。"
        
        else:
            return None # 不符合严选条件的全部排除

        return {
            '日期': last_row['日期'],
            '代码': f"'{code}", # 加单引号防止Excel乱码
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
    if not os.path.exists(NAMES_FILE):
        print(f"找不到名称映射文件: {NAMES_FILE}")
        return

    names_df = pd.read_csv(NAMES_FILE)
    stock_names = dict(zip(names_df['code'].astype(str).str.zfill(6), names_df['name']))
    
    csv_files = glob.glob(os.path.join(DATA_DIR, '*.csv'))
    
    with ProcessPoolExecutor() as executor:
        from functools import partial
        func = partial(analyze_stock, stock_names=stock_names)
        results = list(executor.map(func, csv_files))
    
    final_list = [res for res in results if res is not None]
    
    if final_list:
        output_df = pd.DataFrame(final_list)
        output_df = output_df.sort_values(by='信号强度', ascending=False)
        
        now = datetime.now()
        dir_name = now.strftime('%Y-%m')
        os.makedirs(dir_name, exist_ok=True)
        
        file_path = os.path.join(dir_name, f"volume_price_strategy_{now.strftime('%Y%m%d')}.csv")
        output_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"精选复盘完成，今日入选 {len(output_df)} 只。")
    else:
        print("今日无符合条件的精选个股，建议空仓休息。")

if __name__ == "__main__":
    main()
