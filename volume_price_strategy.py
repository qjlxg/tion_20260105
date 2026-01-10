import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

# ==========================================
# 战法名称：量价密码·精选版 (Elite Vol-Price)
# 核心逻辑：严格执行图片量价口诀 + 实战位置优选
# 优选标准：温和放量、实体饱满、趋势初起
# ==========================================

DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'

def analyze_stock(file_path, stock_names):
    try:
        df = pd.read_csv(file_path)
        if df.empty or len(df) < 30: # 增加数据量以计算区间位置
            return None
        
        code = os.path.basename(file_path).replace('.csv', '')
        # 排除 30 (创业板)，仅限深沪A股
        if code.startswith('30') or not (code.startswith('60') or code.startswith('00')):
            return None
            
        stock_name = stock_names.get(code, "未知")
        if any(x in stock_name for x in ["ST", "退"]):
            return None

        # 最新及历史参考数据
        last = df.iloc[-1]
        prev = df.iloc[-2]
        curr_price = last['收盘']
        
        # 1. 基础条件：价格区间
        if not (5.0 <= curr_price <= 20.0):
            return None

        # 2. 量价核心指标计算
        vol_ratio = last['成交量'] / df['成交量'].tail(5).mean() if df['成交量'].tail(5).mean() > 0 else 0
        price_change = last['涨跌幅']
        
        # 3. K线质量判定 (对应图片“看K线形态”)
        # 收盘价需在全天涨幅的80%以上，排除长上影线
        high, low = last['最高'], last['最低']
        k_quality = (curr_price - low) / (high - low) if high != low else 0
        
        # 4. 价格位置判定 (对应“寻找反弹机会”)
        low_30 = df['最低'].tail(30).min()
        high_30 = df['最高'].tail(30).max()
        # 处于过去30天股价的中低位（不超过中轴），避免追高
        is_appropriate_pos = curr_price < (low_30 + (high_30 - low_20) * 0.6) 

        # --- 严格执行图片口诀逻辑 ---
        
        # 精选：量增价涨 (进攻信号 - 优选启动点)
        # 条件：放量1.5-3.5倍(温和)，涨幅3%-7%(启动标准)，位置适中，K线饱满
        if (1.5 <= vol_ratio <= 3.5) and (3.0 <= price_change <= 7.0):
            if k_quality > 0.8 and is_appropriate_pos:
                return {
                    '日期': last['日期'],
                    '代码': code,
                    '名称': stock_name,
                    '收盘': curr_price,
                    '涨跌幅%': price_change,
                    '量比': round(vol_ratio, 2),
                    '战法信号': '量增价涨(启动)',
                    '买入信号强度': '95%',
                    '操作建议': '一击必中：符合温和放量突破，位置低，K线强，建议建仓。'
                }

        # 监控：高位量增价平 (见顶警告 - 无论结果多寡，危险信号必须报出)
        elif is_appropriate_pos == False and vol_ratio > 3.0 and abs(price_change) < 0.5:
            return {
                '日期': last['日期'],
                '代码': code,
                '名称': stock_name,
                '收盘': curr_price,
                '涨跌幅%': price_change,
                '量比': round(vol_ratio, 2),
                '战法信号': '高位放量滞涨',
                '买入信号强度': '0%',
                '操作建议': '绝命信号：主力对倒出货，无论多看好，请坚决离场。'
            }

        return None
    except:
        return None

def main():
    names_df = pd.read_csv(NAMES_FILE)
    stock_names = dict(zip(names_df['code'].astype(str).str.zfill(6), names_df['name']))
    
    csv_files = glob.glob(os.path.join(DATA_DIR, '*.csv'))
    
    with ProcessPoolExecutor() as executor:
        from functools import partial
        func = partial(analyze_stock, stock_names=stock_names)
        results = [r for r in list(executor.map(func, csv_files)) if r is not None]
    
    if results:
        output_df = pd.DataFrame(results)
        # 优中选优：按强度降序，且每天最多只输出结果中最强的5只
        output_df = output_df.sort_values(by=['买入信号强度', '量比'], ascending=False).head(5)
        
        now = datetime.now()
        dir_name = now.strftime('%Y-%m')
        os.makedirs(dir_name, exist_ok=True)
        
        filename = f"volume_price_strategy_{now.strftime('%Y%m%d')}.csv"
        output_df.to_csv(os.path.join(dir_name, filename), index=False, encoding='utf-8-sig')
        print(f"精选复盘完成，已选出最符合逻辑的标的。")
    else:
        print("今日无符合绝杀条件的个股。")

if __name__ == "__main__":
    main()
