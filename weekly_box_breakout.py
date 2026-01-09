import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import multiprocessing

# ==========================================
# 战法名称：周线突破箱体（V2.0 涨停定向优化版）
# 优化背景：基于利欧股份(002131)等成功涨停标的复盘建模
# 核心逻辑：
# 1. 位置：5-20元低价区。
# 2. 蓄势：12周以上窄幅震荡（振幅<20%），筹码高度集中。
# 3. 爆发：周线带量突破（>1.8倍量），收盘价站稳箱顶3%以上。
# 4. 确认：5、10、20周均线多头排列，60周线向上。
# ==========================================

def screen_stock(file_path, stock_names):
    try:
        df = pd.read_csv(file_path)
        if df.empty or len(df) < 80: 
            return None

        code = os.path.basename(file_path).replace('.csv', '')
        
        # --- 严格过滤：排除ST、创业板、科创板 ---
        if code.startswith(('30', '68', '4', '8')): return None
        name = stock_names.get(code, '未知')
        if 'ST' in name or '*' in name: return None
        
        # --- 数据预处理：转周线 ---
        df['日期'] = pd.to_datetime(df['日期'])
        df.set_index('日期', inplace=True)
        logic_df = df.resample('W').agg({
            '开盘': 'first',
            '最高': 'max',
            '最低': 'min',
            '收盘': 'last',
            '成交量': 'sum'
        }).dropna()

        if len(logic_df) < 30: return None

        # --- 价格过滤 ---
        last_close = logic_df['收盘'].iloc[-1]
        if not (5.0 <= last_close <= 20.0): return None

        # --- 均线计算 ---
        logic_df['MA5'] = logic_df['收盘'].rolling(5).mean()
        logic_df['MA10'] = logic_df['收盘'].rolling(10).mean()
        logic_df['MA20'] = logic_df['收盘'].rolling(20).mean()
        logic_df['MA60'] = logic_df['收盘'].rolling(60).mean()
        
        # --- 箱体与量能分析 (关键优化区) ---
        obs_period = logic_df.iloc[-13:-1] 
        box_max = obs_period['最高'].max()
        box_min = obs_period['最低'].min()
        box_amplitude = (box_max - box_min) / box_min
        avg_volume = obs_period['成交量'].mean()
        
        current = logic_df.iloc[-1]
        prev = logic_df.iloc[-2]

        # --- 核心判断逻辑 ---
        # 1. 窄幅控盘 (利欧股份特征：振幅极小)
        cond_box = box_amplitude <= 0.20
        # 2. 真钱爆量 (利欧股份特征：成交量断层式放大)
        vol_ratio = current['成交量'] / avg_volume
        cond_volume = vol_ratio >= 1.8
        # 3. 突破强度 (收盘需为阳线且过顶3%)
        cond_price = (current['收盘'] >= box_max * 1.03) and (current['收盘'] > current['开盘'])
        # 4. 趋势共振
        cond_ma = (current['MA5'] > current['MA10'] > current['MA20']) and (current['MA60'] > prev['MA60'])

        if cond_box and cond_volume and cond_price and cond_ma:
            # --- 自动复盘打分 ---
            score = 60
            if vol_ratio >= 2.5: score += 15     # 巨量加分
            if box_amplitude <= 0.12: score += 15 # 极窄箱体加分
            if last_close < 10: score += 10       # 低价加分

            if score >= 85:
                advice = "【一击必中】主力极度控盘且放量坚决，极具涨停基因，建议重点关注。"
            else:
                advice = "【观察标的】形态符合，建议等回踩箱顶不破时再考虑。"

            return {
                '代码': code,
                '名称': name,
                '最新价': round(last_close, 2),
                '箱体振幅': f"{box_amplitude:.2%}",
                '量能倍数': round(vol_ratio, 2),
                '信号强度': score,
                '操作建议': advice,
                '止损参考': round(box_max, 2)
            }
    except:
        return None
    return None

def main():
    names_df = pd.read_csv('stock_names.csv', dtype={'code': str})
    stock_names = dict(zip(names_df['code'], names_df['name']))

    file_list = glob.glob(os.path.join('stock_data', '*.csv'))
    
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        results = pool.starmap(screen_stock, [(f, stock_names) for f in file_list])

    final_res = [r for r in results if r is not None]
    
    if final_res:
        output_df = pd.DataFrame(final_res).sort_values(by='信号强度', ascending=False)
        dir_name = datetime.now().strftime('%Y-%m')
        os.makedirs(dir_name, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_df.to_csv(os.path.join(dir_name, f"weekly_box_breakout_{timestamp}.csv"), index=False, encoding='utf-8-sig')
        print(f"复盘完成，筛选出 {len(final_res)} 只优选标的。")
    else:
        print("未发现符合条件的标的。")

if __name__ == '__main__':
    main()
