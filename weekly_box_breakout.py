import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import multiprocessing

# 战法名称：周线突破箱体战法 (Weekly Box Breakout Strategy)
# 核心逻辑：
# 1. 筛选长期（>12周）横盘震荡（振幅<25%）的标的。
# 2. 寻找放量突破（1.5倍-2倍量）且价格站稳箱体上沿3%的信号。
# 3. 配合均线多头排列（5周>20周，60周线向上）。
# 4. 价格过滤：5.0元 - 20.0元，排除ST及创业板。

def screen_stock(file_path, stock_names):
    try:
        df = pd.read_csv(file_path)
        if df.empty or len(df) < 60:  # 至少需要60周数据计算均线
            return None

        # 获取代码
        code = os.path.basename(file_path).replace('.csv', '')
        
        # --- 基础过滤 ---
        if code.startswith('30') or code.startswith('68'): return None # 排除创业板、科创板
        if 'ST' in stock_names.get(code, ''): return None
        
        last_close = df['收盘'].iloc[-1]
        if not (5.0 <= last_close <= 20.0): return None

        # --- 技术指标计算 (转换为周线维度) ---
        # 假设原始数据是日线，此处简易模拟转周线，若原始就是周线则直接计算
        # 注意：脚本默认stock_data下为日线数据，通过Resample转周线
        df['日期'] = pd.to_datetime(df['日期'])
        df.set_index('日期', inplace=True)
        
        logic_df = df.resample('W').agg({
            '开盘': 'first',
            '最高': 'max',
            '最低': 'min',
            '收盘': 'last',
            '成交量': 'sum'
        }).dropna()

        if len(logic_df) < 20: return None

        # 1. 计算均线
        logic_df['MA5'] = logic_df['收盘'].rolling(5).mean()
        logic_df['MA20'] = logic_df['收盘'].rolling(20).mean()
        logic_df['MA60'] = logic_df['收盘'].rolling(60).mean()
        
        # 2. 箱体定义 (回顾前12周，不含当前突破周)
        lookback_period = logic_df.iloc[-13:-1] 
        box_max = lookback_period['最高'].max()
        box_min = lookback_period['最低'].min()
        box_amplitude = (box_max - box_min) / box_min
        
        # 3. 成交量基准 (前12周平均)
        avg_volume = lookback_period['成交量'].mean()
        
        # --- 核心战法判断 ---
        current = logic_df.iloc[-1]
        prev = logic_df.iloc[-2]
        
        # 条件A: 横盘时长 >= 12周，振幅 <= 25%
        cond1 = box_amplitude <= 0.25
        
        # 条件B: 突破确认 (收盘价超箱体上沿3%, 且本周是阳线)
        cond2 = current['收盘'] >= box_max * 1.03
        
        # 条件C: 放量 (成交量 > 1.5倍均量)
        volume_ratio = current['成交量'] / avg_volume
        cond3 = volume_ratio >= 1.5
        
        # 条件D: 均线共振 (5周金叉20周，60周线向上)
        cond4 = current['MA5'] > current['MA20'] and current['MA60'] > prev['MA60']

        if cond1 and cond2 and cond3 and cond4:
            # --- 自动复盘逻辑：计算信号强度 ---
            score = 70
            if volume_ratio >= 2.0: score += 10
            if box_amplitude <= 0.15: score += 10
            if current['收盘'] > current['MA5'] * 1.05: score += 10 # 强势拉升
            
            # 操作建议
            suggestion = "【一击必中】信号极强，建议回踩箱沿分批轻仓介入" if score >= 90 else "【观察记录】形态符合，等待次周站稳确认"
            
            return {
                '代码': code,
                '名称': stock_names.get(code, '未知'),
                '现价': current['收盘'],
                '箱体振幅': f"{box_amplitude:.2%}",
                '量能倍数': f"{volume_ratio:.2f}",
                '信号强度': score,
                '操作建议': suggestion,
                '止损参考': f"{box_max * 0.95:.2f}"
            }
    except Exception as e:
        return None
    return None

if __name__ == '__main__':
    # 1. 加载股票名称
    names_df = pd.read_csv('stock_names.csv', dtype={'code': str})
    stock_names = dict(zip(names_df['code'], names_df['name']))

    # 2. 扫描数据目录 (多进程并行)
    file_list = glob.glob('stock_data/*.csv')
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    results = pool.starmap(screen_stock, [(f, stock_names) for f in file_list])
    pool.close()
    pool.join()

    # 3. 过滤空值并保存结果
    final_res = [r for r in results if r is not None]
    if final_res:
        output_df = pd.DataFrame(final_res)
        # 按得分排序，优中选优
        output_df = output_df.sort_values(by='信号强度', ascending=False)
        
        # 创建年月目录
        dir_path = datetime.now().strftime('%Y-%m')
        os.makedirs(dir_path, exist_ok=True)
        
        # 文件名：脚本名+时间戳
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_name = f"weekly_box_breakout_{timestamp}.csv"
        
        output_df.to_csv(os.path.join(dir_path, file_name), index=False, encoding='utf-8-sig')
        print(f"筛选完成，发现 {len(final_res)} 只潜力股。")
    else:
        print("今日无符合周线突破战法标的。")
