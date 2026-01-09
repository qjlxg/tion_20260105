import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import multiprocessing

# ==========================================
# 战法名称：周线突破箱体（优中选优版）
# 战法逻辑说明：
# 1. 筛选逻辑：周线级别箱体震荡 >= 12周，振幅 <= 20%（越窄控盘度越高）。
# 2. 突破逻辑：周收盘价站稳箱顶 > 3%，且当周量能 > 前12周均量 1.8倍（真钱入场）。
# 3. 趋势逻辑：5周、10周、20周均线多头排列，60周线必须向上（趋势护航）。
# 4. 优选逻辑：低价股（5-20元）优先，排除ST、创业板、科创板。
# ==========================================

def screen_stock(file_path, stock_names):
    try:
        # 读取数据
        df = pd.read_csv(file_path)
        if df.empty or len(df) < 80: # 确保有足够数据计算均线和对比
            return None

        # 获取并识别代码
        code = os.path.basename(file_path).replace('.csv', '')
        
        # --- 严格过滤准则 ---
        # 1. 排除创业板(30)、科创板(68)、北交所(4/8)
        if code.startswith(('30', '68', '4', '8')): return None
        # 2. 排除ST
        name = stock_names.get(code, '未知')
        if 'ST' in name or '*' in name: return None
        
        # --- 数据预处理：转周线 ---
        df['日期'] = pd.to_datetime(df['日期'])
        df.set_index('日期', inplace=True)
        # 聚合为周线：开盘(周一)、收盘(周五)、最高/最低(全周)、成交量(累计)
        logic_df = df.resample('W').agg({
            '开盘': 'first',
            '最高': 'max',
            '最低': 'min',
            '收盘': 'last',
            '成交量': 'sum'
        }).dropna()

        if len(logic_df) < 30: return None

        # --- 价格过滤 (最新收盘价) ---
        last_close = logic_df['收盘'].iloc[-1]
        if not (5.0 <= last_close <= 20.0): return None

        # --- 技术指标计算 ---
        logic_df['MA5'] = logic_df['收盘'].rolling(5).mean()
        logic_df['MA10'] = logic_df['收盘'].rolling(10).mean()
        logic_df['MA20'] = logic_df['收盘'].rolling(20).mean()
        logic_df['MA60'] = logic_df['收盘'].rolling(60).mean()
        
        # --- 箱体与量能分析 ---
        # 取前12周数据（不含当前周）作为观察期
        obs_period = logic_df.iloc[-13:-1] 
        box_max = obs_period['最高'].max()
        box_min = obs_period['最低'].min()
        box_amplitude = (box_max - box_min) / box_min # 震荡振幅
        avg_volume = obs_period['成交量'].mean()     # 观察期平均量
        
        current = logic_df.iloc[-1]
        prev = logic_df.iloc[-2]

        # --- 核心判断逻辑 (一击必中筛选法) ---
        
        # 条件1：窄幅箱体 (振幅越小，筹码越集中)
        cond_box = box_amplitude <= 0.20
        
        # 条件2：放量突破 (突破周成交量需达均量1.8倍以上)
        vol_ratio = current['成交量'] / avg_volume
        cond_volume = vol_ratio >= 1.8
        
        # 条件3：价格站稳 (收盘价站上箱体最高点3%以上，且本周是阳线)
        cond_price = (current['收盘'] >= box_max * 1.03) and (current['收盘'] > current['开盘'])
        
        # 条件4：均线多头 (5>10>20周均线，且60周趋势向上)
        cond_ma = (current['MA5'] > current['MA10'] > current['MA20']) and (current['MA60'] > prev['MA60'])

        if cond_box and cond_volume and cond_price and cond_ma:
            # --- 自动复盘与强度打分 ---
            score = 60
            if vol_ratio >= 2.5: score += 15     # 超倍量，真钱推动
            if box_amplitude <= 0.12: score += 15 # 极窄箱体，高度控盘
            if last_close < 10: score += 10       # 低价优势（利欧股份特征）

            # 制定复盘建议
            if score >= 90:
                advice = "【一击必中】信号极强。主力极度控盘且放量坚决，建议回踩箱顶不破时果断介入。"
            elif score >= 80:
                advice = "【优选标的】形态标准。属于标准突破，建议底仓试错，观察次周续航能力。"
            else:
                advice = "【普通观察】符合战法但爆发力存疑，建议仅作复盘跟踪，暂不重仓。"

            return {
                '代码': code,
                '名称': name,
                '最新价': round(last_close, 2),
                '箱体振幅': f"{box_amplitude:.2%}",
                '成交量倍数': round(vol_ratio, 2),
                '信号强度': score,
                '操作建议': advice,
                '止损位参考': round(box_max, 2)
            }
    except Exception as e:
        return None
    return None

def main():
    print(f"[{datetime.now()}] 开始执行‘周线突破箱体’优选脚本...")
    
    # 1. 加载股票名称字典
    if not os.path.exists('stock_names.csv'):
        print("错误：未找到 stock_names.csv 文件")
        return
    names_df = pd.read_csv('stock_names.csv', dtype={'code': str})
    stock_names = dict(zip(names_df['code'], names_df['name']))

    # 2. 扫描数据并并行处理
    data_dir = 'stock_data'
    if not os.path.exists(data_dir):
        print(f"错误：目录 {data_dir} 不存在")
        return
        
    file_list = glob.glob(os.path.join(data_dir, '*.csv'))
    print(f"扫描到 {len(file_list)} 个数据文件，正在并行筛选...")
    
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        results = pool.starmap(screen_stock, [(f, stock_names) for f in file_list])

    # 3. 汇总结果
    final_res = [r for r in results if r is not None]
    
    if final_res:
        output_df = pd.DataFrame(final_res)
        # 按得分从高到低排序
        output_df = output_df.sort_values(by='信号强度', ascending=False)
        
        # 4. 保存结果
        dir_name = datetime.now().strftime('%Y-%m')
        os.makedirs(dir_name, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_name = f"weekly_box_breakout_{timestamp}.csv"
        full_path = os.path.join(dir_name, file_name)
        
        output_df.to_csv(full_path, index=False, encoding='utf-8-sig')
        print(f"复盘完成！筛选出 {len(final_res)} 只优选标的，结果已保存至: {full_path}")
    else:
        print("今日未发现完全符合‘一击必中’逻辑的标的。")

if __name__ == '__main__':
    main()
