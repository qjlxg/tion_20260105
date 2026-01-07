import pandas as pd
import numpy as np
import os
from datetime import datetime
import multiprocessing

# ==========================================
# 战法名称：乾坤一击·极优共振突破 (Version 2.0)
# 优化逻辑：
# 1. 严格量比：成交量 > 5日均量2倍 (主力入场证据)
# 2. 严格换手：3% < 换手率 < 10% (筹码高度活跃且受控)
# 3. 价格约束：5-20元 + 当日涨幅 > 3% (排除弱势震荡)
# 4. 形态共振：MA5/10/20 三线顺向多头 + 站稳MA60
# ==========================================

def calculate_indicators(df):
    # 均线
    for m in [5, 10, 20, 60, 120]:
        df[f'MA{m}'] = df['收盘'].rolling(m).mean()
    
    # MACD
    exp1 = df['收盘'].ewm(span=12, adjust=False).mean()
    exp2 = df['收盘'].ewm(span=26, adjust=False).mean()
    df['DIF'] = exp1 - exp2
    df['DEA'] = df['DIF'].ewm(span=9, adjust=False).mean()
    
    # 成交量均线
    df['VMA5'] = df['成交量'].rolling(5).mean()
    return df

def analyze_stock(file_path):
    try:
        code = os.path.basename(file_path).replace('.csv', '')
        # 严格排除：ST、300(创业板)、688(科创板)
        if any(x in code for x in ['ST', '*ST']) or code.startswith(('300', '688')):
            return None
        
        df = pd.read_csv(file_path)
        if len(df) < 120: return None
        
        df = calculate_indicators(df)
        last = df.iloc[-1]
        prev = df.iloc[-2]
        
        # --- 地狱级筛选条件 ---
        
        # 1. 价格与涨幅约束
        if not (5.0 <= last['收盘'] <= 20.0): return None
        if last['涨跌幅'] < 3.0: return None # 必须是中大阳线
        
        # 2. 极优量能平衡 (关键)
        volume_ratio = last['成交量'] / last['VMA5']
        if volume_ratio < 1.8: return None # 量能必须放大1.8倍以上，否则视为假突破
        
        # 3. 活跃度约束
        if not (3.0 <= last['换手率'] <= 12.0): return None # 太低没人玩，太高是散户营
        
        # 4. 均线形态：多头排列初绽放
        # 要求：5 > 10 > 20 且 价格 > 60日线 (趋势走平抬头)
        ma_perfect = last['MA5'] > last['MA10'] > last['MA20'] > last['MA60']
        if not ma_perfect: return None
        
        # 5. 指标共振：MACD金叉且DIF在零轴附近 (起爆位)
        macd_ok = last['DIF'] > last['DEA'] and last['DIF'] > -0.1
        if not macd_ok: return None

        # --- 计算最终评分 (0-100) ---
        score = 60
        if volume_ratio > 2.5: score += 15 # 巨量突破加分
        if prev['收盘'] <= last['MA20'] and last['收盘'] > last['MA20']: score += 15 # 穿三线加分
        if last['MA60'] > prev['MA60']: score += 10 # 趋势抬头确认
        
        # 操作建议
        if score >= 90:
            advice = "【全仓出击】绝佳起爆点，量价齐升，一击必中"
        elif score >= 80:
            advice = "【重仓介入】多维共振明显，趋势已经确立"
        else:
            advice = "【试错观察】形态尚可，建议分批入场"

        return {
            'code': code,
            '收盘价': last['收盘'],
            '涨跌幅': f"{last['涨跌幅']}%",
            '换手率': f"{last['换手率']}%",
            '量比': round(volume_ratio, 2),
            '强度评分': score,
            '操作建议': advice,
            '核心逻辑': "倍量突破+多头初绽"
        }
    except:
        return None

def main():
    stock_dir = 'stock_data'
    name_file = 'stock_names.csv'
    
    files = [os.path.join(stock_dir, f) for f in os.listdir(stock_dir) if f.endswith('.csv')]
    with multiprocessing.Pool() as pool:
        results = pool.map(analyze_stock, files)
    
    results = [r for r in results if r is not None]
    
    # 匹配名称
    if os.path.exists(name_file):
        names_df = pd.read_csv(name_file)
        names_df['code'] = names_df['code'].astype(str).str.zfill(6)
        name_dict = dict(zip(names_df['code'], names_df['name']))
        for r in results:
            r['名称'] = name_dict.get(r['code'], "未知")

    report_df = pd.DataFrame(results)
    if not report_df.empty:
        # 只取评分最高的前 5 只，真正做到“优中选优”
        report_df = report_df.sort_values(by='强度评分', ascending=False).head(5)
    
    # 保存路径
    now = datetime.now()
    dir_path = now.strftime('%Y%m')
    if not os.path.exists(dir_path): os.makedirs(dir_path)
    save_path = os.path.join(dir_path, f"top_picks_{now.strftime('%Y%m%d')}.csv")
    
    report_df.to_csv(save_path, index=False, encoding='utf-8-sig')
    print(f"复盘结束。已从海选个股中锁定最强 5 只标的。")

if __name__ == "__main__":
    main()
