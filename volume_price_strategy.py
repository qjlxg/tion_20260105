import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

# ==========================================
# 战法名称：图片原版-量价密码战法
# 战法逻辑：基于用户上传的量价关系核心图谱
# 核心口诀：量增价涨看多，量缩价涨警惕，高位量大不涨必撤
# ==========================================

DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'

def analyze_stock(file_path, stock_names):
    try:
        df = pd.read_csv(file_path)
        if df.empty or len(df) < 10:
            return None
        
        # 提取股票代码
        code = os.path.basename(file_path).replace('.csv', '')
        # 严格排除逻辑：创业板(30)、排除其它非沪深A股
        if not (code.startswith('60') or code.startswith('00') or code.startswith('60')):
            return None
        if code.startswith('30'):
            return None
            
        stock_name = stock_names.get(code, "未知")
        if any(x in stock_name for x in ["ST", "退"]):
            return None

        # 提取最新数据
        last = df.iloc[-1]
        prev = df.iloc[-2]
        
        curr_price = last['收盘']
        # 严格价格区间
        if not (5.0 <= curr_price <= 20.0):
            return None

        # 计算量价变化情况
        price_up = last['涨跌幅'] > 0
        vol_up = last['成交量'] > prev['成交量'] * 1.05 # 温和放量定义
        vol_down = last['成交量'] < prev['成交量'] * 0.95
        
        # 计算价格位置（用于判定高低位）
        high_20 = df['最高'].tail(20).max()
        low_20 = df['最低'].tail(20).min()
        is_high_pos = curr_price > (low_20 + (high_20 - low_20) * 0.8)
        is_low_pos = curr_price < (low_20 + (high_20 - low_20) * 0.3)

        res = {
            '日期': last['日期'],
            '代码': code, # 修复：直接输出纯净代码
            '名称': stock_name,
            '收盘': curr_price,
            '涨跌幅%': last['涨跌幅'],
            '成交量': last['成交量']
        }

        # --- 匹配图片核心量价口诀 ---
        
        # 1. 量增价涨 (理想入场点)
        if vol_up and last['涨跌幅'] > 1.5:
            res.update({'战法信号': '量增价涨', '买入信号强度': '90%', '操作建议': '健康拉升：主力推高，理想加仓/持有点。'})
        
        # 2. 量缩价涨 (诱多警惕)
        elif vol_down and last['涨跌幅'] > 1.5:
            res.update({'战法信号': '量缩价涨', '买入信号强度': '40%', '操作建议': '量价背离：跟风不足，主力诱多，注意见顶风险。'})
            
        # 3. 低位量平价升 (洗盘结束)
        elif is_low_pos and abs(last['成交量']/prev['成交量']-1) < 0.1 and last['涨跌幅'] > 0.5:
            res.update({'战法信号': '低位量平价升', '买入信号强度': '75%', '操作建议': '洗盘结束：温和拉升信号，可试错观察。'})

        # 4. 高位量增价平 (极其危险)
        elif is_high_pos and vol_up and abs(last['涨跌幅']) < 0.5:
            res.update({'战法信号': '高位量增价平', '买入信号强度': '0%', '操作建议': '绝命信号：主力高位对倒出货，坚决撤离。'})

        # 5. 放量下跌
        elif vol_up and last['涨跌幅'] < -2:
            res.update({'战法信号': '量增价跌', '买入信号强度': '0%', '操作建议': '恐慌抛售：主力清仓，不可抄底。'})
            
        else:
            return None # 不符合核心6种量价形态的排除，以减少结果数量

        return res
    except:
        return None

def main():
    # 读取名称
    names_df = pd.read_csv(NAMES_FILE)
    stock_names = dict(zip(names_df['code'].astype(str).str.zfill(6), names_df['name']))
    
    # 扫描
    csv_files = glob.glob(os.path.join(DATA_DIR, '*.csv'))
    
    with ProcessPoolExecutor() as executor:
        from functools import partial
        func = partial(analyze_stock, stock_names=stock_names)
        results = [r for r in list(executor.map(func, csv_files)) if r is not None]
    
    if results:
        output_df = pd.DataFrame(results)
        # 按照强度降序，只选精锐
        output_df = output_df.sort_values(by='买入信号强度', ascending=False)
        
        # 按年月归档
        now = datetime.now()
        dir_name = now.strftime('%Y-%m')
        os.makedirs(dir_name, exist_ok=True)
        
        filename = f"volume_price_strategy_{now.strftime('%Y%m%d_%H%M')}.csv"
        output_df.to_csv(os.path.join(dir_name, filename), index=False, encoding='utf-8-sig')
        print(f"复盘已完成。")

if __name__ == "__main__":
    main()
