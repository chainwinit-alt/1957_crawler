import os
import pandas as pd
import time
import random
import warnings
from pytrends.request import TrendReq

# 1. 環境設定
warnings.simplefilter(action='ignore', category=FutureWarning)

def get_desktop_path():
    return os.path.join(os.path.expanduser("~"), "Desktop")

def discover_with_empty_record(root_keyword, max_retries=3):
    """
    核心挖掘函數：若查無資料，會回傳一筆「查無趨勢」的紀錄
    """
    pytrends = TrendReq(hl='zh-TW', tz=-480)
    retries = 0
    
    while retries < max_retries:
        try:
            print(f"🚀 正在挖掘「{root_keyword}」... (嘗試第 {retries+1} 次)")
            pytrends.build_payload([root_keyword], timeframe='today 12-m', geo='TW')
            related_payload = pytrends.related_queries()
            
            discovered_data = []
            # 邏輯判斷：檢查 API 是否有回傳內容
            has_data = False
            if root_keyword in related_payload:
                # 抓取 Top 與 Rising
                for category in ['top', 'rising']:
                    if related_payload[root_keyword][category] is not None:
                        temp_df = related_payload[root_keyword][category]
                        if not temp_df.empty:
                            temp_df['來源類型'] = '熱門榜' if category == 'top' else '飆升榜'
                            discovered_data.append(temp_df)
                            has_data = True
            
            if has_data:
                final_df = pd.concat(discovered_data, ignore_index=True)
                final_df.columns = ['挖掘到的關鍵字', '關聯熱度', '來源類型']
                final_df['原始種子詞'] = root_keyword
                return final_df
            else:
                # 核心邏輯：若查無資料，建立一筆「證明紀錄」
                # 為什麼使用此公式？確保 CSV 中會出現該種子詞，證明程式有執行但市場無數據。
                return pd.DataFrame([{
                    '挖掘到的關鍵字': '【系統訊息】該詞彙目前查無相關趨勢',
                    '關聯熱度': 0,
                    '來源類型': '無數據',
                    '原始種子詞': root_keyword
                }])

        except Exception as e:
            if "429" in str(e):
                wait = 60 + random.uniform(10, 20)
                print(f"⚠️ 頻率限制，冷卻 {wait:.1f} 秒...")
                time.sleep(wait)
                retries += 1
            else:
                print(f"❌ 錯誤: {e}")
                break
    
    # 若多次失敗（如持續 429），也回傳一筆錯誤紀錄
    return pd.DataFrame([{'挖掘到的關鍵字': f'連線失敗: {root_keyword}', '關聯熱度': -1, '來源類型': '連線異常', '原始種子詞': root_keyword}])

# --- 執行區 ---
if __name__ == "__main__":
    root_seeds = ['補助', '津貼', '社福', '福利', '給錢'] 
    all_dfs = []
    
    for seed in root_seeds:
        # 隨機等待公式：模擬真實用戶行為
        time.sleep(random.uniform(15, 25))
        df = discover_with_empty_record(seed)
        all_dfs.append(df)
    
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        
        # 存檔路徑
        file_path = os.path.join(get_desktop_path(), '基金會關鍵字挖掘.csv')
        
        # 使用 utf-8-sig 公式，確保 Excel 開啟不亂碼
        final_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        
        print("\n" + "="*50)
        print(f"✅ 挖掘任務完成！")
        print(f"📁 報告已存至桌面：{file_path}")
        print(f"📊 總筆數（含無數據證明）：{len(final_df)} 筆")
        print("="*50)