import os
import re
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest, DateRange, Dimension, Metric, 
    FilterExpression, Filter
)
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# ===================== 參數設置區 =====================
PROPERTY_ID = "356023664"
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']

MODE = 'news' 

if MODE == 'welfare':
    TARGET_PATH_KEYWORD = "articles/welfare"
    OUTPUT_FILE = "GA4_福利專欄.csv"
    MAX_PAGE_ID = 78 
elif MODE == 'news':
    TARGET_PATH_KEYWORD = "news/info"
    OUTPUT_FILE = "GA4_最新消息.csv"
    MAX_PAGE_ID = 50
elif MODE == 'lazy':
    TARGET_PATH_KEYWORD = "articles/lazy"
    OUTPUT_FILE = "GA4_懶人包.csv"
    MAX_PAGE_ID = 28
else:
    TARGET_PATH_KEYWORD = "news/info"
    OUTPUT_FILE = "GA4_流量來源統整報告.csv"
    MAX_PAGE_ID = 50
# =====================================================

def get_credentials():
    base_path = os.path.dirname(os.path.abspath(__file__))
    client_secrets_path = os.path.join(base_path, 'client_secrets.json')
    token_path = os.path.join(base_path, 'token.json')
    creds = None
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secrets_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, 'w') as token:
            token.write(creds.to_json())
    return creds

def classify_source(source_name):
    s = str(source_name).lower()
    if 'google' in s: return 'Google搜尋'
    if any(x in s for x in ['facebook', 'fb', 'm.facebook', 'l.facebook']): return 'Facebook'
    if 'line' in s: return 'LINE'
    if 'direct' in s: return '直接流量'
    if 'instagram' in s or 'ig' in s: return 'Instagram'
    if 'threads' in s: return 'Threads'
    return '其他'

def run_combined_source_report():
    creds = get_credentials()
    client = BetaAnalyticsDataClient(credentials=creds)

    # 1. 向 GA4 請求數據
    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        dimensions=[Dimension(name="unifiedPageScreen"), Dimension(name="sessionSource")],
        metrics=[
            Metric(name="activeUsers"), 
            Metric(name="screenPageViews"), 
            Metric(name="userEngagementDuration")
        ],
        date_ranges=[DateRange(start_date="7daysAgo", end_date="today")],
        dimension_filter=FilterExpression(
            filter=Filter(
                field_name="unifiedPageScreen",
                string_filter=Filter.StringFilter(
                    match_type=Filter.StringFilter.MatchType.CONTAINS,
                    value=TARGET_PATH_KEYWORD
                )
            )
        )
    )
    response = client.run_report(request)

    data_list = []
    for row in response.rows:
        data_list.append({
            "Page_URL": row.dimension_values[0].value,
            "Raw_Source": row.dimension_values[1].value,
            "Users": int(row.metric_values[0].value),
            "Views": int(row.metric_values[1].value),
            "Duration": float(row.metric_values[2].value)
        })
    
    df = pd.DataFrame(data_list)
    if df.empty: return print("❌ 找不到數據")

    # 2. 清洗與 ID 提取
    df['Source'] = df['Raw_Source'].apply(classify_source)
    def clean_id(val):
        match = re.search(r'id=(\d+)', str(val))
        if match:
            num = int(match.group(1))
            return num if 1 <= num <= MAX_PAGE_ID else None
        return None
    df['Pure_ID'] = df['Page_URL'].apply(clean_id)
    df = df[df['Pure_ID'].notnull()].copy()

    # 3. 計算頁面總數
    page_totals = df.groupby('Pure_ID').agg({
        'Users': 'sum',
        'Views': 'sum',
        'Duration': 'sum'
    }).reset_index()

    # 4. 計算來源占比並「統整欄位」
    source_stats = df.groupby(['Pure_ID', 'Source'])['Users'].sum().reset_index()
    source_stats = source_stats.merge(page_totals[['Pure_ID', 'Users']], on='Pure_ID', suffixes=('', '_Total'))
    source_stats['Share'] = (source_stats['Users'] / source_stats['Users_Total'] * 100).round(1)
    
    # 【核心改動】：將來源與占比合併為一個字串
    source_stats['Combined_Info'] = source_stats['Source'] + " (" + source_stats['Share'].astype(str) + "%)"
    
    # 5. 提取前 3 大來源
    source_stats = source_stats.sort_values(['Pure_ID', 'Users'], ascending=[True, False])

    def get_top_3_combined(group):
        infos = group['Combined_Info'].tolist()
        res = (infos + [None, None, None])[:3]
        return pd.Series(res, index=['第一來源(占比)', '第二來源(占比)', '第三來源(占比)'])

    top_3_df = source_stats.groupby('Pure_ID').apply(get_top_3_combined, include_groups=False).reset_index()

    # 6. 最後整合與匯出
    final_df = page_totals.merge(top_3_df, on='Pure_ID', how='left')
    final_df['平均停留(秒)'] = (final_df['Duration'] / (final_df['Views'] + 1e-5)).round(2)
    
    # 排序並過濾最終欄位
    final_df = final_df.sort_values('Pure_ID', ascending=False)
    
    cols_order = ['Pure_ID', 'Users', 'Views', '平均停留(秒)', '第一來源(占比)', '第二來源(占比)', '第三來源(占比)']
    final_output = final_df[cols_order].copy()

    final_output.rename(columns={
        'Pure_ID': '頁面ID',
        'Users': '總使用者數',
        'Views': '總瀏覽數'
    }, inplace=True)

    final_output.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    print(f"✅ 報表已產出：{OUTPUT_FILE}")

if __name__ == "__main__":
    run_combined_source_report()