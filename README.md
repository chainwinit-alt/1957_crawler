# 1957 Crawler

這個 repo 目前包含兩支和 1957 政策資料整理有關的 Python 腳本：

- `crawler_1957.py`
  直接抓取衛福部 1957 網站的縣市福利與全國福利政策，輸出原始工作表 `raw_1957`，以及可對接 IFare 資料庫格式的 `db_ifare_policy`。
- `replace_ifare_policy_from_excel.py`
  讀取 `db_ifare_policy` 工作表，先備份 `IFare` 的政策相關資料表，再把新的政策資料寫入 SQL Server。

## 安裝

```powershell
pip install -r requirements.txt
```

## 使用方式

全量爬取 1957 政策並輸出到桌面：

```powershell
python .\crawler_1957.py
```

另外輸出可供後續匯入的 JSON：

```powershell
python .\crawler_1957.py --db-json-output .\db_ifare_policy.json
```

把最新桌面 Excel 匯入 `localhost\SQLEXPRESS` 的 `IFare`：

```powershell
python .\replace_ifare_policy_from_excel.py
```

## 依賴

- `pandas`
- `openpyxl`
- `requests`
- `beautifulsoup4`
- `urllib3`
- `pyodbc`

## 注意

- `replace_ifare_policy_from_excel.py` 會先備份 `IFarePolicy` 與四張關聯表，再執行替換。
- 匯入腳本預設連到 `localhost\SQLEXPRESS`，資料庫名稱是 `IFare`。
