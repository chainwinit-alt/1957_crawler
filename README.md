# 1957 福利政策新爬蟲

這支程式不再使用 UiPath selector 模擬點擊，而是直接抓 1957 網站目前的三種頁面：

- `category.jsp?cateId=...`
- `QADataList.jsp?cateId=...`
- `servlet/QACtrl?func=QAView&sid=...`

## 檔案

- 主程式: [crawler_1957.py](/D:/Users/jason.hsieh/Documents/UiPath/1957/crawler_1957.py)

## 抓取範圍

- `縣市福利` 底下各縣市的福利政策
- 非縣市分支的全國福利政策
- 各縣市 `服務窗口` 類別會自動排除，不抓

## 直接執行

全量抓取縣市福利 + 全國福利：

```powershell
python D:\Users\jason.hsieh\Documents\UiPath\1957\crawler_1957.py
```

預設會輸出到桌面，檔名類似：

```text
D:\Users\jason.hsieh\Desktop\1957縣市政策_20260402_163000.xlsx
```

## 小範圍測試

只抓新北市的社會救助，最多 5 筆：

```powershell
python D:\Users\jason.hsieh\Documents\UiPath\1957\crawler_1957.py --county 新北市 --category 社會救助 --max-policies 5
```

指定輸出檔：

```powershell
python D:\Users\jason.hsieh\Documents\UiPath\1957\crawler_1957.py --output D:\Users\jason.hsieh\Desktop\1957_測試.xlsx
```

只抓全國福利的 `社會保險 > 勞工保險`：

```powershell
python D:\Users\jason.hsieh\Documents\UiPath\1957\crawler_1957.py --root-cate-id 3 --category 勞工保險
```

## 主要輸出欄位

- `第一層分類`
- `第二層分類`
- `第三層分類`
- `縣市`
- `福利分類`
- `福利分類系項`
- `政策群組`
- `政策標題`
- `政策連結`
- `更新時間`
- `內文`
- `資料來源名稱`
- `資料來源連結`
- `附件名稱列表`

## 注意

- 這台機器用 Python 直連 1957 站台時，會遇到 SSL 憑證鏈問題，所以程式內已經處理。
- 如果之後要再接回 UiPath，可以改成由 UiPath 呼叫這支 Python，再讀取輸出的 Excel。
