"""將 db_ifare_policy 工作表整批寫入 IFare DB 的工具。

這支程式不是單純 insert，而是完整的「覆蓋式匯入」流程：
1. 找到桌面最新的爬蟲 Excel。
2. 讀取 db_ifare_policy 工作表並做資料正規化。
3. 驗證必要欄位是否齊全。
4. 將現有政策主表與關聯表完整備份。
5. 產生可回滾的 restore SQL。
6. 清空舊資料後重新寫入新政策。

因此它最重要的設計目標不是速度，而是：
- 可還原
- 可檢查
- 失敗時不要留下半套資料
"""

from __future__ import annotations

import argparse
import ast
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyodbc


# 連線目標目前寫死在本機 SQL Server instance。
# 若網站實際使用的是 default instance 或其他主機，這裡一定要同步調整。
SERVER = r"localhost\SQLEXPRESS"
DATABASE = "IFare"
# 新版 crawler 最終只輸出一張工作表 ifare_policy，
# 欄位內容沿用原本 db_ifare_policy 的結構。
SHEET_NAME = "ifare_policy"
# 建立者 / 異動者目前固定寫同一位系統帳號。
CREATE_USER_ID = 1
DEFAULT_OFFICE_UNIT_ID = 1
DRIVER_CANDIDATES = (
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "SQL Server Native Client 11.0",
    "SQL Server",
)

POLICY_CODE_OVERRIDES = {
    "婦女福利": 4,
    "家庭及婦女福利": 4,
    "新住民福利": 4,
    "特殊境遇家庭": 4,
    "家暴暨性侵防治": 4,
    "兒童及青少年福利": 3,
    "兒童及青少年服務": 3,
    "原住民": 10,
    "其他局處福利": 11,
    "醫療照護": 11,
    "社會救助專戶、國民年金": 2,
}

# 主表與多對多關聯表。
# 程式會先清除關聯表，再清主表，最後按同樣順序寫回資料。
POLICY_TABLE = "dbo.IFarePolicy"
LINK_TABLE_SPECS = (
    ("dbo.IFarePolicy_CodeKeyword", "CodeKeyword_ID", "CodeKeywordIDs"),
    ("dbo.IFarePolicy_CodeRecipient", "CodeRecipient_ID", "CodeRecipientIDs"),
    ("dbo.IFarePolicy_CodeIncome", "CodeIncome_ID", "CodeIncomeIDs"),
    ("dbo.IFarePolicy_CodeIdentity", "CodeIdentity_ID", "CodeIndentityIDs"),
)
ALL_POLICY_TABLES = tuple(spec[0] for spec in LINK_TABLE_SPECS) + (POLICY_TABLE,)


def parse_args() -> argparse.Namespace:
    """保留 argparse 入口介面。

    目前實作上沒有真的讀 CLI 參數，而是走自動抓桌面最新 Excel 的流程。
    這個函式算是未來若要改成命令列版時的保留點。
    """

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    workspace = Path.cwd()
    return argparse.ArgumentParser(
        description="Backup existing IFare policy tables, then replace them with ifare_policy data from Excel."
    ).parse_args(
        []
    )


def latest_excel_on_desktop() -> Path:
    """找桌面上最新的 1957 爬蟲 Excel。"""

    desktop = Path.home() / "Desktop"
    candidates = sorted(desktop.glob("1957政策_*.xlsx"), key=lambda path: path.stat().st_mtime, reverse=True)
    if not candidates:
        raise FileNotFoundError("桌面找不到 1957政策_*.xlsx")
    return candidates[0]


def build_runtime_args() -> dict[str, Any]:
    """建立本次執行會用到的檔案路徑與時間標籤。"""

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    workspace = Path.cwd()
    return {
        "excel": latest_excel_on_desktop(),
        "backup_label": stamp,
        "restore_sql": workspace / f"restore_ifare_policy_{stamp}.sql",
        "report_json": workspace / f"replace_ifare_policy_report_{stamp}.json",
    }


def choose_driver() -> str:
    """從候選清單中挑選本機可用的 SQL Server ODBC driver。"""

    installed = set(pyodbc.drivers())
    for driver in DRIVER_CANDIDATES:
        if driver in installed:
            return driver
    raise RuntimeError(f"找不到可用 SQL Server ODBC Driver，已安裝驅動：{sorted(installed)}")


def connect(*, autocommit: bool) -> pyodbc.Connection:
    """建立 pyodbc 連線。

    autocommit=True 通常用在查詢、備份表建立。
    autocommit=False 用在需要交易保護的覆蓋寫入流程。
    """

    driver = choose_driver()
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, autocommit=autocommit)


def normalize_text(value: Any) -> str | None:
    """把 Excel 讀進來的值轉成可預期的字串或 None。"""

    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def normalize_required_text(value: Any) -> str:
    """必填文字欄位版本；缺值時回空字串而不是 None。"""

    text = normalize_text(value)
    return text or ""


def normalize_int(value: Any) -> int | None:
    """把數值欄位轉成 int，缺值則回 None。"""

    if value is None or pd.isna(value):
        return None
    return int(value)


def normalize_datetime(value: Any) -> datetime | None:
    """將 Excel 讀進來的日期欄位統一成 datetime。"""

    if value is None or pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value
    return pd.Timestamp(value).to_pydatetime()


def normalize_enabled(value: Any) -> str:
    """把布林/數字/字串狀態統一轉成 DB 使用的 啟用 / 停用。"""

    if isinstance(value, bool):
        return "啟用" if value else "停用"
    if isinstance(value, (int, float)) and not pd.isna(value):
        return "啟用" if int(value) != 0 else "停用"
    text = (normalize_text(value) or "").lower()
    return "啟用" if text in {"1", "true", "yes", "y", "啟用"} else "停用"


def parse_id_list(value: Any) -> list[int]:
    """解析多選 code 欄位。

    Excel 這些欄位可能長成：
    - 真正的 list/tuple
    - JSON 字串
    - Python list 字串
    - 單一數值

    這裡集中處理，避免主流程到處判斷型別。
    """

    if value is None or pd.isna(value):
        return []
    if isinstance(value, list):
        return [int(item) for item in value]
    if isinstance(value, tuple):
        return [int(item) for item in value]
    if isinstance(value, (int, float)):
        return [] if pd.isna(value) else [int(value)]

    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "[]"}:
        return []

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        parsed = ast.literal_eval(text)

    if isinstance(parsed, (list, tuple, set)):
        return [int(item) for item in parsed if item is not None and str(item).strip() != ""]
    return [int(parsed)]


def infer_policy_code_id(row: dict[str, Any]) -> tuple[int | None, str | None]:
    """補判斷 CodePolicyID。

    正常情況下爬蟲輸出就應該已有 CodePolicyID，
    這裡是匯入前最後一道保底，處理少數 label 與實際內容不完全一致的案例。
    """

    current_id = normalize_int(row.get("CodePolicyID"))
    if current_id is not None:
        return current_id, None

    label = normalize_text(row.get("CodePolicyLabel"))
    if not label:
        return None, None

    blob = " ".join(
        value
        for value in (
            normalize_text(row.get("Title")),
            normalize_text(row.get("Qualification")),
            normalize_text(row.get("WelfareInfo")),
            normalize_text(row.get("Evidence")),
            normalize_text(row.get("Remark")),
        )
        if value
    )

    if label == "社會救助專戶、國民年金" and "國民年金" in blob:
        return 1, "社會救助專戶、國民年金 -> 社會保險"
    if label == "醫療照護" and any(keyword in blob for keyword in ("健保", "勞保", "農保", "保險", "國民年金")):
        return 1, "醫療照護 -> 社會保險"

    override_id = POLICY_CODE_OVERRIDES.get(label)
    if override_id is None:
        return None, None
    return override_id, f"{label} -> {override_id}"


def load_rows(excel_path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """讀取 ifare_policy 工作表，並整理成可寫 DB 的 rows。

    除了欄位正規化，這裡還會統計：
    - override 使用次數
    - 仍未解決的福利分類
    - validation error
    - 關聯表預計寫入筆數
    """

    df = pd.read_excel(excel_path, sheet_name=SHEET_NAME)
    rows = df.to_dict(orient="records")

    override_counter: Counter[str] = Counter()
    unresolved_labels: Counter[str] = Counter()
    prepared_rows: list[dict[str, Any]] = []

    for row in rows:
        code_policy_id, override_note = infer_policy_code_id(row)
        if code_policy_id is None:
            unresolved_labels[normalize_text(row.get("CodePolicyLabel")) or "(空白)"] += 1
        if override_note:
            override_counter[override_note] += 1

        prepared_rows.append(
            {
                # 這一層 prepared_rows 的欄位名稱，已經盡量直接對齊 DB insert 流程。
                "Title": normalize_required_text(row.get("Title")),
                "Qualification": normalize_required_text(row.get("Qualification")),
                "WelfareInfo": normalize_required_text(row.get("WelfareInfo")),
                "Evidence": normalize_required_text(row.get("Evidence")),
                "IFareOfficeUnitID": normalize_int(row.get("IFareOfficeUnitID")) or DEFAULT_OFFICE_UNIT_ID,
                "OfficeUnitInfo": normalize_required_text(row.get("OfficeUnitInfo")),
                "OfficeUnitTel": normalize_required_text(row.get("OfficeUnitTel")),
                "CodePolicyID": code_policy_id,
                "CodeDomicileID": normalize_int(row.get("CodeDomicileID")),
                "CodeIndentityIDs": parse_id_list(row.get("CodeIndentityIDs")),
                "CodeIncomeIDs": parse_id_list(row.get("CodeIncomeIDs")),
                "CodeRecipientIDs": parse_id_list(row.get("CodeRecipientIDs")),
                "CodeKeywordIDs": parse_id_list(row.get("CodeKeywordIDs")),
                "CompetentAuthority": normalize_required_text(row.get("CompetentAuthority")),
                "ReleaseTime": normalize_datetime(row.get("ReleaseTime")),
                "DiscontinuedTime": normalize_datetime(row.get("DiscontinuedTime")),
                "Remark": normalize_required_text(row.get("Remark")),
                "State": normalize_enabled(row.get("IsEnabled")),
                "CodePolicyLabel": normalize_text(row.get("CodePolicyLabel")),
                "SourceUrl": normalize_text(row.get("SourceUrl")),
                "PolicyUrl": normalize_text(row.get("PolicyUrl")),
                "SID": normalize_text(row.get("SID")),
                "MappingWarnings": normalize_text(row.get("MappingWarnings")),
            }
        )

    validation_errors: list[str] = []
    missing_policy = sum(1 for row in prepared_rows if row["CodePolicyID"] is None)
    missing_domicile = sum(1 for row in prepared_rows if row["CodeDomicileID"] is None)
    missing_release = sum(1 for row in prepared_rows if row["ReleaseTime"] is None)
    missing_title = sum(1 for row in prepared_rows if not row["Title"])

    # 這些是正式寫 DB 前的最低門檻；任何一項缺漏都直接中止。
    if missing_policy:
        validation_errors.append(f"仍有 {missing_policy} 筆缺少 CodePolicyID")
    if missing_domicile:
        validation_errors.append(f"仍有 {missing_domicile} 筆缺少 CodeDomicileID")
    if missing_release:
        validation_errors.append(f"仍有 {missing_release} 筆缺少 ReleaseTime")
    if missing_title:
        validation_errors.append(f"仍有 {missing_title} 筆缺少 Title")

    child_counts = {
        link_column: sum(len(row[excel_column]) for row in prepared_rows)
        for _, link_column, excel_column in LINK_TABLE_SPECS
    }
    metadata = {
        "row_count": len(prepared_rows),
        "override_counter": dict(override_counter),
        "unresolved_labels": dict(unresolved_labels),
        "validation_errors": validation_errors,
        "child_counts": child_counts,
        "warning_row_count": sum(1 for row in prepared_rows if row["MappingWarnings"]),
    }
    return prepared_rows, metadata


def quote_ident(name: str) -> str:
    """安全包裝 SQL 識別名稱，避免名稱中有 ] 時出錯。"""

    return f"[{name.replace(']', ']]')}]"


def build_backup_table_map(label: str) -> dict[str, str]:
    """依本次執行時間標籤，產生備份表名稱。"""

    return {
        POLICY_TABLE: f"dbo._bak_{label}_IFarePolicy",
        "dbo.IFarePolicy_CodeKeyword": f"dbo._bak_{label}_IFarePolicy_CodeKeyword",
        "dbo.IFarePolicy_CodeRecipient": f"dbo._bak_{label}_IFarePolicy_CodeRecipient",
        "dbo.IFarePolicy_CodeIncome": f"dbo._bak_{label}_IFarePolicy_CodeIncome",
        "dbo.IFarePolicy_CodeIdentity": f"dbo._bak_{label}_IFarePolicy_CodeIdentity",
    }


def table_exists(cursor: pyodbc.Cursor, table_name: str) -> bool:
    """確認指定資料表是否已存在。"""

    schema_name, object_name = table_name.split(".", 1)
    sql = """
    SELECT 1
    FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = ? AND t.name = ?
    """
    return cursor.execute(sql, schema_name, object_name).fetchone() is not None


def create_backup_tables(label: str) -> dict[str, str]:
    """將現有政策資料完整備份成新表。

    這裡故意不用覆蓋舊備份；
    只要同名備份表已存在，就直接報錯停止，避免把還原點洗掉。
    """

    backup_table_map = build_backup_table_map(label)
    with connect(autocommit=True) as conn:
        cursor = conn.cursor()
        for backup_table in backup_table_map.values():
            if table_exists(cursor, backup_table):
                raise RuntimeError(f"備份表已存在，避免覆蓋：{backup_table}")

        for source_table, backup_table in backup_table_map.items():
            cursor.execute(f"SELECT * INTO {backup_table} FROM {source_table};")
    return backup_table_map


def build_restore_sql(label: str, backup_table_map: dict[str, str]) -> str:
    """產生可完整回滾本次匯入的 SQL 腳本。

    設計重點：
    - 先刪關聯表，再刪主表
    - 用 IDENTITY_INSERT 保留原本備份中的 ID
    - 用交易包住整個還原流程
    """

    def table_object(name: str) -> str:
        schema_name, object_name = name.split(".", 1)
        return f"{quote_ident(schema_name)}.{quote_ident(object_name)}"

    lines = [
        f"USE {quote_ident(DATABASE)};",
        "SET NOCOUNT ON;",
        "",
        "BEGIN TRY",
        "    BEGIN TRAN;",
        "",
        "    DELETE FROM [dbo].[IFarePolicy_CodeKeyword];",
        "    DELETE FROM [dbo].[IFarePolicy_CodeRecipient];",
        "    DELETE FROM [dbo].[IFarePolicy_CodeIncome];",
        "    DELETE FROM [dbo].[IFarePolicy_CodeIdentity];",
        "    DELETE FROM [dbo].[IFarePolicy];",
        "",
        "    SET IDENTITY_INSERT [dbo].[IFarePolicy] ON;",
        "    INSERT INTO [dbo].[IFarePolicy] (",
        "        [ID], [CreateTime], [UpdateTime], [Title], [CodePolicy_ID], [CodeDomicile_ID],",
        "        [IFareOfficeUnit_ID], [OfficeUnit_Info], [OfficeUnit_Tel], [CompetentAuthority],",
        "        [Qualification], [WelfareInfo], [Evidence], [Remark], [State],",
        "        [ReleaseTime], [DiscontinuedTime], [CreateUser_ID], [UpdateUser_ID]",
        "    )",
        f"    SELECT [ID], [CreateTime], [UpdateTime], [Title], [CodePolicy_ID], [CodeDomicile_ID],",
        f"           [IFareOfficeUnit_ID], [OfficeUnit_Info], [OfficeUnit_Tel], [CompetentAuthority],",
        f"           [Qualification], [WelfareInfo], [Evidence], [Remark], [State],",
        f"           [ReleaseTime], [DiscontinuedTime], [CreateUser_ID], [UpdateUser_ID]",
        f"    FROM {table_object(backup_table_map[POLICY_TABLE])};",
        "    SET IDENTITY_INSERT [dbo].[IFarePolicy] OFF;",
        "",
    ]

    for table_name, link_column, _ in LINK_TABLE_SPECS:
        lines.extend(
            [
                f"    SET IDENTITY_INSERT {table_object(table_name)} ON;",
                f"    INSERT INTO {table_object(table_name)} ([ID], [CreateTime], [IFarePolicy_ID], [{link_column}])",
                f"    SELECT [ID], [CreateTime], [IFarePolicy_ID], [{link_column}]",
                f"    FROM {table_object(backup_table_map[table_name])};",
                f"    SET IDENTITY_INSERT {table_object(table_name)} OFF;",
                "",
            ]
        )

    lines.extend(
        [
            "    COMMIT TRAN;",
            "END TRY",
            "BEGIN CATCH",
            "    IF @@TRANCOUNT > 0 ROLLBACK TRAN;",
            "    THROW;",
            "END CATCH;",
            "",
            f"-- 備份版本：{label}",
        ]
    )
    return "\n".join(lines)


def write_restore_sql(path: Path, content: str) -> None:
    """把 restore SQL 寫到檔案，供之後回滾使用。"""

    path.write_text(content, encoding="utf-8")


def fetch_table_counts() -> dict[str, int]:
    """抓主表與關聯表目前筆數，方便比對匯入前後差異。"""

    counts: dict[str, int] = {}
    with connect(autocommit=True) as conn:
        cursor = conn.cursor()
        for table_name in ALL_POLICY_TABLES:
            count = cursor.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
            counts[table_name] = int(count)
    return counts


def reset_identity(cursor: pyodbc.Cursor, table_name: str) -> None:
    """把 identity reseed 回 0，讓新資料從 1 開始累加。"""

    cursor.execute(f"DBCC CHECKIDENT ('{table_name}', RESEED, 0) WITH NO_INFOMSGS;")


def replace_policy_data(rows: list[dict[str, Any]]) -> dict[str, int]:
    """以交易方式整批覆蓋 IFare 政策資料。

    流程是：
    1. 清關聯表
    2. 清主表
    3. reset identity
    4. 寫主表
    5. 收集並批次寫入多對多關聯表
    6. commit

    只要中間任何一步失敗，因為 autocommit=False，整批交易都不會落地。
    """

    with connect(autocommit=False) as conn:
        cursor = conn.cursor()
        cursor.fast_executemany = True

        # 刪除順序必須先子表再主表，否則會撞到 FK。
        for table_name, _, _ in LINK_TABLE_SPECS:
            cursor.execute(f"DELETE FROM {table_name};")
        cursor.execute(f"DELETE FROM {POLICY_TABLE};")

        # 既然是全量重建，就順便把 identity 重新歸零，讓 ID 重新連續。
        reset_identity(cursor, POLICY_TABLE)
        for table_name, _, _ in LINK_TABLE_SPECS:
            reset_identity(cursor, table_name)

        main_insert_sql = """
        INSERT INTO dbo.IFarePolicy (
            CreateTime, UpdateTime, Title, CodePolicy_ID, CodeDomicile_ID, IFareOfficeUnit_ID,
            OfficeUnit_Info, OfficeUnit_Tel, CompetentAuthority, Qualification, WelfareInfo,
            Evidence, Remark, State, ReleaseTime, DiscontinuedTime, CreateUser_ID, UpdateUser_ID
        )
        OUTPUT INSERTED.ID
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """

        child_buffers: dict[str, list[tuple[datetime, int, int]]] = {
            table_name: [] for table_name, _, _ in LINK_TABLE_SPECS
        }
        now = datetime.now()

        for row in rows:
            # 主表先寫入，拿到 IFarePolicy.ID 後，才能建立關聯表資料。
            cursor.execute(
                main_insert_sql,
                now,
                now,
                row["Title"],
                row["CodePolicyID"],
                row["CodeDomicileID"],
                row["IFareOfficeUnitID"],
                row["OfficeUnitInfo"],
                row["OfficeUnitTel"],
                row["CompetentAuthority"],
                row["Qualification"],
                row["WelfareInfo"],
                row["Evidence"],
                row["Remark"],
                row["State"],
                row["ReleaseTime"],
                row["DiscontinuedTime"],
                CREATE_USER_ID,
                CREATE_USER_ID,
            )
            policy_id = int(cursor.fetchone()[0])

            for table_name, link_column, excel_column in LINK_TABLE_SPECS:
                child_buffers[table_name].extend(
                    (now, policy_id, code_id) for code_id in row[excel_column]
                )

        # 關聯表改用 executemany，避免一筆一筆 insert 過慢。
        for table_name, link_column, _ in LINK_TABLE_SPECS:
            rows_to_insert = child_buffers[table_name]
            if not rows_to_insert:
                continue
            cursor.executemany(
                f"INSERT INTO {table_name} (CreateTime, IFarePolicy_ID, {link_column}) VALUES (?, ?, ?);",
                rows_to_insert,
            )

        conn.commit()

    return fetch_table_counts()


def write_report(path: Path, payload: dict[str, Any]) -> None:
    """輸出本次執行報告，方便追蹤與稽核。"""

    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    """主執行流程。

    真正寫 DB 前的關鍵防線有兩道：
    - load_rows() 的 validation_errors
    - create_backup_tables() 成功建立可回滾資料
    """

    runtime = build_runtime_args()
    excel_path: Path = runtime["excel"]
    backup_label: str = runtime["backup_label"]
    restore_sql_path: Path = runtime["restore_sql"]
    report_path: Path = runtime["report_json"]

    rows, metadata = load_rows(excel_path)
    if metadata["validation_errors"]:
        raise RuntimeError("；".join(metadata["validation_errors"]))

    # 先記錄匯入前筆數，再建立備份與還原腳本，最後才正式覆蓋資料。
    counts_before = fetch_table_counts()
    backup_table_map = create_backup_tables(backup_label)
    write_restore_sql(restore_sql_path, build_restore_sql(backup_label, backup_table_map))
    counts_after = replace_policy_data(rows)

    report = {
        "server": SERVER,
        "database": DATABASE,
        "excel_path": str(excel_path),
        "sheet_name": SHEET_NAME,
        "backup_label": backup_label,
        "backup_tables": backup_table_map,
        "restore_sql_path": str(restore_sql_path),
        "counts_before": counts_before,
        "counts_after": counts_after,
        "prepared_rows": metadata["row_count"],
        "warning_row_count": metadata["warning_row_count"],
        "override_counter": metadata["override_counter"],
        "unresolved_labels": metadata["unresolved_labels"],
        "child_counts_expected": metadata["child_counts"],
        "executed_at": datetime.now().isoformat(timespec="seconds"),
    }
    write_report(report_path, report)

    print(f"備份版本：{backup_label}")
    print(f"Excel：{excel_path}")
    print(f"還原 SQL：{restore_sql_path}")
    print(f"報告：{report_path}")
    print(json.dumps(report["counts_after"], ensure_ascii=False))


if __name__ == "__main__":
    main()
