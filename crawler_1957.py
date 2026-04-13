from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


def _load_base_module():
    current_file = Path(__file__).resolve()
    base_module_path = current_file.with_name("crawler_1957_core.py")
    candidates = [
        base_module_path,
    ]

    for candidate in candidates:
        if not candidate.exists():
            continue
        if candidate.resolve() == current_file:
            continue

        spec = importlib.util.spec_from_file_location("crawler_1957_core", candidate)
        if spec is None or spec.loader is None:
            continue

        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module

    raise FileNotFoundError(
        f"找不到可載入的 crawler_1957 核心檔案：{base_module_path}"
    )


base = _load_base_module()


def __getattr__(name: str):


    return getattr(base, name)


def preserve_full_text(value: str, legacy_limit: int, field_name: str, warnings: list[str]) -> str:
    """保留完整字串，只在超過舊版欄位長度時留下提醒。"""

    text = base.normalize_space(value)
    if len(text) > legacy_limit:
        warnings.append(f"{field_name} 超過舊版欄位長度 {legacy_limit}，本版已保留全文；請先套用 schema SQL")
    return text


EVIDENCE_DIRECT_MARKERS = (
    "應備文件",
    "應備證件",
    "應附文件",
    "共同應備文件",
    "檢附文件",
    "應檢附",
    "檢附下列",
    "檢具下列",
    "書表文件",
)

EVIDENCE_DOC_TOKENS = (
    "檢附",
    "檢具",
    "申請表",
    "戶口名簿",
    "戶籍謄本",
    "證明文件",
    "證明書",
    "診斷書",
    "存簿",
    "存摺",
    "收據",
    "影本",
    "切結書",
    "同意書",
    "身分證",
    "健保卡",
    "照片",
    "印章",
    "名簿",
    "謄本",
)

EVIDENCE_CONTACT_TOKENS = (
    "承辦人員",
    "聯絡電話",
    "連絡電話",
    "客服電話",
    "電話",
    "傳真",
    "EMAIL",
    "E-mail",
    "電子郵件",
    "@",
)


def _is_numbered_line(text: str) -> bool:
    return bool(
        base.re.match(r"^[（(]?[一二三四五六七八九十\d]+[）)].*", text)
        or base.re.match(r"^\d+[.、].*", text)
        or base.re.match(r"^\(\d+\).*", text)
    )


def infer_evidence_from_apply_text(apply_text: str) -> str:
    """
    1957 有不少頁面把「應備文件」混在「申請方式」段落，
    這裡做保守補救：只在原本 Evidence 為空時，從 ApplyInfo 中
    抽出明顯屬於證件/文件的句子，避免把真的「無須申請」案件誤補。
    """

    lines = base.split_non_empty_lines(apply_text)
    if not lines:
        return ""

    evidence_lines: list[str] = []
    in_docs_block = False

    for line in lines:
        normalized = base.normalize_space(line)
        if not normalized:
            continue

        if any(token in normalized for token in EVIDENCE_CONTACT_TOKENS):
            in_docs_block = False
            continue

        direct_hit = any(marker in normalized for marker in EVIDENCE_DIRECT_MARKERS)
        doc_token_hits = sum(1 for token in EVIDENCE_DOC_TOKENS if token in normalized)
        doc_like = direct_hit or "檢附" in normalized or "檢具" in normalized or doc_token_hits >= 2

        if doc_like:
            evidence_lines.append(normalized)
            in_docs_block = True
            continue

        if in_docs_block and _is_numbered_line(normalized):
            evidence_lines.append(normalized)
            continue

        in_docs_block = False

    return "\n".join(dict.fromkeys(evidence_lines)).strip()


def build_default_output() -> Path:

    desktop = Path.home() / "Desktop"
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return desktop / f"1957政策_{stamp}.xlsx"


EXPORT_COLUMNS = [
    "Title",
    "Qualification",
    "WelfareInfo",
    "Evidence",
    "IFareOfficeUnitID",
    "OfficeUnitInfo",
    "OfficeUnitTel",
    "CodePolicyID",
    "CodeDomicileID",
    "CodeIndentityIDs",
    "CodeIncomeIDs",
    "CodeRecipientIDs",
    "CodeKeywordIDs",
    "CompetentAuthority",
    "ReleaseTime",
    "DiscontinuedTime",
    "Remark",
    "IsEnabled",
    "CodePolicyLabel",
    "SourceUrl",
    "PolicyUrl",
    "SID",
    "MappingWarnings",
]


class Mohw1957CountyCrawler(base.Mohw1957CountyCrawler):

    def _build_db_ready_row(self, raw_row: dict[str, Any], detail: dict[str, str]) -> tuple[dict[str, Any], dict[str, Any]]:
        warnings: list[str] = []
        raw_title = base.normalize_space(str(raw_row["政策標題"]))
        title = preserve_full_text(raw_title, 50, "Title", warnings)
        title_was_trimmed = False

        code_policy_id, code_policy_label = self._lookup_policy_code(str(raw_row["福利分類"]))
        if code_policy_id is None:
            warnings.append(f"找不到 CodePolicyID: {raw_row['福利分類']}")

        code_domicile_id, code_domicile_label = self._lookup_domicile_code(str(raw_row["縣市"]))
        if code_domicile_id is None:
            warnings.append(f"找不到 CodeDomicileID: {raw_row['縣市']}")

        evidence_text = detail["evidence_text"]
        if not evidence_text:
            evidence_text = infer_evidence_from_apply_text(detail["apply_text"])
            if evidence_text:
                warnings.append("Evidence 原始為空，已由申請方式段落中的應備文件內容補入")

        content_blob = "\n".join(
            part
            for part in [
                raw_title,
                detail["qualification_text"],
                detail["benefit_text"],
                detail["apply_text"],
                evidence_text,
                detail["remark_text"],
                detail["content_text"],
            ]
            if part
        )

        keyword_ids, keyword_labels = self._infer_multi_codes(content_blob, base.RAW_CODE_KEYWORD_MAP, base.KEYWORD_PATTERNS, None)
        if not keyword_ids and base.re.search(r"補助|津貼|補貼|給付|救助", content_blob):
            keyword_ids = [base.RAW_CODE_KEYWORD_MAP["經濟補助"]]
            keyword_labels = ["經濟補助"]

        recipient_ids, recipient_labels = self._infer_multi_codes(
            content_blob, base.RAW_CODE_RECIPIENT_MAP, base.RECIPIENT_PATTERNS, "全選"
        )
        income_ids, income_labels = self._infer_multi_codes(
            content_blob, base.RAW_CODE_INCOME_MAP, base.INCOME_PATTERNS, "全選"
        )
        identity_ids, identity_labels = self._infer_multi_codes(
            content_blob, base.RAW_CODE_IDENTITY_MAP, base.IDENTITY_PATTERNS, "全選"
        )

        welfare_html_preview = self._compose_welfare_html(detail)
        welfare_info = base.quote_html_fragment(welfare_html_preview)

        office_unit_info_raw = self._extract_office_unit_info(detail["apply_text"], detail["source_name"])
        office_unit_info = preserve_full_text(office_unit_info_raw, 100, "OfficeUnitInfo", warnings)
        office_unit_tel = preserve_full_text(
            base.first_phone("\n".join([detail["apply_text"], detail["content_text"]])),
            100,
            "OfficeUnitTel",
            warnings,
        )
        competent_authority = preserve_full_text(detail["source_name"], 50, "CompetentAuthority", warnings)
        remark = preserve_full_text(detail["remark_text"], 100, "Remark", warnings)
        office_unit_id = self._infer_office_unit_id(office_unit_info, competent_authority, detail["apply_text"])

        payload: dict[str, Any] = {
            "Title": title,
            "Qualification": detail["qualification_text"],
            "WelfareInfo": welfare_info,
            "Evidence": evidence_text,
            "IFareOfficeUnitID": office_unit_id,
            "OfficeUnitInfo": office_unit_info,
            "OfficeUnitTel": office_unit_tel,
            "CodePolicyID": code_policy_id,
            "CodeDomicileID": code_domicile_id,
            "CodeIndentityIDs": identity_ids,
            "CodeIncomeIDs": income_ids,
            "CodeRecipientIDs": recipient_ids,
            "CodeKeywordIDs": keyword_ids,
            "CompetentAuthority": competent_authority,
            "ReleaseTime": raw_row["更新時間"] or None,
            "DiscontinuedTime": base.DEFAULT_DISCONTINUED_TIME,
            "Remark": remark,
            "IsEnabled": True,
        }

        # 這張表就是你最後要匯入 DB 的工作表 ifare_policy。
        db_row = {
            "Title": title,
            "Qualification": detail["qualification_text"],
            "WelfareInfo": welfare_info,
            "Evidence": evidence_text,
            "IFareOfficeUnitID": office_unit_id,
            "OfficeUnitInfo": office_unit_info,
            "OfficeUnitTel": office_unit_tel,
            "CodePolicyID": code_policy_id,
            "CodeDomicileID": code_domicile_id,
            "CodeIndentityIDs": base.json_compact(identity_ids),
            "CodeIncomeIDs": base.json_compact(income_ids),
            "CodeRecipientIDs": base.json_compact(recipient_ids),
            "CodeKeywordIDs": base.json_compact(keyword_ids),
            "CompetentAuthority": competent_authority,
            "ReleaseTime": raw_row["更新時間"] or "",
            "DiscontinuedTime": base.DEFAULT_DISCONTINUED_TIME,
            "Remark": remark,
            "IsEnabled": True,
            "CodePolicyLabel": code_policy_label,
            "CodeDomicileLabel": code_domicile_label,
            "CodeIdentityLabels": " | ".join(identity_labels),
            "CodeIncomeLabels": " | ".join(income_labels),
            "CodeRecipientLabels": " | ".join(recipient_labels),
            "CodeKeywordLabels": " | ".join(keyword_labels),
            "RawTitle": raw_title,
            "TitleWasTrimmed": title_was_trimmed,
            "SourceName": detail["source_name"],
            "SourceUrl": detail["source_url"],
            "PolicyUrl": raw_row["政策連結"],
            "SID": raw_row["SID"],
            "UpdatedAt": raw_row["更新時間"],
            "WelfareInfoHtmlPreview": welfare_html_preview,
            "ApplyInfoPreview": detail["apply_text"],
            "MappingWarnings": " | ".join(warnings),
            "PolicyPayloadJson": base.json_compact(payload),
        }
        return db_row, payload


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    """定義桌面正式版 crawler 的 CLI 參數。"""

    parser = argparse.ArgumentParser(description="爬取衛福部 1957 政策資料，輸出單一 ifare_policy 工作表")
    parser.add_argument(
        "--output",
        type=Path,
        default=build_default_output(),
        help="輸出的 Excel 路徑，預設為桌面 1957政策_日期時間.xlsx",
    )
    parser.add_argument(
        "--db-json-output",
        type=Path,
        default=None,
        help="另存一份可直接餵 API/後續匯入程式的 JSON payload",
    )
    parser.add_argument(
        "--root-cate-id",
        default=base.DEFAULT_ROOT_CATE_ID,
        help="起始分類 cateId，預設為 1（首頁全部）",
    )
    parser.add_argument(
        "--county",
        action="append",
        default=[],
        help="只抓指定縣市，可重複指定，例如 --county 新北市 --county 台北市",
    )
    parser.add_argument(
        "--category",
        action="append",
        default=[],
        help="只抓指定福利分類，可重複指定，例如 --category 社會救助",
    )
    parser.add_argument(
        "--max-policies",
        type=int,
        default=None,
        help="最多抓幾筆政策，用於測試",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="每次請求後暫停秒數",
    )
    return parser.parse_args(list(argv))


def main(argv: Iterable[str]) -> int:

    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    args = parse_args(argv)

    crawler = Mohw1957CountyCrawler(
        root_cate_id=str(args.root_cate_id),
        county_filters={item.strip() for item in args.county if item.strip()},
        category_filters={item.strip() for item in args.category if item.strip()},
        sleep_seconds=args.sleep,
        max_policies=args.max_policies,
    )

    try:
        crawler.crawl()
    except Exception as exc:
        print(f"爬取失敗：{exc}", file=sys.stderr)
        return 1

    db_df = crawler.db_ready_dataframe()
    export_df = db_df.loc[:, [column for column in EXPORT_COLUMNS if column in db_df.columns]].map(base.sanitize_for_excel)

    base.ensure_parent_dir(args.output)
    with pd.ExcelWriter(args.output) as writer:
        export_df.to_excel(writer, index=False, sheet_name="ifare_policy")

    if args.db_json_output is not None:
        base.ensure_parent_dir(args.db_json_output)
        args.db_json_output.write_text(json.dumps(crawler.db_payloads, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"完成，共 {len(export_df)} 筆。Excel 已輸出到：{args.output}")
    if args.db_json_output is not None:
        print(f"DB-ready JSON 已輸出到：{args.db_json_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
