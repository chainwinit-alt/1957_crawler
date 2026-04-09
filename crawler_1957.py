from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from html import escape
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, quote, urljoin, urlparse

import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup, NavigableString, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_URL = "https://1957.mohw.gov.tw"
DEFAULT_ROOT_CATE_ID = "1"
DEFAULT_TIMEOUT = 30
DEFAULT_DISCONTINUED_TIME = "3333-03-31 00:00:00"
DEFAULT_OFFICE_UNIT_ID = 1
ILLEGAL_EXCEL_CHAR_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")
PHONE_RE = re.compile(r"(?:\+886[-\s]?)?0\d{1,2}[-\s]?\d{3,4}[-\s]?\d{3,4}(?:#\d+)?")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@dataclass(frozen=True)
class CategoryCard:
    label: str
    action: str
    cate_id: str


def normalize_space(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"[\s\u3000]+", " ", text).strip()


def normalize_group_title(text: str | None) -> str:
    title = normalize_space(text)
    if title in {"", ":", "："}:
        return ""
    return title.rstrip(":：").strip()


def sanitize_for_excel(value: object) -> object:
    if value is None:
        return ""
    if not isinstance(value, str):
        return value
    sanitized = ILLEGAL_EXCEL_CHAR_RE.sub("", value)
    return sanitized.replace("\ufeff", "")


def roc_datetime_to_ad(text: str) -> str:
    match = re.search(r"(\d{2,3})/(\d{1,2})/(\d{1,2})\s+(\d{2}:\d{2}:\d{2})", text)
    if not match:
        return ""
    year = int(match.group(1)) + 1911
    month = int(match.group(2))
    day = int(match.group(3))
    return f"{year:04d}-{month:02d}-{day:02d} {match.group(4)}"


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def build_default_output() -> Path:
    desktop = Path.home() / "Desktop"
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return desktop / f"1957縣市政策_{stamp}.xlsx"


def normalize_lookup_key(text: str | None) -> str:
    value = normalize_space(text)
    value = value.replace("臺", "台").replace("／", "/").replace("＆", "&")
    value = value.replace("‧", "").replace("·", "").replace(" ", "")
    return value


def build_lookup(raw_map: dict[str, int]) -> dict[str, int]:
    return {normalize_lookup_key(label): code_id for label, code_id in raw_map.items()}


def split_non_empty_lines(text: str) -> list[str]:
    return [line for line in (normalize_space(part) for part in text.splitlines()) if line]


def html_fragment_to_text(fragment: str) -> str:
    html = re.sub(r"<br\s*/?>", "\n", fragment or "", flags=re.IGNORECASE)
    soup = BeautifulSoup(html, "html.parser")
    return "\n".join(split_non_empty_lines(soup.get_text("\n", strip=True)))


def text_to_html_paragraphs(text: str) -> str:
    paragraphs = split_non_empty_lines(text)
    return "".join(f"<p>{line}</p>" for line in paragraphs)


def quote_html_fragment(fragment: str) -> str:
    html = fragment.strip()
    return quote(html, safe="") if html else ""


def truncate_for_db(value: str, limit: int, field_name: str, warnings: list[str]) -> str:
    text = normalize_space(value)
    if len(text) <= limit:
        return text
    warnings.append(f"{field_name} 超過 {limit} 字已截斷")
    return text[:limit]


def first_phone(text: str) -> str:
    match = PHONE_RE.search(text or "")
    return match.group(0) if match else ""


def json_compact(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


RAW_CODE_POLICY_MAP = {
    "社會保險": 1,
    "社會救助": 2,
    "兒少福利": 3,
    "家庭福利": 4,
    "老人福利": 5,
    "長期照顧": 6,
    "身心障礙服務": 7,
    "勞工福利": 8,
    "住宅福利": 9,
    "原民福利": 10,
    "其他福利": 11,
    "身心障礙福利": 12,
    "微型保險": 13,
    "各式民間資源": 14,
}


RAW_CODE_DOMICILE_MAP = {
    "全國": 1,
    "新北市": 2,
    "台北市": 3,
    "桃園市": 4,
    "台中市": 5,
    "台南市": 6,
    "高雄市": 7,
    "基隆市": 8,
    "新竹縣": 9,
    "新竹市": 10,
    "苗栗縣": 11,
    "彰化縣": 12,
    "南投縣": 13,
    "雲林縣": 14,
    "嘉義縣": 15,
    "嘉義市": 16,
    "屏東縣": 17,
    "宜蘭縣": 18,
    "花蓮縣": 19,
    "台東縣": 20,
    "澎湖縣": 21,
    "金門縣": 22,
    "連江縣": 23,
}


RAW_CODE_KEYWORD_MAP = {
    "失業": 1,
    "就業": 2,
    "健康": 3,
    "生育": 4,
    "托育": 5,
    "租屋": 6,
    "急難": 7,
    "喪葬": 8,
    "健保": 9,
    "勞保": 10,
    "國保": 11,
    "農保": 12,
    "醫療": 13,
    "重大傷病": 14,
    "身心障礙": 15,
    "特殊境遇": 16,
    "法律協助": 17,
    "經濟補助": 18,
    "教育": 19,
    "輔具": 20,
    "敬老愛心卡": 21,
    "老年基礎保證年金": 22,
    "老人津貼": 23,
    "長期照顧": 24,
    "原住民": 25,
    "共餐送餐": 26,
    "老花眼鏡": 27,
    "中低收入老人生活津貼": 28,
    "住宅修繕補助": 29,
    "低收入戶": 30,
    "中低收入戶": 31,
    "假牙補助": 32,
    "防走失手鍊": 33,
    "緊急救援系統": 34,
    "老農津貼": 35,
    "日間照顧": 36,
    "住宿式照顧": 37,
    "聯合奠祭": 38,
    "環保自然葬": 39,
    "新住民": 40,
    "看護": 41,
    "志願服務": 42,
    "社會住宅": 43,
    "獨居老人": 44,
    "社會安全網": 45,
    "災害": 46,
    "銀髮家園": 47,
    "罕見疾病": 48,
    "中高齡": 49,
    "微型保險": 50,
    "喘息服務": 51,
    "心理健康": 52,
    "交通": 53,
    "早期療育": 54,
    "法律": 55,
}


RAW_CODE_RECIPIENT_MAP = {
    "全選": 1,
    "嬰幼兒": 2,
    "兒童＆青少年": 3,
    "成人": 4,
    "老人": 5,
}


RAW_CODE_INCOME_MAP = {
    "全選": 1,
    "經濟弱勢": 2,
    "中低收入戶": 3,
    "低收入戶": 4,
}


RAW_CODE_IDENTITY_MAP = {
    "全選": 1,
    "身心障礙": 2,
    "特殊境遇": 3,
    "重大傷病": 4,
    "原住民": 5,
    "新住民": 6,
    "無": 7,
}


CODE_POLICY_LOOKUP = build_lookup(RAW_CODE_POLICY_MAP)
CODE_DOMICILE_LOOKUP = build_lookup(RAW_CODE_DOMICILE_MAP)

POLICY_LABEL_ALIASES = {
    normalize_lookup_key("兒童及少年福利"): "兒少福利",
    normalize_lookup_key("兒童少年福利"): "兒少福利",
    normalize_lookup_key("原住民福利"): "原民福利",
    normalize_lookup_key("原住民族福利"): "原民福利",
    normalize_lookup_key("長期照護"): "長期照顧",
    normalize_lookup_key("身心障礙服務"): "身心障礙福利",
}


DOMICILE_LABEL_ALIASES = {
    normalize_lookup_key("全國福利"): "全國",
    normalize_lookup_key("中央"): "全國",
    normalize_lookup_key("臺北市"): "台北市",
    normalize_lookup_key("臺中市"): "台中市",
    normalize_lookup_key("臺南市"): "台南市",
    normalize_lookup_key("臺東縣"): "台東縣",
}


SECTION_ALIAS_GROUPS = {
    "qualification": [
        "申請資格",
        "資格條件",
        "申請條件",
        "申辦資格",
        "服務對象",
        "補助對象",
        "適用對象",
        "請領資格",
        "資格",
        "申請對象",
        "實施對象",
        "收托資格",
        "收托對象",
    ],
    "benefit": [
        "補助內容",
        "給付標準",
        "補助內容/給付標準",
        "給付內容",
        "補助標準",
        "補助額度",
        "服務內容",
        "補助項目",
        "補貼標準",
        "優惠內容",
        "檢查項目",
        "檢查費用",
        "內容",
        "補助原則",
        "補助對象及內容",
    ],
    "apply": [
        "申請方式",
        "申請流程",
        "申請期限",
        "申請方式/流程/期限",
        "申辦方式",
        "申辦流程",
        "辦理方式",
        "辦理流程",
        "辦理期限",
        "申請時間",
        "受理申請時間",
        "受理申請期間",
        "申請說明",
        "流程",
        "交付方式",
        "檢查地點",
        "檢查流程",
        "補助方式",
        "辦理單位",
        "承辦資訊",
        "承辦單位",
        "聯繫單位",
        "聯絡資訊",
        "承辦資訊",
    ],
    "evidence": ["應備文件", "應附文件", "檢附文件", "應備資料", "應檢附文件", "應備證件", "檢附資料", "準備文件"],
    "remark": ["備註", "注意事項", "請領限制", "法規依據", "實施時間"],
    "source": ["資料來源", "資訊來源"],
}


SECTION_LOOKUP = {
    normalize_lookup_key(alias): canonical
    for canonical, aliases in SECTION_ALIAS_GROUPS.items()
    for alias in aliases
}


PLAIN_TEXT_HEADING_RE = re.compile(
    r"^(?P<prefix>(?:[※＊*]\s*)?(?:[（(]?[一二三四五六七八九十百千\d]+[）).、．]\s*)?)(?P<title>[^：:]{1,40})(?P<suffix>[：:]?.*)$"
)


KEYWORD_PATTERNS = {
    "生育": [r"生育", r"育兒", r"孕前", r"懷孕", r"孕婦", r"生產"],
    "托育": [r"托育", r"托嬰", r"幼兒", r"育兒"],
    "租屋": [r"租屋", r"租金", r"租賃"],
    "急難": [r"急難", r"紓困", r"緊急救助"],
    "喪葬": [r"喪葬", r"死亡給付", r"殯葬"],
    "勞保": [r"勞保", r"勞工保險"],
    "國保": [r"國民年金", r"國保"],
    "農保": [r"農保", r"農民"],
    "醫療": [r"醫療", r"住院", r"看護"],
    "教育": [r"教育", r"就學", r"學生", r"學校", r"助學金", r"獎助學金", r"學雜費"],
    "法律協助": [r"法律協助", r"法律扶助", r"法律諮詢"],
    "經濟補助": [r"補助", r"津貼", r"補貼", r"給付", r"救助"],
    "長期照顧": [r"長期照顧", r"長照"],
    "原住民": [r"原住民"],
    "新住民": [r"新住民"],
    "身心障礙": [r"身心障礙", r"身障"],
    "重大傷病": [r"重大傷病"],
    "特殊境遇": [r"特殊境遇"],
    "社會住宅": [r"社會住宅"],
    "看護": [r"看護"],
    "早期療育": [r"早期療育"],
}


RECIPIENT_PATTERNS = {
    "嬰幼兒": [r"嬰幼兒", r"幼兒", r"嬰兒", r"托育", r"托嬰", r"育兒", r"未滿2歲"],
    "兒童＆青少年": [r"兒童", r"少年", r"青少年", r"子女", r"學生", r"就學", r"高中", r"高職", r"國中", r"國小", r"大專"],
    "成人": [r"成人", r"勞工", r"就業", r"失業", r"申請人", r"配偶", r"租金"],
    "老人": [r"老人", r"老年", r"長者", r"敬老", r"65歲", r"70歲"],
}


INCOME_PATTERNS = {
    "經濟弱勢": [r"經濟弱勢", r"弱勢", r"清寒", r"生活困難"],
    "中低收入戶": [r"中低收入戶", r"中低收"],
    "低收入戶": [r"低收入戶"],
}


IDENTITY_PATTERNS = {
    "身心障礙": [r"身心障礙", r"身障"],
    "特殊境遇": [r"特殊境遇"],
    "重大傷病": [r"重大傷病"],
    "原住民": [r"原住民"],
    "新住民": [r"新住民"],
}


class Mohw1957CountyCrawler:
    def __init__(
        self,
        root_cate_id: str = DEFAULT_ROOT_CATE_ID,
        county_filters: set[str] | None = None,
        category_filters: set[str] | None = None,
        sleep_seconds: float = 0.0,
        max_policies: int | None = None,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> None:
        self.root_cate_id = str(root_cate_id)
        self.county_filters = county_filters or set()
        self.category_filters = category_filters or set()
        self.sleep_seconds = sleep_seconds
        self.max_policies = max_policies
        self.timeout = timeout
        self.rows: list[dict[str, Any]] = []
        self.db_ready_rows: list[dict[str, Any]] = []
        self.db_payloads: list[dict[str, Any]] = []
        self.visited_category_ids: set[str] = set()
        self.visited_qa_ids: set[str] = set()
        self.session = self._build_session()

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.trust_env = False
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.verify = False
        session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/135.0.0.0 Safari/537.36"
                )
            }
        )
        return session

    def _fetch_html(self, path_or_url: str) -> str:
        url = path_or_url if path_or_url.startswith("http") else urljoin(BASE_URL, path_or_url)
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()
        declared = (response.encoding or "").strip().lower()
        detected = (response.apparent_encoding or "").strip().lower()

        if declared and declared not in {"ascii", "iso-8859-1"}:
            response.encoding = declared
        elif detected and detected != "ascii":
            response.encoding = detected
        else:
            response.encoding = "big5"
        if self.sleep_seconds > 0:
            time.sleep(self.sleep_seconds)
        return response.text

    def _parse_soup(self, path_or_url: str) -> BeautifulSoup:
        return BeautifulSoup(self._fetch_html(path_or_url), "html.parser")

    def _extract_breadcrumbs(self, soup: BeautifulSoup) -> list[str]:
        labels = [normalize_space(item.get_text(" ", strip=True)) for item in soup.select("#breadcrumbLink li")]
        return [label for label in labels if label and label != "社會福利"]

    def _parse_category_cards(self, soup: BeautifulSoup) -> list[CategoryCard]:
        cards: list[CategoryCard] = []
        for anchor in soup.select("a.categoryCls[onclick]"):
            onclick = anchor.get("onclick", "")
            match = re.search(r"javascript:(goToCategory|showQAData)\('(\d+)'\)", onclick)
            if not match:
                continue
            label = normalize_space(anchor.get_text(" ", strip=True))
            if not label:
                continue
            cards.append(CategoryCard(label=label, action=match.group(1), cate_id=match.group(2)))
        return cards

    def _should_skip_card(self, current_path: list[str], card: CategoryCard) -> bool:
        if self.county_filters and current_path == ["縣市福利"] and card.label not in self.county_filters:
            return True
        if current_path and current_path[0] == "縣市福利" and "服務窗口" in card.label:
            return True
        if self.category_filters and card.action == "showQAData" and card.label not in self.category_filters:
            return True
        return False

    def crawl(self) -> pd.DataFrame:
        if self.root_cate_id.lower() in {"1", "home", "homepage", "index"}:
            self._crawl_homepage()
        else:
            self._crawl_category(self.root_cate_id, path=None)
        df = pd.DataFrame(self.rows)
        if df.empty:
            return pd.DataFrame(
                columns=[
                    "根分類",
                    "第一層分類",
                    "第二層分類",
                    "第三層分類",
                    "縣市",
                    "福利分類",
                    "福利分類系項",
                    "政策群組",
                    "類別路徑",
                    "分類頁CateId",
                    "分類頁連結",
                    "政策標題",
                    "政策連結",
                    "SID",
                    "更新時間原文",
                    "更新時間",
                    "資料來源名稱",
                    "資料來源連結",
                    "附件名稱列表",
                    "附件ID列表",
                    "申請資格",
                    "補助內容",
                    "申請方式",
                    "應備文件",
                    "備註",
                    "內文",
                ]
            )
        return df

    def db_ready_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame(self.db_ready_rows)
        if df.empty:
            return pd.DataFrame(
                columns=[
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
                    "CodeDomicileLabel",
                    "CodeIdentityLabels",
                    "CodeIncomeLabels",
                    "CodeRecipientLabels",
                    "CodeKeywordLabels",
                    "RawTitle",
                    "TitleWasTrimmed",
                    "SourceName",
                    "SourceUrl",
                    "PolicyUrl",
                    "SID",
                    "UpdatedAt",
                    "WelfareInfoHtmlPreview",
                    "ApplyInfoPreview",
                    "MappingWarnings",
                    "PolicyPayloadJson",
                ]
            )
        return df

    def _crawl_homepage(self) -> None:
        soup = self._parse_soup("/")
        cards = self._parse_category_cards(soup)
        print(f"[homepage] {len(cards)} 個主分類")

        for card in cards:
            if self.max_policies is not None and len(self.rows) >= self.max_policies:
                return
            if card.action == "goToCategory":
                self._crawl_category(card.cate_id, [card.label])
            else:
                self._crawl_qa_list(card.cate_id, [card.label])

    def _crawl_category(self, cate_id: str, path: list[str] | None) -> None:
        if cate_id in self.visited_category_ids:
            return
        if self.max_policies is not None and len(self.rows) >= self.max_policies:
            return

        self.visited_category_ids.add(cate_id)
        soup = self._parse_soup(f"/category.jsp?cateId={cate_id}")
        current_path = path or self._extract_breadcrumbs(soup)
        cards = self._parse_category_cards(soup)

        if not cards:
            return

        print(f"[category] {' > '.join(current_path) or cate_id} -> {len(cards)} 個子項")

        for card in cards:
            if self.max_policies is not None and len(self.rows) >= self.max_policies:
                return
            if self._should_skip_card(current_path, card):
                continue

            child_path = current_path + [card.label]
            if card.action == "goToCategory":
                self._crawl_category(card.cate_id, child_path)
            else:
                self._crawl_qa_list(card.cate_id, child_path)

    def _crawl_qa_list(self, cate_id: str, path: list[str]) -> None:
        if cate_id in self.visited_qa_ids:
            return
        if self.max_policies is not None and len(self.rows) >= self.max_policies:
            return

        self.visited_qa_ids.add(cate_id)
        soup = self._parse_soup(f"/QADataList.jsp?cateId={cate_id}")
        list_url = urljoin(BASE_URL, f"/QADataList.jsp?cateId={cate_id}")
        qa_table = soup.select_one("#QADataTable")
        if qa_table is None:
            return

        tbody = qa_table.find("tbody") or qa_table
        outer_rows = tbody.find_all("tr", recursive=False)
        print(f"[qa] {' > '.join(path)} -> {len(outer_rows)} 個群組")

        for group_row in outer_rows:
            if self.max_policies is not None and len(self.rows) >= self.max_policies:
                return

            group_td = group_row.find("td", recursive=False) or group_row.find("td")
            if group_td is None:
                continue

            group_title = self._extract_group_title(group_td)
            for anchor in group_td.select("table#QADataListTB a[href*='QACtrl?func=QAView']"):
                if self.max_policies is not None and len(self.rows) >= self.max_policies:
                    return
                raw_row, db_row, payload = self._build_rows_from_listing(anchor, path, group_title, cate_id, list_url)
                self.rows.append(raw_row)
                self.db_ready_rows.append(db_row)
                self.db_payloads.append(payload)
                print(f"  - {raw_row['政策標題']}")

    def _extract_group_title(self, group_td: Tag) -> str:
        parts: list[str] = []
        for child in group_td.children:
            if isinstance(child, NavigableString):
                text = normalize_space(str(child))
                if text:
                    parts.append(text)
            elif isinstance(child, Tag):
                if child.name == "br":
                    break
                if child.name == "table":
                    break
                text = normalize_space(child.get_text(" ", strip=True))
                if text:
                    parts.append(text)
        return normalize_group_title(" ".join(parts))

    def _build_rows_from_listing(
        self,
        anchor: Tag,
        path: list[str],
        group_title: str,
        cate_id: str,
        list_url: str,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        policy_title = normalize_space(anchor.get_text(" ", strip=True))
        policy_url = urljoin(BASE_URL, anchor.get("href", ""))
        sid = parse_qs(urlparse(policy_url).query).get("sid", [""])[0]

        detail = self._parse_detail_page(policy_url)
        first_level, second_level, third_level, county_label, welfare_category, welfare_subcategory = self._map_path_fields(
            path, group_title
        )

        raw_row = {
            "根分類": first_level,
            "第一層分類": first_level,
            "第二層分類": second_level,
            "第三層分類": third_level,
            "縣市": county_label,
            "福利分類": welfare_category,
            "福利分類系項": welfare_subcategory,
            "政策群組": group_title,
            "類別路徑": " > ".join(path),
            "分類頁CateId": cate_id,
            "分類頁連結": list_url,
            "政策標題": policy_title,
            "政策連結": policy_url,
            "SID": sid,
            "更新時間原文": detail["updated_raw"],
            "更新時間": detail["updated_ad"],
            "資料來源名稱": detail["source_name"],
            "資料來源連結": detail["source_url"],
            "附件名稱列表": detail["attachment_names"],
            "附件ID列表": detail["attachment_ids"],
            "申請資格": detail["qualification_text"],
            "補助內容": detail["benefit_text"],
            "申請方式": detail["apply_text"],
            "應備文件": detail["evidence_text"],
            "備註": detail["remark_text"],
            "內文": detail["content_text"],
        }
        db_row, payload = self._build_db_ready_row(raw_row, detail)
        return raw_row, db_row, payload

    def _map_path_fields(self, path: list[str], group_title: str) -> tuple[str, str, str, str, str, str]:
        first_level = path[0] if len(path) > 0 else ""
        second_level = path[1] if len(path) > 1 else ""
        third_level = path[2] if len(path) > 2 else ""

        if first_level == "縣市福利":
            county_label = second_level
            welfare_category = third_level
            welfare_subcategory = group_title
        else:
            county_label = "全國"
            welfare_category = first_level
            welfare_subcategory = second_level

        return first_level, second_level, third_level, county_label, welfare_category, welfare_subcategory

    def _parse_detail_page(self, policy_url: str) -> dict[str, str]:
        soup = self._parse_soup(policy_url)

        title_node = soup.select_one("font[color='#ea5413']")
        content_div = None
        if title_node:
            title_row = title_node.find_parent("div", class_="row")
            if title_row:
                next_row = title_row.find_next_sibling("div", class_="row")
                if next_row:
                    content_div = next_row.find("div", class_="col-lg-12")

        if content_div is None:
            content_div = soup.select_one("#QADetailWinBodyDiv")

        updated_raw = ""
        updated_ad = ""
        source_name = ""
        source_url = ""
        attachment_names: list[str] = []
        attachment_ids: list[str] = []
        content_text = ""
        qualification_text = ""
        benefit_text = ""
        benefit_html = ""
        apply_text = ""
        apply_html = ""
        evidence_text = ""
        remark_text = ""
        source_section_text = ""
        attachment_html = ""
        attachment_text = ""

        if content_div is not None:
            content_text, updated_raw = self._extract_text_and_updated_time(content_div)
            updated_ad = roc_datetime_to_ad(updated_raw)

            sections = self._extract_sections(content_div)
            if not sections or not any(section.get("text") for section in sections.values()):
                sections = self._extract_sections_from_plain_text(content_text)
            else:
                fallback_sections = self._extract_sections_from_plain_text(content_text)
                for key, section in fallback_sections.items():
                    if key not in sections or not sections[key].get("text"):
                        sections[key] = section

            qualification_text = sections.get("qualification", {}).get("text", "")
            benefit_text = sections.get("benefit", {}).get("text", "")
            benefit_html = sections.get("benefit", {}).get("html", "")
            apply_text = sections.get("apply", {}).get("text", "")
            apply_html = sections.get("apply", {}).get("html", "")
            evidence_text = sections.get("evidence", {}).get("text", "")
            remark_text = sections.get("remark", {}).get("text", "")
            source_section_text = sections.get("source", {}).get("text", "")

            for anchor in content_div.find_all("a", href=True):
                href = anchor.get("href", "").strip()
                label = normalize_space(anchor.get_text(" ", strip=True))
                if not href or not label:
                    continue
                if "downloadFile" in (anchor.get("onclick") or ""):
                    continue
                if not source_name:
                    source_name = label
                    source_url = urljoin(BASE_URL, href)

            for anchor in content_div.find_all("a", onclick=True):
                onclick = anchor.get("onclick", "")
                match = re.search(r'downloadFile\("?(\d+)"?\)', onclick)
                if not match:
                    continue
                attachment_ids.append(match.group(1))
                attachment_names.append(normalize_space(anchor.get_text(" ", strip=True)))

            if not source_name and source_section_text:
                source_name = source_section_text.splitlines()[0]

            attachment_html, attachment_text = self._build_attachment_section(attachment_names, attachment_ids)
            if attachment_html:
                if benefit_html:
                    benefit_html = "\n".join(part for part in [benefit_html, attachment_html] if part)
                elif content_text:
                    benefit_html = "\n".join(
                        part for part in [text_to_html_paragraphs(content_text), attachment_html] if part
                    )
                else:
                    benefit_html = attachment_html

            if attachment_text:
                benefit_text = "\n".join(part for part in [benefit_text, attachment_text] if part)

            if not any([qualification_text, benefit_text, apply_text, evidence_text, remark_text]) and content_text:
                benefit_text = content_text
                benefit_html = text_to_html_paragraphs(content_text)

        return {
            "updated_raw": updated_raw,
            "updated_ad": updated_ad,
            "source_name": source_name,
            "source_url": source_url,
            "source_section_text": source_section_text,
            "attachment_names": " | ".join(name for name in attachment_names if name),
            "attachment_ids": " | ".join(attachment_ids),
            "content_text": content_text,
            "qualification_text": qualification_text,
            "benefit_text": benefit_text,
            "benefit_html": benefit_html,
            "apply_text": apply_text,
            "apply_html": apply_html,
            "evidence_text": evidence_text,
            "remark_text": remark_text,
            "attachment_text": attachment_text,
            "attachment_html": attachment_html,
        }

    def _extract_sections(self, content_div: Tag) -> dict[str, dict[str, str]]:
        sections: dict[str, dict[str, str]] = {}
        html = content_div.decode_contents()
        heading_re = re.compile(
            r"<p[^>]*>\s*(?:&#10148;|&#10146;|➤|►)?\s*(?P<title>[^:<：<]{1,40}(?:/[^:<：<]{1,40})?)\s*[:：]\s*</p>",
            re.IGNORECASE,
        )

        matches: list[tuple[re.Match[str], str]] = []
        for match in heading_re.finditer(html):
            key = SECTION_LOOKUP.get(normalize_lookup_key(match.group("title")))
            if key:
                matches.append((match, key))

        for index, (match, key) in enumerate(matches):
            start = match.end()
            end = matches[index + 1][0].start() if index + 1 < len(matches) else len(html)
            body = html[start:end].strip()
            body = re.sub(r"^\s*</p>\s*", "", body, flags=re.IGNORECASE)
            body = re.sub(r"\s*</p>\s*$", "", body, flags=re.IGNORECASE)
            body = re.sub(r"<span[^>]*>[^<]*資料修改時間[^<]*</span>", "", body, flags=re.IGNORECASE)
            self._flush_section(sections, key, [body])

        return sections

    def _extract_sections_from_plain_text(self, content_text: str) -> dict[str, dict[str, str]]:
        sections: dict[str, dict[str, str]] = {}
        current_key = ""
        current_lines: list[str] = []

        def flush() -> None:
            nonlocal current_key, current_lines
            if not current_key or not current_lines:
                current_key = ""
                current_lines = []
                return
            self._flush_section(sections, current_key, [text_to_html_paragraphs("\n".join(current_lines))])
            current_key = ""
            current_lines = []

        for raw_line in split_non_empty_lines(content_text):
            if raw_line == "附件下載：" or raw_line == "附件下載":
                flush()
                continue

            heading_key, inline_body = self._extract_text_section_heading(raw_line)
            if heading_key:
                flush()
                current_key = heading_key
                if inline_body:
                    current_lines.append(inline_body)
                continue

            if current_key:
                current_lines.append(raw_line)

        flush()
        return sections

    def _extract_section_heading(self, node: Tag | NavigableString) -> str:
        if not isinstance(node, Tag):
            return ""
        text = normalize_space(node.get_text(" ", strip=True))
        if not text or len(text) > 40:
            return ""
        text = text.lstrip("➜►●■◆※")
        text = normalize_group_title(text)
        return SECTION_LOOKUP.get(normalize_lookup_key(text), "")

    def _extract_text_section_heading(self, line: str) -> tuple[str, str]:
        text = normalize_space(line)
        if not text:
            return "", ""

        match = PLAIN_TEXT_HEADING_RE.match(text)
        if not match:
            return "", ""

        title = normalize_space(match.group("title"))
        suffix = normalize_space(match.group("suffix"))
        title = title.lstrip("※＊*").strip()

        candidates = [title]
        if "：" in title:
            candidates.append(normalize_space(title.split("：", 1)[0]))
        if ":" in title:
            candidates.append(normalize_space(title.split(":", 1)[0]))

        for candidate in candidates:
            normalized = normalize_lookup_key(candidate)
            direct = SECTION_LOOKUP.get(normalized)
            if direct:
                inline_body = ""
                if suffix.startswith("：") or suffix.startswith(":"):
                    inline_body = normalize_space(suffix[1:])
                return direct, inline_body

            for alias_key, canonical in SECTION_LOOKUP.items():
                alias_plain = normalize_space(alias_key)
                if normalized.startswith(alias_key):
                    inline_body = normalize_space(candidate[len(alias_plain) :])
                    if inline_body.startswith("：") or inline_body.startswith(":"):
                        inline_body = normalize_space(inline_body[1:])
                    return canonical, inline_body

        return "", ""

    def _flush_section(self, sections: dict[str, dict[str, str]], key: str, fragments: list[str]) -> None:
        if not key:
            return
        html = "".join(fragments).strip()
        text = html_fragment_to_text(html)
        if not html and not text:
            return
        if key in sections:
            sections[key]["html"] = "\n".join(part for part in [sections[key]["html"], html] if part)
            sections[key]["text"] = "\n".join(part for part in [sections[key]["text"], text] if part)
            return
        sections[key] = {"html": html, "text": text}

    def _build_attachment_section(self, attachment_names: list[str], attachment_ids: list[str]) -> tuple[str, str]:
        items = [(name, attachment_id) for name, attachment_id in zip(attachment_names, attachment_ids) if name and attachment_id]
        if not items:
            return "", ""

        html_items = []
        text_items = ["附件下載："]
        for name, attachment_id in items:
            download_url = urljoin(BASE_URL, f"/servlet/KBDataCtrl?func=downloadFile&sId={attachment_id}")
            html_items.append(f'<li><a href="{download_url}" target="_blank" rel="noopener noreferrer">{escape(name)}</a></li>')
            text_items.append(name)

        html = "<p>附件下載：</p><ul>" + "".join(html_items) + "</ul>"
        text = "\n".join(text_items)
        return html, text

    def _extract_text_and_updated_time(self, content_div: Tag) -> tuple[str, str]:
        content_html = str(content_div)
        content_html = re.sub(r"<br\s*/?>", "\n", content_html, flags=re.IGNORECASE)
        text_soup = BeautifulSoup(content_html, "html.parser")
        raw_text = text_soup.get_text("\n", strip=True)

        lines: list[str] = []
        updated_raw = ""
        for line in raw_text.splitlines():
            cleaned = normalize_space(line)
            if not cleaned:
                continue
            if "資料修改時間" in cleaned:
                updated_raw = cleaned
                continue
            lines.append(cleaned)

        return ("\n".join(lines).strip(), updated_raw)

    def _build_db_ready_row(self, raw_row: dict[str, Any], detail: dict[str, str]) -> tuple[dict[str, Any], dict[str, Any]]:
        warnings: list[str] = []
        raw_title = normalize_space(str(raw_row["政策標題"]))
        title = truncate_for_db(raw_title, 50, "Title", warnings)
        title_was_trimmed = title != raw_title

        code_policy_id, code_policy_label = self._lookup_policy_code(str(raw_row["福利分類"]))
        if code_policy_id is None:
            warnings.append(f"找不到 CodePolicyID: {raw_row['福利分類']}")

        code_domicile_id, code_domicile_label = self._lookup_domicile_code(str(raw_row["縣市"]))
        if code_domicile_id is None:
            warnings.append(f"找不到 CodeDomicileID: {raw_row['縣市']}")

        content_blob = "\n".join(
            part
            for part in [
                raw_title,
                detail["qualification_text"],
                detail["benefit_text"],
                detail["apply_text"],
                detail["evidence_text"],
                detail["remark_text"],
                detail["content_text"],
            ]
            if part
        )

        keyword_ids, keyword_labels = self._infer_multi_codes(content_blob, RAW_CODE_KEYWORD_MAP, KEYWORD_PATTERNS, None)
        if not keyword_ids and re.search(r"補助|津貼|補貼|給付|救助", content_blob):
            keyword_ids = [RAW_CODE_KEYWORD_MAP["經濟補助"]]
            keyword_labels = ["經濟補助"]

        recipient_ids, recipient_labels = self._infer_multi_codes(content_blob, RAW_CODE_RECIPIENT_MAP, RECIPIENT_PATTERNS, "全選")
        income_ids, income_labels = self._infer_multi_codes(content_blob, RAW_CODE_INCOME_MAP, INCOME_PATTERNS, "全選")
        identity_ids, identity_labels = self._infer_multi_codes(content_blob, RAW_CODE_IDENTITY_MAP, IDENTITY_PATTERNS, "全選")

        welfare_html_preview = self._compose_welfare_html(detail)
        welfare_info = quote_html_fragment(welfare_html_preview)

        office_unit_info_raw = self._extract_office_unit_info(detail["apply_text"], detail["source_name"])
        office_unit_info = truncate_for_db(office_unit_info_raw, 100, "OfficeUnitInfo", warnings)
        office_unit_tel = truncate_for_db(first_phone("\n".join([detail["apply_text"], detail["content_text"]])), 100, "OfficeUnitTel", warnings)
        competent_authority = truncate_for_db(detail["source_name"], 50, "CompetentAuthority", warnings)
        remark = truncate_for_db(detail["remark_text"], 100, "Remark", warnings)
        office_unit_id = self._infer_office_unit_id(office_unit_info, competent_authority, detail["apply_text"])

        payload: dict[str, Any] = {
            "Title": title,
            "Qualification": detail["qualification_text"],
            "WelfareInfo": welfare_info,
            "Evidence": detail["evidence_text"],
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
            "DiscontinuedTime": DEFAULT_DISCONTINUED_TIME,
            "Remark": remark,
            "IsEnabled": True,
        }

        db_row = {
            "Title": title,
            "Qualification": detail["qualification_text"],
            "WelfareInfo": welfare_info,
            "Evidence": detail["evidence_text"],
            "IFareOfficeUnitID": office_unit_id,
            "OfficeUnitInfo": office_unit_info,
            "OfficeUnitTel": office_unit_tel,
            "CodePolicyID": code_policy_id,
            "CodeDomicileID": code_domicile_id,
            "CodeIndentityIDs": json_compact(identity_ids),
            "CodeIncomeIDs": json_compact(income_ids),
            "CodeRecipientIDs": json_compact(recipient_ids),
            "CodeKeywordIDs": json_compact(keyword_ids),
            "CompetentAuthority": competent_authority,
            "ReleaseTime": raw_row["更新時間"] or "",
            "DiscontinuedTime": DEFAULT_DISCONTINUED_TIME,
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
            "PolicyPayloadJson": json_compact(payload),
        }
        return db_row, payload

    def _lookup_policy_code(self, label: str) -> tuple[int | None, str]:
        key = normalize_lookup_key(label)
        canonical = POLICY_LABEL_ALIASES.get(key, normalize_space(label))
        return CODE_POLICY_LOOKUP.get(normalize_lookup_key(canonical)), canonical

    def _lookup_domicile_code(self, label: str) -> tuple[int | None, str]:
        key = normalize_lookup_key(label)
        canonical = DOMICILE_LABEL_ALIASES.get(key, normalize_space(label))
        return CODE_DOMICILE_LOOKUP.get(normalize_lookup_key(canonical)), canonical

    def _infer_multi_codes(
        self,
        text: str,
        raw_map: dict[str, int],
        pattern_map: dict[str, list[str]],
        default_label: str | None,
    ) -> tuple[list[int], list[str]]:
        labels: list[str] = []
        for label in raw_map:
            if label == "全選":
                continue
            patterns = pattern_map.get(label)
            if not patterns:
                if label == "無":
                    continue
                patterns = [re.escape(label)]
            if any(re.search(pattern, text, re.IGNORECASE) for pattern in patterns):
                labels.append(label)
        if not labels and default_label:
            labels.append(default_label)
        ids = [raw_map[label] for label in labels]
        return ids, labels

    def _compose_welfare_html(self, detail: dict[str, str]) -> str:
        parts: list[str] = []
        if detail["benefit_html"]:
            parts.append(f"<p>補助內容/給付標準：</p>{detail['benefit_html']}")
        if detail["apply_html"]:
            parts.append(f"<p>申請方式/流程/期限：</p>{detail['apply_html']}")
        if not parts and detail["benefit_text"]:
            parts.append(text_to_html_paragraphs(detail["benefit_text"]))
        if not parts and detail["apply_text"]:
            parts.append(text_to_html_paragraphs(detail["apply_text"]))
        return "".join(parts).strip()

    def _extract_office_unit_info(self, apply_text: str, source_name: str) -> str:
        if apply_text:
            one_line = " ".join(split_non_empty_lines(apply_text))
            patterns = [
                r"向(?P<value>[^。；，]{2,50}?公所)提出申請",
                r"向(?P<value>[^。；，]{2,80}?)提出申請",
                r"於(?P<value>[^。；，]{2,80}?)提出申請",
                r"至(?P<value>[^。；，]{2,80}?)申請",
                r"向(?P<value>[^。；，]{2,80}?)辦理",
                r"洽(?P<value>[^。；，]{2,80}?)辦理",
            ]
            for pattern in patterns:
                match = re.search(pattern, one_line)
                if match:
                    return normalize_space(match.group("value"))
            lines = split_non_empty_lines(apply_text)
            if lines:
                return re.sub(r"^[（(]?[一二三四五六七八九十\d]+[）)]", "", lines[0]).strip()
        return normalize_space(source_name)

    def _infer_office_unit_id(self, office_unit_info: str, competent_authority: str, apply_text: str) -> int:
        text = "\n".join(part for part in [office_unit_info, competent_authority, apply_text] if part)
        if "輔具中心" in text:
            return 3
        if "戶政" in text:
            return 4
        if "社會安全網" in text:
            return 5
        if "公所" in text:
            return 2
        return DEFAULT_OFFICE_UNIT_ID


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="爬取衛福部 1957 縣市福利與全國福利政策資料，並輸出 IFare DB-ready 格式")
    parser.add_argument(
        "--output",
        type=Path,
        default=build_default_output(),
        help="輸出的 Excel 路徑，預設為桌面時間戳檔名",
    )
    parser.add_argument(
        "--db-json-output",
        type=Path,
        default=None,
        help="另存一份可直接餵 API/後續匯入程式的 JSON payload",
    )
    parser.add_argument(
        "--root-cate-id",
        default=DEFAULT_ROOT_CATE_ID,
        help="起始分類 cateId，預設為 1（首頁全部）。可指定 2 只抓縣市福利，或 3/4... 測單一分支",
    )
    parser.add_argument(
        "--county",
        action="append",
        default=[],
        help="只抓指定縣市，可重複指定，例如 --county 新北市 --county 臺北市",
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
        raw_df = crawler.crawl()
    except Exception as exc:
        print(f"爬取失敗：{exc}", file=sys.stderr)
        return 1

    db_df = crawler.db_ready_dataframe()
    raw_df = raw_df.map(sanitize_for_excel)
    db_df = db_df.map(sanitize_for_excel)

    ensure_parent_dir(args.output)
    with pd.ExcelWriter(args.output) as writer:
        raw_df.to_excel(writer, index=False, sheet_name="raw_1957")
        db_df.to_excel(writer, index=False, sheet_name="db_ifare_policy")

    if args.db_json_output is not None:
        ensure_parent_dir(args.db_json_output)
        args.db_json_output.write_text(json.dumps(crawler.db_payloads, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"完成，共 {len(raw_df)} 筆。Excel 已輸出到：{args.output}")
    if args.db_json_output is not None:
        print(f"DB-ready JSON 已輸出到：{args.db_json_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
