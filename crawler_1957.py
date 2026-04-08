from __future__ import annotations

import argparse
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, urljoin, urlparse

import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup, NavigableString, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_URL = "https://1957.mohw.gov.tw"
DEFAULT_ROOT_CATE_ID = "1"
DEFAULT_TIMEOUT = 30
ILLEGAL_EXCEL_CHAR_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")

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
        self.rows: list[dict[str, str]] = []
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

        # Prefer the server-declared charset when present. A few detail pages
        # advertise Big5 correctly but chardet guesses an incorrect Latin
        # encoding, which turns the content into mojibake.
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
                    "內文",
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
                row = self._build_row_from_listing(anchor, path, group_title, cate_id, list_url)
                self.rows.append(row)
                print(f"  - {row['政策標題']}")

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

    def _build_row_from_listing(
        self,
        anchor: Tag,
        path: list[str],
        group_title: str,
        cate_id: str,
        list_url: str,
    ) -> dict[str, str]:
        policy_title = normalize_space(anchor.get_text(" ", strip=True))
        policy_url = urljoin(BASE_URL, anchor.get("href", ""))
        sid = parse_qs(urlparse(policy_url).query).get("sid", [""])[0]

        detail = self._parse_detail_page(policy_url)
        first_level, second_level, third_level, county_label, welfare_category, welfare_subcategory = self._map_path_fields(
            path, group_title
        )

        return {
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
            "內文": detail["content_text"],
        }

    def _map_path_fields(self, path: list[str], group_title: str) -> tuple[str, str, str, str, str, str]:
        first_level = path[0] if len(path) > 0 else ""
        second_level = path[1] if len(path) > 1 else ""
        third_level = path[2] if len(path) > 2 else ""

        if first_level == "縣市福利":
            county_label = second_level
            welfare_category = third_level
            welfare_subcategory = group_title
        else:
            county_label = "全國福利"
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

        if content_div is not None:
            content_text, updated_raw = self._extract_text_and_updated_time(content_div)
            updated_ad = roc_datetime_to_ad(updated_raw)

            for anchor in content_div.find_all("a", href=True):
                href = anchor.get("href", "").strip()
                label = normalize_space(anchor.get_text(" ", strip=True))
                if not href:
                    continue
                if not source_name and "資料來源" in content_text and label and "downloadFile" not in (anchor.get("onclick") or ""):
                    source_name = label
                    source_url = urljoin(BASE_URL, href)

            for anchor in content_div.find_all("a", onclick=True):
                onclick = anchor.get("onclick", "")
                match = re.search(r"downloadFile\(\"?(\d+)\"?\)", onclick)
                if not match:
                    continue
                attachment_ids.append(match.group(1))
                attachment_names.append(normalize_space(anchor.get_text(" ", strip=True)))

        return {
            "updated_raw": updated_raw,
            "updated_ad": updated_ad,
            "source_name": source_name,
            "source_url": source_url,
            "attachment_names": " | ".join([name for name in attachment_names if name]),
            "attachment_ids": " | ".join(attachment_ids),
            "content_text": content_text,
        }

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


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="爬取衛福部 1957 縣市福利與全國福利政策資料")
    parser.add_argument(
        "--output",
        type=Path,
        default=build_default_output(),
        help="輸出的 Excel 路徑，預設為桌面時間戳檔名",
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
        df = crawler.crawl()
    except Exception as exc:
        print(f"爬取失敗：{exc}", file=sys.stderr)
        return 1

    df = df.map(sanitize_for_excel)
    ensure_parent_dir(args.output)
    df.to_excel(args.output, index=False)
    print(f"完成，共 {len(df)} 筆，已輸出到：{args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
