from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

import httpx
from bs4 import BeautifulSoup


@dataclass(frozen=True)
class Consensus26Y:
    pbr: Optional[float]
    per: Optional[float]
    eps: Optional[float]


_RE_NUM = re.compile(r"[-+]?\d+(?:,\d+)*(?:\.\d+)?")
_RE_YEAR = re.compile(r"\b(\d{4}/\d{2}(?:\([A-Z]\))?)\b")


def _to_float(text: str) -> Optional[float]:
    if not text:
        return None
    m = _RE_NUM.search(text.replace(" ", ""))
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", ""))
    except ValueError:
        return None


def _extract_year_token(cell_text: str) -> str:
    """
    헤더에 '(E) : ... 2026/12(E)' 같은 설명이 섞이므로, 연도 토큰만 뽑습니다.
    """
    cell_text = (cell_text or "").strip()
    # 우선 2026/12(E) 패턴을 찾고, 없으면 raw 반환
    m = re.search(r"\d{4}/\d{2}\(E\)", cell_text)
    if m:
        return m.group(0)
    m2 = re.search(r"\d{4}/\d{2}", cell_text)
    if m2:
        return m2.group(0)
    return cell_text


def fetch_fnguide_main_html(*, ticker: str, timeout_s: int = 30) -> str:
    ticker = ticker.zfill(6)
    # gicode: A + 6자리
    url = f"https://comp.fnguide.com/SVO2/ASP/SVD_main.asp?pGB=1&gicode=A{ticker}&MenuYn=Y&NewMenuID=11"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://comp.fnguide.com/",
    }
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        resp = client.get(url, headers=headers)
        resp.raise_for_status()
        return resp.text


def parse_consensus_26y_from_main(html: str) -> Consensus26Y:
    soup = BeautifulSoup(html, "lxml")

    target_tbl = None
    for t in soup.find_all("table"):
        txt = t.get_text(" ", strip=True)
        if "Financial Highlight" in txt and "2026/12" in txt and "EPS(원)" in txt and "PER(배)" in txt and "PBR(배)" in txt:
            target_tbl = t
            break

    if not target_tbl:
        return Consensus26Y(pbr=None, per=None, eps=None)

    # 헤더(연도) 추출: TR 1이 연도들
    header_years: list[str] = []
    header_tr = None
    trs = target_tbl.find_all("tr")
    if len(trs) >= 2:
        header_tr = trs[1]
    if header_tr:
        ths = header_tr.find_all("th")
        header_years = [_extract_year_token(th.get_text(" ", strip=True)) for th in ths]

    # rows는 첫 cell이 라벨, 이후가 값. header_years는 라벨 제외하고 값열과 1:1 매칭
    # 실제 table row 구조: [라벨(th), 값(td)*N]
    year_to_idx: dict[str, int] = {}
    for i, y in enumerate(header_years):
        year_to_idx[y] = i  # 값 셀 인덱스

    # 2026년 목표 컬럼 찾기
    target_year = None
    for y in year_to_idx.keys():
        if y.startswith("2026/12"):
            target_year = y
            break
    if not target_year:
        return Consensus26Y(pbr=None, per=None, eps=None)

    target_i = year_to_idx[target_year]

    def pick_row(label_prefix: str) -> Optional[float]:
        for tr in target_tbl.find_all("tr"):
            th = tr.find("th")
            if not th:
                continue
            label = th.get_text(" ", strip=True)
            if not label.startswith(label_prefix):
                continue
            tds = tr.find_all("td")
            if target_i < 0 or target_i >= len(tds):
                return None
            return _to_float(tds[target_i].get_text(" ", strip=True))
        return None

    eps = pick_row("EPS(원)")
    per = pick_row("PER(배)")
    pbr = pick_row("PBR(배)")
    return Consensus26Y(pbr=pbr, per=per, eps=eps)


def fetch_consensus_26y(*, ticker: str) -> Consensus26Y:
    html = fetch_fnguide_main_html(ticker=ticker)
    return parse_consensus_26y_from_main(html)

