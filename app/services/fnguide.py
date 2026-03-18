from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Optional

import httpx
from bs4 import BeautifulSoup


@dataclass(frozen=True)
class Consensus26Y:
    pbr: Optional[float]
    per: Optional[float]
    eps: Optional[float]

@dataclass(frozen=True)
class MainInfo:
    consensus_years: dict[int, Consensus26Y]
    category_l: Optional[str]
    category_m: Optional[str]


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


def _norm_space(s: str) -> str:
    return " ".join((s or "").replace("\xa0", " ").split()).strip()


def parse_categories_from_main(html: str) -> tuple[Optional[str], Optional[str]]:
    """
    FnGuide 헤더에서 분류 문자열을 뽑습니다.
    - category_l: 시장/업종(예: "코스피 전기·전자")
    - category_m: FICS(예: "FICS 반도체 및 관련장비")
    """
    soup = BeautifulSoup(html, "lxml")
    grp = soup.select_one("p.stxt_group")
    if not grp:
        return (None, None)
    st1 = grp.select_one("span.stxt.stxt1")
    st2 = grp.select_one("span.stxt.stxt2")
    cat_l = _norm_space(st1.get_text(" ", strip=True) if st1 else "")
    cat_m = _norm_space(st2.get_text(" ", strip=True) if st2 else "")

    # stxt1 예: "KSE  코스피 전기·전자" -> 앞의 "KSE" 제거
    if cat_l.upper().startswith("KSE "):
        cat_l = _norm_space(cat_l[3:])
    if not cat_l:
        cat_l = None
    if not cat_m:
        cat_m = None
    return (cat_l, cat_m)


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


def parse_consensus_years_from_main(html: str) -> dict[int, Consensus26Y]:
    """
    FnGuide Financial Highlight에서 여러 연도(예: 2026/12(E), 2027/12(E)...)의
    EPS/PER/PBR 값을 한 번에 추출합니다.
    """
    soup = BeautifulSoup(html, "lxml")

    # 테이블 후보 중 "추정치(…/12(E))" 컬럼이 있는 Financial Highlight를 우선 선택
    now_y = date.today().year
    best_tbl = None
    best_score = -1
    for t in soup.find_all("table"):
        txt = t.get_text(" ", strip=True)
        if "Financial Highlight" not in txt:
            continue
        if not ("EPS(원)" in txt and "PER(배)" in txt and "PBR(배)" in txt):
            continue

        # (E) 추정치 연도 컬럼이 많을수록 점수↑, 최소한 당년/내년 추정치가 있으면 가산
        e_years = re.findall(r"\b20\d{2}/12\(E\)\b", txt)
        score = len(set(e_years)) * 10
        if f"{now_y}/12" in txt:
            score += 3
        if f"{now_y + 1}/12" in txt:
            score += 2
        if "(E)" in txt:
            score += 1

        if score > best_score:
            best_score = score
            best_tbl = t

    target_tbl = best_tbl
    if not target_tbl:
        return {}

    header_years: list[str] = []
    trs = target_tbl.find_all("tr")
    header_tr = trs[1] if len(trs) >= 2 else None
    if header_tr:
        ths = header_tr.find_all("th")
        header_years = [_extract_year_token(th.get_text(" ", strip=True)) for th in ths]

    year_to_idx: dict[int, int] = {}
    for i, y in enumerate(header_years):
        m = re.search(r"(\d{4})/12", y)
        if not m:
            continue
        year_to_idx[int(m.group(1))] = i

    if not year_to_idx:
        return {}

    def pick_row(label_prefix: str, *, idx: int) -> Optional[float]:
        for tr in target_tbl.find_all("tr"):
            th = tr.find("th")
            if not th:
                continue
            label = th.get_text(" ", strip=True)
            if not label.startswith(label_prefix):
                continue
            tds = tr.find_all("td")
            if idx < 0 or idx >= len(tds):
                return None
            return _to_float(tds[idx].get_text(" ", strip=True))
        return None

    out: dict[int, Consensus26Y] = {}
    for year, idx in sorted(year_to_idx.items()):
        eps = pick_row("EPS(원)", idx=idx)
        per = pick_row("PER(배)", idx=idx)
        pbr = pick_row("PBR(배)", idx=idx)
        out[year] = Consensus26Y(pbr=pbr, per=per, eps=eps)
    return out


def fetch_consensus_years(*, ticker: str) -> dict[int, Consensus26Y]:
    html = fetch_fnguide_main_html(ticker=ticker)
    return parse_consensus_years_from_main(html)


def fetch_main_info(*, ticker: str) -> MainInfo:
    html = fetch_fnguide_main_html(ticker=ticker)
    consensus_years = parse_consensus_years_from_main(html)
    cat_l, cat_m = parse_categories_from_main(html)
    return MainInfo(consensus_years=consensus_years, category_l=cat_l, category_m=cat_m)

