from __future__ import annotations

from datetime import datetime
import re
from typing import Optional

import httpx
from bs4 import BeautifulSoup


POSITIVE_WORDS = (
    "상승",
    "급등",
    "호재",
    "흑자",
    "성장",
    "신고가",
    "강세",
    "수혜",
    "개선",
)

NEGATIVE_WORDS = (
    "하락",
    "급락",
    "악재",
    "적자",
    "약세",
    "우려",
    "리스크",
    "쇼크",
    "부진",
)

KEYWORD_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("실적", ("실적", "매출", "영업이익", "순이익", "흑자", "적자", "어닝", "가이던스")),
    ("수주/계약", ("수주", "계약", "공급", "납품", "체결", "공급계약")),
    ("정책/규제", ("정부", "정책", "규제", "법안", "승인", "허가", "제재", "관세")),
    ("M&A/투자", ("인수", "합병", "M&A", "지분", "투자유치", "증자", "감자", "매각")),
    ("신제품/기술", ("출시", "신제품", "기술", "특허", "AI", "반도체", "플랫폼", "개발")),
    ("수급/주주", ("자사주", "배당", "공매도", "외국인", "기관", "리밸런싱", "지수편입")),
    ("리스크", ("소송", "리콜", "사고", "중단", "부진", "우려", "쇼크", "리스크")),
    ("환율/거시", ("환율", "금리", "인플레이션", "침체", "유가", "경기", "지표")),
]

_RE_CLEAN = re.compile(r"[^0-9A-Za-z가-힣\s]")
_STOPWORDS = {
    "오늘",
    "속보",
    "단독",
    "특징주",
    "관련주",
    "기자",
    "시장",
    "투자",
    "증권",
    "뉴스",
}


def classify_sentiment(title: str) -> str:
    t = (title or "").strip()
    if any(k in t for k in POSITIVE_WORDS):
        return "긍정"
    if any(k in t for k in NEGATIVE_WORDS):
        return "부정"
    return "중립"


def extract_keyword(*, title: str, content: str = "") -> str:
    text = f"{title} {content}".strip()
    if not text:
        return "기타"
    for keyword, pats in KEYWORD_RULES:
        if any(p in text for p in pats):
            return keyword
    return "기타"


def _normalize_date(text: str) -> str:
    s = (text or "").strip()
    if not s:
        return "-"
    # 예: 2026.03.23 09:12
    try:
        dt = datetime.strptime(s, "%Y.%m.%d %H:%M")
        return dt.strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return s


def _normalize_title(text: str) -> str:
    s = (text or "").strip().lower()
    s = _RE_CLEAN.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _tokenize(text: str) -> set[str]:
    toks = [t for t in _normalize_title(text).split(" ") if len(t) >= 2 and t not in _STOPWORDS]
    return set(toks)


def _is_similar_title(a: str, b: str) -> bool:
    na = _normalize_title(a)
    nb = _normalize_title(b)
    if not na or not nb:
        return False
    if na == nb:
        return True
    ta = _tokenize(na)
    tb = _tokenize(nb)
    if not ta or not tb:
        return False
    inter = len(ta & tb)
    union = len(ta | tb)
    score = inter / union if union else 0.0
    return score >= 0.55


def fetch_company_news(*, ticker: str, limit: int = 10, timeout_s: int = 20) -> list[dict]:
    """
    네이버 금융 종목 뉴스에서 최신 기사를 가져옵니다.
    """
    t = ticker.zfill(6)
    url = f"https://finance.naver.com/item/news_news.naver?code={t}&page=1&sm=title_entity_id.basic&clusterId="
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://finance.naver.com/",
    }

    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        resp = client.get(url, headers=headers)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

    out: list[dict] = []
    for tr in soup.select("table.type5 tr"):
        a = tr.select_one("td.title a")
        if not a:
            continue
        title = a.get_text(" ", strip=True)
        href = a.get("href", "").strip()
        press = (tr.select_one("td.info") or {}).get_text(strip=True) if tr.select_one("td.info") else "-"
        date_text = (tr.select_one("td.date") or {}).get_text(strip=True) if tr.select_one("td.date") else "-"

        if href.startswith("/"):
            link = f"https://finance.naver.com{href}"
        elif href.startswith("http://") or href.startswith("https://"):
            link = href
        else:
            link = "-"

        out.append(
            {
                "title": title,
                "link": link,
                "press": press,
                "date": _normalize_date(date_text),
                "sentiment": classify_sentiment(title),
                "keyword": extract_keyword(title=title),
            }
        )
        if len(out) >= max(1, int(limit)):
            break
    return out


def group_similar_news(items: list[dict]) -> list[dict]:
    """
    유사 제목을 묶어서 중복을 줄입니다.
    """
    groups: list[dict] = []
    for n in items:
        title = n.get("title") or ""
        keyword = n.get("keyword") or "기타"
        matched = None
        for g in groups:
            if g["keyword"] != keyword:
                continue
            if _is_similar_title(title, g["title"]):
                matched = g
                break
        if matched is None:
            groups.append(
                {
                    "title": title,
                    "link": n.get("link") or "-",
                    "presses": {n.get("press") or "-"},
                    "date": n.get("date") or "-",
                    "sentiment": n.get("sentiment") or "중립",
                    "keyword": keyword,
                    "count": 1,
                }
            )
        else:
            matched["count"] += 1
            matched["presses"].add(n.get("press") or "-")

    out: list[dict] = []
    for g in groups:
        count = int(g["count"])
        presses = sorted(g["presses"])
        press = presses[0] if len(presses) == 1 else f"{presses[0]} 외 {len(presses)-1}"
        title = g["title"]
        if count > 1:
            title = f"{title} ({count})"
        out.append(
            {
                "title": title,
                "link": g["link"],
                "press": press,
                "date": g["date"],
                "sentiment": g["sentiment"],
                "keyword": g["keyword"],
                "count": count,
            }
        )
    return out
