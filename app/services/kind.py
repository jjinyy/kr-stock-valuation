from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

import httpx
from bs4 import BeautifulSoup


@dataclass(frozen=True)
class KindCompanyRow:
    ticker: str
    name: str
    market: str | None


def _market_normalize(raw: str) -> str | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    # KIND "유가/코스닥/코넥스" 등 표기
    if raw in {"유가", "유가증권"}:
        return "KOSPI"
    if raw in {"코스닥"}:
        return "KOSDAQ"
    if raw in {"코넥스"}:
        return "KONEX"
    return raw


def download_corp_list_html(*, market_type: str, timeout_s: int = 60) -> str:
    """
    KIND 상장법인목록의 EXCEL 다운로드는 실제로 HTML을 'application/vnd.ms-excel'로 내려줍니다.
    """
    url = "https://kind.krx.co.kr/corpgeneral/corpList.do"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://kind.krx.co.kr/corpgeneral/corpList.do?method=loadInitPage",
    }
    data = {
        "method": "download",
        "pageIndex": "1",
        "currentPageSize": "3000",
        # corpList는 '전체' 멀티선택이 아니라 단일 시장 선택입니다.
        "marketType": market_type,
    }

    # EUC-KR charset
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        resp = client.post(url, data=data, headers=headers)
        resp.raise_for_status()
        resp.encoding = "euc-kr"
        return resp.text


def parse_corp_list(html: str) -> list[KindCompanyRow]:
    soup = BeautifulSoup(html, "lxml")
    rows: list[KindCompanyRow] = []

    # 헤더가 있는 본문 테이블은 보통 가장 큰 표(상장법인 목록)입니다.
    tables = soup.find_all("table")
    if not tables:
        return rows

    best = max(tables, key=lambda t: len(t.find_all("tr")))
    for tr in best.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue

        name = tds[0].get_text(strip=True)
        market_raw = tds[1].get_text(strip=True)
        ticker = tds[2].get_text(strip=True)

        if not ticker.isdigit():
            continue
        ticker = ticker.zfill(6)
        if not name:
            continue

        rows.append(
            KindCompanyRow(
                ticker=ticker,
                name=name,
                market=_market_normalize(market_raw),
            )
        )

    # 중복 제거(간혹 반복될 수 있음)
    dedup: dict[str, KindCompanyRow] = {}
    for r in rows:
        dedup[r.ticker] = r
    return list(dedup.values())


def fetch_kind_companies() -> tuple[list[KindCompanyRow], str]:
    markets = ["stockMkt", "kosdaqMkt", "konexMkt"]
    dedup: dict[str, KindCompanyRow] = {}
    for m in markets:
        html = download_corp_list_html(market_type=m)
        for r in parse_corp_list(html):
            dedup[r.ticker] = r
    rows = list(dedup.values())
    asof = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return rows, asof

