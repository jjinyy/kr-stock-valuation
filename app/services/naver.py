from __future__ import annotations

import re
from typing import Optional

import httpx
from bs4 import BeautifulSoup


_RE_INT = re.compile(r"[0-9]+")


def _to_int(text: str) -> Optional[int]:
    if not text:
        return None
    digits = "".join(_RE_INT.findall(text))
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def fetch_current_price(*, ticker: str, timeout_s: int = 30) -> Optional[int]:
    """
    네이버(구 finance.naver.com) 종목 메인에서 현재가 파싱.
    - selector: p.no_today span.blind (현재가)
    """
    ticker = ticker.zfill(6)
    url = f"https://finance.naver.com/item/main.nhn?code={ticker}"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://finance.naver.com/",
    }

    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        resp = client.get(url, headers=headers)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        node = soup.select_one("p.no_today span.blind")
        if not node:
            return None
        return _to_int(node.get_text(strip=True))

