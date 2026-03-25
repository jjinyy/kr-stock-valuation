from __future__ import annotations

import re
from typing import Optional

import httpx
from bs4 import BeautifulSoup


_RE_INT = re.compile(r"[0-9]+")
_DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://finance.naver.com/",
}
# 요청마다 Client를 만들지 않도록 재사용
_CLIENT = httpx.Client(
    timeout=httpx.Timeout(8.0, connect=3.0),
    follow_redirects=True,
    limits=httpx.Limits(max_connections=30, max_keepalive_connections=15),
)


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


def fetch_current_price(*, ticker: str, timeout_s: int = 8) -> Optional[int]:
    """
    네이버(구 finance.naver.com) 종목 메인에서 현재가 파싱.
    - selector: p.no_today span.blind (현재가)
    """
    ticker = ticker.zfill(6)
    url = f"https://finance.naver.com/item/main.nhn?code={ticker}"
    # 1회 재시도
    last_err: Exception | None = None
    for _ in range(2):
        try:
            resp = _CLIENT.get(url, headers=_DEFAULT_HEADERS, timeout=timeout_s)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")
            node = soup.select_one("p.no_today span.blind")
            if not node:
                return None
            return _to_int(node.get_text(strip=True))
        except httpx.HTTPError as e:
            last_err = e
    if last_err:
        raise last_err
    return None

