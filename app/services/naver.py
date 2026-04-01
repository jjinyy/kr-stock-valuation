from __future__ import annotations

import re
import os
from time import time
from threading import Lock, local
from typing import Optional

import httpx
from bs4 import BeautifulSoup


_RE_INT = re.compile(r"[0-9]+")
_RE_PRICE_BLIND = re.compile(r"<p[^>]*class=['\"]no_today['\"][^>]*>.*?<span[^>]*class=['\"]blind['\"][^>]*>([^<]+)</span>", re.IGNORECASE | re.DOTALL)
_DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://finance.naver.com/",
}
_tls = local()


def _get_client() -> httpx.Client:
    """
    httpx.Client는 스레드 안전을 보장하지 않으므로, bulk(스레드풀)에서 병렬 호출 시
    스레드별로 Client를 분리해 keep-alive 효율을 유지합니다.
    """
    cli = getattr(_tls, "client", None)
    if cli is None:
        cli = httpx.Client(
            timeout=httpx.Timeout(8.0, connect=3.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=30, max_keepalive_connections=15),
        )
        _tls.client = cli
    return cli

_cache_lock = Lock()
_cache: dict[str, tuple[float, Optional[int]]] = {}


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


def _cache_ttl_s() -> int:
    raw = os.getenv("NAVER_PRICE_CACHE_TTL", "").strip()
    if not raw:
        return 60  # 1분: 너무 오래 캐시하면 장중 가격이 굳을 수 있음
    try:
        return max(0, min(int(raw), 3600))
    except ValueError:
        return 60


def _cache_get(ticker: str) -> Optional[int] | object:
    ttl = _cache_ttl_s()
    if ttl <= 0:
        return object()
    now = time()
    with _cache_lock:
        hit = _cache.get(ticker)
        if not hit:
            return object()
        ts, val = hit
        if now - ts > ttl:
            _cache.pop(ticker, None)
            return object()
        return val


def _cache_set(ticker: str, val: Optional[int]) -> None:
    ttl = _cache_ttl_s()
    if ttl <= 0:
        return
    with _cache_lock:
        _cache[ticker] = (time(), val)


def fetch_current_price(*, ticker: str, timeout_s: int = 8) -> Optional[int]:
    """
    네이버(구 finance.naver.com) 종목 메인에서 현재가 파싱.
    - selector: p.no_today span.blind (현재가)
    """
    ticker = ticker.zfill(6)
    cached = _cache_get(ticker)
    if cached is not object():
        return cached  # type: ignore[return-value]
    url = f"https://finance.naver.com/item/main.nhn?code={ticker}"
    # 1회 재시도
    last_err: Exception | None = None
    for _ in range(2):
        try:
            resp = _get_client().get(url, headers=_DEFAULT_HEADERS, timeout=timeout_s)
            resp.raise_for_status()
            # 빠른 파싱(정규식) 우선, 실패하면 BeautifulSoup fallback
            m = _RE_PRICE_BLIND.search(resp.text or "")
            if m:
                val = _to_int(m.group(1))
                _cache_set(ticker, val)
                return val

            soup = BeautifulSoup(resp.text, "lxml")
            node = soup.select_one("p.no_today span.blind")
            if not node:
                _cache_set(ticker, None)
                return None
            val = _to_int(node.get_text(strip=True))
            _cache_set(ticker, val)
            return val
        except httpx.HTTPError as e:
            last_err = e
    if last_err:
        raise last_err
    return None

