from __future__ import annotations

from datetime import date, datetime

import json
import httpx
from sqlmodel import select

from app.db import get_session, init_db
from app.models import Company, Snapshot
from app.services.fnguide import fetch_main_info
from app.services.kind import fetch_kind_companies
from app.services.naver import fetch_current_price


def _latest_snapshot_today(session, *, ticker: str, today: str) -> Snapshot | None:
    return session.exec(
        select(Snapshot)
        .where(Snapshot.ticker == ticker)
        .where(Snapshot.asof == today)
        .order_by(Snapshot.created_at.desc())
        .limit(1)
    ).first()


def _get_or_create_today_snapshot(session, *, ticker: str, today: str) -> Snapshot:
    snap = _latest_snapshot_today(session, ticker=ticker, today=today)
    if snap:
        return snap
    snap = Snapshot(ticker=ticker, asof=today)
    session.add(snap)
    return snap


def refresh_companies_from_kind() -> dict:
    init_db()
    rows, asof = fetch_kind_companies()

    upserts = 0
    with get_session() as session:
        for r in rows:
            existing = session.get(Company, r.ticker)
            if existing:
                changed = False
                if existing.name != r.name:
                    existing.name = r.name
                    changed = True
                if existing.market != r.market:
                    existing.market = r.market
                    changed = True
                if getattr(r, "category_l", None) and existing.category_l != r.category_l:
                    existing.category_l = r.category_l
                    changed = True
                if getattr(r, "category_m", None) and existing.category_m != r.category_m:
                    existing.category_m = r.category_m
                    changed = True
                if changed:
                    existing.updated_at = datetime.utcnow()
                    upserts += 1
            else:
                session.add(
                    Company(
                        ticker=r.ticker,
                        name=r.name,
                        market=r.market,
                        category_l=getattr(r, "category_l", None),
                        category_m=getattr(r, "category_m", None),
                    )
                )
                upserts += 1
        session.commit()

    return {"asof": asof, "count": len(rows), "upserts": upserts}


def refresh_snapshot_for_ticker(*, ticker: str, ensure_init: bool = True) -> dict:
    if ensure_init:
        init_db()
    ticker = ticker.zfill(6)
    today = date.today().isoformat()
    primary_year = date.today().year

    current_price = None
    consensus_years: dict[int, object] | None = None
    errors: list[str] = []

    try:
        current_price = fetch_current_price(ticker=ticker)
    except httpx.HTTPError as e:
        errors.append(f"naver: {type(e).__name__}")

    try:
        info = fetch_main_info(ticker=ticker)
        consensus_years = info.consensus_years
    except httpx.HTTPError as e:
        errors.append(f"fnguide: {type(e).__name__}")
        # 실패하더라도 "오늘은 시도했음"을 표시해 중복 호출을 줄인다.
        consensus_years = {}

    def to_json(d: dict[int, object] | None) -> str | None:
        # d=None: FnGuide 호출 자체가 실패/미시도
        # d={} : 호출은 됐지만(파싱 결과) 컨센서스가 없을 수 있음 -> "{}"로 저장해서 "오늘 조회됨"을 표시
        if d is None:
            return None
        if not d:
            return "{}"
        # JSON에서는 key가 string이 되므로, year를 문자열로 저장
        payload: dict[str, dict[str, float | None]] = {}
        for y, c in d.items():
            payload[str(int(y))] = {
                "pbr": getattr(c, "pbr", None),
                "per": getattr(c, "per", None),
                "eps": getattr(c, "eps", None),
            }
        return json.dumps(payload, ensure_ascii=False)

    primary = (consensus_years or {}).get(primary_year) if consensus_years else None
    consensus_json = to_json(consensus_years)
    consensus_payload = json.loads(consensus_json) if consensus_json else None

    with get_session() as session:
        snap = _get_or_create_today_snapshot(session, ticker=ticker, today=today)
        snap.current_price = current_price
        # 기존 컬럼은 "현재 선택 기본값(=올해)"로 채워서 UI/로직 호환
        snap.pbr_26y = getattr(primary, "pbr", None) if primary else None
        snap.per_26y = getattr(primary, "per", None) if primary else None
        snap.eps_26y = getattr(primary, "eps", None) if primary else None
        snap.consensus_json = consensus_json
        snap.consensus_primary_year = primary_year
        session.commit()

        # company may not exist yet; keep it as-is

    return {
        "ticker": ticker,
        "asof": today,
        "current_price": current_price,
        "pbr_26y": getattr(primary, "pbr", None) if primary else None,
        "per_26y": getattr(primary, "per", None) if primary else None,
        "eps_26y": getattr(primary, "eps", None) if primary else None,
        "consensus_years": consensus_payload,
        "consensus_primary_year": primary_year,
        "errors": errors,
    }


def refresh_price_for_ticker(*, ticker: str, ensure_init: bool = True) -> dict:
    """
    네이버 현재가만 갱신합니다. (컨센서스는 기존 저장값을 재사용)
    """
    if ensure_init:
        init_db()
    ticker = ticker.zfill(6)
    today = date.today().isoformat()

    errors: list[str] = []
    current_price = None
    try:
        current_price = fetch_current_price(ticker=ticker)
    except httpx.HTTPError as e:
        errors.append(f"naver: {type(e).__name__}")

    # 기존 컨센서스는 최신 스냅샷에서 복사
    consensus_json = None
    consensus_primary_year = None
    pbr = per = eps = None
    with get_session() as session:
        prev = _latest_snapshot_today(session, ticker=ticker, today=today)
        if prev:
            consensus_json = prev.consensus_json
            consensus_primary_year = prev.consensus_primary_year
            pbr = prev.pbr_26y
            per = prev.per_26y
            eps = prev.eps_26y

        snap = _get_or_create_today_snapshot(session, ticker=ticker, today=today)
        snap.current_price = current_price
        snap.pbr_26y = pbr
        snap.per_26y = per
        snap.eps_26y = eps
        snap.consensus_json = consensus_json
        snap.consensus_primary_year = consensus_primary_year
        session.commit()

    return {
        "ticker": ticker,
        "asof": today,
        "current_price": current_price,
        "errors": errors,
    }


def refresh_consensus_for_ticker(
    *,
    ticker: str,
    primary_year: int | None = None,
    ensure_init: bool = True,
    skip_if_already_today: bool = False,
) -> dict:
    """
    FnGuide 컨센서스만 갱신합니다. (현재가는 기존 저장값을 재사용)
    """
    if ensure_init:
        init_db()
    ticker = ticker.zfill(6)
    today = date.today().isoformat()
    if primary_year is None:
        primary_year = date.today().year

    # 단건 액션에서 "일 1회면 충분" 정책을 원할 때, 오늘 조회된 컨센서스는 재조회 생략
    with get_session() as session:
        prev = _latest_snapshot_today(session, ticker=ticker, today=today)
        if skip_if_already_today and prev and (
            prev.consensus_json
            or prev.pbr_26y is not None
            or prev.per_26y is not None
            or prev.eps_26y is not None
        ):
            payload = None
            if prev.consensus_json:
                try:
                    raw = json.loads(prev.consensus_json)
                    if isinstance(raw, dict):
                        payload = raw
                except Exception:
                    payload = None
            return {
                "ticker": ticker,
                "asof": today,
                "current_price": prev.current_price,
                "consensus_years": payload,
                "consensus_primary_year": int(primary_year),
                "errors": [],
                "skipped": True,
            }

    errors: list[str] = []
    consensus_years: dict[int, object] | None = None

    try:
        info = fetch_main_info(ticker=ticker)
        consensus_years = info.consensus_years
    except httpx.HTTPError as e:
        errors.append(f"fnguide: {type(e).__name__}")
        # 실패하더라도 "오늘은 시도했음"을 표시해 중복 호출을 줄인다.
        consensus_years = {}

    payload: dict[str, dict[str, float | None]] | None = None
    consensus_json: str | None = None
    if consensus_years is not None:
        payload = {}
        for y, c in consensus_years.items():
            payload[str(int(y))] = {
                "pbr": getattr(c, "pbr", None),
                "per": getattr(c, "per", None),
                "eps": getattr(c, "eps", None),
            }
        # 빈 dict라도 "{}"로 저장해 "오늘 조회됨"을 표시 (only_missing 스킵에 사용)
        consensus_json = json.dumps(payload, ensure_ascii=False)

    primary = (consensus_years or {}).get(int(primary_year)) if consensus_years else None

    # 기존 현재가는 최신 스냅샷에서 복사
    current_price = None
    with get_session() as session:
        prev = _latest_snapshot_today(session, ticker=ticker, today=today)
        if prev:
            current_price = prev.current_price

        snap = _get_or_create_today_snapshot(session, ticker=ticker, today=today)
        snap.current_price = current_price
        snap.pbr_26y = getattr(primary, "pbr", None) if primary else None
        snap.per_26y = getattr(primary, "per", None) if primary else None
        snap.eps_26y = getattr(primary, "eps", None) if primary else None
        snap.consensus_json = consensus_json
        snap.consensus_primary_year = int(primary_year)
        session.commit()

    return {
        "ticker": ticker,
        "asof": today,
        "current_price": current_price,
        "consensus_years": payload,
        "consensus_primary_year": int(primary_year),
        "errors": errors,
        "skipped": False,
    }


def refresh_snapshots_for_all(*, limit: int = 200) -> dict:
    """
    MVP용: 전체 기업을 바로 다 돌리면 오래 걸릴 수 있어서 기본 limit을 둡니다.
    """
    init_db()
    with get_session() as session:
        tickers = session.exec(select(Company.ticker).order_by(Company.name).limit(limit)).all()

    ok = 0
    fail = 0
    for t in tickers:
        try:
            refresh_snapshot_for_ticker(ticker=t)
            ok += 1
        except Exception:
            fail += 1
    return {"requested": len(tickers), "ok": ok, "fail": fail}

