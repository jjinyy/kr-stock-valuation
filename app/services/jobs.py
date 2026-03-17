from __future__ import annotations

from datetime import date, datetime

import httpx
from sqlmodel import select

from app.db import get_session, init_db
from app.models import Company, Snapshot
from app.services.fnguide import fetch_consensus_26y
from app.services.kind import fetch_kind_companies
from app.services.naver import fetch_current_price


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
                if changed:
                    existing.updated_at = datetime.utcnow()
                    upserts += 1
            else:
                session.add(
                    Company(
                        ticker=r.ticker,
                        name=r.name,
                        market=r.market,
                    )
                )
                upserts += 1
        session.commit()

    return {"asof": asof, "count": len(rows), "upserts": upserts}


def refresh_snapshot_for_ticker(*, ticker: str) -> dict:
    init_db()
    ticker = ticker.zfill(6)
    today = date.today().isoformat()

    current_price = None
    consensus = None
    errors: list[str] = []

    try:
        current_price = fetch_current_price(ticker=ticker)
    except httpx.HTTPError as e:
        errors.append(f"naver: {type(e).__name__}")

    try:
        consensus = fetch_consensus_26y(ticker=ticker)
    except httpx.HTTPError as e:
        errors.append(f"fnguide: {type(e).__name__}")
        consensus = None

    snap = Snapshot(
        ticker=ticker,
        asof=today,
        current_price=current_price,
        pbr_26y=consensus.pbr if consensus else None,
        per_26y=consensus.per if consensus else None,
        eps_26y=consensus.eps if consensus else None,
    )
    with get_session() as session:
        session.add(snap)
        session.commit()

        # company may not exist yet; keep it as-is

    return {
        "ticker": ticker,
        "asof": today,
        "current_price": current_price,
        "pbr_26y": consensus.pbr if consensus else None,
        "per_26y": consensus.per if consensus else None,
        "eps_26y": consensus.eps if consensus else None,
        "errors": errors,
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

