from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException
import httpx
from sqlmodel import Session, col, func, select

from app.db import get_session, init_db
from app.models import Company, Snapshot
from app.services.calc import calc_fair_price_and_gap
from app.services.jobs import refresh_companies_from_kind, refresh_snapshot_for_ticker, refresh_snapshots_for_all
from app.services.bulk import get_bulk_status, start_bulk_fill


router = APIRouter(prefix="/api")


@router.get("/rows")
def rows(q: str = "", limit: int = 200):
    init_db()
    today = date.today().isoformat()
    q = (q or "").strip()

    with get_session() as session:  # type: Session
        # 최초 실행 시 상장사 목록이 비어있으면 자동으로 1회 채움
        company_count = session.exec(select(func.count()).select_from(Company)).one()
        if company_count == 0:
            try:
                refresh_companies_from_kind()
            except httpx.HTTPError:
                # 네트워크가 막힌 환경이면 일단 빈 상태로 응답
                pass

        base = select(Company)
        if q:
            if q.isdigit():
                base = base.where(Company.ticker.contains(q))
            else:
                base = base.where(col(Company.name).contains(q))

        total = session.exec(select(func.count()).select_from(base.subquery())).one()
        companies = session.exec(base.order_by(Company.name).limit(limit)).all()

        out = []
        for c in companies:
            snap = session.exec(
                select(Snapshot)
                .where(Snapshot.ticker == c.ticker)
                .where(Snapshot.asof == today)
                .order_by(Snapshot.created_at.desc())
                .limit(1)
            ).first()

            current_price: Optional[int] = None
            pbr: Optional[float] = None
            per: Optional[float] = None
            eps: Optional[float] = None

            if snap:
                current_price = snap.current_price
                pbr = snap.pbr_26y
                per = snap.per_26y
                eps = snap.eps_26y

            calc = calc_fair_price_and_gap(
                current_price=current_price,
                pbr=pbr,
                per=per,
                eps=eps,
            )

            out.append(
                {
                    "ticker": c.ticker,
                    "name": c.name,
                    "current_price": current_price,
                    "pbr": pbr,
                    "per": per,
                    "eps": eps,
                    "fair_price": calc.fair_price,
                    "gap_ratio": calc.gap_ratio,
                }
            )

        return {"rows": out, "total": total, "asof": today}


@router.post("/admin/refresh/companies")
def admin_refresh_companies():
    """
    개발/MVP용 수동 갱신 버튼.
    운영 전에는 인증/권한을 붙이는 것을 권장.
    """
    return refresh_companies_from_kind()


@router.post("/admin/refresh/snapshot/{ticker}")
def admin_refresh_snapshot(ticker: str):
    return refresh_snapshot_for_ticker(ticker=ticker)


@router.post("/admin/refresh/snapshot_by_query")
def admin_refresh_snapshot_by_query(q: str):
    """
    검색어로 회사 1개를 찾아 스냅샷 갱신.
    - q가 숫자면 ticker 포함 매칭 우선
    - 그 외에는 회사명 포함 매칭
    """
    init_db()
    q = (q or "").strip()
    if not q:
        raise HTTPException(status_code=400, detail="q is required")

    with get_session() as session:
        base = select(Company)
        if q.isdigit():
            base = base.where(Company.ticker.contains(q)).order_by(Company.ticker)
        else:
            base = base.where(col(Company.name).contains(q)).order_by(Company.name)
        c = session.exec(base.limit(1)).first()

    if not c:
        raise HTTPException(status_code=404, detail="company not found")
    return refresh_snapshot_for_ticker(ticker=c.ticker)


@router.post("/admin/refresh/snapshots")
def admin_refresh_snapshots(limit: int = 200):
    return refresh_snapshots_for_all(limit=limit)


@router.post("/admin/refresh/visible")
def admin_refresh_visible(q: str = "", limit: int = 50):
    """
    현재 화면(검색 필터)에서 보이는 상위 N개만 갱신.
    - Naver/FnGuide 호출이 많아질 수 있으니 기본 50개 제한
    """
    init_db()
    q = (q or "").strip()
    limit = max(1, min(int(limit), 200))

    with get_session() as session:
        base = select(Company)
        if q:
            if q.isdigit():
                base = base.where(Company.ticker.contains(q))
            else:
                base = base.where(col(Company.name).contains(q))
        companies = session.exec(base.order_by(Company.name).limit(limit)).all()

    ok = 0
    fail = 0
    for c in companies:
        try:
            refresh_snapshot_for_ticker(ticker=c.ticker)
            ok += 1
        except Exception:
            fail += 1
    return {"requested": len(companies), "ok": ok, "fail": fail}


@router.post("/admin/fill")
def admin_fill(q: str = "", limit: int = 2000, only_missing: bool = True):
    """
    백그라운드로 다수 종목의 현재가/컨센서스를 채움.
    """
    st = start_bulk_fill(q=q, limit=limit, only_missing=only_missing)
    return {"job_id": st.job_id, "requested": st.requested}


@router.get("/admin/fill/{job_id}")
def admin_fill_status(job_id: str):
    st = get_bulk_status(job_id)
    if not st:
        raise HTTPException(status_code=404, detail="job not found")
    return {
        "job_id": st.job_id,
        "requested": st.requested,
        "done": st.done,
        "ok": st.ok,
        "fail": st.fail,
        "last_ticker": st.last_ticker,
        "started_at": st.started_at,
        "finished_at": st.finished_at,
    }

