from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from threading import Lock, Thread
from time import time
from typing import Optional
from uuid import uuid4

from sqlmodel import Session, col, func, select

from app.db import get_session, init_db
from app.models import Company, Snapshot
from app.services.jobs import refresh_consensus_for_ticker, refresh_price_for_ticker, refresh_snapshot_for_ticker


@dataclass
class BulkStatus:
    job_id: str
    started_at: float
    finished_at: Optional[float]
    requested: int
    done: int
    ok: int
    fail: int
    last_ticker: Optional[str]


_lock = Lock()
_jobs: dict[str, BulkStatus] = {}


def _has_today_snapshot_with_values(session: Session, *, ticker: str, today: str) -> bool:
    snap = session.exec(
        select(Snapshot)
        .where(Snapshot.ticker == ticker)
        .where(Snapshot.asof == today)
        .order_by(Snapshot.created_at.desc())
        .limit(1)
    ).first()
    if not snap:
        return False
    # 현재가 + (PBR/PER/EPS) 3개가 모두 있으면 "완료"로 간주
    return (
        snap.current_price is not None
        and snap.pbr_26y is not None
        and snap.per_26y is not None
        and snap.eps_26y is not None
    )


def _has_today_consensus(session: Session, *, ticker: str, today: str) -> bool:
    """
    오늘 기준으로 컨센서스가 "이미 채워진" 스냅샷이 있으면 True.
    - 원래 컨센서스가 없는 종목은 (값이 비어있어도) 중복 호출을 피하기 위해 True로 취급.
      즉, "한 번 조회해봤다"는 사실만으로 오늘은 스킵.
    """
    snap = session.exec(
        select(Snapshot)
        .where(Snapshot.ticker == ticker)
        .where(Snapshot.asof == today)
        .order_by(Snapshot.created_at.desc())
        .limit(1)
    ).first()
    if not snap:
        return False
    # JSON이 있으면 "컨센서스 조회됨"으로 간주 (값이 없더라도 오늘은 스킵)
    if snap.consensus_json:
        return True
    # 구버전 호환: 단일 컬럼 중 하나라도 있으면 조회됨으로 간주
    return snap.pbr_26y is not None or snap.per_26y is not None or snap.eps_26y is not None


def start_bulk_fill(*, q: str = "", limit: int = 2000, only_missing: bool = True) -> BulkStatus:
    """
    백그라운드로 다수 종목 스냅샷을 채웁니다.
    - only_missing=True: 오늘 스냅샷 값이 이미 있으면 스킵
    """
    init_db()
    q = (q or "").strip()
    limit = max(1, min(int(limit), 5000))
    today = date.today().isoformat()

    with get_session() as session:
        base = select(Company)
        if q:
            if q.isdigit():
                base = base.where(Company.ticker.contains(q))
            else:
                base = base.where(col(Company.name).contains(q))
        companies = session.exec(base.order_by(Company.name).limit(limit)).all()

        tickers: list[str] = []
        if only_missing:
            for c in companies:
                if not _has_today_snapshot_with_values(session, ticker=c.ticker, today=today):
                    tickers.append(c.ticker)
        else:
            tickers = [c.ticker for c in companies]

    job_id = uuid4().hex[:12]
    st = BulkStatus(
        job_id=job_id,
        started_at=time(),
        finished_at=None,
        requested=len(tickers),
        done=0,
        ok=0,
        fail=0,
        last_ticker=None,
    )
    with _lock:
        _jobs[job_id] = st

    def runner():
        ok = 0
        fail = 0
        done = 0
        last = None
        for t in tickers:
            last = t
            try:
                r = refresh_snapshot_for_ticker(ticker=t)
                # "컨센서스가 원래 없음"은 실패로 보지 않음.
                # - 현재가가 없으면 실패
                # - 컨센서스(PBR/PER/EPS)가 전부 없더라도, fnguide 호출 자체가 실패한 경우에만 실패로 카운트
                #   (파싱 결과로 값이 비어있는 케이스는 OK로 둔다)
                errors = r.get("errors") or []
                has_price = r.get("current_price") is not None
                has_any_consensus = not (
                    r.get("pbr_26y") is None and r.get("per_26y") is None and r.get("eps_26y") is None
                )
                fnguide_failed = any(str(e).startswith("fnguide:") for e in errors)

                if not has_price:
                    fail += 1
                elif (not has_any_consensus) and fnguide_failed:
                    fail += 1
                else:
                    ok += 1
            except Exception:
                fail += 1
            done += 1
            with _lock:
                s = _jobs.get(job_id)
                if s:
                    s.done = done
                    s.ok = ok
                    s.fail = fail
                    s.last_ticker = last
        with _lock:
            s = _jobs.get(job_id)
            if s:
                s.finished_at = time()

    Thread(target=runner, daemon=True).start()
    return st


def start_bulk_price_fill(*, q: str = "", limit: int = 2000) -> BulkStatus:
    """
    현재가(네이버)만 백그라운드로 채웁니다.
    """
    init_db()
    q = (q or "").strip()
    limit = max(1, min(int(limit), 5000))
    today = date.today().isoformat()

    with get_session() as session:
        base = select(Company)
        if q:
            if q.isdigit():
                base = base.where(Company.ticker.contains(q))
            else:
                base = base.where(col(Company.name).contains(q))
        companies = session.exec(base.order_by(Company.name).limit(limit)).all()
        tickers = [c.ticker for c in companies]

    job_id = uuid4().hex[:12]
    st = BulkStatus(
        job_id=job_id,
        started_at=time(),
        finished_at=None,
        requested=len(tickers),
        done=0,
        ok=0,
        fail=0,
        last_ticker=None,
    )
    with _lock:
        _jobs[job_id] = st

    def runner():
        ok = 0
        fail = 0
        done = 0
        last = None
        for t in tickers:
            last = t
            try:
                r = refresh_price_for_ticker(ticker=t)
                if r.get("current_price") is None:
                    fail += 1
                else:
                    ok += 1
            except Exception:
                fail += 1
            done += 1
            with _lock:
                s = _jobs.get(job_id)
                if s:
                    s.done = done
                    s.ok = ok
                    s.fail = fail
                    s.last_ticker = last
        with _lock:
            s = _jobs.get(job_id)
            if s:
                s.finished_at = time()

    Thread(target=runner, daemon=True).start()
    return st


def start_bulk_consensus_fill(
    *, q: str = "", limit: int = 2000, primary_year: int | None = None, only_missing: bool = True
) -> BulkStatus:
    """
    컨센서스(FnGuide)만 백그라운드로 채웁니다.
    """
    init_db()
    q = (q or "").strip()
    limit = max(1, min(int(limit), 5000))
    if primary_year is None:
        primary_year = date.today().year
    today = date.today().isoformat()

    with get_session() as session:
        base = select(Company)
        if q:
            if q.isdigit():
                base = base.where(Company.ticker.contains(q))
            else:
                base = base.where(col(Company.name).contains(q))
        companies = session.exec(base.order_by(Company.name).limit(limit)).all()
        if only_missing:
            tickers = [c.ticker for c in companies if not _has_today_consensus(session, ticker=c.ticker, today=today)]
        else:
            tickers = [c.ticker for c in companies]

    job_id = uuid4().hex[:12]
    st = BulkStatus(
        job_id=job_id,
        started_at=time(),
        finished_at=None,
        requested=len(tickers),
        done=0,
        ok=0,
        fail=0,
        last_ticker=None,
    )
    with _lock:
        _jobs[job_id] = st

    def runner():
        ok = 0
        fail = 0
        done = 0
        last = None
        for t in tickers:
            last = t
            try:
                r = refresh_consensus_for_ticker(ticker=t, primary_year=primary_year)
                errors = r.get("errors") or []
                has_any = False
                cy = r.get("consensus_years") or {}
                if isinstance(cy, dict):
                    for v in cy.values():
                        if not isinstance(v, dict):
                            continue
                        if v.get("pbr") is not None or v.get("per") is not None or v.get("eps") is not None:
                            has_any = True
                            break
                fnguide_failed = any(str(e).startswith("fnguide:") for e in errors)
                if fnguide_failed and not has_any:
                    fail += 1
                else:
                    ok += 1
            except Exception:
                fail += 1
            done += 1
            with _lock:
                s = _jobs.get(job_id)
                if s:
                    s.done = done
                    s.ok = ok
                    s.fail = fail
                    s.last_ticker = last
        with _lock:
            s = _jobs.get(job_id)
            if s:
                s.finished_at = time()

    Thread(target=runner, daemon=True).start()
    return st


def get_bulk_status(job_id: str) -> Optional[BulkStatus]:
    with _lock:
        return _jobs.get(job_id)

