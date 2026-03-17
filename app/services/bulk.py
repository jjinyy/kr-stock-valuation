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
from app.services.jobs import refresh_snapshot_for_ticker


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
                # 네트워크 실패 등으로 값이 비었으면 fail로 카운트
                if r.get("current_price") is None or (r.get("pbr_26y") is None and r.get("per_26y") is None and r.get("eps_26y") is None):
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

