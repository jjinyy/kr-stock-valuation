from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
import logging
import os
from threading import Lock, Thread
from time import time
from typing import Optional
from uuid import uuid4

from sqlmodel import Session, col, select

from app.db import get_session, init_db
from app.models import Company, Snapshot
from app.services.jobs import (
    refresh_consensus_for_ticker_in_session,
    refresh_price_for_ticker_in_session,
    refresh_snapshot_for_ticker_in_session,
)


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
_MAX_BULK_WORKERS = 6
logger = logging.getLogger(__name__)


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
    # 현재가+지표가 모두 있으면 완료로 봄
    return (
        snap.current_price is not None
        and snap.pbr_26y is not None
        and snap.per_26y is not None
        and snap.eps_26y is not None
    )


def _has_today_consensus(session: Session, *, ticker: str, today: str) -> bool:
    """
    오늘 컨센서스를 이미 확인했으면 True.
    (원래 컨센서스가 없는 종목도 중복 호출을 피하려고 오늘은 스킵)
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
    # JSON이 있으면 오늘은 조회된 것으로 봄
    if snap.consensus_json:
        return True
    # 구버전 단일 컬럼 호환
    return snap.pbr_26y is not None or snap.per_26y is not None or snap.eps_26y is not None


def _resolve_bulk_workers(total: int) -> int:
    raw = os.getenv("BULK_WORKERS", "").strip()
    val = _MAX_BULK_WORKERS
    if raw:
        try:
            val = int(raw)
        except ValueError:
            logger.warning("invalid BULK_WORKERS=%r; fallback to default=%d", raw, _MAX_BULK_WORKERS)
            val = _MAX_BULK_WORKERS
    return max(1, min(val, total))


def _resolve_commit_every() -> int:
    raw = os.getenv("BULK_COMMIT_EVERY", "").strip()
    if not raw:
        return 25
    try:
        v = int(raw)
        return max(1, min(v, 500))
    except ValueError:
        logger.warning("invalid BULK_COMMIT_EVERY=%r; fallback to default=25", raw)
        return 25


def _chunk(tickers: list[str], n: int) -> list[list[str]]:
    if n <= 1:
        return [tickers]
    out: list[list[str]] = [[] for _ in range(n)]
    for i, t in enumerate(tickers):
        out[i % n].append(t)
    return [c for c in out if c]


def _latest_today_snapshots_map(session: Session, *, tickers: list[str], today: str) -> dict[str, Snapshot]:
    if not tickers:
        return {}
    rows = session.exec(
        select(Snapshot)
        .where(Snapshot.asof == today)
        .where(Snapshot.ticker.in_(tickers))
        .order_by(Snapshot.ticker, Snapshot.created_at.desc())
    ).all()
    out: dict[str, Snapshot] = {}
    for r in rows:
        if r.ticker not in out:
            out[r.ticker] = r
    return out


def start_bulk_fill(*, q: str = "", limit: int = 2000, only_missing: bool = True) -> BulkStatus:
    """
    백그라운드로 여러 종목을 처리합니다.
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
            latest_map = _latest_today_snapshots_map(
                session, tickers=[c.ticker for c in companies], today=today
            )
            for c in companies:
                snap = latest_map.get(c.ticker)
                has_values = bool(
                    snap
                    and snap.current_price is not None
                    and snap.pbr_26y is not None
                    and snap.per_26y is not None
                    and snap.eps_26y is not None
                )
                if not has_values:
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
        started_at = time()
        ok = 0
        fail = 0
        done = 0
        workers = _resolve_bulk_workers(len(tickers))
        commit_every = _resolve_commit_every()

        def run_chunk(chunk: list[str]) -> list[tuple[str, bool]]:
            out: list[tuple[str, bool]] = []
            today = date.today().isoformat()
            primary_year = date.today().year
            with get_session() as session:
                n_since_commit = 0
                for t in chunk:
                    try:
                        r = refresh_snapshot_for_ticker_in_session(
                            session, ticker=t, today=today, primary_year=primary_year, commit=False
                        )
                        # 컨센서스가 원래 없는 케이스는 실패로 보지 않음
                        errors = r.get("errors") or []
                        has_price = r.get("current_price") is not None
                        has_any_consensus = not (
                            r.get("pbr_26y") is None and r.get("per_26y") is None and r.get("eps_26y") is None
                        )
                        fnguide_failed = any(str(e).startswith("fnguide:") for e in errors)
                        is_ok = has_price and not ((not has_any_consensus) and fnguide_failed)
                        out.append((t, bool(is_ok)))
                        n_since_commit += 1
                        if n_since_commit >= commit_every:
                            session.commit()
                            n_since_commit = 0
                    except Exception:
                        try:
                            session.rollback()
                        except Exception:
                            pass
                        out.append((t, False))
                if n_since_commit:
                    session.commit()
            return out

        chunks = _chunk(tickers, workers)
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [ex.submit(run_chunk, c) for c in chunks]
            for fut in as_completed(futures):
                results = fut.result()
                for t, is_ok in results:
                    if is_ok:
                        ok += 1
                    else:
                        fail += 1
                    done += 1
                    with _lock:
                        s = _jobs.get(job_id)
                        if s:
                            s.done = done
                            s.ok = ok
                            s.fail = fail
                            s.last_ticker = t
        with _lock:
            s = _jobs.get(job_id)
            if s:
                s.finished_at = time()
        elapsed = max(time() - started_at, 0.001)
        logger.info(
            "bulk_fill finished job_id=%s requested=%d done=%d ok=%d fail=%d workers=%d elapsed=%.2fs rate=%.2f/s",
            job_id,
            len(tickers),
            done,
            ok,
            fail,
            workers,
            elapsed,
            done / elapsed,
        )

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
        started_at = time()
        ok = 0
        fail = 0
        done = 0
        workers = _resolve_bulk_workers(len(tickers))
        commit_every = _resolve_commit_every()

        def run_chunk(chunk: list[str]) -> list[tuple[str, bool]]:
            out: list[tuple[str, bool]] = []
            today = date.today().isoformat()
            with get_session() as session:
                n_since_commit = 0
                for t in chunk:
                    try:
                        r = refresh_price_for_ticker_in_session(session, ticker=t, today=today, commit=False)
                        out.append((t, r.get("current_price") is not None))
                        n_since_commit += 1
                        if n_since_commit >= commit_every:
                            session.commit()
                            n_since_commit = 0
                    except Exception:
                        try:
                            session.rollback()
                        except Exception:
                            pass
                        out.append((t, False))
                if n_since_commit:
                    session.commit()
            return out

        chunks = _chunk(tickers, workers)
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [ex.submit(run_chunk, c) for c in chunks]
            for fut in as_completed(futures):
                results = fut.result()
                for t, is_ok in results:
                    if is_ok:
                        ok += 1
                    else:
                        fail += 1
                    done += 1
                    with _lock:
                        s = _jobs.get(job_id)
                        if s:
                            s.done = done
                            s.ok = ok
                            s.fail = fail
                            s.last_ticker = t
        with _lock:
            s = _jobs.get(job_id)
            if s:
                s.finished_at = time()
        elapsed = max(time() - started_at, 0.001)
        logger.info(
            "bulk_price_fill finished job_id=%s requested=%d done=%d ok=%d fail=%d workers=%d elapsed=%.2fs rate=%.2f/s",
            job_id,
            len(tickers),
            done,
            ok,
            fail,
            workers,
            elapsed,
            done / elapsed,
        )

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
            latest_map = _latest_today_snapshots_map(
                session, tickers=[c.ticker for c in companies], today=today
            )
            tickers = []
            for c in companies:
                snap = latest_map.get(c.ticker)
                has_consensus = bool(
                    snap and (snap.consensus_json or snap.pbr_26y is not None or snap.per_26y is not None or snap.eps_26y is not None)
                )
                if not has_consensus:
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
        started_at = time()
        ok = 0
        fail = 0
        done = 0
        workers = _resolve_bulk_workers(len(tickers))
        commit_every = _resolve_commit_every()

        def run_chunk(chunk: list[str]) -> list[tuple[str, bool]]:
            out: list[tuple[str, bool]] = []
            today = date.today().isoformat()
            with get_session() as session:
                n_since_commit = 0
                for t in chunk:
                    try:
                        r = refresh_consensus_for_ticker_in_session(
                            session,
                            ticker=t,
                            today=today,
                            primary_year=int(primary_year),
                            skip_if_already_today=False,
                            commit=False,
                        )
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
                        out.append((t, not (fnguide_failed and not has_any)))
                        n_since_commit += 1
                        if n_since_commit >= commit_every:
                            session.commit()
                            n_since_commit = 0
                    except Exception:
                        try:
                            session.rollback()
                        except Exception:
                            pass
                        out.append((t, False))
                if n_since_commit:
                    session.commit()
            return out

        chunks = _chunk(tickers, workers)
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [ex.submit(run_chunk, c) for c in chunks]
            for fut in as_completed(futures):
                results = fut.result()
                for t, is_ok in results:
                    if is_ok:
                        ok += 1
                    else:
                        fail += 1
                    done += 1
                    with _lock:
                        s = _jobs.get(job_id)
                        if s:
                            s.done = done
                            s.ok = ok
                            s.fail = fail
                            s.last_ticker = t
        with _lock:
            s = _jobs.get(job_id)
            if s:
                s.finished_at = time()
        elapsed = max(time() - started_at, 0.001)
        logger.info(
            "bulk_consensus_fill finished job_id=%s requested=%d done=%d ok=%d fail=%d workers=%d elapsed=%.2fs rate=%.2f/s",
            job_id,
            len(tickers),
            done,
            ok,
            fail,
            workers,
            elapsed,
            done / elapsed,
        )

    Thread(target=runner, daemon=True).start()
    return st


def get_bulk_status(job_id: str) -> Optional[BulkStatus]:
    with _lock:
        return _jobs.get(job_id)

