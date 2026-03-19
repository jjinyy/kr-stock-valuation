from __future__ import annotations

from datetime import date
import json
from typing import Optional

from fastapi import APIRouter, HTTPException
import httpx
from sqlalchemy import text
from sqlmodel import Session, col, func, select

from app.db import get_session, init_db
from app.models import Company, Snapshot
from app.services.calc import calc_fair_price_and_gap
from app.services.jobs import (
    refresh_companies_from_kind,
    refresh_consensus_for_ticker,
    refresh_price_for_ticker,
    refresh_snapshot_for_ticker,
    refresh_snapshots_for_all,
)
from app.services.bulk import get_bulk_status, start_bulk_consensus_fill, start_bulk_fill, start_bulk_price_fill


router = APIRouter(prefix="/api")

# SQLite SQLITE_MAX_VARIABLE_NUMBER(구버전 999) 대비
_SNAPSHOT_IN_CHUNK = 400


def _fetch_latest_snapshots_by_ticker(session: Session, *, asof: str, tickers: list[str]) -> dict[str, Snapshot]:
    """
    오늘(asof) 기준 티커당 스냅샷 1건만 선택.
    - 값이 있는 행(가격/26y 지표 중 하나라도 NOT NULL)을 우선
    - 그다음 created_at DESC
    기존 구현은 동일 조건의 행을 전부 읽어와 Python에서 골랐기 때문에,
    스냅샷이 많이 쌓이면 /api/rows가 매우 느려질 수 있음.
    """
    if not tickers:
        return {}
    all_ids: list[int] = []
    for start in range(0, len(tickers), _SNAPSHOT_IN_CHUNK):
        chunk = tickers[start : start + _SNAPSHOT_IN_CHUNK]
        placeholders = ", ".join([f":t{i}" for i in range(len(chunk))])
        params: dict = {"asof": asof}
        for i, t in enumerate(chunk):
            params[f"t{i}"] = t
        sql = f"""
        WITH ranked AS (
          SELECT id,
                 ROW_NUMBER() OVER (
                   PARTITION BY ticker
                   ORDER BY
                     (CASE WHEN current_price IS NOT NULL OR pbr_26y IS NOT NULL
                                OR per_26y IS NOT NULL OR eps_26y IS NOT NULL
                           THEN 0 ELSE 1 END),
                     created_at DESC
                 ) AS rn
          FROM snapshot
          WHERE asof = :asof AND ticker IN ({placeholders})
        )
        SELECT id FROM ranked WHERE rn = 1
        """
        result = session.execute(text(sql), params)
        all_ids.extend(row[0] for row in result.fetchall())
    if not all_ids:
        return {}
    snaps = session.exec(select(Snapshot).where(Snapshot.id.in_(all_ids))).all()
    return {s.ticker: s for s in snaps}


@router.get("/rows")
def rows(
    q: str = "",
    # pagination
    page: int = 1,
    page_size: int = 200,
    # backward-compat (old param)
    limit: int | None = None,
    base_year: int | None = None,
    sort_key: str = "name",
    sort_dir: str = "asc",
):
    init_db()
    today = date.today().isoformat()
    if base_year is None:
        base_year = date.today().year
    q = (q or "").strip()
    years_window = [int(base_year), int(base_year) + 1, int(base_year) + 2]

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

        total = int(session.exec(select(func.count()).select_from(base.subquery())).one())

        if limit is not None:
            page_size = int(limit)
        page = max(1, int(page))
        page_size = max(10, min(int(page_size), 1000))

        sort_key = (sort_key or "name").strip()
        sort_dir = (sort_dir or "asc").strip().lower()
        if sort_dir not in {"asc", "desc"}:
            sort_dir = "asc"

        # 중요: 기존에는 "이름순으로 limit개만 가져온 뒤" 프론트에서 정렬했기 때문에
        # gap_ratio 같은 지표로 정렬 시 전체 기준 상위 종목이 화면에 안 나오는 문제가 있었다.
        # -> 지표 정렬일 때는 서버에서 정렬 후 상위 limit개를 반환한다.
        needs_server_sort = sort_key in {
            "name",
            "category_l",
            "category_m",
            "current_price",
            "pbr",
            "per",
            "eps",
            "fair_price",
            "gap_ratio",
        }
        # 빠른 경로: 단순 컬럼 정렬(이름/카테고리)은 DB에서 offset/limit로 페이지네이션 처리
        if sort_key in {"name", "category_l", "category_m"}:
            total_pages = max(1, (total + page_size - 1) // page_size)
            if page > total_pages:
                page = total_pages
            off = (page - 1) * page_size
            order_expr = Company.name
            if sort_key == "category_l":
                order_expr = Company.category_l
            elif sort_key == "category_m":
                order_expr = Company.category_m
            if sort_dir == "desc":
                order_expr = order_expr.desc()
            companies = session.exec(base.order_by(order_expr, Company.name).offset(off).limit(page_size)).all()
        else:
            # 지표 정렬은 전체(또는 충분히 큰) 후보에서 계산 후 잘라야 "전체 기준 상위"가 나온다.
            candidate_limit = page_size
            # KRX 전체가 2천여개 수준이라 전부 계산해도 부담이 크지 않음 (안전 상한 5000)
            candidate_limit = min(max(int(total), int(page_size)), 5000)
            companies = session.exec(base.order_by(Company.name).limit(candidate_limit)).all()

        # 성능: 기존에는 회사마다 Snapshot을 별도 쿼리로 조회(N+1)해서 느렸음.
        # 오늘(asof) 스냅샷을 한 번에 가져와 ticker별로 선택한다.
        tickers = [c.ticker for c in companies]
        snaps_by_ticker: dict[str, Snapshot] = {}
        if tickers:
            snaps_by_ticker = _fetch_latest_snapshots_by_ticker(session, asof=today, tickers=tickers)

        out = []
        for c in companies:
            snap = snaps_by_ticker.get(c.ticker)

            current_price: Optional[int] = None
            # UI에서 year 선택을 지원하기 위해, year별 값을 내려줌
            consensus_window: dict[str, dict[str, float | None]] = {}

            if snap:
                current_price = snap.current_price
                raw = None
                if snap.consensus_json:
                    try:
                        raw = json.loads(snap.consensus_json)
                    except Exception:
                        raw = None
                if isinstance(raw, dict):
                    for y in years_window:
                        yk = str(y)
                        v = raw.get(yk) or {}
                        consensus_window[yk] = {
                            "pbr": v.get("pbr"),
                            "per": v.get("per"),
                            "eps": v.get("eps"),
                        }
                else:
                    # 구버전 데이터: 단일 컬럼만 있으면 base_year로 매핑
                    yk = str(base_year)
                    consensus_window[yk] = {
                        "pbr": snap.pbr_26y,
                        "per": snap.per_26y,
                        "eps": snap.eps_26y,
                    }

            # 기본 표시값은 base_year(선택 기준년도)의 값
            base_key = str(base_year)
            base_vals = consensus_window.get(base_key) or {}
            pbr = base_vals.get("pbr")
            per = base_vals.get("per")
            eps = base_vals.get("eps")
            calc = calc_fair_price_and_gap(current_price=current_price, pbr=pbr, per=per, eps=eps)

            # 연도별 계산값도 함께 내려줘서, 프론트에서 즉시 전환 가능
            consensus_out: dict[str, dict[str, float | int | None]] = {}
            for y in years_window:
                yk = str(y)
                vals = consensus_window.get(yk) or {"pbr": None, "per": None, "eps": None}
                ccalc = calc_fair_price_and_gap(
                    current_price=current_price,
                    pbr=vals.get("pbr"),
                    per=vals.get("per"),
                    eps=vals.get("eps"),
                )
                consensus_out[yk] = {
                    "pbr": vals.get("pbr"),
                    "per": vals.get("per"),
                    "eps": vals.get("eps"),
                    "fair_price": ccalc.fair_price,
                    "gap_ratio": ccalc.gap_ratio,
                }

            out.append(
                {
                    "ticker": c.ticker,
                    "name": c.name,
                    "category_l": c.category_l,
                    "category_m": c.category_m,
                    "current_price": current_price,
                    # 선택 연도(base_year) 기준 표시값
                    "pbr": pbr,
                    "per": per,
                    "eps": eps,
                    "fair_price": calc.fair_price,
                    "gap_ratio": calc.gap_ratio,
                    # 3개 연도 창
                    "consensus": consensus_out,
                }
            )

        if needs_server_sort:
            reverse = sort_dir == "desc"
            if sort_key == "name":
                out.sort(key=lambda r: (r.get("name") or ""), reverse=reverse)
            else:
                def key_num(v: float | int | None):
                    # None은 항상 뒤로
                    if v is None:
                        return (1, 0.0)
                    fv = float(v)
                    return (0, -fv if reverse else fv)

                def key_generic(r):
                    v = r.get(sort_key)
                    try:
                        return key_num(v)  # type: ignore[arg-type]
                    except Exception:
                        # 숫자 변환이 안 되면 문자열로 비교 (None은 뒤로)
                        if v is None:
                            return (1, "")
                        s = str(v)
                        return (0, s)

                out.sort(key=key_generic)

        # 페이지 슬라이스: 지표 정렬 경로에서만 필요 (단순 컬럼 정렬은 이미 DB에서 paging 처리됨)
        total_pages = max(1, (total + page_size - 1) // page_size)
        if sort_key not in {"name", "category_l", "category_m"}:
            if page > total_pages:
                page = total_pages
            start = (page - 1) * page_size
            end = start + page_size
            out = out[start:end]

        return {
            "rows": out,
            "total": total,
            "asof": today,
            "base_year": base_year,
            "years_window": years_window,
            "server_sorted": True,
            "sort_key": sort_key,
            "sort_dir": sort_dir,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
        }


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


@router.post("/admin/refresh/price_by_query")
def admin_refresh_price_by_query(q: str):
    """
    검색어로 회사 1개를 찾아 "현재가만" 갱신.
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
    return refresh_price_for_ticker(ticker=c.ticker)


@router.post("/admin/refresh/consensus_by_query")
def admin_refresh_consensus_by_query(q: str, primary_year: int | None = None):
    """
    검색어로 회사 1개를 찾아 "컨센서스만" 갱신.
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
    return refresh_consensus_for_ticker(ticker=c.ticker, primary_year=primary_year)


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


@router.post("/admin/refresh/price_visible")
def admin_refresh_price_visible(q: str = "", limit: int = 50):
    """
    현재 화면(검색 필터)에서 보이는 상위 N개만 "현재가만" 갱신.
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
            r = refresh_price_for_ticker(ticker=c.ticker)
            if r.get("current_price") is None:
                fail += 1
            else:
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


@router.post("/admin/fill_price")
def admin_fill_price(q: str = "", limit: int = 2000):
    """
    현재가만 백그라운드로 채움.
    """
    st = start_bulk_price_fill(q=q, limit=limit)
    return {"job_id": st.job_id, "requested": st.requested}


@router.post("/admin/fill_consensus")
def admin_fill_consensus(
    q: str = "", limit: int = 2000, primary_year: int | None = None, only_missing: bool = True
):
    """
    컨센서스만 백그라운드로 채움.
    """
    st = start_bulk_consensus_fill(q=q, limit=limit, primary_year=primary_year, only_missing=only_missing)
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

