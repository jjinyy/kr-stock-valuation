from __future__ import annotations

from datetime import date
import json
from typing import Optional

from fastapi import APIRouter, HTTPException
import httpx
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
            "current_price",
            "pbr",
            "per",
            "eps",
            "fair_price",
            "gap_ratio",
        }
        # 지표 정렬은 전체(또는 충분히 큰) 후보에서 계산 후 잘라야 "전체 기준 상위"가 나온다.
        candidate_limit = page_size
        if needs_server_sort and sort_key != "name":
            # KRX 전체가 2천여개 수준이라 전부 계산해도 부담이 크지 않음 (안전 상한 5000)
            candidate_limit = min(max(int(total), int(page_size)), 5000)
        else:
            # 이름 정렬은 DB에서 limit/offset로 처리 가능하지만, 여기선 단순성을 위해 동일 경로 유지
            candidate_limit = min(max(int(total), int(page_size) * 20), 5000)

        companies = session.exec(base.order_by(Company.name).limit(candidate_limit)).all()

        out = []
        for c in companies:
            # 갱신을 여러 번 시도하다가 실패하면 "값이 비어있는 스냅샷"이 최신으로 저장될 수 있음.
            # UI에서는 가능한 한 "값이 있는 최신 스냅샷"을 우선 사용한다.
            base = (
                select(Snapshot)
                .where(Snapshot.ticker == c.ticker)
                .where(Snapshot.asof == today)
                .order_by(Snapshot.created_at.desc())
            )

            snap = session.exec(
                base.where(
                    (Snapshot.current_price.is_not(None))
                    | (Snapshot.pbr_26y.is_not(None))
                    | (Snapshot.per_26y.is_not(None))
                    | (Snapshot.eps_26y.is_not(None))
                ).limit(1)
            ).first()
            if not snap:
                snap = session.exec(base.limit(1)).first()

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

        # 페이지 슬라이스 (None은 항상 뒤로 정렬된 상태)
        total_pages = max(1, (total + page_size - 1) // page_size)
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

