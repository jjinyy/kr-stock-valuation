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
from app.services.news import fetch_company_news, group_similar_news


router = APIRouter(prefix="/api")

# SQLite 바인딩 변수 제한 대비
_SNAPSHOT_IN_CHUNK = 400


def _extract_year_consensus(snap: Snapshot, *, year: int) -> tuple[float | None, float | None, float | None]:
    """
    스냅샷에서 특정 연도의 (pbr, per, eps)를 꺼냅니다.
    - consensus_json이 있으면 해당 연도 값을 우선
    - 없거나 파싱 실패면 legacy 컬럼을 사용(단, 요청 연도가 base_year일 때만 의미가 있을 수 있음)
    """
    if snap.consensus_json:
        try:
            raw = json.loads(snap.consensus_json)
            if isinstance(raw, dict):
                yv = raw.get(str(year), {}) or {}
                if isinstance(yv, dict):
                    return (yv.get("pbr"), yv.get("per"), yv.get("eps"))
        except Exception:
            pass
    return (snap.pbr_26y, snap.per_26y, snap.eps_26y)


def _norm_cat(v: str | None) -> str:
    s = (v or "").strip()
    return s if s else "미분류"


@router.get("/categories")
def categories(category_l: str | None = None):
    """
    카테고리(대/중) 목록을 반환합니다.
    - category_l을 주면 해당 대분류에 속한 중분류만 반환
    - category_l이 없으면 중분류는 전체에서 반환(요청대로 대분류 무시)
    """
    init_db()
    with get_session() as session:
        l_rows = session.exec(select(Company.category_l).distinct().order_by(Company.category_l)).all()
        if category_l is not None and category_l.strip():
            base = (
                select(Company.category_m)
                .where(Company.category_l == category_l.strip())
                .distinct()
                .order_by(Company.category_m)
            )
            m_rows = session.exec(base).all()
        else:
            m_rows = session.exec(select(Company.category_m).distinct().order_by(Company.category_m)).all()

    out_l = [_norm_cat(v) for v in l_rows if (v or "").strip()]
    out_m = [_norm_cat(v) for v in m_rows if (v or "").strip()]
    return {"category_l": out_l, "category_m": out_m}


@router.get("/category/summary")
def category_summary(
    level: str = "category_m",
    year: int | None = None,
    base_year: int | None = None,
    category_l: str | None = None,
):
    """
    대/중분류별 괴리율 요약(평균) 목록.
    - level: category_l | category_m
    - category_l: (선택) 중분류 요약을 '특정 대분류'로 제한할 때 사용
    """
    init_db()
    today = date.today().isoformat()
    if base_year is None:
        base_year = date.today().year
    if year is None:
        year = int(base_year)
    level = (level or "category_m").strip()
    if level not in {"category_l", "category_m"}:
        raise HTTPException(status_code=400, detail="level must be category_l or category_m")
    cat_l = (category_l or "").strip() or None

    with get_session() as session:
        base = select(Company).order_by(Company.name).limit(5000)
        if level == "category_m" and cat_l:
            base = base.where(Company.category_l == cat_l)
        companies = session.exec(base).all()
        tickers = [c.ticker for c in companies]
        snaps_by_ticker = _fetch_latest_snapshots_by_ticker(session, asof=today, tickers=tickers)

    by_ticker = {c.ticker: c for c in companies}
    groups: dict[str, list[float]] = {}
    counts: dict[str, int] = {}
    for ticker in tickers:
        c = by_ticker[ticker]
        snap = snaps_by_ticker.get(ticker)
        key = _norm_cat(getattr(c, level))
        counts[key] = counts.get(key, 0) + 1
        if not snap or snap.current_price is None:
            continue
        pbr, per, eps = _extract_year_consensus(snap, year=year)
        calc = calc_fair_price_and_gap(current_price=snap.current_price, pbr=pbr, per=per, eps=eps)
        if calc.gap_ratio is None:
            continue
        groups.setdefault(key, []).append(float(calc.gap_ratio))

    out = []
    for k, vals in groups.items():
        if not vals:
            continue
        avg = sum(vals) / float(len(vals))
        out.append(
            {
                "key": k,
                "avg_gap_ratio": avg,
                "n_total": counts.get(k, 0),
                "n_with_gap": len(vals),
            }
        )
    out.sort(key=lambda r: r["avg_gap_ratio"], reverse=True)
    return {
        "asof": today,
        "base_year": int(base_year),
        "year": int(year),
        "level": level,
        "category_l": cat_l,
        "groups": out,
    }


@router.get("/category/top5")
def category_top5(
    level: str = "category_m",
    key: str = "",
    year: int | None = None,
    base_year: int | None = None,
    category_l: str | None = None,
):
    """
    선택한 분류(key)에서 괴리율 Top5(저평가/고평가)를 반환합니다.
    - 저평가(+) Top5: gap_ratio 내림차순
    - 고평가(-) Top5: gap_ratio 오름차순
    """
    init_db()
    today = date.today().isoformat()
    if base_year is None:
        base_year = date.today().year
    if year is None:
        year = int(base_year)
    level = (level or "category_m").strip()
    if level not in {"category_l", "category_m"}:
        raise HTTPException(status_code=400, detail="level must be category_l or category_m")
    key = _norm_cat((key or "").strip())
    cat_l = (category_l or "").strip() or None

    with get_session() as session:
        base = select(Company).order_by(Company.name).limit(5000)
        if level == "category_m" and cat_l:
            base = base.where(Company.category_l == cat_l)
        companies = session.exec(base).all()
        tickers = [c.ticker for c in companies]
        snaps_by_ticker = _fetch_latest_snapshots_by_ticker(session, asof=today, tickers=tickers)

    by_ticker = {c.ticker: c for c in companies}
    scored: list[dict] = []
    for ticker in tickers:
        c = by_ticker[ticker]
        if _norm_cat(getattr(c, level)) != key:
            continue
        snap = snaps_by_ticker.get(ticker)
        if not snap or snap.current_price is None:
            continue
        pbr, per, eps = _extract_year_consensus(snap, year=year)
        calc = calc_fair_price_and_gap(current_price=snap.current_price, pbr=pbr, per=per, eps=eps)
        if calc.gap_ratio is None:
            continue
        scored.append({"ticker": c.ticker, "name": c.name, "gap_ratio": float(calc.gap_ratio)})

    top_pos = sorted([r for r in scored if r["gap_ratio"] > 0], key=lambda x: x["gap_ratio"], reverse=True)[:5]
    top_neg = sorted([r for r in scored if r["gap_ratio"] < 0], key=lambda x: x["gap_ratio"])[:5]
    return {
        "asof": today,
        "base_year": int(base_year),
        "year": int(year),
        "level": level,
        "category_l": cat_l,
        "key": key,
        "top_undervalued": top_pos,
        "top_overvalued": top_neg,
        "n_scored": len(scored),
    }


def _fetch_latest_snapshots_by_ticker(session: Session, *, asof: str, tickers: list[str]) -> dict[str, Snapshot]:
    """
    오늘(asof) 기준으로 티커당 1건만 고릅니다.
    값이 있는 행을 우선하고, 그다음 최신(created_at DESC)을 씁니다.
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
    # legacy param
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
        # 첫 실행 시 목록이 비어 있으면 한 번 채움
        company_count = session.exec(select(func.count()).select_from(Company)).one()
        if company_count == 0:
            try:
                refresh_companies_from_kind()
            except httpx.HTTPError:
                # 네트워크가 막힌 환경이면 빈 결과로 둠
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

        # 지표 정렬은 서버 기준으로 정렬한 뒤 페이지를 잘라야 함
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
        # 이름/카테고리는 DB paging으로 처리
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
            # 지표 정렬은 충분한 후보를 잡아 계산 후 잘라냄
            candidate_limit = page_size
            # 안전 상한
            candidate_limit = min(max(int(total), int(page_size)), 5000)
            companies = session.exec(base.order_by(Company.name).limit(candidate_limit)).all()

        # 오늘 스냅샷은 한 번에 가져와 매핑
        tickers = [c.ticker for c in companies]
        snaps_by_ticker: dict[str, Snapshot] = {}
        if tickers:
            snaps_by_ticker = _fetch_latest_snapshots_by_ticker(session, asof=today, tickers=tickers)

        out = []
        for c in companies:
            snap = snaps_by_ticker.get(c.ticker)

            current_price: Optional[int] = None
            # 연도 선택을 위해 year별 값을 내려줌
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
                    # 구버전 단일 컬럼 호환
                    yk = str(base_year)
                    consensus_window[yk] = {
                        "pbr": snap.pbr_26y,
                        "per": snap.per_26y,
                        "eps": snap.eps_26y,
                    }

            # 기본 표시값은 base_year 기준
            base_key = str(base_year)
            base_vals = consensus_window.get(base_key) or {}
            pbr = base_vals.get("pbr")
            per = base_vals.get("per")
            eps = base_vals.get("eps")
            calc = calc_fair_price_and_gap(current_price=current_price, pbr=pbr, per=per, eps=eps)

            # 연도별 계산값도 함께 내려줌
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
                    "pbr": pbr,
                    "per": per,
                    "eps": eps,
                    "fair_price": calc.fair_price,
                    "gap_ratio": calc.gap_ratio,
                    "consensus": consensus_out,
                }
            )

        if needs_server_sort:
            reverse = sort_dir == "desc"
            if sort_key == "name":
                out.sort(key=lambda r: (r.get("name") or ""), reverse=reverse)
            else:
                def key_num(v: float | int | None):
                    if v is None:
                        return (1, 0.0)
                    fv = float(v)
                    return (0, -fv if reverse else fv)

                def key_generic(r):
                    v = r.get(sort_key)
                    try:
                        return key_num(v)  # type: ignore[arg-type]
                    except Exception:
                        # 숫자 변환이 안 되면 문자열로 비교
                        if v is None:
                            return (1, "")
                        s = str(v)
                        return (0, s)

                out.sort(key=key_generic)

        # 지표 정렬에서만 페이지 슬라이스
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


@router.get("/top5-news")
def top5_news(base_year: int | None = None, per_company: int = 10):
    init_db()
    today = date.today().isoformat()
    if base_year is None:
        base_year = date.today().year
    per_company = max(1, min(int(per_company), 20))

    with get_session() as session:
        companies = session.exec(select(Company).order_by(Company.name).limit(5000)).all()
        tickers = [c.ticker for c in companies]
        snaps_by_ticker = _fetch_latest_snapshots_by_ticker(session, asof=today, tickers=tickers)

    scored: list[dict] = []
    by_ticker = {c.ticker: c for c in companies}
    for ticker in tickers:
        c = by_ticker[ticker]
        snap = snaps_by_ticker.get(ticker)
        if not snap:
            continue
        current_price = snap.current_price
        if current_price is None:
            continue

        pbr = None
        per = None
        eps = None
        if snap.consensus_json:
            try:
                raw = json.loads(snap.consensus_json)
                yv = raw.get(str(base_year), {}) if isinstance(raw, dict) else {}
                pbr = yv.get("pbr")
                per = yv.get("per")
                eps = yv.get("eps")
            except Exception:
                pbr = snap.pbr_26y
                per = snap.per_26y
                eps = snap.eps_26y
        else:
            pbr = snap.pbr_26y
            per = snap.per_26y
            eps = snap.eps_26y

        calc = calc_fair_price_and_gap(current_price=current_price, pbr=pbr, per=per, eps=eps)
        if calc.gap_ratio is None or calc.gap_ratio <= 0:
            continue
        scored.append(
            {
                "ticker": c.ticker,
                "name": c.name,
                "gap_ratio": calc.gap_ratio,
            }
        )

    top5 = sorted(scored, key=lambda x: x["gap_ratio"], reverse=True)[:5]
    rows_out: list[dict] = []
    for item in top5:
        try:
            news = fetch_company_news(ticker=item["ticker"], limit=per_company * 2)
            news = group_similar_news(news)
        except Exception:
            news = []
        for n in news[:per_company]:
            rows_out.append(
                {
                    "ticker": item["ticker"],
                    "company_name": item["name"],
                    "title": n["title"],
                    "link": n["link"],
                    "sentiment": n["sentiment"],
                    "keyword": n.get("keyword") or "기타",
                    "press": n["press"],
                    "date": n["date"],
                    "count": n.get("count") or 1,
                }
            )

    return {
        "base_year": base_year,
        "companies": top5,
        "rows": rows_out,
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
    return refresh_consensus_for_ticker(
        ticker=c.ticker, primary_year=primary_year, skip_if_already_today=True
    )


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

