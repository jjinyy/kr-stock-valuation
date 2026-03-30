import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import "./App.css";

const fmtNum = (v: number | null | undefined) =>
  v === null || v === undefined ? "-" : Number(v).toLocaleString("ko-KR");
const fmtFloat = (v: number | null | undefined) =>
  v === null || v === undefined ? "-" : Number(v).toFixed(2);
const fmtPct = (v: number | null | undefined) =>
  v === null || v === undefined ? "-" : `${(Number(v) * 100).toFixed(2)}%`;

type ConsensusSlice = {
  pbr?: number | null;
  per?: number | null;
  eps?: number | null;
  fair_price?: number | null;
  gap_ratio?: number | null;
};

interface ApiRow {
  ticker: string;
  name: string;
  category_l?: string | null;
  category_m?: string | null;
  current_price?: number | null;
  consensus?: Record<string, ConsensusSlice>;
}

interface Row extends ApiRow {
  pbr: number | null;
  per: number | null;
  eps: number | null;
  fair_price: number | null;
  gap_ratio: number | null;
}

interface NewsRow {
  company_name?: string;
  keyword?: string;
  title?: string;
  link?: string;
  sentiment?: string;
  press?: string;
  date?: string;
}

interface CategorySummaryRow {
  key: string;
  avg_gap_ratio: number;
  n_total: number;
  n_with_gap: number;
}

interface CategoryTop5Row {
  ticker: string;
  name: string;
  gap_ratio: number;
}

interface SortState {
  key: string;
  dir: "asc" | "desc";
  type: string;
}

const MAIN_COLUMNS: { key: string; type: string; label: string }[] = [
  { key: "category_l", type: "string", label: "업종" },
  { key: "category_m", type: "string", label: "주요제품" },
  { key: "name", type: "string", label: "기업명" },
  { key: "current_price", type: "number", label: "현재주가" },
  { key: "pbr", type: "number", label: "PBR" },
  { key: "per", type: "number", label: "PER" },
  { key: "eps", type: "number", label: "EPS" },
  { key: "fair_price", type: "number", label: "적정주가" },
  { key: "gap_ratio", type: "number", label: "괴리율" },
];

function withConsensusYear(r: ApiRow, year: number): Row {
  const yk = String(year);
  const c = r.consensus?.[yk] ?? {};
  return {
    ...r,
    pbr: c.pbr ?? null,
    per: c.per ?? null,
    eps: c.eps ?? null,
    fair_price: c.fair_price ?? null,
    gap_ratio: c.gap_ratio ?? null,
  };
}

function applySort(rows: Row[], sort: SortState): Row[] {
  const { key, dir, type } = sort;
  const mult = dir === "asc" ? 1 : -1;
  const norm = (v: unknown) => (v === null || v === undefined ? null : v);
  const get = (r: Row) => norm((r as Record<string, unknown>)[key]);
  const cmp = (a: Row, b: Row) => {
    const av = get(a);
    const bv = get(b);
    if (av === null && bv === null) return 0;
    if (av === null) return 1;
    if (bv === null) return -1;
    if (type === "number") return (Number(av) - Number(bv)) * mult;
    return String(av).localeCompare(String(bv), "ko") * mult;
  };
  return [...rows].sort(cmp);
}

async function postJson(url: string): Promise<Record<string, unknown>> {
  const res = await fetch(url, { method: "POST" });
  let body: Record<string, unknown> | null = null;
  try {
    body = (await res.json()) as Record<string, unknown>;
  } catch {
    /* ignore */
  }
  if (!res.ok) {
    const detail = body && typeof body.detail === "string" ? ` (${body.detail})` : "";
    throw new Error(`요청 실패: ${res.status}${detail}`);
  }
  return body ?? {};
}

export default function App() {
  const cy = useMemo(() => new Date().getFullYear(), []);
  const [view, setView] = useState<"table" | "category">("table");
  const [baseYear, setBaseYear] = useState(cy);
  const [year, setYear] = useState(cy);
  const [qInput, setQInput] = useState("");
  const [qApplied, setQApplied] = useState("");
  const [sort, setSort] = useState<SortState>({ key: "name", dir: "asc", type: "string" });
  const [serverSorted, setServerSorted] = useState(true);
  const [rows, setRows] = useState<ApiRow[]>([]);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(200);
  const [totalPages, setTotalPages] = useState(1);
  const [meta, setMeta] = useState("데이터를 불러오는 중…");
  const [statusHtml, setStatusHtml] = useState("");
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [top5Open, setTop5Open] = useState(false);
  const [newsRows, setNewsRows] = useState<NewsRow[] | "loading" | "error" | "empty">([]);
  const [consensusBusy, setConsensusBusy] = useState(false);

  // category analytics
  const [catLAll, setCatLAll] = useState<string[]>([]);
  const [catMAll, setCatMAll] = useState<string[]>([]);
  const [catLSelected, setCatLSelected] = useState<string>("");
  const [catMSelected, setCatMSelected] = useState<string>("");
  const [catLSummary, setCatLSummary] = useState<CategorySummaryRow[] | "loading" | "error">([]);
  const [catMSummary, setCatMSummary] = useState<CategorySummaryRow[] | "loading" | "error">([]);
  const [top5Level, setTop5Level] = useState<"category_l" | "category_m">("category_m");
  const [catTop5, setCatTop5] = useState<
    | { key: string; undervalued: CategoryTop5Row[]; overvalued: CategoryTop5Row[]; n_scored: number }
    | "loading"
    | "error"
    | null
  >(null);

  const searchTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pollTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const yearRef = useRef(year);
  yearRef.current = year;

  const yearOptions = useMemo(() => [baseYear, baseYear + 1, baseYear + 2], [baseYear]);

  useEffect(() => {
    if (!yearOptions.includes(year)) {
      setYear(baseYear);
    }
  }, [yearOptions, year, baseYear]);

  const displayRows = useMemo(() => {
    const base = rows.map((r) => withConsensusYear(r, year));
    return serverSorted ? base : applySort(base, sort);
  }, [rows, year, serverSorted, sort]);

  const clearPoll = () => {
    if (pollTimer.current !== null) {
      clearTimeout(pollTimer.current);
      pollTimer.current = null;
    }
  };

  useEffect(() => () => {
    if (searchTimer.current !== null) clearTimeout(searchTimer.current);
    clearPoll();
  }, []);

  const load = useCallback(async () => {
    const q = qApplied.trim();
    setMeta("데이터를 불러오는 중…");
    try {
      const res = await fetch(
        `/api/rows?q=${encodeURIComponent(q)}&base_year=${encodeURIComponent(baseYear)}&sort_key=${encodeURIComponent(
          sort.key
        )}&sort_dir=${encodeURIComponent(sort.dir)}&page=${encodeURIComponent(page)}&page_size=${encodeURIComponent(pageSize)}`
      );
      if (!res.ok) {
        const t = await res.text();
        throw new Error(`API 오류: ${res.status} ${t.slice(0, 120)}`);
      }
      const data = (await res.json()) as {
        rows?: ApiRow[];
        server_sorted?: boolean;
        total?: number;
        page?: number;
        page_size?: number;
        total_pages?: number;
        asof?: string;
      };
      const list = data.rows ?? [];
      setRows(list);
      setServerSorted(Boolean(data.server_sorted));
      setPage(Number(data.page ?? page));
      setPageSize(Number(data.page_size ?? pageSize));
      setTotalPages(Number(data.total_pages ?? 1));
      const asof = data.asof ?? "-";
      const yShow = yearRef.current;
      setMeta(
        `현재 ${list.length}개를 보여주고 있어요 (전체 ${Number(data.total ?? 0)}개) · 페이지 ${Number(data.page ?? 1)}/${Number(data.total_pages ?? 1)} · 기준일 ${asof} · 표시연도 ${yShow}`
      );
      setStatusHtml("");
    } catch (e) {
      setRows([]);
      setMeta("데이터를 불러오지 못했습니다.");
      setStatusHtml(`<strong>로드 실패</strong>: ${e instanceof Error ? e.message : String(e)}`);
    }
  }, [qApplied, baseYear, sort.key, sort.dir, page, pageSize]);

  useEffect(() => {
    void load();
  }, [load]);

  const loadCategoryLs = useCallback(async () => {
    try {
      const res = await fetch(`/api/categories`);
      if (!res.ok) throw new Error(String(res.status));
      const data = (await res.json()) as { category_l?: string[] };
      setCatLAll(data.category_l ?? []);
    } catch {
      setCatLAll([]);
    }
  }, []);

  const loadCategoryMs = useCallback(async (categoryL: string) => {
    try {
      const qs = categoryL.trim() ? `?category_l=${encodeURIComponent(categoryL.trim())}` : "";
      const res = await fetch(`/api/categories${qs}`);
      if (!res.ok) throw new Error(String(res.status));
      const data = (await res.json()) as { category_m?: string[] };
      setCatMAll(data.category_m ?? []);
    } catch {
      setCatMAll([]);
    }
  }, []);

  const loadCategorySummaries = useCallback(async () => {
    setCatLSummary("loading");
    setCatMSummary("loading");
    try {
      // 대분류 요약(항상 전체)
      const pL = new URLSearchParams();
      pL.set("level", "category_l");
      pL.set("base_year", String(baseYear));
      pL.set("year", String(year));
      const rL = await fetch(`/api/category/summary?${pL.toString()}`);
      if (!rL.ok) throw new Error(String(rL.status));
      const dL = (await rL.json()) as { groups?: CategorySummaryRow[] };
      setCatLSummary(dL.groups ?? []);
    } catch {
      setCatLSummary("error");
    }

    try {
      // 중분류 요약(대분류 선택 시에만 제한)
      const pM = new URLSearchParams();
      pM.set("level", "category_m");
      pM.set("base_year", String(baseYear));
      pM.set("year", String(year));
      if (catLSelected.trim()) pM.set("category_l", catLSelected.trim());
      const rM = await fetch(`/api/category/summary?${pM.toString()}`);
      if (!rM.ok) throw new Error(String(rM.status));
      const dM = (await rM.json()) as { groups?: CategorySummaryRow[] };
      setCatMSummary(dM.groups ?? []);
    } catch {
      setCatMSummary("error");
    }
  }, [baseYear, year, catLSelected]);

  const loadCategoryTop5 = useCallback(
    async (level: "category_l" | "category_m", key: string) => {
      setCatTop5("loading");
      try {
        const params = new URLSearchParams();
        params.set("level", level);
        params.set("key", key);
        params.set("base_year", String(baseYear));
        params.set("year", String(year));
        if (level === "category_m" && catLSelected.trim()) {
          params.set("category_l", catLSelected.trim());
        }
        const res = await fetch(`/api/category/top5?${params.toString()}`);
        if (!res.ok) throw new Error(String(res.status));
        const data = (await res.json()) as {
          key: string;
          top_undervalued?: CategoryTop5Row[];
          top_overvalued?: CategoryTop5Row[];
          n_scored?: number;
        };
        setCatTop5({
          key: data.key,
          undervalued: data.top_undervalued ?? [],
          overvalued: data.top_overvalued ?? [],
          n_scored: Number(data.n_scored ?? 0),
        });
      } catch {
        setCatTop5("error");
      }
    },
    [baseYear, year, catLSelected]
  );

  const handleQueryInput = (v: string) => {
    setQInput(v);
    setPage(1);
    if (searchTimer.current !== null) clearTimeout(searchTimer.current);
    if (!v.trim()) {
      searchTimer.current = null;
      setQApplied("");
      return;
    }
    searchTimer.current = setTimeout(() => {
      setQApplied(v);
      searchTimer.current = null;
    }, 250);
  };

  const reload = () => {
    setQApplied(qInput.trim());
  };

  const onBaseYearChange = (y: number) => {
    setBaseYear(y);
    setYear(y);
    setPage(1);
  };

  const onYearChange = (y: number) => {
    setYear(y);
    setMeta((m) => m.replace(/표시연도: .+$/, `표시연도: ${y}`));
  };

  const onSortClick = (key: string, type: string) => {
    setSort((s) => {
      if (s.key === key) {
        return { key, dir: s.dir === "asc" ? "desc" : "asc", type };
      }
      return { key, dir: "asc", type };
    });
    setPage(1);
  };

  const loadTop5News = async () => {
    setNewsRows("loading");
    try {
      const res = await fetch(`/api/top5-news?base_year=${encodeURIComponent(baseYear)}&per_company=10`);
      if (!res.ok) {
        const t = await res.text();
        throw new Error(`API 오류: ${res.status} ${t.slice(0, 120)}`);
      }
      const data = (await res.json()) as { rows?: NewsRow[] };
      const list = data.rows ?? [];
      setNewsRows(list.length ? list : "empty");
    } catch {
      setNewsRows("error");
    }
  };

  const toggleTop5 = async () => {
    const next = !top5Open;
    setTop5Open(next);
    if (next) await loadTop5News();
  };

  const startFillPoll = (label: string, qForLabel: string) => {
    clearPoll();
    const run = async (jobId: string) => {
      try {
        const res = await fetch(`/api/admin/fill/${encodeURIComponent(jobId)}`);
        const st = (await res.json()) as {
          requested: number;
          done: number;
          ok: number;
          fail: number;
          last_ticker?: string;
          finished_at?: number | null;
        };
        const pct = st.requested > 0 ? `${Math.floor((st.done / st.requested) * 100)}%` : "100%";
        const tail = st.last_ticker ? ` · 최근: ${st.last_ticker}` : "";
        setStatusHtml(
          `<strong>${qForLabel || "전체"}</strong> ${label}: ${pct} (${st.done}/${st.requested}) · 성공 ${st.ok} · 데이터 없음 ${st.fail}${tail}`
        );
        if (st.finished_at) {
          await load();
          return;
        }
        pollTimer.current = setTimeout(() => void run(jobId), 1500);
      } catch {
        setStatusHtml(`<strong>${qForLabel || "전체"}</strong> 진행 확인 실패`);
      }
    };
    return run;
  };

  const baseYearSelectOpts = useMemo(() => [cy, cy + 1], [cy]);

  // when switching to category view, load lists/summary
  useEffect(() => {
    if (view !== "category") return;
    void loadCategoryLs();
  }, [view, loadCategoryLs]);

  useEffect(() => {
    if (view !== "category") return;
    // 대분류가 비어있으면(전체) 중분류는 전체를 가져옴
    void loadCategoryMs(catLSelected);
  }, [view, loadCategoryMs, catLSelected]);

  useEffect(() => {
    if (view !== "category") return;
    void loadCategorySummaries();
  }, [view, loadCategorySummaries]);

  const filteredM = catMAll;

  return (
    <div className="layout">
      <aside className="sidebar">
        <div className="brand">kr-analyze</div>
        <div className="sidebar-section-title">Menu</div>
        <div className="nav">
          <button
            type="button"
            className={view === "table" ? "active" : ""}
            onClick={() => setView("table")}
          >
            종목 표
          </button>
          <button
            type="button"
            className={view === "category" ? "active" : ""}
            onClick={() => setView("category")}
          >
            카테고리 분석
          </button>
        </div>
        <div className="foot" style={{ marginTop: 12 }}>
          서버 기준일: <span className="kbd">{meta.includes("기준일") ? meta.split("기준일 ")[1]?.split(" ·")[0] : "-"}</span>
        </div>
      </aside>

      <main className="content">
        <div className="wrap">
          {view === "table" ? (
            <>
              <div className="header">
        <div>
          <h1>
            kr-analyze
            <span className="pill strong">컨센서스 · 적정주가 · 괴리율</span>
          </h1>
          <div className="sub">
            국내 상장기업(주 1회 갱신) · 현재주가(네이버 등) · FnGuide 컨센서스(연도 선택)
            <span className="pill">정렬: 컬럼명 클릭</span>
            <span className="pill">검색: 입력 후 0.25초</span>
          </div>
        </div>
        <div className="controls">
          <input
            value={qInput}
            onChange={(e) => handleQueryInput(e.target.value)}
            placeholder="기업명/종목코드 검색 (예: 삼성전자 또는 005930)"
          />
          <button type="button" onClick={reload}>
            조회
          </button>
          <select
            title="기준년도(3개 연도 창 시작)"
            value={baseYear}
            onChange={(e) => onBaseYearChange(Number(e.target.value))}
          >
            {baseYearSelectOpts.map((y) => (
              <option key={y} value={y}>
                {y}년
              </option>
            ))}
          </select>
          <select title="표시할 연도(기준년도~+2)" value={year} onChange={(e) => onYearChange(Number(e.target.value))}>
            {yearOptions.map((y) => (
              <option key={y} value={y}>
                {y}년
              </option>
            ))}
          </select>
          <div className="sep" />
          <button
            type="button"
            className="secondary"
            title="현재 검색어로 1개 종목의 현재가만 갱신"
            onClick={async () => {
              const q = qInput.trim();
              if (!q) {
                setStatusHtml("검색어를 입력해주세요. (기업명 또는 종목코드)");
                return;
              }
              setStatusHtml(`<strong>${q}</strong> 현재가 갱신 중… (네이버)`);
              try {
                const r = (await postJson(`/api/admin/refresh/price_by_query?q=${encodeURIComponent(q)}`)) as {
                  ticker?: string;
                  current_price?: number | null;
                };
                setStatusHtml(
                  `<strong>${r.ticker}</strong> 현재가를 갱신했어요 · 현재가 ${fmtNum(r.current_price)}`
                );
                await load();
              } catch (e) {
                setStatusHtml(`<strong>${q}</strong> 현재가 갱신 실패: ${e instanceof Error ? e.message : String(e)}`);
              }
            }}
          >
            현재가 갱신
          </button>
          <button
            type="button"
            className="secondary"
            disabled={consensusBusy}
            title="현재 검색어로 1개 종목의 컨센서스만 갱신"
            onClick={async () => {
              const q = qInput.trim();
              if (!q) {
                setStatusHtml("검색어를 입력해주세요. (기업명 또는 종목코드)");
                return;
              }
              setConsensusBusy(true);
              setStatusHtml(`<strong>${q}</strong> 컨센서스 갱신 중… (FnGuide)`);
              try {
                const r = (await postJson(
                  `/api/admin/refresh/consensus_by_query?q=${encodeURIComponent(q)}&primary_year=${encodeURIComponent(baseYear)}`
                )) as { ticker?: string; skipped?: boolean; consensus_primary_year?: number };
                if (r.skipped) {
                  setStatusHtml(`<strong>${r.ticker}</strong> 컨센서스는 오늘 이미 조회돼서 다시 가져오지 않았어요`);
                } else {
                  setStatusHtml(
                    `<strong>${r.ticker}</strong> 컨센서스를 갱신했어요 · 기준년도 ${r.consensus_primary_year}`
                  );
                }
                await load();
              } catch (e) {
                setStatusHtml(`<strong>${q}</strong> 컨센서스 갱신 실패: ${e instanceof Error ? e.message : String(e)}`);
              } finally {
                setConsensusBusy(false);
              }
            }}
          >
            {consensusBusy ? "갱신 중…" : "컨센서스 갱신"}
          </button>
          <button type="button" className="secondary" onClick={() => setAdvancedOpen((v) => !v)}>
            고급
          </button>
          <button type="button" className="secondary" onClick={() => void toggleTop5()}>
            상위 5개 뉴스
          </button>
        </div>
      </div>

      <div className="card">
        {advancedOpen ? (
          <div style={{ marginBottom: 10 }}>
            <div className="controls" style={{ marginBottom: 8 }}>
              <button
                type="button"
                className="secondary"
                onClick={async () => {
                  const q = qInput.trim();
                  setStatusHtml(`<strong>${q || "전체"}</strong> 현재가 모두 채우기 시작… (백그라운드)`);
                  try {
                    const start = (await postJson(`/api/admin/fill_price?q=${encodeURIComponent(q)}&limit=2000`)) as {
                      job_id: string;
                      requested: number;
                    };
                    const qLabel = q || "전체";
                    setStatusHtml(
                      `<strong>${qLabel}</strong> 현재가를 불러오기 시작했어요 · 대상 ${start.requested}개 · 진행 상황을 확인하는 중…`
                    );
                    void startFillPoll("현재가 모두 채우기", qLabel)(start.job_id);
                  } catch (e) {
                    setStatusHtml(
                      `<strong>${q || "전체"}</strong> 현재가 모두 채우기 시작 실패: ${e instanceof Error ? e.message : String(e)}`
                    );
                  }
                }}
              >
                현재가 모두 채우기
              </button>
              <button
                type="button"
                className="secondary"
                onClick={async () => {
                  const q = qInput.trim();
                  setStatusHtml(`<strong>${q || "전체"}</strong> 컨센서스 모두 채우기 시작… (백그라운드)`);
                  try {
                    const start = (await postJson(
                      `/api/admin/fill_consensus?q=${encodeURIComponent(q)}&limit=2000&only_missing=true&primary_year=${encodeURIComponent(baseYear)}`
                    )) as { job_id: string; requested: number };
                    const qLabel = q || "전체";
                    setStatusHtml(
                      `<strong>${qLabel}</strong> 컨센서스를 불러오기 시작했어요 · 대상 ${start.requested}개 · 진행 상황을 확인하는 중…`
                    );
                    void startFillPoll("컨센서스 모두 채우기", qLabel)(start.job_id);
                  } catch (e) {
                    setStatusHtml(
                      `<strong>${q || "전체"}</strong> 컨센서스 모두 채우기 시작 실패: ${e instanceof Error ? e.message : String(e)}`
                    );
                  }
                }}
              >
                컨센서스 모두 채우기
              </button>
              <button
                type="button"
                className="secondary"
                onClick={async () => {
                  const q = qInput.trim();
                  setStatusHtml(`<strong>${q || "전체"}</strong> 값 채우기 시작… (백그라운드)`);
                  try {
                    const start = (await postJson(
                      `/api/admin/fill?q=${encodeURIComponent(q)}&limit=2000&only_missing=true`
                    )) as { job_id: string; requested: number };
                    const qLabel = q || "전체";
                    setStatusHtml(
                      `<strong>${qLabel}</strong> 값을 불러오기 시작했어요 · 대상 ${start.requested}개 · 진행 상황을 확인하는 중…`
                    );
                    void startFillPoll("채우기", qLabel)(start.job_id);
                  } catch (e) {
                    setStatusHtml(`<strong>${q || "전체"}</strong> 채우기 시작 실패: ${e instanceof Error ? e.message : String(e)}`);
                  }
                }}
              >
                전체 채우기
              </button>
              <div className="sep" />
              <button
                type="button"
                className="secondary"
                onClick={async () => {
                  setStatusHtml("<strong>상장사목록</strong> 갱신 중…");
                  try {
                    const r = (await postJson("/api/admin/refresh/companies")) as {
                      count?: number;
                      upserts?: number;
                      asof?: string;
                    };
                    setStatusHtml(
                      `<strong>상장사목록</strong> 갱신 완료: ${r.count}개 (업서트 ${r.upserts}개) · ${r.asof}`
                    );
                    await load();
                  } catch (e) {
                    setStatusHtml(`<strong>상장사목록</strong> 갱신 실패: ${e instanceof Error ? e.message : String(e)}`);
                  }
                }}
              >
                상장사목록 갱신
              </button>
            </div>
            <div className="foot">
              고급 작업은 외부 호출이 많아 느릴 수 있어요. 평소에는 <span className="kbd">현재가 갱신</span> /{" "}
              <span className="kbd">컨센서스 갱신</span>만으로 충분합니다.
            </div>
          </div>
        ) : null}

        {top5Open ? (
          <div style={{ marginBottom: 10 }}>
            <div className="mini" style={{ marginBottom: 8 }}>
              괴리율 양수 상위 5개 기업의 최신 기사(기업당 최대 10개)
            </div>
            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>기업명</th>
                    <th>키워드</th>
                    <th>뉴스 제목</th>
                    <th>긍정/부정</th>
                    <th>언론사</th>
                    <th>일자</th>
                  </tr>
                </thead>
                <tbody>
                  {newsRows === "loading" ? (
                    <tr>
                      <td colSpan={6} style={{ textAlign: "center" }}>
                        뉴스를 불러오는 중…
                      </td>
                    </tr>
                  ) : newsRows === "empty" ? (
                    <tr>
                      <td colSpan={6} style={{ textAlign: "center" }}>
                        표시할 뉴스가 없습니다.
                      </td>
                    </tr>
                  ) : newsRows === "error" ? (
                    <tr>
                      <td colSpan={6} style={{ textAlign: "center" }}>
                        뉴스를 불러오지 못했습니다.
                      </td>
                    </tr>
                  ) : (
                    newsRows.map((r, i) => {
                      const sentimentCls = r.sentiment === "긍정" ? "pos" : r.sentiment === "부정" ? "neg" : "";
                      const title = (r.title || "").trim();
                      const safeTitle = title || "-";
                      const href = r.link && r.link !== "-" ? r.link : "";
                      return (
                        <tr key={`${r.link}-${i}`}>
                          <td>{r.company_name || "-"}</td>
                          <td>{r.keyword || "기타"}</td>
                          <td>
                            {href ? (
                              <a className="news-link" href={href} target="_blank" rel="noopener noreferrer">
                                {safeTitle}
                              </a>
                            ) : (
                              safeTitle
                            )}
                          </td>
                          <td className={sentimentCls}>{r.sentiment || "-"}</td>
                          <td>{r.press || "-"}</td>
                          <td>{r.date || "-"}</td>
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
          </div>
        ) : null}

        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                {MAIN_COLUMNS.map((col) => (
                  <th
                    key={col.key}
                    className={`sortable${sort.key === col.key ? " active" : ""}`}
                    onClick={() => onSortClick(col.key, col.type)}
                  >
                    {col.label}{" "}
                    <span className="sort" aria-hidden="true">
                      {sort.key === col.key ? (sort.dir === "asc" ? "▲" : "▼") : ""}
                    </span>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {displayRows.map((r) => {
                const gap = r.gap_ratio;
                const cls = gap === null || gap === undefined ? "" : gap >= 0 ? "pos" : "neg";
                const catL = (r.category_l || "").trim();
                const catM = (r.category_m || "").trim();
                const catLShort = catL.length > 14 ? `${catL.slice(0, 14)}…` : catL;
                const catMShort = catM.length > 18 ? `${catM.slice(0, 18)}…` : catM;
                return (
                  <tr key={r.ticker}>
                    <td title={catL}>{catL ? catLShort : "-"}</td>
                    <td title={catM}>{catM ? catMShort : "-"}</td>
                    <td>
                      {r.name} <span className="pill">{r.ticker}</span>
                    </td>
                    <td>{fmtNum(r.current_price)}</td>
                    <td>{fmtFloat(r.pbr)}</td>
                    <td>{fmtFloat(r.per)}</td>
                    <td>{fmtNum(r.eps)}</td>
                    <td>{fmtNum(r.fair_price)}</td>
                    <td className={cls}>{fmtPct(r.gap_ratio)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        <div className="controls pager" style={{ marginTop: 10, justifyContent: "space-between" }}>
          <div className="controls" style={{ gap: 8 }}>
            <button
              type="button"
              className="secondary"
              disabled={page <= 1}
              onClick={() => setPage((p) => Math.max(1, p - 1))}
            >
              이전
            </button>
            <div className="pill">
              페이지 {page}/{totalPages}
            </div>
            <button
              type="button"
              className="secondary"
              disabled={page >= totalPages}
              onClick={() => setPage((p) => p + 1)}
            >
              다음
            </button>
          </div>
          <div className="controls" style={{ gap: 8 }}>
            <span className="pill">이동</span>
            <input
              type="number"
              min={1}
              max={totalPages}
              value={page}
              onChange={(e) => {
                const v = Number(e.target.value);
                if (!Number.isFinite(v)) return;
                setPage(Math.max(1, Math.min(totalPages, Math.floor(v))));
              }}
            />
            <span className="pill">표시</span>
            <select
              value={pageSize}
              onChange={(e) => {
                setPageSize(Number(e.target.value) || 200);
                setPage(1);
              }}
            >
              <option value={50}>50</option>
              <option value={100}>100</option>
              <option value={200}>200</option>
              <option value={500}>500</option>
            </select>
          </div>
        </div>

        <div className="controls pager" style={{ justifyContent: "space-between" }}>
          <div className="mini">{meta}</div>
          <div className="mini">팁: 괴리율 정렬은 서버 기준 상위부터 표시됩니다.</div>
        </div>
        <div className="status" dangerouslySetInnerHTML={{ __html: statusHtml }} />
      </div>
            </>
          ) : (
            <>
              <div className="header">
                <div>
                  <h1>
                    카테고리 분석 <span className="pill strong">Top5 · 평균 괴리율</span>
                  </h1>
                  <div className="sub">
                    평균 괴리율 \(gap_ratio\)이 <span className="pill">+</span>면 저평가(적정주가 &gt; 현재가),
                    <span className="pill">-</span>면 고평가(적정주가 &lt; 현재가)로 해석합니다.
                  </div>
                </div>
              </div>

              <div className="stack">
                <div className="panel">
                  <div className="panel-title">
                    <h2>필터</h2>
                    <button
                      type="button"
                      className="secondary"
                      onClick={() => {
                        setCatTop5(null);
                        void loadCategorySummaries();
                      }}
                    >
                      새로고침
                    </button>
                  </div>

                  <div className="grid2">
                    <div className="field">
                      <div className="label">대분류</div>
                      <select
                        value={catLSelected}
                        onChange={(e) => {
                          const v = e.target.value;
                          setCatLSelected(v);
                          setCatMSelected("");
                          setCatTop5(null);
                          setTop5Level("category_m");
                        }}
                      >
                        <option value="">대분류 전체</option>
                        {catLAll.map((v) => (
                          <option key={v} value={v}>
                            {v}
                          </option>
                        ))}
                      </select>
                      <div className="mini" style={{ marginTop: 8 }}>
                        기준년도 {baseYear} · 표시연도 {year}
                      </div>
                    </div>

                    <div className="field">
                      <div className="label">중분류</div>
                      <select
                        value={catMSelected}
                        onChange={(e) => {
                          const v = e.target.value;
                          setCatMSelected(v);
                          setCatTop5(null);
                          if (v) {
                            setTop5Level("category_m");
                            void loadCategoryTop5("category_m", v);
                          }
                        }}
                      >
                        <option value="">중분류 선택(선택 안 하면 요약만)</option>
                        {filteredM.map((v) => (
                          <option key={v} value={v}>
                            {v}
                          </option>
                        ))}
                      </select>

                      <div className="mini" style={{ marginTop: 8 }}>
                        대분류 선택 시 중분류 목록이 바뀝니다. (대분류 전체면 전체 중분류)
                      </div>
                    </div>
                  </div>
                </div>

                <div className="card">
                  <div className="controls" style={{ justifyContent: "space-between" }}>
                    <div className="mini">팁: 요약 행을 클릭하면 그 분류의 Top5를 불러옵니다.</div>
                    <div className="mini">요약은 평균 괴리율 기준으로 정렬됩니다.</div>
                  </div>

                  <div className="grid2">
                    <div className="table-scroll">
                      <table>
                        <thead>
                          <tr>
                            <th>대분류</th>
                            <th>평균 괴리율</th>
                            <th>표본(괴리율)</th>
                            <th>표본(전체)</th>
                          </tr>
                        </thead>
                        <tbody>
                          {catLSummary === "loading" ? (
                            <tr>
                              <td colSpan={4} style={{ textAlign: "center" }}>
                                불러오는 중…
                              </td>
                            </tr>
                          ) : catLSummary === "error" ? (
                            <tr>
                              <td colSpan={4} style={{ textAlign: "center" }}>
                                요약을 불러오지 못했습니다.
                              </td>
                            </tr>
                          ) : (
                            catLSummary.map((g) => {
                              const cls = g.avg_gap_ratio >= 0 ? "pos" : "neg";
                              return (
                                <tr
                                  key={g.key}
                                  style={{ cursor: "pointer" }}
                                  onClick={() => {
                                    setCatLSelected(g.key);
                                    setCatMSelected("");
                                    setTop5Level("category_l");
                                    void loadCategoryTop5("category_l", g.key);
                                  }}
                                  title="클릭하면 Top5를 불러옵니다"
                                >
                                  <td>{g.key}</td>
                                  <td className={cls}>{fmtPct(g.avg_gap_ratio)}</td>
                                  <td>{fmtNum(g.n_with_gap)}</td>
                                  <td>{fmtNum(g.n_total)}</td>
                                </tr>
                              );
                            })
                          )}
                        </tbody>
                      </table>
                    </div>

                    <div className="table-scroll">
                      <table>
                        <thead>
                          <tr>
                            <th>중분류</th>
                            <th>평균 괴리율</th>
                            <th>표본(괴리율)</th>
                            <th>표본(전체)</th>
                          </tr>
                        </thead>
                        <tbody>
                          {catMSummary === "loading" ? (
                            <tr>
                              <td colSpan={4} style={{ textAlign: "center" }}>
                                불러오는 중…
                              </td>
                            </tr>
                          ) : catMSummary === "error" ? (
                            <tr>
                              <td colSpan={4} style={{ textAlign: "center" }}>
                                요약을 불러오지 못했습니다.
                              </td>
                            </tr>
                          ) : (
                            catMSummary.map((g) => {
                              const cls = g.avg_gap_ratio >= 0 ? "pos" : "neg";
                              return (
                                <tr
                                  key={g.key}
                                  style={{ cursor: "pointer" }}
                                  onClick={() => {
                                    setCatMSelected(g.key);
                                    setTop5Level("category_m");
                                    void loadCategoryTop5("category_m", g.key);
                                  }}
                                  title="클릭하면 Top5를 불러옵니다"
                                >
                                  <td>{g.key}</td>
                                  <td className={cls}>{fmtPct(g.avg_gap_ratio)}</td>
                                  <td>{fmtNum(g.n_with_gap)}</td>
                                  <td>{fmtNum(g.n_total)}</td>
                                </tr>
                              );
                            })
                          )}
                        </tbody>
                      </table>
                    </div>
                  </div>

                {catTop5 ? (
                  <div style={{ marginTop: 12 }}>
                    {catTop5 === "loading" ? (
                      <div className="mini">Top5를 불러오는 중…</div>
                    ) : catTop5 === "error" ? (
                      <div className="mini">Top5를 불러오지 못했습니다.</div>
                    ) : (
                      <>
                        <div className="mini" style={{ marginBottom: 8 }}>
                          선택 분류:{" "}
                          <span className="pill strong">
                            {top5Level === "category_l" ? "대분류" : "중분류"} · {catTop5.key}
                          </span>{" "}
                          (표본 {catTop5.n_scored}개)
                        </div>
                        <div className="controls" style={{ alignItems: "stretch" }}>
                          <div style={{ flex: "1 1 360px" }}>
                            <div className="mini" style={{ marginBottom: 6 }}>
                              저평가 Top5 (괴리율 +)
                            </div>
                            <div className="table-scroll">
                              <table>
                                <thead>
                                  <tr>
                                    <th>기업</th>
                                    <th>괴리율</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {catTop5.undervalued.length ? (
                                    catTop5.undervalued.map((r) => (
                                      <tr key={r.ticker}>
                                        <td>
                                          {r.name} <span className="pill">{r.ticker}</span>
                                        </td>
                                        <td className="pos">{fmtPct(r.gap_ratio)}</td>
                                      </tr>
                                    ))
                                  ) : (
                                    <tr>
                                      <td colSpan={2} style={{ textAlign: "center" }}>
                                        표시할 항목이 없습니다.
                                      </td>
                                    </tr>
                                  )}
                                </tbody>
                              </table>
                            </div>
                          </div>
                          <div style={{ flex: "1 1 360px" }}>
                            <div className="mini" style={{ marginBottom: 6 }}>
                              고평가 Top5 (괴리율 -)
                            </div>
                            <div className="table-scroll">
                              <table>
                                <thead>
                                  <tr>
                                    <th>기업</th>
                                    <th>괴리율</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {catTop5.overvalued.length ? (
                                    catTop5.overvalued.map((r) => (
                                      <tr key={r.ticker}>
                                        <td>
                                          {r.name} <span className="pill">{r.ticker}</span>
                                        </td>
                                        <td className="neg">{fmtPct(r.gap_ratio)}</td>
                                      </tr>
                                    ))
                                  ) : (
                                    <tr>
                                      <td colSpan={2} style={{ textAlign: "center" }}>
                                        표시할 항목이 없습니다.
                                      </td>
                                    </tr>
                                  )}
                                </tbody>
                              </table>
                            </div>
                          </div>
                        </div>
                      </>
                    )}
                  </div>
                ) : null}
                </div>
              </div>
            </>
          )}
        </div>
      </main>
    </div>
  );
}
