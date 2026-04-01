# 최적화 정리 (Performance / Reliability)

이 문서는 **최적화를 위해 실제로 적용한 변경들**을 한 곳에 모아둔 기록입니다.  
“왜 느렸는지 → 무엇을 바꿨는지 → 어떻게 조절/확인하는지” 중심으로 정리합니다.

---

## 범위

- **포함**: 이 저장소에 반영된 코드/설정 기반 최적화(커밋/파일 기준), 그리고 최근 세션에서 반영한 개선.
- **미포함**: 저장소 밖 인프라(서버 스펙, 프록시/캐시, 운영 환경 튜닝) 최적화.

---

## 큰 그림 (병목 분류)

- **UI 체감 병목**: 버튼 클릭 후 “무슨 작업이 진행 중인지” 피드백 부족
- **DB 병목**: 대량 작업에서 N+1 조회, 잦은 `commit()`/세션 생성
- **외부 호출 병목**: (네이버/FnGuide) 요청 수가 많아질수록 네트워크 대기 + HTML 파싱 CPU 비용 증가
- **동시성/안정성 병목**: 스레드 풀에서 외부 HTTP 클라이언트를 안전하게 공유하기 어려움

---

## 1) 프론트엔드: 사용자 피드백(로딩/진행 표시)

### 적용

- **상단 진행 배너 + 스피너**: 작업 진행 상황을 즉시 표시
- **버튼 단위 busy 상태**: 실행 중 `disabled` + 라벨 변경(예: “갱신 중…”)
- **벌크 작업 진행률 표시**: 폴링 응답(done/requested 등)을 배너에 실시간 반영
- **검색어 없을 때 동작**:
  - 검색어가 있으면: 1개 종목 갱신
  - 검색어가 없으면: “전체 대상으로 모두 채우기(백그라운드)”로 실행

### 관련 파일

- `frontend/src/App.tsx`
- `frontend/src/App.css`

---

## 2) DB/SQLite: 동시성·조회 성능 기초 튜닝

### 적용

- **WAL + synchronous NORMAL + busy_timeout** 적용으로 읽기/쓰기 동시성 및 응답성 개선
- 스냅샷 조회용 인덱스 생성:
  - `snapshot(asof, ticker, created_at DESC)` (최신 스냅샷 빠르게 찾기)

### 관련 파일

- `app/db.py`

---

## 3) 벌크 작업(bulk fill): N+1 제거 + 처리량 튜닝 포인트 추가

### 3.1 N+1 조회 제거 (only_missing 필터링)

#### 문제

- `only_missing=true`에서 “오늘 스냅샷 존재 여부”를 종목마다 개별 조회 → DB 왕복이 티커 수만큼 증가(N+1)

#### 해결

- 오늘 스냅샷을 **한 번에 조회**해 `ticker -> latest snapshot` 맵으로 만들고, 메모리에서 필터링

#### 관련 파일

- `app/services/bulk.py` (`_latest_today_snapshots_map`, `start_bulk_fill`, `start_bulk_consensus_fill`)

### 3.2 워커 수(동시 처리) 환경변수화

- 기본값: 6
- 환경변수: `BULK_WORKERS`

#### 목적

- 서버/네트워크 상황에 따라 “너무 적어서 느림” 또는 “너무 많아서 외부 사이트 제한/실패 증가”를 피하려고 런타임에서 조절 가능하게 함

### 3.3 완료 시 처리량 로그 추가

- 벌크 완료 시 다음을 로그로 기록:
  - requested/done/ok/fail/workers/elapsed/rate(/s)

---

## 4) 벌크 작업: DB 쓰기 비용 줄이기(배치 commit)

### 문제

- 티커 1개 처리할 때마다 `get_session()` + `commit()`이 반복되면 DB 오버헤드가 누적됨

### 해결

- 벌크 처리에서 “티커 1개 = future” 방식 대신 **티커 묶음(chunk) = future**로 변경
- 워커(스레드)마다 **세션 1개를 재사용**하고, N건 처리마다 `commit()`
- 개별 티커 처리 중 예외가 나면 해당 티커만 실패 처리 + `rollback()`

### 조절 변수

- `BULK_COMMIT_EVERY` (기본 25)

### 관련 파일

- `app/services/bulk.py`
- `app/services/jobs.py` (`*_in_session` 함수들)

---

## 5) 외부 호출 최적화: 캐시 + 파싱 비용 절감

### 5.1 네이버 현재가: TTL 캐시 + 빠른 파싱

- **TTL 캐시**: `NAVER_PRICE_CACHE_TTL` (기본 60초, 0이면 비활성)
  - 같은 티커를 짧은 시간에 여러 번 요청하는 상황(단건+벌크/재시도/중복 클릭)에서 네트워크 호출 감소
- **정규식 빠른 파싱 우선** + 실패 시 BeautifulSoup fallback
  - 대량 처리 시 HTML 파싱 CPU 비용 감소

관련 파일: `app/services/naver.py`

### 5.2 FnGuide: HTML TTL 캐시

- **HTML TTL 캐시**: `FNGUIDE_HTML_CACHE_TTL` (기본 600초, 0이면 비활성)
  - 같은 티커에 대해 반복 fetch/parse 되는 비용 감소

관련 파일: `app/services/fnguide.py`

---

## 6) 동시성 안정성: 스레드별 httpx Client 분리

### 문제

- 벌크는 `ThreadPoolExecutor` 기반인데, `httpx.Client` 전역 공유는 스레드 환경에서 안전/성능 면에서 좋지 않을 수 있음

### 해결

- 스레드 로컬(thread-local)로 **스레드별 `httpx.Client`**를 생성/재사용(keep-alive 효율 유지)

관련 파일:

- `app/services/naver.py` (`_get_client`)
- `app/services/fnguide.py` (`_get_client`)

---

## 환경변수 요약 (Tuning)

| 변수 | 기본값 | 의미 |
|---|---:|---|
| `BULK_WORKERS` | 6 | 벌크 동시 처리 워커 수 |
| `BULK_COMMIT_EVERY` | 25 | 벌크에서 N건마다 DB commit |
| `NAVER_PRICE_CACHE_TTL` | 60 | 네이버 현재가 TTL 캐시(초), 0이면 끔 |
| `FNGUIDE_HTML_CACHE_TTL` | 600 | FnGuide HTML TTL 캐시(초), 0이면 끔 |

PowerShell 예시:

```powershell
$env:BULK_WORKERS=8
$env:BULK_COMMIT_EVERY=50
$env:NAVER_PRICE_CACHE_TTL=30
$env:FNGUIDE_HTML_CACHE_TTL=600
```

---

## 확인/측정 방법

- **서버 로그**에서 `bulk_*_fill finished ... rate=.../s`를 비교
- `BULK_WORKERS`, `BULK_COMMIT_EVERY`를 바꿔가며
  - 실패율(429/503/timeout) 증가 여부
  - 전체 완료 시간/처리량
  - DB 락/대기(busy_timeout) 체감
  를 같이 확인

---

## 관련 커밋(요약)

> 아래는 “최적화/속도 개선” 성격의 커밋들(최근 30개 로그 기준)입니다.

- `b9c4fdc` **Optimize snapshot lookup and update docs/UI**
  - DB/조회 최적화 및 문서/화면 개선 포함
- `d31dc99` **Speed up bulk refresh and reduce unnecessary external fetches**
  - 벌크 처리/외부 fetch 최적화 방향의 변경 포함
- `77f523e` **Stop tracking SQLite temp files and polish UI copy**
  - WAL/SHM 파일 git 추적 제거 + 전반 정리(일부 성능/운영 편의 포함)
- `a157906` **Add React frontend and category analytics**
  - React 전환 및 UI 구조 정리(UX/피드백 개선 기반)

