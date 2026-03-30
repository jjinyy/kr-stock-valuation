# KR Stock Valuation Dashboard

A web dashboard that calculates the fair value of KRX-listed stocks using FnGuide consensus estimates (PBR/PER/EPS) and compares them against current market prices.

[![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=flat-square&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)

---

## What it does

Pulls FnGuide 2026 consensus data (PBR/PER/EPS) for all KRX-listed companies, calculates a fair value price, then shows the gap against the current market price in a searchable table.

Fair value formula: `(PBR / PER) × 100 × EPS`

| Column | Description |
|---|---|
| Company | KRX-listed stock |
| Current price | Real-time from Naver Finance |
| PBR / PER / EPS | FnGuide 2026 consensus |
| Fair value | (PBR / PER) × 100 × EPS |
| Gap ratio | (Fair value − Current price) / Current price |

---

## Project structure
```
analyze/
├── app/
│   ├── api.py          # FastAPI routes (data fetch + admin refresh)
│   ├── db.py           # SQLModel session
│   ├── models.py       # Company / Snapshot models
│   ├── services/       # FnGuide scraping, fair value calculation
│   └── web/
│       └── dist/       # React production build (`frontend`에서 `npm run build`, gitignored)
├── frontend/           # Vite + React + TypeScript UI (`npm run dev` / `npm run build`)
├── scripts/            # Batch scripts
└── requirements.txt
```

---

## Getting started

### Backend / production-style (built UI)

Requires **Node.js 18+** once, to build the SPA into `app/web/dist/`:

```bash
cd frontend
npm install
npm run build
cd ..
```

Then run the API (serves React from `app/web/dist/` when present):

```bash
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Open `http://127.0.0.1:8000` in your browser.

### Local UI development (hot reload)

Terminal 1 — FastAPI:

```bash
uvicorn app.main:app --reload
```

Terminal 2 — Vite dev server (proxies `/api` and `/health` to port 8000):

```bash
cd frontend
npm install
npm run dev
```

Open `http://127.0.0.1:5173`. `http://127.0.0.1:8000` 는 `app/web/dist/` 가 있을 때만 같은 UI를 제공합니다(없으면 빌드 안내 페이지).

---

## Tech stack

| Layer | Technology |
|---|---|
| Backend | Python, FastAPI, SQLModel |
| Frontend | React 18, TypeScript, Vite 5 |
| Data collection | httpx, BeautifulSoup4 (FnGuide / Naver Finance) |
| Scheduling | APScheduler (weekly auto-refresh) |
| Storage | SQLite |

---

## Disclaimer

Fair value estimates are based on analyst consensus data from FnGuide. This tool is for reference only and should not be used as the sole basis for investment decisions.