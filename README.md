# KR Stock Valuation Dashboard

A web dashboard that calculates the fair value of KRX-listed stocks using FnGuide consensus estimates (PBR/PER/EPS) and compares them against current market prices.

[![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=flat-square&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)

---

## What it does

Pulls FnGuide 2026 consensus data (PBR/PER/EPS) for all KRX-listed companies, calculates a fair value price, then shows the gap against the current market price in a searchable table.

Fair value formula: `PBR × PER × EPS`

| Column | Description |
|---|---|
| Company | KRX-listed stock |
| Current price | Real-time from Naver Finance |
| PBR / PER / EPS | FnGuide 2026 consensus |
| Fair value | PBR × PER × EPS |
| Gap ratio | (Fair value − Current price) / Current price |

---

## Project structure
```
kr-stock-valuation/
├── app/
│   ├── api.py          # FastAPI routes (data fetch + admin refresh)
│   ├── db.py           # SQLModel session
│   ├── models.py       # Company / Snapshot models
│   ├── services/       # FnGuide scraping, fair value calculation
│   └── web/            # Frontend
├── scripts/            # Batch scripts
└── requirements.txt
```

---

## Getting started
```bash
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Open `http://127.0.0.1:8000` in your browser.

---

## Tech stack

| Layer | Technology |
|---|---|
| Backend | Python, FastAPI, SQLModel |
| Data collection | httpx, BeautifulSoup4 (FnGuide / Naver Finance) |
| Scheduling | APScheduler (weekly auto-refresh) |
| Storage | SQLite |

---

## Disclaimer

Fair value estimates are based on analyst consensus data from FnGuide. This tool is for reference only and should not be used as the sole basis for investment decisions.