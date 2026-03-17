from __future__ import annotations

import argparse
import sys
from pathlib import Path

# allow `python scripts/refresh.py ...` without installing as a package
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.services.jobs import (
    refresh_companies_from_kind,
    refresh_snapshot_for_ticker,
    refresh_snapshots_for_all,
)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--companies", action="store_true", help="KIND 상장법인목록 갱신")
    p.add_argument("--ticker", type=str, default="", help="단일 종목 스냅샷 갱신 (예: 005930)")
    p.add_argument("--snapshots", action="store_true", help="여러 종목 스냅샷 갱신")
    p.add_argument("--limit", type=int, default=200, help="snapshots 대상 개수 제한")
    args = p.parse_args()

    if args.companies:
        print(refresh_companies_from_kind())
    if args.ticker:
        print(refresh_snapshot_for_ticker(ticker=args.ticker))
    if args.snapshots:
        print(refresh_snapshots_for_all(limit=args.limit))


if __name__ == "__main__":
    main()

