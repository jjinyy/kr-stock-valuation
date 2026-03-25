from __future__ import annotations

from apscheduler.schedulers.background import BackgroundScheduler

from app.services.jobs import refresh_companies_from_kind
from app.services.bulk import start_bulk_consensus_fill

def start_scheduler() -> BackgroundScheduler:
    """
    정기 작업 스케줄러.
    """
    scheduler = BackgroundScheduler(timezone="Asia/Seoul")
    # 주 1회(월 06:10): 상장사 목록 갱신
    scheduler.add_job(refresh_companies_from_kind, "cron", day_of_week="mon", hour=6, minute=10, id="kind_companies_weekly", replace_existing=True)
    # 일 1회(06:20): 컨센서스 갱신
    scheduler.add_job(
        lambda: start_bulk_consensus_fill(limit=2000),
        "cron",
        hour=6,
        minute=20,
        id="fnguide_consensus_daily",
        replace_existing=True,
    )
    scheduler.start()
    return scheduler

