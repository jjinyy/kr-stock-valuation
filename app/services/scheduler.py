from __future__ import annotations

from apscheduler.schedulers.background import BackgroundScheduler

from app.services.jobs import refresh_companies_from_kind

def start_scheduler() -> BackgroundScheduler:
    """
    1) 상장기업 목록(KIND 엑셀) 주 1회 갱신
    2) (추후) 현재가/컨센서스 수집 배치
    """
    scheduler = BackgroundScheduler(timezone="Asia/Seoul")
    # 주 1회: 월요일 06:10 (장 시작 전) 상장법인 목록 갱신
    scheduler.add_job(refresh_companies_from_kind, "cron", day_of_week="mon", hour=6, minute=10, id="kind_companies_weekly", replace_existing=True)
    scheduler.start()
    return scheduler

