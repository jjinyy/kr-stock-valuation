from __future__ import annotations

from pathlib import Path

from sqlmodel import Session, SQLModel, create_engine
import sqlite3

DB_PATH = Path(__file__).resolve().parent.parent / "data.sqlite3"
engine = create_engine(
    f"sqlite:///{DB_PATH}",
    echo=False,
    connect_args={"check_same_thread": False},
)


def _sqlite_ensure_columns() -> None:
    """
    SQLite는 create_all이 기존 테이블에 컬럼을 추가해주지 않으므로,
    최소한의 마이그레이션(ADD COLUMN)만 수행합니다.
    """
    if not DB_PATH.exists():
        return
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()
        def cols_for(table: str) -> set[str]:
            cur.execute(f"PRAGMA table_info({table})")
            return {row[1] for row in cur.fetchall()}  # name at index 1

        snapshot_cols = cols_for("snapshot")
        company_cols = cols_for("company")

        alters: list[str] = []
        if "category_l" not in company_cols:
            alters.append("ALTER TABLE company ADD COLUMN category_l TEXT")
        if "category_m" not in company_cols:
            alters.append("ALTER TABLE company ADD COLUMN category_m TEXT")
        if "consensus_json" not in snapshot_cols:
            alters.append("ALTER TABLE snapshot ADD COLUMN consensus_json TEXT")
        if "consensus_primary_year" not in snapshot_cols:
            alters.append("ALTER TABLE snapshot ADD COLUMN consensus_primary_year INTEGER")

        for sql in alters:
            cur.execute(sql)
        # /api/rows: asof+티커별 최신 스냅샷 조회(윈도우 쿼리) 속도 향상
        cur.execute(
            "CREATE INDEX IF NOT EXISTS ix_snapshot_asof_ticker_created "
            "ON snapshot (asof, ticker, created_at DESC)"
        )
        con.commit()
    finally:
        con.close()

def _sqlite_apply_pragmas() -> None:
    """
    동시성/응답성 개선:
    - WAL: 읽기/쓰기가 더 잘 공존
    - busy_timeout: 락 경합 시 즉시 실패 대신 대기
    """
    if not DB_PATH.exists():
        return
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute("PRAGMA busy_timeout=5000;")
        con.commit()
    finally:
        con.close()


def init_db() -> None:
    SQLModel.metadata.create_all(engine)
    _sqlite_ensure_columns()
    _sqlite_apply_pragmas()


def get_session() -> Session:
    return Session(engine)

