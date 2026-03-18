from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import Column, Text
from sqlmodel import Field, SQLModel


class Company(SQLModel, table=True):
    ticker: str = Field(primary_key=True, index=True)  # 6-digit, e.g. 005930
    name: str = Field(index=True)
    market: Optional[str] = Field(default=None, index=True)  # KOSPI/KOSDAQ/KONEX etc.
    # FnGuide 분류(표시용): 예) "코스피 전기·전자", "FICS 반도체 및 관련장비"
    category_l: Optional[str] = Field(default=None, index=True)
    category_m: Optional[str] = Field(default=None, index=True)
    updated_at: datetime = Field(default_factory=datetime.utcnow, index=True)


class Snapshot(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    ticker: str = Field(index=True)
    asof: str = Field(index=True)  # e.g. 2026-03-17

    current_price: Optional[int] = Field(default=None)

    # consensus (26y) - floats can be null
    pbr_26y: Optional[float] = Field(default=None)
    per_26y: Optional[float] = Field(default=None)
    eps_26y: Optional[float] = Field(default=None)

    # year -> {pbr, per, eps} as JSON string
    consensus_json: Optional[str] = Field(default=None, sa_column=Column(Text))
    consensus_primary_year: Optional[int] = Field(default=None, index=True)

    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)

