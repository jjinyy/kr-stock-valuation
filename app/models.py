from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import Column, Text
from sqlmodel import Field, SQLModel


class Company(SQLModel, table=True):
    ticker: str = Field(primary_key=True, index=True)  # e.g. 005930
    name: str = Field(index=True)
    market: Optional[str] = Field(default=None, index=True)  # KOSPI/KOSDAQ/KONEX
    category_l: Optional[str] = Field(default=None, index=True)
    category_m: Optional[str] = Field(default=None, index=True)
    updated_at: datetime = Field(default_factory=datetime.utcnow, index=True)


class Snapshot(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    ticker: str = Field(index=True)
    asof: str = Field(index=True)  # e.g. 2026-03-17

    current_price: Optional[int] = Field(default=None)

    # legacy(단일 연도) 호환용
    pbr_26y: Optional[float] = Field(default=None)
    per_26y: Optional[float] = Field(default=None)
    eps_26y: Optional[float] = Field(default=None)

    # year -> {pbr, per, eps}
    consensus_json: Optional[str] = Field(default=None, sa_column=Column(Text))
    consensus_primary_year: Optional[int] = Field(default=None, index=True)

    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)

