from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class CalcResult:
    fair_price: Optional[int]
    gap_ratio: Optional[float]


def calc_fair_price_and_gap(
    *,
    current_price: Optional[int],
    pbr: Optional[float],
    per: Optional[float],
    eps: Optional[float],
) -> CalcResult:
    """
    사용자 정의 공식:
      적정주가 = (PBR / PER) × 100 × EPS
      괴리율 = (적정주가 - 현재주가) / 현재주가
    """
    if pbr is None or per is None or eps is None:
        return CalcResult(fair_price=None, gap_ratio=None)

    if per == 0:
        return CalcResult(fair_price=None, gap_ratio=None)

    fair = (pbr / per) * 100.0 * eps
    if fair != fair:  # NaN
        return CalcResult(fair_price=None, gap_ratio=None)

    fair_i = int(round(fair))
    if current_price is None or current_price <= 0:
        return CalcResult(fair_price=fair_i, gap_ratio=None)
    return CalcResult(fair_price=fair_i, gap_ratio=(fair_i - current_price) / current_price)

