"""Reference implementations of the certified wealth-management metrics.

The gold SQL layer computes these at scale; the functions here are the *specification*
(used in unit tests to prove the SQL is right, and in notebooks for ad-hoc analysis).
Formulas are documented in docs/metric_definitions.md.
"""
from __future__ import annotations

import math
from typing import Iterable, Sequence


def daily_return(mv_end: float, mv_begin: float | None, net_external_flow: float) -> float | None:
    """Daily net-of-fee return assuming external flows arrive at end of day.

    r_t = (MV_t - MV_{t-1} - F_t) / MV_{t-1}. Undefined (None) when there is no
    positive opening value (new or funded-today accounts).
    """
    if mv_begin is None or mv_begin <= 0:
        return None
    return (mv_end - mv_begin - net_external_flow) / mv_begin


def chain_link(returns: Iterable[float | None]) -> float:
    """Geometric linking of sub-period returns: prod(1 + r_i) - 1 (None treated as 0)."""
    acc = 0.0
    for r in returns:
        if r is None or (isinstance(r, float) and math.isnan(r)):
            continue
        acc += math.log1p(r)
    return math.expm1(acc)


def modified_dietz(mv_begin: float, mv_end: float, flows: Sequence[tuple[float, float]]) -> float | None:
    """Modified Dietz return. ``flows`` = [(amount, weight)] where weight is the fraction
    of the period the cash was invested: (period_days - day_of_flow) / period_days.
    """
    net = sum(a for a, _ in flows)
    denom = mv_begin + sum(a * w for a, w in flows)
    if denom <= 0:
        return None
    return (mv_end - mv_begin - net) / denom


def annualize(total_return: float, days: int, basis: int = 365) -> float:
    return (1 + total_return) ** (basis / days) - 1 if days > 0 else float("nan")


def organic_growth_rate(net_new_assets: float, aum_begin: float, periods_per_year: int = 12) -> float | None:
    """Annualized organic growth: NNA / beginning AUM, scaled to a year."""
    if aum_begin <= 0:
        return None
    return net_new_assets / aum_begin * periods_per_year


def return_on_assets_bps(revenue: float, avg_aum: float, periods_per_year: int = 12) -> float | None:
    """Annualized revenue yield on average AUM in basis points."""
    if avg_aum <= 0:
        return None
    return revenue / avg_aum * periods_per_year * 10_000


def nps(scores: Iterable[int]) -> float | None:
    """Net Promoter Score: %promoters (9-10) - %detractors (0-6), range -100..100."""
    s = list(scores)
    if not s:
        return None
    promoters = sum(1 for x in s if x >= 9)
    detractors = sum(1 for x in s if x <= 6)
    return 100.0 * (promoters - detractors) / len(s)


def sla_breach_rate(resolution_hours: Iterable[float], sla_hours: float = 72) -> float | None:
    h = [x for x in resolution_hours if x is not None]
    if not h:
        return None
    return sum(1 for x in h if x > sla_hours) / len(h)
