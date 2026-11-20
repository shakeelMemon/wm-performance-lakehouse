"""Incremental-impact measurement for randomized business initiatives.

Implements a household-level difference-in-differences (DiD) estimator with a
cluster (household) bootstrap, a pre-period parallel-trends check, and a
quantity-vs-quality guardrail classification. Pure numpy/pandas so it runs in a
serverless notebook, a job, or a unit test.

Why DiD on a randomized design? Randomization makes the simple post-period difference
unbiased, but DiD on each household's own change removes persistent between-household
variation (a UHNW family's flows dwarf a mass-affluent one's) and so tightens the
confidence interval considerably for heavy-tailed wealth metrics.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np
import pandas as pd

# metric -> (dimension, direction where "higher is better")
METRIC_REGISTRY: dict[str, tuple[str, int]] = {
    "net_flows_cad": ("Quantity", +1),
    "gross_inflows_cad": ("Quantity", +1),
    "deposit_count": ("Quantity", +1),
    "interactions": ("Quantity", +1),
    "digital_sessions": ("Quantity", +1),
    "digital_active_flag": ("Quantity", +1),
    "complaints": ("Quality", -1),
    "service_cases": ("Quality", -1),
    "avg_resolution_hours": ("Quality", -1),
    "attrition_flag": ("Quality", -1),
}
MIN_HOUSEHOLDS_PER_ARM = 30   # below this, report the estimate but make no call


@dataclass
class ImpactResult:
    campaign_id: str
    metric: str
    dimension: str
    n_treatment: int
    n_control: int
    pre_mean_treatment: float
    pre_mean_control: float
    post_mean_treatment: float
    post_mean_control: float
    did_estimate: float            # per household per month
    ci_low: float
    ci_high: float
    p_value: float
    relative_lift: float | None    # did / post control mean
    total_incremental: float       # did * treated households * post months
    pre_trend_gap: float           # slope difference in the pre-period (should be ~0)
    pre_trend_ok: bool
    verdict: str

    def as_dict(self) -> dict:
        return asdict(self)


def _household_changes(panel: pd.DataFrame, metric: str) -> pd.DataFrame:
    """Per household: mean metric in pre and post periods -> change."""
    g = panel.groupby(["household_id", "assignment_group", "period"])[metric].mean().unstack("period")
    g = g.dropna(subset=["pre", "post"]).reset_index()
    g["delta"] = g["post"] - g["pre"]
    return g


def _pre_trend_gap(panel: pd.DataFrame, metric: str) -> float:
    """Difference (T - C) of the linear slope of the monthly mean over the pre-period."""
    pre = panel[panel.period == "pre"]
    if pre.rel_month.nunique() < 2:
        return 0.0
    slopes = {}
    for grp, d in pre.groupby("assignment_group"):
        m = d.groupby("rel_month")[metric].mean()
        slopes[grp] = np.polyfit(m.index.values.astype(float), m.values.astype(float), 1)[0]
    return float(slopes.get("Treatment", 0) - slopes.get("Control", 0))


def did_estimate(panel: pd.DataFrame, metric: str, campaign_id: str = "", n_boot: int = 500,
                 seed: int = 7, alpha: float = 0.05) -> ImpactResult:
    """Difference-in-differences on a household x month panel.

    ``panel`` columns: household_id, assignment_group ('Treatment'/'Control'),
    period ('pre'/'post'), rel_month (int, months relative to launch), <metric>.
    """
    # Spark DECIMAL columns arrive in pandas as Python Decimals; the estimator works in float
    panel = panel.assign(**{metric: pd.to_numeric(panel[metric].astype(object), errors="coerce").astype(float)})
    ch = _household_changes(panel, metric)
    t = ch[ch.assignment_group == "Treatment"]
    c = ch[ch.assignment_group == "Control"]
    est = float(t.delta.mean() - c.delta.mean())
    rng = np.random.default_rng(seed)
    td, cd = t.delta.to_numpy(), c.delta.to_numpy()
    boots = np.array([rng.choice(td, len(td)).mean() - rng.choice(cd, len(cd)).mean() for _ in range(n_boot)])
    lo, hi = np.quantile(boots, [alpha / 2, 1 - alpha / 2])
    se = boots.std(ddof=1) if n_boot > 1 else np.nan
    # two-sided p-value from the normal approximation of the bootstrap distribution
    from math import erf, sqrt
    z = abs(est) / se if se and se > 0 else 0.0
    p = 2 * (1 - 0.5 * (1 + erf(z / sqrt(2))))
    post_months = panel.loc[panel.period == "post", "rel_month"].nunique()
    ctrl_post = float(c.post.mean())
    dim, direction = METRIC_REGISTRY.get(metric, ("Quantity", +1))
    gap = _pre_trend_gap(panel, metric)
    pre_sd = float(panel.loc[panel.period == "pre", metric].std() or 0)
    trend_ok = abs(gap) <= max(0.1 * pre_sd, 1e-9) or abs(gap) < abs(est) * 0.25
    significant = not (lo <= 0 <= hi)
    if min(len(t), len(c)) < MIN_HOUSEHOLDS_PER_ARM:
        verdict = "Insufficient sample"
    elif not significant:
        verdict = "No detectable effect"
    elif est * direction > 0:
        verdict = "Improved" if dim == "Quality" else "Incremental lift"
    else:
        verdict = "Degraded (guardrail breach)" if dim == "Quality" else "Negative impact"
    return ImpactResult(
        campaign_id=campaign_id, metric=metric, dimension=dim,
        n_treatment=len(t), n_control=len(c),
        pre_mean_treatment=float(t.pre.mean()), pre_mean_control=float(c.pre.mean()),
        post_mean_treatment=float(t.post.mean()), post_mean_control=ctrl_post,
        did_estimate=est, ci_low=float(lo), ci_high=float(hi), p_value=float(p),
        relative_lift=(est / ctrl_post) if ctrl_post else None,
        total_incremental=est * len(t) * post_months,
        pre_trend_gap=gap, pre_trend_ok=bool(trend_ok), verdict=verdict)


def evaluate_campaign(panel: pd.DataFrame, campaign_id: str, metrics: list[str] | None = None,
                      n_boot: int = 500) -> pd.DataFrame:
    metrics = metrics or [m for m in METRIC_REGISTRY if m in panel.columns]
    rows = [did_estimate(panel, m, campaign_id, n_boot=n_boot).as_dict() for m in metrics]
    return pd.DataFrame(rows)
