"""The impact estimator must recover a known effect and stay quiet when there is none."""
import numpy as np
import pandas as pd
import pytest

from wm_common.generator import TRUE_EFFECTS
from wm_common.impact import did_estimate, evaluate_campaign


def _panel(effect: float, n: int = 400, seed: int = 1) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    for h in range(n):
        grp = "Treatment" if h % 2 else "Control"
        base = rng.lognormal(3, 1)                      # heavy-tailed household level
        for rel in range(-3, 4):
            post = rel >= 0
            y = base + rng.normal(0, 2) + (effect if (post and grp == "Treatment") else 0)
            rows.append(dict(household_id=f"H{h}", assignment_group=grp, rel_month=rel,
                             period="post" if post else "pre", net_flows_cad=y, complaints=rng.poisson(0.1)))
    return pd.DataFrame(rows)


def test_recovers_known_effect():
    r = did_estimate(_panel(effect=5.0), "net_flows_cad", "TEST")
    assert r.ci_low < 5.0 < r.ci_high
    assert r.verdict == "Incremental lift"
    assert r.pre_trend_ok


def test_accepts_spark_decimal_columns():
    from decimal import Decimal
    p = _panel(effect=5.0)
    p["net_flows_cad"] = [Decimal(str(round(v, 2))) for v in p.net_flows_cad]
    assert did_estimate(p, "net_flows_cad", "TEST", n_boot=100).did_estimate > 0


def test_null_effect_is_not_significant():
    r = did_estimate(_panel(effect=0.0, seed=3), "net_flows_cad", "TEST")
    assert r.ci_low < 0 < r.ci_high
    assert r.verdict == "No detectable effect"


def test_quality_guardrail_dimension():
    out = evaluate_campaign(_panel(effect=0.0), "TEST", ["net_flows_cad", "complaints"], n_boot=200)
    assert set(out.dimension) == {"Quantity", "Quality"}


def test_digital_campaign_effect_on_generated_data(spark, lakehouse):
    """Digital nudge (+35% session rate) should show a positive estimate on the gold panel."""
    p = spark.sql("SELECT * FROM gold__fct_campaign_household_monthly WHERE campaign_id = 'CMP-2026-02'").toPandas()
    if p.empty or p.period.nunique() < 2:
        pytest.skip("campaign window not covered by this sample")
    r = did_estimate(p, "digital_sessions", "CMP-2026-02", n_boot=300)
    assert r.did_estimate > 0
    assert TRUE_EFFECTS["CMP-2026-02"]["session_rate_mult"] > 1
