"""End-to-end: landing files -> silver (same rules/transforms) -> gold (same SQL files).

Validates the reporting datasets against the generator's ground truth.
"""
import numpy as np
import pandas as pd
import pytest

pytest.importorskip("pyspark")


def _pdf(spark, sql):
    return spark.sql(sql).toPandas()


def test_all_gold_tables_build(lakehouse):
    expected = {"dim_date", "dim_advisor", "dim_advisor_history", "dim_household", "dim_client",
                "dim_account", "dim_security", "fct_account_daily", "fct_account_monthly_performance",
                "fct_aum_daily", "fct_aum_rollforward_monthly", "fct_asset_mix_monthly",
                "fct_household_monthly", "fct_advisor_scorecard_monthly", "fct_service_quality_monthly",
                "fct_digital_engagement_monthly", "fct_campaign_household_monthly", "fct_campaign_trend"}
    assert expected <= set(lakehouse["gold"])


def test_silver_dedup_and_quarantine(spark, lakehouse, frames):
    tx = spark.table("silver__transactions")
    assert tx.count() == tx.select("txn_id").distinct().count()
    q = spark.table("silver__transactions_quarantine").toPandas()
    raw = frames["transactions"]
    expected_bad = raw.account_id.isna() | raw.txn_id.str.startswith(("Y", "Z"))
    assert len(q) >= expected_bad.sum() - raw[expected_bad].duplicated().sum()
    v = spark.table("silver__valuations")
    assert v.where("market_value_cad < 0 OR account_id = 'AC_UNKNOWN'").count() == 0
    assert v.count() == v.select("account_id", "as_of_date").distinct().count()


def test_daily_returns_match_ground_truth(spark, lakehouse, frames):
    g = _pdf(spark, "SELECT account_id, CAST(as_of_date AS STRING) d, daily_return FROM gold__fct_account_daily")
    t = frames["_truth"].assign(d=lambda x: x.as_of_date.astype(str))
    m = g.merge(t, left_on=["account_id", "d"], right_on=["account_id", "d"]).dropna(subset=["true_return"])
    assert len(m) > 10_000
    assert np.abs(m.daily_return - m.true_return).max() < 1e-9


def test_monthly_twr_equals_chain_linked_truth(spark, lakehouse, frames):
    g = _pdf(spark, "SELECT account_id, CAST(month_start AS STRING) m, twr FROM gold__fct_account_monthly_performance")
    t = frames["_truth"].dropna(subset=["true_return"]).copy()
    t["m"] = pd.to_datetime(t.as_of_date).dt.to_period("M").dt.start_time.dt.strftime("%Y-%m-%d")
    truth = t.groupby(["account_id", "m"]).true_return.apply(lambda r: np.expm1(np.log1p(r).sum())).rename("tt")
    x = g.merge(truth.reset_index(), on=["account_id", "m"])
    assert len(x) > 500
    assert np.abs(x.twr - x.tt).max() < 1e-9


def test_aum_rollforward_bridges_and_reconciles(spark, lakehouse, frames):
    rf = _pdf(spark, """SELECT month_start, sum(begin_aum_cad) b, sum(net_flows_cad) f,
                               sum(market_movement_cad) mm, sum(end_aum_cad) e
                        FROM gold__fct_aum_rollforward_monthly GROUP BY month_start ORDER BY month_start""")
    rf[["b", "f", "mm", "e"]] = rf[["b", "f", "mm", "e"]].astype(float)
    assert np.allclose(rf.b + rf.f + rf.mm, rf.e, atol=0.05)
    assert np.allclose(rf.b.iloc[1:].values, rf.e.iloc[:-1].values, atol=0.05), "end of month N = begin of N+1"
    truth = frames["_truth"]
    last_day = truth.as_of_date.max()
    assert rf.e.iloc[-1] == pytest.approx(truth[truth.as_of_date == last_day].market_value_cad.sum(), rel=1e-9)


def test_holdings_reconcile_to_valuations(spark, lakehouse):
    r = _pdf(spark, """
        WITH h AS (SELECT account_id, as_of_date, sum(market_value_cad) hv FROM silver__holdings GROUP BY 1, 2)
        SELECT count(*) n, max(abs(h.hv - v.market_value_cad)) max_gap
        FROM h JOIN silver__valuations v USING (account_id, as_of_date)""")
    assert r.n[0] > 100
    assert float(r.max_gap[0]) < 1.0   # position rounding only


def test_scd2_point_in_time_attribution(spark, lakehouse):
    """AUM before an advisor's transfer stays with the old branch."""
    moved = _pdf(spark, "SELECT advisor_id, count(*) c FROM gold__dim_advisor_history GROUP BY 1 HAVING c > 1")
    if moved.empty:
        pytest.skip("no transfers in this sample")
    adv = moved.advisor_id[0]
    br = _pdf(spark, f"SELECT DISTINCT branch_name FROM gold__fct_aum_daily WHERE advisor_id = '{adv}'")
    hist = _pdf(spark, f"SELECT valid_from FROM gold__dim_advisor_history WHERE advisor_id = '{adv}'")
    in_window = (pd.to_datetime(hist.valid_from) > pd.Timestamp("2025-10-01")).sum()
    assert len(br) == 1 + in_window


def test_fiscal_calendar(spark, lakehouse):
    r = _pdf(spark, """SELECT CAST(date_key AS STRING) d, fiscal_year, fiscal_quarter FROM gold__dim_date
                       WHERE date_key IN (DATE'2025-10-31', DATE'2025-11-01', DATE'2026-01-15', DATE'2026-08-01')
                       ORDER BY date_key""")
    assert list(r.fiscal_year) == [2025, 2026, 2026, 2026]
    assert list(r.fiscal_quarter) == [4, 1, 1, 4]


def test_scorecard_quality_and_quantity_bounds(spark, lakehouse):
    s = _pdf(spark, "SELECT * FROM gold__fct_advisor_scorecard_monthly")
    assert s.quantity_score.between(0, 100).all() and s.quality_score.between(0, 100).all()
    assert s.digital_adoption_pct.dropna().between(0, 1).all()
    assert s.nps.dropna().between(-100, 100).all()
    assert s.roa_bps.dropna().median() == pytest.approx(80, abs=45)


def test_household_monthly_reconciles_to_rollforward(spark, lakehouse):
    a = _pdf(spark, "SELECT sum(net_flows_cad) x FROM gold__fct_household_monthly").x[0]
    b = _pdf(spark, "SELECT sum(net_flows_cad) x FROM gold__fct_aum_rollforward_monthly").x[0]
    assert float(a) == pytest.approx(float(b), abs=0.01)


def test_campaign_panel_is_well_formed(spark, lakehouse):
    p = _pdf(spark, "SELECT * FROM gold__fct_campaign_household_monthly")
    assert set(p.period) == {"pre", "post"}
    assert p.rel_month.min() == -3
    assert not p.duplicated(["campaign_id", "household_id", "month_start"]).any()
