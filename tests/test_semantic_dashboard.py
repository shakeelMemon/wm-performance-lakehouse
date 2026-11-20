"""Semantic layer + dashboard contract tests.

Local Spark has no metric views, so ``emulate`` rewrites a metric-view query into plain
SQL from the YAML definition (MEASURE(`x`) -> measure expr, `Dim` -> dimension expr,
FROM view -> source + joins). That proves every dimension/measure expression and every
dashboard dataset query is valid against the real gold schemas.
"""
import json
import re
from pathlib import Path

import pandas as pd
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
MV_DIR = ROOT / "src" / "semantic" / "metric_views"
DASH = ROOT / "dashboards" / "wm_performance.lvdash.json"
CATALOG = yaml.safe_load((ROOT / "src" / "semantic" / "metric_catalog.yml").read_text())["metrics"]

SERVING = {  # wm_semantic views -> local gold equivalents (governance adds only row filters)
    "wm_semantic.v_aum_daily": "gold__fct_aum_daily",
    "wm_semantic.v_asset_mix": "gold__fct_asset_mix_monthly",
    "wm_semantic.v_campaign_trend": "gold__fct_campaign_trend",
    "wm_semantic.v_campaign_impact": "local_campaign_impact",
    "wm_semantic.v_platform_health": "local_platform_health",
}


def _mv_specs():
    return {p.stem: yaml.safe_load(p.read_text().replace("__CATALOG__.wm_gold.", "gold__")) for p in MV_DIR.glob("*.yml")}


def emulate(sql: str, specs: dict) -> str:
    m = re.search(r"FROM wm_semantic\.(wm_\w+)", sql)
    if not m or m.group(1) not in specs:
        return sql
    spec = specs[m.group(1)]
    for meas in spec["measures"]:
        sql = sql.replace(f"MEASURE(`{meas['name']}`)", f"({meas['expr']})")
    for dim in sorted(spec["dimensions"], key=lambda d: -len(d["name"])):
        sql = sql.replace(f"`{dim['name']}`", f"({dim['expr']})")
    src = f"{spec['source']} source" + "".join(
        # PyYAML (YAML 1.1) reads the key `on` as boolean True; Databricks reads it as "on"
        f" LEFT JOIN {j['source']} {j['name']} ON {j.get('on', j.get(True))}" for j in spec.get("joins", []))
    return sql.replace(m.group(0), f"FROM {src}")


def test_metric_views_are_well_formed():
    for p in MV_DIR.glob("*.yml"):
        yaml.safe_load(p.read_text())   # raw file (with placeholder) must be valid YAML too
    specs = _mv_specs()
    assert {"wm_aum_flows_revenue", "wm_client_experience", "wm_service_quality"} <= set(specs)
    for name, s in specs.items():
        assert str(s["version"]) == "0.1" and s["source"].startswith("gold__"), name
        names = [x["name"] for x in s["dimensions"] + s["measures"]]
        assert len(names) == len(set(names)), f"duplicate field names in {name}"


def test_metric_catalog_points_at_real_measures():
    specs = _mv_specs()
    for m in CATALOG:
        assert m["dimension"] in ("Quantity", "Quality"), m["id"]
        if m["metric_view"] in specs:
            measures = {x["name"] for x in specs[m["metric_view"]]["measures"]}
            assert m["measure"] in measures, f"{m['id']} -> {m['measure']} not in {m['metric_view']}"
    ids = [m["id"] for m in CATALOG]
    assert len(ids) == len(set(ids))


def test_every_measure_and_dimension_executes(spark, lakehouse):
    for name, s in _mv_specs().items():
        dims = ", ".join(f"`{d['name']}`" for d in s["dimensions"])
        meas = ", ".join(f"MEASURE(`{x['name']}`)" for x in s["measures"])
        sql = emulate(f"SELECT {dims}, {meas} FROM wm_semantic.{name} GROUP BY ALL", _mv_specs())
        assert spark.sql(sql).count() > 0, name


def test_ratio_measures_are_time_window_stable(spark, lakehouse):
    """ROA over the whole period must equal the revenue-weighted definition (not a sum of ratios)."""
    specs = _mv_specs()
    roa = spark.sql(emulate("SELECT MEASURE(`Return on Assets (bps)`) r FROM wm_semantic.wm_aum_flows_revenue",
                            specs)).collect()[0].r
    raw = spark.sql("""SELECT sum(total_revenue_cad) * 12 * 10000 / sum((begin_aum_cad + end_aum_cad) / 2) r
                       FROM gold__fct_aum_rollforward_monthly""").collect()[0].r
    assert float(roa) == pytest.approx(float(raw))


def test_dashboard_datasets_execute(spark, lakehouse):
    pd_impact = pd.DataFrame([dict(campaign_id="C", campaign_name="c", metric="m", dimension="Quantity",
                                   verdict="v", did_estimate=1.0, ci_low=0.0, ci_high=2.0, relative_lift=0.1,
                                   total_incremental=10.0, n_treatment=1, n_control=1, pre_trend_ok=True,
                                   is_primary_metric=True)])
    spark.createDataFrame(pd_impact).createOrReplaceTempView("local_campaign_impact")
    spark.sql("""SELECT 'r1' run_id, current_timestamp() checked_at, 'x' check_name, 'Freshness' category,
                        'CRITICAL' severity, 'PASS' status, 0.0 observed_value, '0' threshold, '' details"""
              ).createOrReplaceTempView("local_platform_health")
    spark.sql("""SELECT s.*, a.advisor_name FROM gold__fct_advisor_scorecard_monthly s
                 JOIN gold__dim_advisor a USING (advisor_id)""").createOrReplaceTempView("local_scorecard")
    SERVING["wm_semantic.v_advisor_scorecard"] = "local_scorecard"

    dash = json.loads(DASH.read_text())
    specs = _mv_specs()
    for ds in dash["datasets"]:
        sql = "".join(ds["queryLines"])
        for k, v in SERVING.items():
            sql = sql.replace(k, v)
        sql = emulate(sql, specs)
        spark.sql(sql).collect()   # must parse and run


def test_dashboard_widgets_reference_known_datasets_and_fields():
    dash = json.loads(DASH.read_text())
    ds_names = {d["name"] for d in dash["datasets"]}
    for page in dash["pages"]:
        for item in page["layout"]:
            w = item["widget"]
            p = item["position"]
            assert 0 <= p["x"] and p["x"] + p["width"] <= 6, w["name"]
            for qq in w.get("queries", []):
                assert qq["query"]["datasetName"] in ds_names, w["name"]
                fields = {fld["name"] for fld in qq["query"]["fields"]}
                enc = w["spec"]["encodings"]
                for key, e in enc.items():
                    if isinstance(e, dict) and "fieldName" in e:
                        assert e["fieldName"] in fields, (w["name"], key)
