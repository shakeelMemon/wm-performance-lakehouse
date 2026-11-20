# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental impact of business initiatives (quantity + quality)
# MAGIC
# MAGIC For every campaign in `wm_silver.campaigns`, estimate the **incremental** effect of
# MAGIC treatment vs. the randomized control group with a household-level
# MAGIC **difference-in-differences** and a cluster bootstrap 95% CI:
# MAGIC
# MAGIC * **Quantity** — net new assets, gross inflows, deposits, interactions, digital sessions / adoption
# MAGIC * **Quality guardrails** — complaints, service cases, attrition (must not degrade)
# MAGIC
# MAGIC Results land in `wm_gold.campaign_impact_results` (dashboard + Genie) and each run is
# MAGIC logged to MLflow for auditability.

# COMMAND ----------

dbutils.widgets.text("catalog", "wm_reporting_dev")
dbutils.widgets.text("n_boot", "1000")
cat = dbutils.widgets.get("catalog")
n_boot = int(dbutils.widgets.get("n_boot"))

# COMMAND ----------

import os
import sys
from datetime import datetime, timezone

import pandas as pd

sys.path.append(os.path.abspath("../"))
from wm_common.impact import METRIC_REGISTRY, evaluate_campaign  # noqa: E402

panel_all = spark.table(f"{cat}.wm_gold.fct_campaign_household_monthly").toPandas()
campaigns = spark.table(f"{cat}.wm_silver.campaigns").toPandas()
print(f"{len(panel_all):,} household-months across {panel_all.campaign_id.nunique()} campaigns")

# COMMAND ----------

frames = []
for c in campaigns.itertuples():
    p = panel_all[panel_all.campaign_id == c.campaign_id]
    if p.empty or p.period.nunique() < 2:
        print(f"skip {c.campaign_id}: no post-period data yet")
        continue
    metrics = [m for m in METRIC_REGISTRY if m in p.columns]
    res = evaluate_campaign(p, c.campaign_id, metrics, n_boot=n_boot)
    res["campaign_name"] = c.campaign_name
    res["primary_metric"] = c.primary_metric
    res["is_primary_metric"] = res.metric == c.primary_metric
    res["post_months_observed"] = int(p.loc[p.period == "post", "rel_month"].nunique())
    res["measurement_complete"] = pd.Timestamp(c.measurement_end_date) <= pd.Timestamp.today()
    frames.append(res)

if not frames:
    dbutils.notebook.exit("No campaign has post-period data yet — nothing to measure")
results = pd.concat(frames, ignore_index=True)
results["run_at"] = datetime.now(timezone.utc)
display(results[["campaign_id", "metric", "dimension", "did_estimate", "ci_low", "ci_high",
                 "relative_lift", "p_value", "pre_trend_ok", "verdict"]])

# COMMAND ----------

(spark.createDataFrame(results)
      .write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable(f"{cat}.wm_gold.campaign_impact_results"))
spark.sql(f"""COMMENT ON TABLE {cat}.wm_gold.campaign_impact_results IS
  'Difference-in-differences incremental impact per campaign x metric (per household per month), with 95% bootstrap CI, pre-trend check and quantity/quality verdict'""")

# COMMAND ----------

# Audit trail in MLflow (one run per campaign) — optional: skipped if MLflow is unavailable
try:
    import mlflow
    mlflow.set_experiment(f"/Shared/wm_incremental_impact_{cat}")
    for cid, g in results.groupby("campaign_id"):
        with mlflow.start_run(run_name=f"{cid}-{datetime.now():%Y%m%d}"):
            mlflow.log_params({"campaign_id": cid, "n_boot": n_boot, "estimator": "household DiD + cluster bootstrap",
                               "n_treatment": int(g.n_treatment.iloc[0]), "n_control": int(g.n_control.iloc[0])})
            for r in g.itertuples():
                mlflow.log_metric(f"{r.metric}_did", r.did_estimate)
                mlflow.log_metric(f"{r.metric}_ci_low", r.ci_low)
                mlflow.log_metric(f"{r.metric}_ci_high", r.ci_high)
            mlflow.log_table(g.drop(columns=["run_at"]), "impact_results.json")
except Exception as e:
    print(f"MLflow logging skipped: {e}")

# COMMAND ----------

headline = results[results.is_primary_metric]
for r in headline.itertuples():
    print(f"{r.campaign_id} {r.metric}: {r.did_estimate:,.2f}/household/month "
          f"(95% CI {r.ci_low:,.2f} to {r.ci_high:,.2f}) -> {r.verdict}")
guard = results[(results.dimension == "Quality") & results.verdict.str.contains("breach")]
if not guard.empty:
    print("QUALITY GUARDRAIL BREACHES:\n", guard[["campaign_id", "metric", "did_estimate"]])
