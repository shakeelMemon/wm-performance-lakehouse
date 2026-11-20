# Databricks notebook source
# MAGIC %md
# MAGIC # Data & pipeline health checks
# MAGIC
# MAGIC Runs after every pipeline refresh and records results in `wm_ops.dq_check_results`
# MAGIC (history) — the source for the *Platform Health* dashboard page.
# MAGIC
# MAGIC | Category | Checks |
# MAGIC |---|---|
# MAGIC | Freshness | latest business date in key silver/gold tables vs. expected business date |
# MAGIC | Volume | today's row count vs. trailing 20-day mean (z-score, % drop) |
# MAGIC | Reconciliation | holdings vs. valuations; AUM bridge closes; gold AUM vs. silver valuations |
# MAGIC | Integrity | orphan accounts in facts; duplicate business keys |
# MAGIC | Expectations | pass rates of every Lakeflow expectation from the pipeline event log |
# MAGIC | Quarantine | share of records rejected by DROP rules |
# MAGIC
# MAGIC `CRITICAL` failures raise, which fails the job task and triggers notifications
# MAGIC before stale or wrong numbers reach a dashboard.

# COMMAND ----------

dbutils.widgets.text("catalog", "wm_reporting_dev")
dbutils.widgets.text("pipeline_id", "")
dbutils.widgets.dropdown("fail_on_critical", "true", ["true", "false"])
cat = dbutils.widgets.get("catalog")
pipeline_id = dbutils.widgets.get("pipeline_id")
fail_on_critical = dbutils.widgets.get("fail_on_critical") == "true"
S, G, OPS = f"{cat}.wm_silver", f"{cat}.wm_gold", f"{cat}.wm_ops"

# COMMAND ----------

import uuid
from datetime import datetime, timezone

import pandas as pd

run_id = str(uuid.uuid4())
now = datetime.now(timezone.utc)
results = []


def record(check, category, severity, passed, observed, threshold, details=""):
    results.append(dict(run_id=run_id, checked_at=now, check_name=check, category=category,
                        severity=severity, status="PASS" if passed else "FAIL",
                        observed_value=float(observed) if observed is not None else None,
                        threshold=str(threshold), details=details))


def scalar(sql):
    return spark.sql(sql).collect()[0][0]


expected_bd = (pd.Timestamp.today().normalize() - pd.offsets.BDay(1)).date()
try:
    expected_bd = pd.Timestamp(dbutils.jobs.taskValues.get("land_daily_files", "landing_through",
                                                           debugValue=str(expected_bd))).date()
except Exception:
    pass

# COMMAND ----------

# Freshness -----------------------------------------------------------------------------
for table, col in [(f"{S}.valuations", "as_of_date"), (f"{S}.transactions", "trade_date"),
                   (f"{S}.prices", "price_date"), (f"{G}.fct_account_daily", "as_of_date"),
                   (f"{G}.fct_aum_daily", "as_of_date")]:
    latest = scalar(f"SELECT max({col}) FROM {table}")
    lag = (expected_bd - latest).days if latest else 999
    record(f"freshness:{table.split('.')[-1]}", "Freshness", "CRITICAL", lag <= 0 or latest >= expected_bd,
           lag, "<= 0 days behind expected business date", f"latest={latest} expected={expected_bd}")

# Volume anomaly --------------------------------------------------------------------------
vol = spark.sql(f"""SELECT as_of_date, count(*) n FROM {S}.valuations
                    WHERE as_of_date >= date_sub(current_date(), 45) GROUP BY 1 ORDER BY 1""").toPandas()
if len(vol) >= 5:
    hist, today = vol.n.iloc[:-1].tail(20), vol.n.iloc[-1]
    z = (today - hist.mean()) / (hist.std() or 1)
    drop = 1 - today / hist.mean()
    record("volume:valuations_zscore", "Volume", "WARNING", abs(z) < 3, round(z, 2), "|z| < 3")
    record("volume:valuations_drop", "Volume", "CRITICAL", drop < 0.2, round(drop, 4), "< 20% drop vs 20d mean")

# Reconciliations -------------------------------------------------------------------------
gap = spark.sql(f"""
    WITH me AS (SELECT max(as_of_date) d FROM {S}.holdings),
    h AS (SELECT account_id, sum(market_value_cad) hv FROM {S}.holdings WHERE as_of_date = (SELECT d FROM me) GROUP BY 1),
    v AS (SELECT account_id, market_value_cad mv FROM {S}.valuations WHERE as_of_date = (SELECT d FROM me))
    SELECT count(*) accounts, count_if(abs(coalesce(h.hv, 0) - coalesce(v.mv, 0)) > 1) breaks,
           sum(abs(coalesce(h.hv, 0) - coalesce(v.mv, 0))) abs_gap
    FROM h FULL OUTER JOIN v USING (account_id)""").collect()[0]
record("recon:holdings_vs_valuations", "Reconciliation", "CRITICAL",
       gap.breaks <= 0.001 * max(gap.accounts, 1), gap.breaks, "<= 0.1% of accounts off by > $1",
       f"accounts={gap.accounts} abs_gap={gap.abs_gap}")

bridge = scalar(f"""SELECT max(abs(begin_aum_cad + net_flows_cad + market_movement_cad - end_aum_cad))
                    FROM {G}.fct_aum_rollforward_monthly""")
record("recon:aum_bridge_closes", "Reconciliation", "CRITICAL", (bridge or 0) < 1, bridge, "< $1")

aum_gap = scalar(f"""SELECT abs((SELECT sum(aum_cad) FROM {G}.fct_aum_daily WHERE as_of_date = (SELECT max(as_of_date) FROM {G}.fct_aum_daily))
                             - (SELECT sum(market_value_cad) FROM {S}.valuations WHERE as_of_date = (SELECT max(as_of_date) FROM {G}.fct_aum_daily)))""")
record("recon:gold_aum_vs_silver", "Reconciliation", "CRITICAL", (aum_gap or 0) < 1, aum_gap, "< $1",
       "gold AUM must equal custodian valuations (no account dropped by joins)")

# Integrity -------------------------------------------------------------------------------
orph = scalar(f"""SELECT count(DISTINCT v.account_id) FROM {S}.valuations v
                  LEFT ANTI JOIN {G}.dim_account a ON v.account_id = a.account_id""")
record("integrity:orphan_valuation_accounts", "Integrity", "CRITICAL", orph == 0, orph, "0")
dups = scalar(f"SELECT count(*) - count(DISTINCT txn_id) FROM {S}.transactions")
record("integrity:duplicate_txn_ids", "Integrity", "CRITICAL", dups == 0, dups, "0")

# Quarantine rate -------------------------------------------------------------------------
for ent in ("transactions", "valuations"):
    q = scalar(f"SELECT count(*) FROM {S}.{ent}_quarantine")
    n = scalar(f"SELECT count(*) FROM {S}.{ent}")
    rate = q / max(n, 1)
    record(f"quarantine:{ent}_rate", "Quarantine", "WARNING", rate < 0.01, round(rate, 5), "< 1%",
           f"quarantined={q} accepted={n}")

# COMMAND ----------

# Lakeflow expectations from the event log (latest update) ---------------------------------
if pipeline_id:
    try:
        exp = spark.sql(f"""
            WITH ev AS (SELECT * FROM event_log('{pipeline_id}')),
            last_update AS (SELECT origin.update_id u FROM ev WHERE event_type = 'create_update'
                            ORDER BY timestamp DESC LIMIT 1),
            e AS (SELECT explode(from_json(details:flow_progress.data_quality.expectations,
                     'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) x
                  FROM ev WHERE event_type = 'flow_progress' AND origin.update_id = (SELECT u FROM last_update))
            SELECT x.dataset, x.name, sum(x.passed_records) passed, sum(x.failed_records) failed
            FROM e GROUP BY 1, 2""").toPandas()
        for r in exp.itertuples():
            total = r.passed + r.failed
            rate = r.passed / total if total else 1.0
            record(f"expectation:{r.dataset.split('.')[-1]}.{r.name}", "Expectations",
                   "WARNING", rate >= 0.99, round(rate, 5), ">= 99% pass", f"failed={r.failed}")
    except Exception as e:  # event log access requires pipeline owner / shared compute
        record("expectation:event_log_access", "Expectations", "WARNING", False, None, "readable", str(e)[:300])

# COMMAND ----------

res = spark.createDataFrame(pd.DataFrame(results))
res.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{OPS}.dq_check_results")
spark.sql(f"""CREATE OR REPLACE VIEW {OPS}.v_latest_health AS
              SELECT * FROM {OPS}.dq_check_results
              WHERE run_id = (SELECT max_by(run_id, checked_at) FROM {OPS}.dq_check_results)""")
display(res.orderBy("status", "severity"))

critical = [r for r in results if r["status"] == "FAIL" and r["severity"] == "CRITICAL"]
if critical and fail_on_critical:
    raise RuntimeError(f"{len(critical)} critical data-health checks failed: "
                       + ", ".join(c["check_name"] for c in critical))
dbutils.notebook.exit(f"{len(results)} checks, {sum(r['status'] == 'FAIL' for r in results)} failed")
