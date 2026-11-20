# Runbook

## Prerequisites

* A Databricks workspace with Unity Catalog, serverless compute (jobs, pipelines, SQL) and Databricks Apps enabled.
* Databricks CLI **v0.295 or later** (`databricks -v`), authenticated with `databricks auth login`.
* A SQL warehouse named **WM Reporting Warehouse** (Serverless, Small is enough). Or override `warehouse_id`.
* A catalog per environment, created once by a metastore admin:
  ```sql
  CREATE CATALOG IF NOT EXISTS wm_reporting_dev;
  ```
* Optional: account groups `wm_data_engineers`, `wm_analysts`, `wm_business_users`, `wm_leadership`, `wm_pii_readers`. The governance step skips grants to groups that don't exist.

## First deployment (dev)

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev        # schemas, volume, pipeline, jobs, dashboard, app
databricks bundle run wm_bootstrap -t dev # full history build at the target's data_scale
databricks bundle run wm_self_serve_app -t dev   # start the app
```

The bootstrap lands two years of history, full-refreshes the pipeline, publishes the metric views and catalog, runs health checks and impact analysis, then applies governance and the serving views. After it finishes, open the dashboard **Wealth Management Performance (dev)** and set up the Genie space (`src/self_serve/genie/genie_space.md`).

## Daily operation

`wm_daily_reporting` runs at 06:30 America/Toronto, Monday to Friday. It is deployed **paused**, so unpause it once the bootstrap has succeeded. Each run:

1. Lands yesterday's files (the generator is append-only, so reruns are safe).
2. Updates the pipeline incrementally.
3. Republishes the semantic layer. This is idempotent, and it smoke-tests every metric view.
4. Runs the health checks. **Any CRITICAL failure fails the run** and emails `notification_email`.
5. On Mondays, re-estimates campaign impact.
6. Refreshes the dashboard, but only if steps 3 and 4 succeeded.

## When something fails

| Symptom | Check | Typical fix |
|---|---|---|
| `freshness:*` FAIL | Did files land? `LIST '/Volumes/<cat>/wm_landing/raw/valuations'` | Upstream delay: rerun the job once the files arrive |
| `recon:holdings_vs_valuations` FAIL | `SELECT * FROM wm_ops.v_latest_health WHERE category='Reconciliation'` | Usually a partial custodian file. Hold the dashboard and ask the custodian to restate |
| `integrity:orphan_valuation_accounts` FAIL | Anti-join valuations to `dim_account` | An account-open event is missing. Check `wm_bronze.account_events` for the account |
| Quarantine rate WARN | `SELECT _failed_rules, count(*) FROM wm_silver.transactions_quarantine GROUP BY 1` | Fix at the source, then re-send. SCD1 CDC makes the reload idempotent |
| Expectation pass rate below 99% | Pipeline UI → Data quality tab, or `event_log()` | Tighten or relax the rule in `wm_common/quality.py` through a PR |
| Pipeline failure | Pipeline UI event log | Fix the code, then `databricks bundle deploy` and rerun. Use full refresh only for gold (bronze transactions and valuations have `pipelines.reset.allowed=false`) |

## Change management

* Every change goes through a PR. CI runs lint, regenerates the dashboard JSON (and fails on drift) and runs the unit and local end-to-end tests.
* A merge to `main` deploys to **test** and runs a smoke bootstrap at `tiny` scale. **Prod** deploys need approval in a protected environment.
* A new metric needs a measure in a `metric_views/*.yml` file, an entry in `metric_catalog.yml` and `make docs`. The contract tests fail if the catalog points at a measure that doesn't exist.
* A new source needs one `Feed(...)` line in `wm_common/config.py`, a silver definition and DQ rules. The bundle tests fail if a bronze feed has no silver consumer.

## Access requests

* **An advisor needs their book in the app or Genie:**
  `INSERT INTO wm_semantic.advisor_access_map VALUES ('ADV0042', 'jane.doe@firm.com', current_user(), current_timestamp())`
* **A leader needs every book:** add them to `wm_leadership`.
* **Someone needs unmasked PII:** add them to `wm_pii_readers`. This requires privacy approval and is logged in the UC audit log.
