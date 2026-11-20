# Databricks notebook source
# MAGIC %md
# MAGIC # Synthetic Wealth Management data → landing volume
# MAGIC
# MAGIC Simulates the upstream systems of a wealth management division (custodian, CRM,
# MAGIC market data, digital, service desk, surveys, marketing) and drops their files into
# MAGIC the Unity Catalog landing volume, exactly as real feeds would arrive.
# MAGIC
# MAGIC | mode | behaviour |
# MAGIC |---|---|
# MAGIC | `history` | backfill from `start_date` to `as_of_date` (monthly files) |
# MAGIC | `incremental` | append only the business days after the last run (daily files) |
# MAGIC
# MAGIC The generator is deterministic and append-only, so the daily job can call it every
# MAGIC morning to "receive" yesterday's files.

# COMMAND ----------

dbutils.widgets.text("catalog", "wm_reporting_dev")
dbutils.widgets.dropdown("mode", "history", ["history", "incremental"])
dbutils.widgets.dropdown("scale", "medium", ["tiny", "small", "medium", "large"])
dbutils.widgets.text("as_of_date", "")   # blank = previous business day

# COMMAND ----------

import os
import sys

import pandas as pd

sys.path.append(os.path.abspath("../"))            # makes src/wm_common importable
from wm_common.config import SCALE_PRESETS, landing_path  # noqa: E402
from wm_common.generator import generate, read_state, write_landing, write_state  # noqa: E402

catalog = dbutils.widgets.get("catalog")
mode = dbutils.widgets.get("mode")
cfg = SCALE_PRESETS[dbutils.widgets.get("scale")]
as_of = dbutils.widgets.get("as_of_date") or str((pd.Timestamp.today().normalize() - pd.offsets.BDay(1)).date())
cfg.end_date = as_of
base = landing_path(catalog)
state = read_state(base)
print(f"catalog={catalog} mode={mode} as_of={as_of} landing={base} state={state}")

# COMMAND ----------

if mode == "incremental" and not state:
    raise ValueError("No generator state found — run once with mode=history first.")
since = state.get("last_written_date") if mode == "incremental" else None
if since and since >= as_of:
    dbutils.notebook.exit(f"Nothing to do: landing already current through {since}")

frames = generate(cfg)
written = write_landing(frames, base, since=since, granularity="day" if mode == "incremental" else "month")
write_state(base, cfg, as_of)

summary = pd.DataFrame(sorted(written.items()), columns=["feed", "rows_written"])
display(summary)

# COMMAND ----------

dbutils.jobs.taskValues.set("landing_through", as_of)
dbutils.notebook.exit(f"Wrote {int(summary.rows_written.sum()):,} rows across {len(summary)} feeds through {as_of}")
