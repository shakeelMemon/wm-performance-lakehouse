# Databricks notebook source
# MAGIC %md
# MAGIC # Publish the semantic layer
# MAGIC
# MAGIC 1. Creates / replaces every **Unity Catalog metric view** defined in `metric_views/*.yml`
# MAGIC 2. Publishes the **certified metric catalog** (`metric_catalog.yml`) as a governed table
# MAGIC 3. Smoke-tests each metric view with a `MEASURE()` query so a broken definition fails the job
# MAGIC
# MAGIC Definitions live in Git; this notebook is the only way they reach the workspace.

# COMMAND ----------

dbutils.widgets.text("catalog", "wm_reporting_dev")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

import glob
import os
from datetime import datetime, timezone

import yaml

HERE = os.getcwd()   # notebook tasks run with the notebook's folder as working directory
VIEW_DIR = os.path.join(HERE, "metric_views")
SEM = f"{catalog}.wm_semantic"

VIEW_COMMENTS = {
    "wm_aum_flows_revenue": "Certified AUM, net new assets, market movement and revenue metrics for Wealth Management.",
    "wm_client_experience": "Certified client engagement, digital adoption, service and NPS metrics (household-month grain).",
    "wm_service_quality": "Certified service operations metrics: volumes, resolution time and 72h SLA adherence.",
}

published = []
for path in sorted(glob.glob(os.path.join(VIEW_DIR, "*.yml"))):
    name = os.path.splitext(os.path.basename(path))[0]
    body = open(path).read().replace("__CATALOG__", catalog)
    yaml.safe_load(body)  # fail fast on malformed YAML
    spark.sql(f"CREATE OR REPLACE VIEW {SEM}.{name}\nWITH METRICS\nLANGUAGE YAML\nAS $$\n{body}\n$$")
    if name in VIEW_COMMENTS:
        spark.sql(f"COMMENT ON TABLE {SEM}.{name} IS '{VIEW_COMMENTS[name]}'")
    spec = yaml.safe_load(body)
    first_dim, first_measure = spec["dimensions"][0]["name"], spec["measures"][0]["name"]
    n = spark.sql(f"SELECT `{first_dim}`, MEASURE(`{first_measure}`) FROM {SEM}.{name} GROUP BY ALL").count()
    published.append((name, len(spec["dimensions"]), len(spec["measures"]), n))
    print(f"published {SEM}.{name}: {len(spec['dimensions'])} dims, {len(spec['measures'])} measures, smoke rows={n}")

# COMMAND ----------

catalog_yaml = yaml.safe_load(open(os.path.join(HERE, "metric_catalog.yml")))
rows = [{**m, "published_at": datetime.now(timezone.utc)} for m in catalog_yaml["metrics"]]
(spark.createDataFrame(rows)
      .write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable(f"{SEM}.metric_catalog"))
spark.sql(f"COMMENT ON TABLE {SEM}.metric_catalog IS "
          "'Certified metric glossary: definition, formula, grain, owner and governed source for every WM metric'")

display(spark.createDataFrame(published, "metric_view string, dimensions int, measures int, smoke_rows long"))
