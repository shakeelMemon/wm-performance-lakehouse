"""Local, laptop-sized replica of the Lakeflow pipeline for tests.

Runs the *same* generator, transforms, DQ rules and gold SQL files as Databricks,
swapping only the pipeline mechanics:

* Auto Loader           -> spark.read on the landing files
* expect_all_or_drop    -> filter on the same rule expressions
* AUTO CDC SCD1 / SCD2  -> latest_by_key / scd2_from_events (batch equivalents)
* materialized views    -> temp views created from the unmodified SELECT bodies
"""
from __future__ import annotations

import re
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from wm_common.config import FEEDS
from wm_common.generator import write_landing
from wm_common.quality import drop_rules
from wm_common.transforms import (
    latest_by_key,
    scd2_from_events,
    standardize_clients,
    standardize_holdings,
    standardize_service_cases,
    standardize_transactions,
    standardize_valuations,
)

ROOT = Path(__file__).resolve().parents[1]
GOLD_FILES = ["gold_dimensions.sql", "gold_performance.sql", "gold_advisor_client.sql", "gold_campaign.sql"]


def _passes(df: DataFrame, entity: str) -> DataFrame:
    rules = list(drop_rules(entity).values())
    if not rules:
        return df
    cond = reduce(lambda a, b: a & b, [F.coalesce(F.expr(r), F.lit(False)) for r in rules])
    return df.where(cond)


def load_bronze(spark: SparkSession, landing: str) -> dict[str, DataFrame]:
    out = {}
    for f in FEEDS:
        path = f"{landing}/{f.name}"
        if not Path(path).exists():
            continue
        r = spark.read.format(f.fmt)
        if f.fmt == "csv":
            r = r.option("header", "true").option("inferSchema", "true")
        df = r.load(path)
        out[f.name] = (df.drop("period")
                       .withColumn("_ingested_at", F.current_timestamp())
                       .withColumn("_source_file", F.input_file_name()))
    return out


def build_silver(spark: SparkSession, b: dict[str, DataFrame]) -> dict[str, DataFrame]:
    s: dict[str, DataFrame] = {}
    s["branches"] = b["branches"].select("branch_id", "branch_name", "province", "region",
                                         F.col("opened_date").cast("date"))
    s["securities"] = b["securities"].select("security_id", "ticker", "security_name", "asset_class",
                                             "currency", "sector")
    adv = b["advisor_events"].withColumn("event_ts", F.col("event_ts").cast("timestamp"))
    s["advisors"] = scd2_from_events(adv.drop("_ingested_at", "_source_file"), ["advisor_id"])
    hh = b["household_events"].withColumn("event_ts", F.col("event_ts").cast("timestamp"))
    s["households"] = scd2_from_events(hh.drop("_ingested_at", "_source_file"), ["household_id"])
    cl = _passes(standardize_clients(b["client_events"]), "clients")
    s["clients"] = scd2_from_events(cl, ["client_id"])
    ac = (b["account_events"].withColumns({"event_ts": F.col("event_ts").cast("timestamp"),
                                           "open_date": F.col("open_date").cast("date"),
                                           "close_date": F.col("close_date").cast("date"),
                                           "fee_bps": F.col("fee_bps").cast("double")})
          .drop("_ingested_at", "_source_file"))
    s["accounts"] = latest_by_key(ac, ["account_id"], ["event_ts"]).drop("operation")
    tx = standardize_transactions(b["transactions"])
    s["transactions"] = latest_by_key(_passes(tx, "transactions"), ["txn_id"], ["file_ts"])
    s["transactions_quarantine"] = tx.subtract(_passes(tx, "transactions"))
    va = standardize_valuations(b["valuations"])
    s["valuations"] = latest_by_key(_passes(va, "valuations"), ["account_id", "as_of_date"], ["file_ts"])
    s["holdings"] = _passes(standardize_holdings(b["holdings"]), "holdings")
    s["prices"] = b["prices"].select("security_id", F.col("price_date").cast("date"),
                                     F.col("close_price").cast("double"), "currency")
    s["crm_interactions"] = b["crm_interactions"].select(
        "interaction_id", "household_id", "advisor_id", F.col("interaction_date").cast("date"),
        "interaction_type", F.col("duration_min").cast("int"), "outcome")
    s["digital_sessions"] = (b["digital_sessions"]
                             .select("session_id", "client_id", "household_id",
                                     F.col("session_ts").cast("timestamp"), "channel", "primary_feature",
                                     F.col("duration_sec").cast("int"))
                             .withColumn("session_date", F.to_date("session_ts")))
    s["service_cases"] = _passes(standardize_service_cases(b["service_cases"]), "service_cases")
    s["nps_responses"] = _passes(b["nps_responses"].select(
        "response_id", "household_id", "advisor_id", F.col("response_date").cast("date"),
        F.col("score").cast("int"), "survey_type"), "nps_responses")
    s["campaigns"] = b["campaigns"].select(
        "campaign_id", "campaign_name", "objective", "channel", F.col("start_date").cast("date"),
        F.col("end_date").cast("date"), F.col("measurement_end_date").cast("date"),
        "eligible_segments", "primary_metric")
    s["campaign_assignments"] = b["campaign_assignments"].select(
        "campaign_id", "household_id", "assignment_group", F.col("assigned_date").cast("date"), "stratum")
    for name, df in s.items():
        df.cache().createOrReplaceTempView(f"silver__{name}")
    return s


_MV = re.compile(r"CREATE OR REFRESH MATERIALIZED VIEW\s+\$\{wm\.catalog\}\.wm_gold\.(\w+)(.*?)\nAS\n(.*)", re.S)


def parse_gold_sql(path: Path) -> list[tuple[str, str]]:
    """Return [(table_name, select_body)] from a pipeline SQL file."""
    text = "\n".join(line for line in path.read_text().splitlines() if not line.strip().startswith("--"))
    stmts = [st.strip() for st in re.split(r";[ \t]*(?:\n|$)", text) if st.strip()]
    out = []
    for st in stmts:
        m = _MV.match(st)
        if not m:
            raise ValueError(f"Unparseable statement in {path.name}: {st[:80]}")
        out.append((m.group(1), m.group(3)))
    return out


def localize(sql: str) -> str:
    return (sql.replace("${wm.catalog}.wm_silver.", "silver__")
               .replace("${wm.catalog}.wm_gold.", "gold__"))


def build_gold(spark: SparkSession) -> list[str]:
    built = []
    for fname in GOLD_FILES:
        for name, body in parse_gold_sql(ROOT / "src" / "pipelines" / "gold" / fname):
            spark.sql(localize(body)).cache().createOrReplaceTempView(f"gold__{name}")
            built.append(name)
    return built


def build_all(spark: SparkSession, frames: dict, landing_dir: str) -> dict:
    write_landing(frames, landing_dir)
    bronze = load_bronze(spark, landing_dir)
    silver = build_silver(spark, bronze)
    gold = build_gold(spark)
    return {"bronze": bronze, "silver": silver, "gold": gold}
