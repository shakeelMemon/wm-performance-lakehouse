"""Silver facts: transactions, valuations, holdings, market data, CRM, digital, service, NPS.

Pattern for the two book-of-record feeds (transactions, valuations):

    bronze ──► *_validated (view, expectations: drop + warn) ──► AUTO CDC SCD1 ──► silver table
          └──► *_quarantine (streaming table: rows failing a DROP rule + rule names)

The SCD1 AUTO CDC target de-duplicates custodian re-sends and restatements by business
key, keeping the latest file — idempotent no matter how many times a file is delivered.
"""
import sys

import dlt
from pyspark.sql import functions as F

sys.path.append(spark.conf.get("wm.src_root"))  # noqa: F821
from wm_common.quality import drop_rules, quarantine_predicate, warn_rules  # noqa: E402
from wm_common.transforms import (  # noqa: E402
    failed_rules_col,
    standardize_holdings,
    standardize_service_cases,
    standardize_transactions,
    standardize_valuations,
)

CATALOG = spark.conf.get("wm.catalog")  # noqa: F821
B = f"{CATALOG}.wm_bronze"
S = f"{CATALOG}.wm_silver"
_auto_cdc = getattr(dlt, "create_auto_cdc_flow", None) or dlt.apply_changes


def _stream(table):
    return spark.readStream.table(f"{B}.{table}")  # noqa: F821


def _quarantine(entity, standardized_df):
    rules = drop_rules(entity)
    return (standardized_df.where(quarantine_predicate(entity))
            .withColumn("_failed_rules", failed_rules_col(rules))
            .withColumn("_quarantined_at", F.current_timestamp()))


# ======================================================================== transactions
@dlt.view(comment="Transactions typed + validated; failing DROP rules are removed here")
@dlt.expect_all_or_drop(drop_rules("transactions"))
@dlt.expect_all(warn_rules("transactions"))
def transactions_validated():
    return standardize_transactions(_stream("transactions"))


dlt.create_streaming_table(
    name=f"{S}.transactions",
    comment="Book-of-record transactions, de-duplicated by txn_id (latest file wins). "
            "net_flow_cad is non-zero only for external flows (deposits, withdrawals, transfers).",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
    cluster_by=["trade_date", "account_id"])
_auto_cdc(target=f"{S}.transactions", source="transactions_validated", keys=["txn_id"],
          sequence_by=F.struct("file_ts", "_ingested_at"), stored_as_scd_type=1)


@dlt.table(name=f"{S}.transactions_quarantine", comment="Transactions rejected by DROP rules, with reasons",
           table_properties={"quality": "quarantine"})
def transactions_quarantine():
    return _quarantine("transactions", standardize_transactions(_stream("transactions")))


# ======================================================================== valuations
@dlt.view(comment="Daily valuations typed + validated")
@dlt.expect_all_or_drop(drop_rules("valuations"))
@dlt.expect_all(warn_rules("valuations"))
def valuations_validated():
    return standardize_valuations(_stream("valuations"))


dlt.create_streaming_table(
    name=f"{S}.valuations",
    comment="Custodian end-of-day account market values; restatements resolved (latest file wins)",
    table_properties={"quality": "silver"},
    cluster_by=["as_of_date", "account_id"])
_auto_cdc(target=f"{S}.valuations", source="valuations_validated", keys=["account_id", "as_of_date"],
          sequence_by=F.struct("file_ts", "_ingested_at"), stored_as_scd_type=1)


@dlt.table(name=f"{S}.valuations_quarantine", comment="Valuations rejected by DROP rules, with reasons",
           table_properties={"quality": "quarantine"})
def valuations_quarantine():
    return _quarantine("valuations", standardize_valuations(_stream("valuations")))


# ======================================================================== holdings
@dlt.table(name=f"{S}.holdings", comment="Month-end position-level holdings",
           table_properties={"quality": "silver"}, cluster_by=["as_of_date"])
@dlt.expect_all_or_drop(drop_rules("holdings"))
@dlt.expect_all(warn_rules("holdings"))
def holdings():
    return standardize_holdings(_stream("holdings"))


# ======================================================================== market data
@dlt.table(name=f"{S}.prices", comment="Daily security closing prices (local currency)",
           table_properties={"quality": "silver"})
@dlt.expect_all_or_drop(drop_rules("prices"))
def prices():
    return _stream("prices").select("security_id", F.col("price_date").cast("date"),
                                    F.col("close_price").cast("double"), "currency")


@dlt.table(name=f"{S}.fx_rates", comment="Daily FX rates to CAD", table_properties={"quality": "silver"})
@dlt.expect_or_drop("positive_rate", "rate_to_cad > 0")
def fx_rates():
    return _stream("fx_rates").select(F.col("rate_date").cast("date"), "currency",
                                      F.col("rate_to_cad").cast("double"))


# ======================================================================== client experience feeds
@dlt.table(name=f"{S}.crm_interactions", comment="Advisor-client interactions (CRM)",
           table_properties={"quality": "silver"})
@dlt.expect_all_or_drop({"valid_interaction": "interaction_id IS NOT NULL AND household_id IS NOT NULL"})
def crm_interactions():
    return _stream("crm_interactions").select(
        "interaction_id", "household_id", "advisor_id", F.col("interaction_date").cast("date"),
        "interaction_type", F.col("duration_min").cast("int"), "outcome")


@dlt.table(name=f"{S}.digital_sessions", comment="Client digital sessions (web + mobile)",
           table_properties={"quality": "silver"}, cluster_by=["session_date"])
@dlt.expect_or_drop("valid_session", "session_id IS NOT NULL AND client_id IS NOT NULL")
def digital_sessions():
    return (_stream("digital_sessions")
            .select("session_id", "client_id", "household_id", F.col("session_ts").cast("timestamp"),
                    "channel", "primary_feature", F.col("duration_sec").cast("int"))
            .withColumn("session_date", F.to_date("session_ts")))


@dlt.view(comment="Service cases typed + validated")
@dlt.expect_all_or_drop(drop_rules("service_cases"))
@dlt.expect_all(warn_rules("service_cases"))
def service_cases_validated():
    return standardize_service_cases(_stream("service_cases")).withColumn("_ingested_at", F.current_timestamp())


dlt.create_streaming_table(name=f"{S}.service_cases", comment="Service cases, latest status per case (SCD1)",
                           table_properties={"quality": "silver"})
_auto_cdc(target=f"{S}.service_cases", source="service_cases_validated", keys=["case_id"],
          sequence_by=F.col("_ingested_at"), stored_as_scd_type=1)


@dlt.table(name=f"{S}.nps_responses", comment="Relationship NPS survey responses",
           table_properties={"quality": "silver"})
@dlt.expect_all_or_drop(drop_rules("nps_responses"))
def nps_responses():
    return _stream("nps_responses").select(
        "response_id", "household_id", "advisor_id", F.col("response_date").cast("date"),
        F.col("score").cast("int"), "survey_type")
