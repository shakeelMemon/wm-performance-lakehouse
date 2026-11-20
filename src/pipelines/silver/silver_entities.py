"""Silver master data: advisors, households, clients (SCD2), accounts (SCD1), reference data.

SCD Type 2 lets gold attribute history correctly — e.g. an advisor who moved from
Markham to Yorkville in March keeps Q1 AUM in Markham's numbers.
"""
import sys

import dlt
from pyspark.sql import functions as F

sys.path.append(spark.conf.get("wm.src_root"))  # noqa: F821
from wm_common.quality import drop_rules, warn_rules  # noqa: E402
from wm_common.transforms import standardize_clients  # noqa: E402

CATALOG = spark.conf.get("wm.catalog")  # noqa: F821
B = f"{CATALOG}.wm_bronze"
S = f"{CATALOG}.wm_silver"

# Newer runtimes name it create_auto_cdc_flow; older ones apply_changes (same semantics).
_auto_cdc = getattr(dlt, "create_auto_cdc_flow", None) or dlt.apply_changes
_LINEAGE = ["_rescued_data", "_source_file", "_file_modified_at", "_ingested_at", "_source_system", "period"]


def _cdc_source_cols(df):
    return df.drop(*[c for c in _LINEAGE if c in df.columns])


# ------------------------------------------------------------------------ reference data
@dlt.table(name=f"{S}.branches", comment="Branch reference (region, province)",
           table_properties={"quality": "silver"})
@dlt.expect_or_drop("valid_branch", "branch_id IS NOT NULL")
def branches():
    return (spark.read.table(f"{B}.branches")  # noqa: F821
            .select("branch_id", "branch_name", "province", "region", F.col("opened_date").cast("date")))


@dlt.table(name=f"{S}.securities", comment="Security master", table_properties={"quality": "silver"})
@dlt.expect_or_drop("valid_security", "security_id IS NOT NULL")
def securities():
    return (spark.read.table(f"{B}.securities")  # noqa: F821
            .select("security_id", "ticker", "security_name", "asset_class", "currency", "sector"))


# ------------------------------------------------------------------------ advisors (SCD2)
@dlt.view(comment="Advisor change events, typed")
@dlt.expect_all_or_drop({"valid_advisor": "advisor_id IS NOT NULL", "valid_event_ts": "event_ts IS NOT NULL"})
def advisor_events_clean():
    return _cdc_source_cols(spark.readStream.table(f"{B}.advisor_events"))  # noqa: F821


dlt.create_streaming_table(name=f"{S}.advisors",
                           comment="Advisor master, SCD Type 2 (branch transfers, designation changes)",
                           table_properties={"quality": "silver"})
_auto_cdc(target=f"{S}.advisors", source="advisor_events_clean", keys=["advisor_id"],
          sequence_by=F.col("event_ts"), stored_as_scd_type=2,
          except_column_list=["operation"])


# ------------------------------------------------------------------------ households (SCD2)
@dlt.view(comment="Household change events, typed")
@dlt.expect_all_or_drop({"valid_household": "household_id IS NOT NULL", "valid_advisor": "advisor_id IS NOT NULL"})
def household_events_clean():
    return _cdc_source_cols(spark.readStream.table(f"{B}.household_events"))  # noqa: F821


dlt.create_streaming_table(name=f"{S}.households", comment="Household master, SCD Type 2 (status, advisor, segment)",
                           table_properties={"quality": "silver"})
_auto_cdc(target=f"{S}.households", source="household_events_clean", keys=["household_id"],
          sequence_by=F.col("event_ts"), stored_as_scd_type=2,
          except_column_list=["operation"],
          track_history_column_list=["advisor_id", "segment", "status", "province"])


# ------------------------------------------------------------------------ clients (SCD2, PII)
@dlt.view(comment="Client change events with PII standardization and DQ")
@dlt.expect_all_or_drop(drop_rules("clients"))
@dlt.expect_all(warn_rules("clients"))
def client_events_clean():
    return standardize_clients(spark.readStream.table(f"{B}.client_events"))  # noqa: F821


dlt.create_streaming_table(
    name=f"{S}.clients",
    comment="Client master, SCD Type 2 (risk profile, status). Contains PII: masked in serving views.",
    table_properties={"quality": "silver", "wm.contains_pii": "true"})
_auto_cdc(target=f"{S}.clients", source="client_events_clean", keys=["client_id"],
          sequence_by=F.col("event_ts"), stored_as_scd_type=2, except_column_list=["operation"],
          track_history_column_list=["risk_profile", "status", "email_opt_in"])


# ------------------------------------------------------------------------ accounts (SCD1)
@dlt.view(comment="Account events, typed")
@dlt.expect_all_or_drop({"valid_account": "account_id IS NOT NULL", "valid_client": "client_id IS NOT NULL"})
@dlt.expect("valid_fee", "fee_bps BETWEEN 0 AND 300")
def account_events_clean():
    return (_cdc_source_cols(spark.readStream.table(f"{B}.account_events"))  # noqa: F821
            .withColumns({"open_date": F.col("open_date").cast("date"),
                          "close_date": F.col("close_date").cast("date"),
                          "fee_bps": F.col("fee_bps").cast("double")}))


dlt.create_streaming_table(name=f"{S}.accounts", comment="Account master, current state (SCD Type 1)",
                           table_properties={"quality": "silver"})
_auto_cdc(target=f"{S}.accounts", source="account_events_clean", keys=["account_id"],
          sequence_by=F.col("event_ts"), stored_as_scd_type=1, except_column_list=["operation"])


# ------------------------------------------------------------------------ campaigns
@dlt.table(name=f"{S}.campaigns", comment="Campaign / initiative definitions",
           table_properties={"quality": "silver"})
def campaigns():
    return (spark.read.table(f"{B}.campaigns")  # noqa: F821
            .select("campaign_id", "campaign_name", "objective", "channel",
                    F.col("start_date").cast("date"), F.col("end_date").cast("date"),
                    F.col("measurement_end_date").cast("date"), "eligible_segments", "primary_metric"))


@dlt.table(name=f"{S}.campaign_assignments", comment="Randomized treatment / control assignment",
           table_properties={"quality": "silver"})
@dlt.expect_all_or_drop({"valid_group": "assignment_group IN ('Treatment','Control')",
                         "valid_household": "household_id IS NOT NULL"})
def campaign_assignments():
    return (spark.read.table(f"{B}.campaign_assignments")  # noqa: F821
            .select("campaign_id", "household_id", "assignment_group",
                    F.col("assigned_date").cast("date"), "stratum")
            .dropDuplicates(["campaign_id", "household_id"]))
