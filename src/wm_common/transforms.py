"""PySpark transformations used by the silver layer.

Kept free of any ``dlt`` import so the exact same code runs inside the Lakeflow
pipeline and in local unit tests (``pytest`` + local Spark).
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from .config import EXTERNAL_FLOW_TYPES, EXTERNAL_INFLOW_TYPES

_EXT = list(EXTERNAL_FLOW_TYPES)
_INF = list(EXTERNAL_INFLOW_TYPES)


def _col_or_null(df: DataFrame, name: str, dtype: str) -> Column:
    return F.col(name).cast(dtype) if name in df.columns else F.lit(None).cast(dtype)


def standardize_transactions(df: DataFrame) -> DataFrame:
    """Type-cast, normalize codes and derive the flow / revenue semantics once."""
    t = F.upper(F.trim(F.col("txn_type")))
    return df.select(
        F.col("txn_id").cast("string").alias("txn_id"),
        F.col("account_id").cast("string").alias("account_id"),
        F.col("trade_date").cast("date").alias("trade_date"),
        F.col("settle_date").cast("date").alias("settle_date"),
        t.alias("txn_type"),
        F.col("security_id").cast("string").alias("security_id"),
        F.col("quantity").cast("double").alias("quantity"),
        F.col("price").cast("double").alias("price"),
        F.col("gross_amount_cad").cast("decimal(18,2)").alias("gross_amount_cad"),
        F.coalesce(F.col("commission_cad").cast("decimal(18,2)"), F.lit(0).cast("decimal(18,2)")).alias("commission_cad"),
        F.initcap(F.trim(F.col("channel"))).alias("channel"),
        F.col("file_ts").cast("timestamp").alias("file_ts"),
        _col_or_null(df, "_ingested_at", "timestamp").alias("_ingested_at"),
        _col_or_null(df, "_source_file", "string").alias("_source_file"),
    ).withColumns({
        "is_external_flow": F.col("txn_type").isin(_EXT),
        "flow_direction": F.when(F.col("txn_type").isin(_INF), "Inflow")
                           .when(F.col("txn_type").isin(_EXT), "Outflow").otherwise("Internal"),
        "net_flow_cad": F.when(F.col("txn_type").isin(_EXT), F.col("gross_amount_cad"))
                         .otherwise(F.lit(0)).cast("decimal(18,2)"),
        "fee_revenue_cad": F.when(F.col("txn_type") == "FEE", -F.col("gross_amount_cad"))
                            .otherwise(F.lit(0)).cast("decimal(18,2)"),
    })


def standardize_valuations(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("account_id").cast("string").alias("account_id"),
        F.col("as_of_date").cast("date").alias("as_of_date"),
        F.col("market_value_cad").cast("decimal(18,2)").alias("market_value_cad"),
        F.col("cash_cad").cast("decimal(18,2)").alias("cash_cad"),
        F.col("currency").cast("string").alias("currency"),
        F.col("source_system").cast("string").alias("source_system"),
        F.col("file_ts").cast("timestamp").alias("file_ts"),
        _col_or_null(df, "_ingested_at", "timestamp").alias("_ingested_at"),
    )


def standardize_holdings(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("account_id").cast("string").alias("account_id"),
        F.col("as_of_date").cast("date").alias("as_of_date"),
        F.col("security_id").cast("string").alias("security_id"),
        F.col("quantity").cast("double").alias("quantity"),
        F.col("price_local").cast("double").alias("price_local"),
        F.col("fx_rate_to_cad").cast("double").alias("fx_rate_to_cad"),
        F.col("market_value_cad").cast("decimal(18,2)").alias("market_value_cad"),
    )


def standardize_clients(df: DataFrame) -> DataFrame:
    return df.select(
        "client_id", "household_id",
        F.initcap(F.trim("first_name")).alias("first_name"),
        F.initcap(F.trim("last_name")).alias("last_name"),
        F.lower(F.trim("email")).alias("email"),
        F.col("birth_year").cast("int").alias("birth_year"),
        F.col("risk_profile").cast("string").alias("risk_profile"),
        F.col("email_opt_in").cast("boolean").alias("email_opt_in"),
        F.col("is_primary").cast("boolean").alias("is_primary"),
        F.col("status").cast("string").alias("status"),
        F.upper(F.col("operation")).alias("operation"),
        F.col("event_ts").cast("timestamp").alias("event_ts"),
    )


def standardize_service_cases(df: DataFrame) -> DataFrame:
    return df.select(
        "case_id", "household_id", "advisor_id", "category", "status", "channel",
        F.col("opened_ts").cast("timestamp").alias("opened_ts"),
        F.col("closed_ts").cast("timestamp").alias("closed_ts"),
    ).withColumn(
        "resolution_hours",
        F.when(F.col("closed_ts").isNotNull(),
               (F.unix_timestamp("closed_ts") - F.unix_timestamp("opened_ts")) / 3600.0))


def latest_by_key(df: DataFrame, keys: list[str], order_by: list[str]) -> DataFrame:
    """Batch equivalent of an SCD1 AUTO CDC flow: keep the latest record per key."""
    w = Window.partitionBy(*keys).orderBy(*[F.col(c).desc_nulls_last() for c in order_by])
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def scd2_from_events(df: DataFrame, keys: list[str], sequence_col: str = "event_ts",
                     drop_cols: tuple[str, ...] = ("operation",)) -> DataFrame:
    """Batch equivalent of an SCD2 AUTO CDC flow (adds __START_AT / __END_AT)."""
    w = Window.partitionBy(*keys).orderBy(sequence_col)
    out = (df.withColumn("__START_AT", F.col(sequence_col))
             .withColumn("__END_AT", F.lead(sequence_col).over(w)))
    return out.drop(*[c for c in drop_cols if c in out.columns])


def failed_rules_col(rules: dict[str, str]) -> Column:
    """Array of the names of rules a row violates (NULL counts as a violation)."""
    checks = [F.when(~F.coalesce(F.expr(expr), F.lit(False)), F.lit(name)) for name, expr in rules.items()]
    return F.array_compact(F.array(*checks)) if checks else F.array().cast("array<string>")
