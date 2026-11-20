"""Shared configuration for the Wealth Management Performance Lakehouse.

Everything that more than one layer needs to agree on lives here: schema names,
landing-zone feed definitions, transaction-type semantics and generator scale presets.
"""
from __future__ import annotations

from dataclasses import dataclass, field

# --------------------------------------------------------------------------------------
# Unity Catalog layout
# --------------------------------------------------------------------------------------
SCHEMAS = {
    "landing": "wm_landing",   # UC volume holding raw custodian / CRM / digital files
    "bronze": "wm_bronze",     # raw, append-only, schema-on-read (Auto Loader)
    "silver": "wm_silver",     # validated, de-duplicated, conformed, SCD history
    "gold": "wm_gold",         # standardized reporting datasets (facts + dims)
    "semantic": "wm_semantic", # metric views, metric catalog, secure serving views
    "ops": "wm_ops",           # data-quality results, freshness, reconciliation
}
LANDING_VOLUME = "raw"


def fq(catalog: str, layer: str, table: str) -> str:
    """Fully-qualified three-part name, e.g. fq('wm_reporting', 'gold', 'dim_date')."""
    return f"{catalog}.{SCHEMAS[layer]}.{table}"


def landing_path(catalog: str) -> str:
    return f"/Volumes/{catalog}/{SCHEMAS['landing']}/{LANDING_VOLUME}"


# --------------------------------------------------------------------------------------
# Landing feeds -> bronze tables. One entry per upstream file drop.
# format / schema hints are consumed by the Auto Loader factory in bronze_ingest.py
# --------------------------------------------------------------------------------------
@dataclass(frozen=True)
class Feed:
    name: str            # folder under the landing volume and bronze table suffix
    fmt: str             # csv | json | parquet
    source_system: str   # lineage tag
    description: str
    schema_hints: str = ""


FEEDS: list[Feed] = [
    Feed("branches", "csv", "HR_MASTER", "Branch reference data",
         "branch_id STRING, opened_date DATE"),
    Feed("advisor_events", "json", "HR_MASTER", "Advisor master change events (CDC)",
         "event_ts TIMESTAMP, hire_date DATE"),
    Feed("household_events", "json", "CRM", "Household master change events (CDC)",
         "event_ts TIMESTAMP, onboard_date DATE"),
    Feed("client_events", "json", "CRM", "Client master change events (CDC)",
         "event_ts TIMESTAMP, birth_year INT"),
    Feed("account_events", "json", "CUSTODIAN", "Account open/close/update events (CDC)",
         "event_ts TIMESTAMP, open_date DATE, close_date DATE, fee_bps DOUBLE"),
    Feed("securities", "csv", "MARKET_DATA", "Security master"),
    Feed("prices", "csv", "MARKET_DATA", "Daily closing prices",
         "price_date DATE, close_price DOUBLE"),
    Feed("fx_rates", "csv", "MARKET_DATA", "Daily FX rates to CAD",
         "rate_date DATE, rate_to_cad DOUBLE"),
    Feed("transactions", "csv", "CUSTODIAN", "Trades, cash movements, fees, income",
         "trade_date DATE, settle_date DATE, quantity DOUBLE, price DOUBLE, "
         "gross_amount_cad DOUBLE, commission_cad DOUBLE, file_ts TIMESTAMP"),
    Feed("valuations", "parquet", "CUSTODIAN", "Daily account market values (EOD)"),
    Feed("holdings", "parquet", "CUSTODIAN", "Month-end position-level holdings"),
    Feed("crm_interactions", "csv", "CRM", "Advisor-client touchpoints",
         "interaction_date DATE, duration_min INT"),
    Feed("digital_sessions", "json", "DIGITAL", "Client digital-channel sessions",
         "session_ts TIMESTAMP, duration_sec INT"),
    Feed("service_cases", "csv", "SERVICE", "Service requests and complaints",
         "opened_ts TIMESTAMP, closed_ts TIMESTAMP"),
    Feed("nps_responses", "csv", "SURVEY", "Relationship NPS survey responses",
         "response_date DATE, score INT"),
    Feed("campaigns", "csv", "MARKETING", "Campaign definitions",
         "start_date DATE, end_date DATE, measurement_end_date DATE"),
    Feed("campaign_assignments", "csv", "MARKETING", "Randomized treatment/control assignment",
         "assigned_date DATE"),
]
FEEDS_BY_NAME = {f.name: f for f in FEEDS}

# --------------------------------------------------------------------------------------
# Transaction semantics (single source of truth for flows vs. internal activity)
# --------------------------------------------------------------------------------------
EXTERNAL_INFLOW_TYPES = ("DEPOSIT", "TRANSFER_IN")
EXTERNAL_OUTFLOW_TYPES = ("WITHDRAWAL", "TRANSFER_OUT")
EXTERNAL_FLOW_TYPES = EXTERNAL_INFLOW_TYPES + EXTERNAL_OUTFLOW_TYPES
INTERNAL_TYPES = ("BUY", "SELL", "DIVIDEND", "INTEREST", "FEE")
VALID_TXN_TYPES = EXTERNAL_FLOW_TYPES + INTERNAL_TYPES

SEGMENTS = ("Mass Affluent", "High Net Worth", "Ultra High Net Worth")
PROGRAMS = ("Discretionary Managed", "Fee-Based", "Commission")
ACCOUNT_TYPES = ("RRSP", "TFSA", "RESP", "RRIF", "FHSA", "Non-Registered")
ASSET_CLASSES = ("Canadian Equity", "US Equity", "International Equity",
                 "Fixed Income", "Cash & Equivalents", "Alternatives")

SERVICE_SLA_HOURS = 72
FISCAL_YEAR_START_MONTH = 11  # Canadian bank fiscal year starts 1 November


# --------------------------------------------------------------------------------------
# Synthetic-data scale presets
# --------------------------------------------------------------------------------------
@dataclass
class GenConfig:
    seed: int = 42
    start_date: str = "2024-10-01"
    end_date: str = "2026-09-22"
    n_branches: int = 12
    n_advisors: int = 60
    n_households: int = 3000
    annual_hh_churn: float = 0.04
    new_hh_share: float = 0.20          # share of households onboarded during the window
    dq_error_rate: float = 0.003        # injected bad / duplicate records
    campaigns: bool = True
    extra: dict = field(default_factory=dict)


SCALE_PRESETS = {
    "tiny":   GenConfig(n_branches=4, n_advisors=6, n_households=160,
                        start_date="2025-10-01", end_date="2026-06-30"),
    "small":  GenConfig(n_branches=8, n_advisors=20, n_households=800),
    "medium": GenConfig(),
    "large":  GenConfig(n_branches=25, n_advisors=250, n_households=25000),
}
