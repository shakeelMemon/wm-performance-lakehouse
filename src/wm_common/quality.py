"""Declarative data-quality rules shared by the Lakeflow pipeline and the ops checks.

Each rule is a SQL boolean expression that must hold for a valid record.
* ``DROP`` rules -> ``@dlt.expect_all_or_drop``; failing rows go to a quarantine table.
* ``WARN`` rules -> ``@dlt.expect_all``; rows kept, pass-rate tracked in the event log.
"""
from __future__ import annotations

from .config import VALID_TXN_TYPES

_types = ", ".join(f"'{t}'" for t in VALID_TXN_TYPES)

RULES: dict[str, dict[str, dict[str, str]]] = {
    "transactions": {
        "DROP": {
            "valid_txn_id": "txn_id IS NOT NULL",
            "valid_account": "account_id IS NOT NULL",
            "valid_txn_type": f"txn_type IN ({_types})",
            "valid_trade_date": "trade_date IS NOT NULL AND trade_date <= current_date()",
            "valid_amount": "gross_amount_cad IS NOT NULL",
        },
        "WARN": {
            "settles_after_trade": "settle_date >= trade_date",
            "trade_has_security": "txn_type NOT IN ('BUY','SELL') OR security_id IS NOT NULL",
            "trade_quantity_positive": "txn_type NOT IN ('BUY','SELL') OR quantity > 0",
        },
    },
    "valuations": {
        "DROP": {
            "valid_account": "account_id IS NOT NULL AND account_id RLIKE '^AC[0-9]{8}$'",
            "valid_date": "as_of_date IS NOT NULL",
            "non_negative_mv": "market_value_cad >= 0",
        },
        "WARN": {
            "cash_within_mv": "cash_cad <= market_value_cad + 0.01",
        },
    },
    "holdings": {
        "DROP": {"valid_keys": "account_id IS NOT NULL AND security_id IS NOT NULL AND as_of_date IS NOT NULL"},
        "WARN": {"non_negative_position": "market_value_cad >= 0"},
    },
    "clients": {
        "DROP": {"valid_client_id": "client_id IS NOT NULL", "valid_household": "household_id IS NOT NULL"},
        "WARN": {"valid_email": "email RLIKE '^[^@\\\\s]+@[^@\\\\s]+\\\\.[a-z]{2,}$'",
                 "plausible_birth_year": "birth_year BETWEEN 1900 AND year(current_date()) - 18"},
    },
    "prices": {
        "DROP": {"valid_price": "close_price > 0 AND price_date IS NOT NULL"},
        "WARN": {},
    },
    "service_cases": {
        "DROP": {"valid_case": "case_id IS NOT NULL AND opened_ts IS NOT NULL"},
        "WARN": {"closed_after_opened": "closed_ts IS NULL OR closed_ts >= opened_ts"},
    },
    "nps_responses": {
        "DROP": {"valid_score": "score BETWEEN 0 AND 10"},
        "WARN": {},
    },
}


def drop_rules(entity: str) -> dict[str, str]:
    return RULES.get(entity, {}).get("DROP", {})


def warn_rules(entity: str) -> dict[str, str]:
    return RULES.get(entity, {}).get("WARN", {})


def quarantine_predicate(entity: str) -> str:
    """Rows that violate at least one DROP rule (NULL-safe)."""
    rules = drop_rules(entity).values()
    if not rules:
        return "false"
    return " OR ".join(f"NOT coalesce(({r}), false)" for r in rules)
