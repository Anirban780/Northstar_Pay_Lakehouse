from __future__ import annotations
import os


# ============================================================
# Environment selection
# Set via bundle target / job env var, e.g. DBX_ENV=dev|staging|prod
# ============================================================
ENV = os.getenv("DBX_ENV", "dev").strip().lower()

if ENV not in {"dev", "staging", "prod"}:
    raise ValueError(f"Unsupported DBX_ENV='{ENV}'. Expected one of: dev, staging, prod")


# ============================================================
# Per-environment catalog / schema configuration
# Update names to match your Unity Catalog naming convention
# ============================================================
ENV_CONFIG = {
    "dev": {
        "catalog": "northstar_dev",
        "schema_bronze": "bronze",
        "schema_silver": "silver",
        "schema_gold": "gold",
        "volume_root": "/Volumes/northstar_dev/landing/raw",
    },
    "staging": {
        "catalog": "northstar_staging",
        "schema_bronze": "bronze",
        "schema_silver": "silver",
        "schema_gold": "gold",
        "volume_root": "/Volumes/northstar_staging/landing/raw",
    },
    "prod": {
        "catalog": "northstar_prod",
        "schema_bronze": "bronze",
        "schema_silver": "silver",
        "schema_gold": "gold",
        "volume_root": "/Volumes/northstar_prod/landing/raw",
    },
}

CURRENT_ENV = ENV_CONFIG[ENV]

CATALOG = CURRENT_ENV["catalog"]
SCHEMA_BRONZE = CURRENT_ENV["schema_bronze"]
SCHEMA_SILVER = CURRENT_ENV["schema_silver"]
SCHEMA_GOLD = CURRENT_ENV["schema_gold"]


# ============================================================
# Volume / landing paths
# Root path + 4 dataset-specific landing locations
# ============================================================
LANDING_ROOT = CURRENT_ENV["volume_root"]

SOURCE_PATHS = {
    "customers": f"{LANDING_ROOT}/customers",
    "merchants": f"{LANDING_ROOT}/merchants",
    "transactions": f"{LANDING_ROOT}/transactions",
    "device_session_logs": f"{LANDING_ROOT}/device_session_logs",
}


# ============================================================
# Table names
# Rename once here, reused everywhere else
# ============================================================
BRONZE_TABLES = {
    "customers": "bronze_customers_raw",
    "merchants": "bronze_merchants_raw",
    "transactions": "bronze_transactions_raw",
    "device_session_logs": "bronze_device_session_logs_raw",
}

SILVER_TABLES = {
    "customers": "silver_customers",
    "merchants": "silver_merchants",
    "transactions": "silver_transactions",
    "device_session_logs": "silver_device_session_logs",
    "transactions_enriched": "silver_transactions_enriched",
    "customers_quarantine": "silver_customers_quarantine",
    "transactions_quarantine": "silver_transactions_quarantine",
    "device_session_logs_quarantine": "silver_device_session_logs_quarantine",
}

GOLD_TABLES = {
    "dim_customer": "dim_customer",
    "dim_merchant": "dim_merchant",
    "dim_date": "dim_date",
    "fact_transactions": "fact_transactions",
    "daily_fraud_summary": "daily_fraud_summary",
    "customer_risk_scorecard": "customer_risk_scorecard",
    "merchant_performance_daily": "merchant_performance_daily",
    "channel_volume_trends": "channel_volume_trends",
}


# ============================================================
# DQ thresholds
# These are the live operational thresholds for validation jobs
# Replace with the exact values from notes.py if needed
# ============================================================
DQ_THRESHOLDS = {
    # Inferred from investigation: 107,232 pre-signup txns / 525,292 total txns
    "pre_signup_rate_pct": 20.41,

    # Assumption: 1,019 impossible velocity pairs / 261,414 device session logs
    # Replace if your notes.py uses a different denominator or exact scorecard rate
    "geo_velocity_anomaly_rate_pct": 0.39,

    # Inferred from investigation: 576 outlier txns / 525,292 total txns
    "amount_outlier_rate_pct": 0.11,

    # Based on your PASS referential checks
    "orphan_rate_pct": 0.00,
}


# ============================================================
# Join keys / FK definitions
# Reused by Silver joins and DQ RI checks
# ============================================================
JOIN_KEYS = {
    "customer_id": "customer_id",
    "merchant_id": "merchant_id",
    "mcc_code": "mcc_code",
    "transaction_id": "transaction_id",
    "device_id": "device_id",
    "session_id": "session_id",
}

PRIMARY_KEYS = {
    "customers": ["customer_id"],
    "merchants": ["merchant_id"],
    "transactions": ["transaction_id"],
    "device_session_logs": ["session_id"],
}

FOREIGN_KEYS = {
    "transactions.customer_id -> customers.customer_id": {
        "child_table": "transactions",
        "child_key": "customer_id",
        "parent_table": "customers",
        "parent_key": "customer_id",
    },
    "transactions.merchant_id -> merchants.merchant_id": {
        "child_table": "transactions",
        "child_key": "merchant_id",
        "parent_table": "merchants",
        "parent_key": "merchant_id",
    },
    "transactions.mcc_code -> merchants.mcc_code": {
        "child_table": "transactions",
        "child_key": "mcc_code",
        "parent_table": "merchants",
        "parent_key": "mcc_code",
    },
    "device_session_logs.customer_id -> customers.customer_id": {
        "child_table": "device_session_logs",
        "child_key": "customer_id",
        "parent_table": "customers",
        "parent_key": "customer_id",
    },
}

SILVER_JOIN_RULES = {
    "transactions_to_customers": ["customer_id"],
    "transactions_to_merchants": ["merchant_id"],
    "transactions_to_merchants_mcc": ["mcc_code"],
    "device_logs_to_customers": ["customer_id"],
}


# ============================================================
# PII column inventory
# Replace this with your exact 13-column inventory from notes.py if names differ
# ============================================================
PII_COLUMNS = [
    "full_name",
    "first_name",
    "last_name",
    "dob",
    "email",
    "phone",
    "address_line_1",
    "address_line_2",
    "city",
    "state",
    "postal_code",
    "country",
    "ip_address",
]


# ============================================================
# Optional convenience helpers
# Safe to use across scripts for fully-qualified names
# ============================================================
def fq_table(schema: str, table: str) -> str:
    return f"{CATALOG}.{schema}.{table}"


def bronze_fq(table_key: str) -> str:
    return fq_table(SCHEMA_BRONZE, BRONZE_TABLES[table_key])


def silver_fq(table_key: str) -> str:
    return fq_table(SCHEMA_SILVER, SILVER_TABLES[table_key])


def gold_fq(table_key: str) -> str:
    return fq_table(SCHEMA_GOLD, GOLD_TABLES[table_key])