"""
Great Expectations validation layer.

This is the most important new concept in Project 3.

What GE does:
  - Defines a set of rules (Expectations) about your data
  - Runs those rules against incoming batches
  - Passes valid rows downstream
  - Routes invalid rows to the dead-letter queue
  - Generates a validation report you can audit

Think of it as a quality gate between Kafka and your database.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import GE_RULES, DEAD_LETTER_DIR

logger = logging.getLogger(__name__)


def build_expectations(validator):
    """
    Define all data quality rules for a transaction batch.
    Each rule is an Expectation — GE tests these against every row.
    """

    # --- Completeness ---
    validator.expect_column_values_to_not_be_null("id")
    validator.expect_column_values_to_not_be_null("reference")
    validator.expect_column_values_to_not_be_null("merchant_id")
    validator.expect_column_values_to_not_be_null("customer_id")
    validator.expect_column_values_to_not_be_null("amount")
    validator.expect_column_values_to_not_be_null("currency")
    validator.expect_column_values_to_not_be_null("created_at")

    # --- Value ranges ---
    validator.expect_column_values_to_be_between(
        "amount",
        min_value=GE_RULES["min_amount"],
        max_value=GE_RULES["max_amount"],
    )

    # --- Accepted values ---
    validator.expect_column_values_to_be_in_set(
        "currency", GE_RULES["valid_currencies"]
    )
    validator.expect_column_values_to_be_in_set(
        "status", GE_RULES["valid_statuses"]
    )
    validator.expect_column_values_to_be_in_set(
        "channel", GE_RULES["valid_channels"]
    )

    # --- Uniqueness ---
    validator.expect_column_values_to_be_unique("id")

    # --- Type checks ---
    validator.expect_column_values_to_match_regex(
        "created_at",
        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
    )


def validate_batch(records: list[dict], write_dead_letter: bool = True) -> dict:
    """
    Run GE validation on a batch of transaction records.

    Args:
        write_dead_letter: if True (default), invalid rows are persisted to
            dead_letter/. Callers that are already re-validating rows pulled
            FROM dead_letter/ (i.e. the replay path) must pass False — rows
            that are permanently invalid (e.g. malformed test data) would
            otherwise fail validation again and get written out as a brand
            new dead-letter file every single replay, forever.

    Returns:
        {
          "valid_records":   [...],   # rows that passed all checks
          "invalid_records": [...],   # rows that failed — go to dead-letter
          "validation_report": {...}, # full GE results for auditing
          "passed": bool,
          "failure_rate": float,
        }
    """
    if not records:
        return {"valid_records": [], "invalid_records": [], "passed": True, "failure_rate": 0.0}

    df = pd.DataFrame(records)

    context = gx.get_context()

    datasource = context.sources.add_or_update_pandas(name="fraud_pipeline")
    asset      = datasource.add_dataframe_asset(name="transactions_batch")
    batch_req  = asset.build_batch_request(dataframe=df)

    expectation_suite_name = "transaction_quality_suite"
    try:
        context.get_expectation_suite(expectation_suite_name)
    except Exception:
        context.add_expectation_suite(expectation_suite_name=expectation_suite_name)

    validator = context.get_validator(
        batch_request=batch_req,
        expectation_suite_name=expectation_suite_name,
    )
    build_expectations(validator)

    results = validator.validate()

    # --- Row-level filtering ---
    # GE validates column-level — we use pandas to identify invalid rows
    invalid_mask = _build_invalid_mask(df)
    valid_df     = df[~invalid_mask]
    invalid_df   = df[invalid_mask]

    valid_records   = valid_df.to_dict(orient="records")
    invalid_records = invalid_df.to_dict(orient="records")

    failure_rate = len(invalid_records) / len(records) if records else 0.0

    # Log summary
    logger.info(
        "GE validation: %d total | %d valid | %d invalid (%.1f%% failure rate)",
        len(records), len(valid_records), len(invalid_records), failure_rate * 100
    )

    if failure_rate > GE_RULES["max_dq_failure_rate"]:
        logger.warning(
            "DATA QUALITY ALERT: failure rate %.1f%% exceeds threshold %.1f%%",
            failure_rate * 100, GE_RULES["max_dq_failure_rate"] * 100
        )

    # Write invalid rows to dead-letter
    if invalid_records and write_dead_letter:
        _write_dead_letter(invalid_records)

    return {
        "valid_records":     valid_records,
        "invalid_records":   invalid_records,
        "validation_report": results.to_json_dict(),
        "passed":            results.success,
        "failure_rate":      failure_rate,
        "total":             len(records),
        "valid_count":       len(valid_records),
        "invalid_count":     len(invalid_records),
    }


def _build_invalid_mask(df: pd.DataFrame) -> pd.Series:
    """Return a boolean mask — True means the row is invalid."""
    mask = pd.Series(False, index=df.index)

    # Nulls in critical columns
    for col in ["id", "reference", "merchant_id", "customer_id", "amount", "currency"]:
        if col in df.columns:
            mask |= df[col].isna() | (df[col].astype(str).str.strip() == "")

    # Amount range
    if "amount" in df.columns:
        numeric_amount = pd.to_numeric(df["amount"], errors="coerce")
        mask |= numeric_amount.isna()
        mask |= numeric_amount < GE_RULES["min_amount"]
        mask |= numeric_amount > GE_RULES["max_amount"]

    # Accepted values
    if "currency" in df.columns:
        mask |= ~df["currency"].isin(GE_RULES["valid_currencies"])
    if "status" in df.columns:
        mask |= ~df["status"].isin(GE_RULES["valid_statuses"])
    if "channel" in df.columns:
        mask |= ~df["channel"].isin(GE_RULES["valid_channels"])

    return mask


def _write_dead_letter(invalid_records: list[dict]) -> str:
    """
    Persist invalid rows to the dead-letter directory.
    Each batch gets its own timestamped file for auditing.
    """
    Path(DEAD_LETTER_DIR).mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    filepath  = os.path.join(DEAD_LETTER_DIR, f"dq_failures_{timestamp}.json")

    with open(filepath, "w") as f:
        json.dump({
            "captured_at":   datetime.utcnow().isoformat(),
            "record_count":  len(invalid_records),
            "records":       invalid_records,
        }, f, indent=2, default=str)

    logger.warning("Dead-letter: %d invalid rows written to %s", len(invalid_records), filepath)
    return filepath
