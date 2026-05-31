from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import DEAD_LETTER_DIR
from validation.ge_validator import validate_batch
from consumer.retry_handler import log_pipeline_failure
from consumer.transaction_consumer import write_valid_transactions, write_dq_report

logger = logging.getLogger(__name__)


def replay_dead_letter_files() -> dict:
    """
    Re-validate records from dq_failures dead letter files.

    Rows that pass validation on the second attempt (e.g. after a rule
    threshold change) are written to raw.transactions.  All processed
    files — whether recovered or still invalid — are moved to
    dead_letter/replayed/ so they are never double-processed.
    """
    dead_letter_path = Path(DEAD_LETTER_DIR)
    replayed_path = dead_letter_path / "replayed"
    replayed_path.mkdir(parents=True, exist_ok=True)

    failure_files = sorted(dead_letter_path.glob("dq_failures_*.json"))

    if not failure_files:
        logger.info("No dead letter files to replay")
        return {"files_processed": 0, "records_recovered": 0, "records_still_invalid": 0}

    total_recovered = 0
    total_still_invalid = 0
    files_processed = 0

    for filepath in failure_files:
        try:
            with open(filepath) as f:
                payload = json.load(f)

            records = payload.get("records", [])
            if not records:
                _archive(filepath, replayed_path)
                files_processed += 1
                continue

            validation = validate_batch(records)

            if validation["valid_records"]:
                write_valid_transactions(validation["valid_records"])
                write_dq_report(
                    validation["validation_report"],
                    len(records),
                    validation["failure_rate"],
                )
                logger.info(
                    "Recovered %d/%d records from %s",
                    validation["valid_count"], len(records), filepath.name,
                )

            if validation["invalid_count"]:
                logger.info(
                    "%d records in %s still fail validation — left in replayed/ for audit",
                    validation["invalid_count"], filepath.name,
                )

            total_recovered += validation["valid_count"]
            total_still_invalid += validation["invalid_count"]

            _archive(filepath, replayed_path)
            files_processed += 1

        except Exception as e:
            log_pipeline_failure("dead_letter_replay", e, {"file": str(filepath)})
            logger.error("Failed to replay %s: %s", filepath.name, e)

    logger.info(
        "Replay complete — files: %d | recovered: %d | still invalid: %d",
        files_processed, total_recovered, total_still_invalid,
    )
    return {
        "files_processed": files_processed,
        "records_recovered": total_recovered,
        "records_still_invalid": total_still_invalid,
    }


def _archive(filepath: Path, replayed_path: Path) -> None:
    filepath.rename(replayed_path / filepath.name)
