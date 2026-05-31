"""
Retry handler with exponential backoff.

Why this matters:
  Production pipelines fail. Networks blip. Postgres restarts.
  A naive pipeline crashes and loses data.
  A production pipeline retries with backoff and logs failures it can't recover.

Pattern used here:
  attempt 1 → wait 2s → attempt 2 → wait 4s → attempt 3 → dead-letter
"""

import time
import logging
import json
import os
from datetime import datetime
from functools import wraps
from pathlib import Path

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import RETRY_CONFIG, DEAD_LETTER_DIR

logger = logging.getLogger(__name__)


def with_retry(fn):
    """
    Decorator: retry fn up to max_attempts with exponential backoff.
    On final failure, logs the error and raises.

    Usage:
        @with_retry
        def load_batch(records):
            ...
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        max_attempts = RETRY_CONFIG["max_attempts"]
        base_delay   = RETRY_CONFIG["base_delay_secs"]
        max_delay    = RETRY_CONFIG["max_delay_secs"]

        for attempt in range(1, max_attempts + 1):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                if attempt == max_attempts:
                    logger.error(
                        "FINAL FAILURE after %d attempts — %s: %s",
                        max_attempts, fn.__name__, e
                    )
                    raise

                delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                logger.warning(
                    "Attempt %d/%d failed for %s. Retrying in %.1fs. Error: %s",
                    attempt, max_attempts, fn.__name__, delay, e
                )
                time.sleep(delay)
    return wrapper


def log_pipeline_failure(stage: str, error: Exception, context: dict = None) -> str:
    """
    Persist a pipeline failure to the dead-letter directory.
    Every unrecoverable failure gets a structured log entry.
    Returns the path to the written log file.
    """
    Path(DEAD_LETTER_DIR).mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    filepath  = os.path.join(DEAD_LETTER_DIR, f"pipeline_failure_{timestamp}.json")

    payload = {
        "captured_at": datetime.utcnow().isoformat(),
        "stage":       stage,
        "error_type":  type(error).__name__,
        "error_msg":   str(error),
        "context":     context or {},
    }

    with open(filepath, "w") as f:
        json.dump(payload, f, indent=2, default=str)

    logger.error("Pipeline failure logged: %s → %s", stage, filepath)
    return filepath


class PipelineHealthCheck:
    """
    Tracks batch-level health metrics across a pipeline run.
    Raises an alert if cumulative failure rate crosses threshold.
    """

    def __init__(self, alert_threshold: float = 0.05):
        self.alert_threshold = alert_threshold
        self.total_processed = 0
        self.total_failed    = 0
        self.stage_counts    = {}

    def record(self, stage: str, processed: int, failed: int):
        self.total_processed += processed
        self.total_failed    += failed
        self.stage_counts[stage] = {
            "processed": processed,
            "failed":    failed,
            "failure_rate": failed / processed if processed else 0,
        }
        self._check_alert(stage, processed, failed)

    def _check_alert(self, stage: str, processed: int, failed: int):
        if processed == 0:
            return
        rate = failed / processed
        if rate > self.alert_threshold:
            logger.warning(
                "HEALTH ALERT [%s]: %.1f%% failure rate (%d/%d) exceeds threshold %.1f%%",
                stage, rate * 100, failed, processed, self.alert_threshold * 100
            )

    def summary(self) -> dict:
        overall_rate = self.total_failed / self.total_processed if self.total_processed else 0
        return {
            "total_processed":    self.total_processed,
            "total_failed":       self.total_failed,
            "overall_failure_rate": overall_rate,
            "stages":             self.stage_counts,
            "healthy":            overall_rate <= self.alert_threshold,
        }
