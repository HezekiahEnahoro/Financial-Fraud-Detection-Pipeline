from __future__ import annotations

import json
import logging
import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer
from datetime import datetime

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import KAFKA_CONFIG, DB_CONFIG
from validation.ge_validator import validate_batch
from consumer.retry_handler import with_retry, log_pipeline_failure, PipelineHealthCheck

logger = logging.getLogger(__name__)


@with_retry
def write_valid_transactions(records: list[dict]) -> int:
    """Insert validated transactions into raw schema. Decorated with retry."""
    if not records:
        return 0

    sql = """
        INSERT INTO raw.transactions (
            id, reference, merchant_id, customer_id,
            amount, amount_ngn, currency, channel,
            status, ip_country, is_fraud_sim, created_at
        )
        VALUES %s
        ON CONFLICT (id) DO NOTHING
    """
    rows = [(
        r["id"], r.get("reference"), r.get("merchant_id"), r.get("customer_id"),
        r.get("amount"), r.get("amount_ngn"), r.get("currency"), r.get("channel"),
        r.get("status"), r.get("ip_country"), r.get("is_fraud_sim", False),
        r.get("created_at", datetime.utcnow()),
    ) for r in records]

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
        conn.commit()

    logger.info("Inserted %d validated transactions into raw.transactions", len(rows))
    return len(rows)


@with_retry
def write_dq_report(report: dict, batch_size: int, failure_rate: float) -> None:
    """Persist a Great Expectations validation report to the database."""
    sql = """
        INSERT INTO raw.dq_reports
            (batch_size, failure_rate, passed, report_json, created_at)
        VALUES (%s, %s, %s, %s, %s)
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                batch_size,
                failure_rate,
                failure_rate < 0.05,
                json.dumps(report),
                datetime.utcnow(),
            ))
        conn.commit()


def consume_and_process(batch_size: int = 100, timeout_ms: int = 10_000) -> dict:
    """
    Main consumer loop:
      1. Pull a batch from Kafka
      2. Validate with Great Expectations
      3. Write valid rows to Postgres (with retry)
      4. Dead-letter invalid rows
      5. Record health metrics
    """
    health = PipelineHealthCheck(alert_threshold=0.05)

    consumer = KafkaConsumer(
        KAFKA_CONFIG["topic"],
        bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
        group_id=KAFKA_CONFIG["consumer_group"],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=timeout_ms,
    )

    batch = []
    total_written = 0

    try:
        for message in consumer:
            batch.append(message.value)

            if len(batch) >= batch_size:
                result = _process_batch(batch, health)
                total_written += result["written"]
                consumer.commit()
                batch = []

        # Process any remaining messages
        if batch:
            result = _process_batch(batch, health)
            total_written += result["written"]
            consumer.commit()

    except Exception as e:
        log_pipeline_failure("kafka_consumer", e, {"batch_size": len(batch)})
        raise
    finally:
        consumer.close()

    summary = health.summary()
    logger.info("Consumer run complete. Written: %d | Health: %s", total_written, summary)
    return {**summary, "total_written": total_written}


def _process_batch(batch: list[dict], health: PipelineHealthCheck) -> dict:
    """Validate one batch and write valid rows."""
    try:
        validation = validate_batch(batch)

        try:
            write_valid_transactions(validation["valid_records"])
            write_dq_report(
                validation["validation_report"],
                len(batch),
                validation["failure_rate"],
            )
        except Exception as e:
            log_pipeline_failure("postgres_write", e, {"batch_size": len(batch)})
            raise

        health.record(
            "validation",
            processed=validation["total"],
            failed=validation["invalid_count"],
        )

        return {"written": validation["valid_count"], "failed": validation["invalid_count"]}

    except Exception as e:
        log_pipeline_failure("batch_processing", e, {"batch_size": len(batch)})
        health.record("batch_processing", processed=len(batch), failed=len(batch))
        return {"written": 0, "failed": len(batch)}
