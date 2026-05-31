from __future__ import annotations

import uuid
import json
import random
import time
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import KAFKA_CONFIG

logger = logging.getLogger(__name__)

CHANNELS   = ["card", "bank_transfer", "ussd", "mobile_money", "qr_code"]
CURRENCIES = ["NGN", "USD", "GBP", "KES", "GHS"]
STATUSES   = ["success", "failed", "pending", "reversed"]

FX = {"NGN": 1, "USD": 1580, "GBP": 2010, "KES": 12.2, "GHS": 107}

# --- Fraud pattern generators ---

def _normal_transaction(merchant_ids, customer_ids):
    currency = random.choices(CURRENCIES, weights=[70,15,8,4,3])[0]
    amount_ngn = round(random.uniform(500, 200_000), 2)
    return {
        "id":          str(uuid.uuid4()),
        "reference":   f"TXN_{uuid.uuid4().hex[:12].upper()}",
        "merchant_id": random.choice(merchant_ids),
        "customer_id": random.choice(customer_ids),
        "amount":      round(amount_ngn / FX[currency], 2),
        "amount_ngn":  amount_ngn,
        "currency":    currency,
        "channel":     random.choice(CHANNELS),
        "status":      random.choices(STATUSES, weights=[78,12,6,4])[0],
        "ip_country":  random.choice(["NG","US","GB","GH","KE"]),
        "created_at":  datetime.utcnow().isoformat(),
        "is_fraud_sim": False,
    }


def _velocity_fraud(customer_id, merchant_ids):
    """Same customer, many small transactions in quick succession."""
    txns = []
    base_time = datetime.utcnow()
    for i in range(random.randint(8, 15)):
        txns.append({
            "id":          str(uuid.uuid4()),
            "reference":   f"VEL_{uuid.uuid4().hex[:12].upper()}",
            "merchant_id": random.choice(merchant_ids),
            "customer_id": customer_id,
            "amount":      round(random.uniform(100, 5000), 2),
            "amount_ngn":  round(random.uniform(100, 5000), 2),
            "currency":    "NGN",
            "channel":     "card",
            "status":      "success",
            "ip_country":  random.choice(["NG","US","CN"]),
            "created_at":  (base_time + timedelta(seconds=i * random.randint(30, 90))).isoformat(),
            "is_fraud_sim": True,
        })
    return txns


def _large_round_amount(merchant_ids, customer_ids):
    """Suspicious round-number large transfer."""
    amount_ngn = random.choice([500_000, 1_000_000, 2_000_000, 5_000_000])
    return {
        "id":          str(uuid.uuid4()),
        "reference":   f"LRG_{uuid.uuid4().hex[:12].upper()}",
        "merchant_id": random.choice(merchant_ids),
        "customer_id": random.choice(customer_ids),
        "amount":      amount_ngn,
        "amount_ngn":  amount_ngn,
        "currency":    "NGN",
        "channel":     "bank_transfer",
        "status":      "success",
        "ip_country":  random.choice(["CN","RU","US"]),
        "created_at":  datetime.utcnow().isoformat(),
        "is_fraud_sim": True,
    }


def _bad_data_row():
    """Intentionally malformed row — GE should catch this."""
    return {
        "id":          str(uuid.uuid4()),
        "reference":   None,           # null reference — GE catches
        "merchant_id": "INVALID_ID",
        "customer_id": random.choice([str(uuid.uuid4())]),
        "amount":      -999,           # negative amount — GE catches
        "amount_ngn":  None,
        "currency":    "XYZ",          # invalid currency — GE catches
        "channel":     "telepathy",    # invalid channel — GE catches
        "status":      "success",
        "ip_country":  "NG",
        "created_at":  datetime.utcnow().isoformat(),
        "is_fraud_sim": False,
    }


def generate_batch(
    merchant_ids: list, customer_ids: list,
    n: int = 100, fraud_pct: float = 0.08, bad_data_pct: float = 0.03
) -> list[dict]:
    batch = []

    # Normal transactions
    normal_count = int(n * (1 - fraud_pct - bad_data_pct))
    for _ in range(normal_count):
        batch.append(_normal_transaction(merchant_ids, customer_ids))

    # Fraud patterns
    fraud_count = int(n * fraud_pct)
    for _ in range(fraud_count // 2):
        victim = random.choice(customer_ids)
        batch.extend(_velocity_fraud(victim, merchant_ids))
    for _ in range(fraud_count // 2):
        batch.append(_large_round_amount(merchant_ids, customer_ids))

    # Bad data rows (GE should reject these)
    bad_count = int(n * bad_data_pct)
    for _ in range(bad_count):
        batch.append(_bad_data_row())

    random.shuffle(batch)
    return batch


def produce_to_kafka(batch: list[dict]) -> dict:
    """Publish a batch of transactions to Kafka with delivery confirmation."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks="all",
        retries=3,
        max_block_ms=10_000,
    )

    sent = failed = 0
    for txn in batch:
        try:
            future = producer.send(KAFKA_CONFIG["topic"], value=txn)
            future.get(timeout=10)
            sent += 1
        except KafkaError as e:
            logger.error("Failed to produce message %s: %s", txn.get("id"), e)
            failed += 1

    producer.flush()
    producer.close()

    logger.info("Produced %d messages (%d failed) to topic '%s'",
                sent, failed, KAFKA_CONFIG["topic"])
    return {"sent": sent, "failed": failed}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import uuid as _uuid
    m_ids = [str(_uuid.uuid4()) for _ in range(20)]
    c_ids = [str(_uuid.uuid4()) for _ in range(100)]
    batch = generate_batch(m_ids, c_ids, n=50)
    print(f"Generated {len(batch)} transactions")
    print(f"  Fraud sims: {sum(1 for t in batch if t['is_fraud_sim'])}")
    print(f"  Bad data:   {sum(1 for t in batch if t.get('currency') == 'XYZ')}")
