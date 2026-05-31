import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5433)),
    "database": os.getenv("POSTGRES_DB", "fraud_db"),
    "user":     os.getenv("POSTGRES_USER", "fraud"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "topic":             os.getenv("KAFKA_TOPIC", "transactions"),
    "consumer_group":    "fraud-pipeline-consumer",
}

# Great Expectations thresholds
GE_RULES = {
    "max_null_pct":         0.02,    # fail if >2% nulls in critical columns
    "min_amount":           0.01,
    "max_amount":           50_000_000,
    "valid_currencies":     ["NGN", "USD", "GBP", "EUR", "KES", "GHS", "ZAR"],
    "valid_statuses":       ["success", "failed", "pending", "reversed"],
    "valid_channels":       ["card", "bank_transfer", "ussd", "mobile_money", "qr_code"],
    "max_dq_failure_rate":  0.05,    # alert if >5% of a batch fails validation
}

# Fraud detection thresholds
FRAUD_RULES = {
    "velocity_window_minutes":     60,
    "velocity_max_transactions":   10,   # flag if >10 txns in 60 min from same customer
    "velocity_max_amount_ngn":     5_000_000,
    "large_amount_threshold_ngn":  2_000_000,
    "night_hours":                 (0, 5),  # midnight to 5am
    "round_amount_multiples":      [100_000, 500_000, 1_000_000],
}

# Retry settings
RETRY_CONFIG = {
    "max_attempts":    3,
    "base_delay_secs": 2,
    "max_delay_secs":  30,
}

DEAD_LETTER_DIR = os.getenv("DEAD_LETTER_DIR", "./dead_letter")
