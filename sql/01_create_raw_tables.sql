CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.transactions (
    id              VARCHAR(36) PRIMARY KEY,
    reference       VARCHAR(100),
    merchant_id     VARCHAR(36),
    customer_id     VARCHAR(36),
    amount          NUMERIC(15,2),
    amount_ngn      NUMERIC(15,2),
    currency        VARCHAR(10),
    channel         VARCHAR(50),
    status          VARCHAR(20),
    ip_country      VARCHAR(5),
    is_fraud_sim    BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.dq_reports (
    id              SERIAL PRIMARY KEY,
    batch_size      INTEGER,
    failure_rate    NUMERIC(6,4),
    passed          BOOLEAN,
    report_json     JSONB,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_txn_customer  ON raw.transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_txn_merchant  ON raw.transactions(merchant_id);
CREATE INDEX IF NOT EXISTS idx_txn_created   ON raw.transactions(created_at);
CREATE INDEX IF NOT EXISTS idx_txn_status    ON raw.transactions(status);
CREATE INDEX IF NOT EXISTS idx_dq_created    ON raw.dq_reports(created_at);
