-- Clean validated events land here
CREATE TABLE IF NOT EXISTS clean_events (
    event_id        VARCHAR(50) PRIMARY KEY,
    user_id         VARCHAR(50) NOT NULL,
    product_id      VARCHAR(50) NOT NULL,
    event_type      VARCHAR(20) NOT NULL,
    amount          NUMERIC(10, 2) NOT NULL,
    currency        VARCHAR(5) NOT NULL,
    timestamp       TIMESTAMP NOT NULL,
    ingested_at     TIMESTAMP DEFAULT NOW()
);

-- Bad / rejected events land here with a reason
CREATE TABLE IF NOT EXISTS bad_events (
    event_id        VARCHAR(50),
    raw_data        TEXT,
    rejection_reason TEXT,
    ingested_at     TIMESTAMP DEFAULT NOW()
);

-- Quality metrics summary per batch
CREATE TABLE IF NOT EXISTS quality_metrics (
    id              SERIAL PRIMARY KEY,
    batch_time      TIMESTAMP DEFAULT NOW(),
    total_received  INT,
    total_clean     INT,
    total_bad       INT,
    bad_rate_pct    NUMERIC(5, 2)
);

-- Rule-level validation failure observability by reporting interval
CREATE TABLE IF NOT EXISTS validation_failure_metrics (
    id              SERIAL PRIMARY KEY,
    batch_time      TIMESTAMP DEFAULT NOW(),
    total_received  INT,
    total_clean     INT,
    total_bad       INT,
    failure_reason  TEXT,
    failure_count   INT
);

-- Alerts for bad-rate spikes crossing configured threshold
CREATE TABLE IF NOT EXISTS quality_alerts (
    id              SERIAL PRIMARY KEY,
    alert_time      TIMESTAMP DEFAULT NOW(),
    total_received  INT,
    total_clean     INT,
    total_bad       INT,
    bad_rate_pct    NUMERIC(5, 2),
    threshold_pct   NUMERIC(5, 2),
    message         TEXT
);
